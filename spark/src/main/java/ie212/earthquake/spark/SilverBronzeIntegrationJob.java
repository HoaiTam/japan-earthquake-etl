package ie212.earthquake.spark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.bronze.BronzeReuseVerifier;
import ie212.earthquake.spark.gold.*;
import ie212.earthquake.spark.silver.*;
import ie212.earthquake.spark.usgs.MinioBronzeObjectStore;
import java.io.*;
import java.net.URI;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.zip.ZipInputStream;
import org.apache.spark.sql.*;

/** Bounded SLV-09 acceptance run: exact verified Bronze -> immutable Silver -> Gold transform only. */
public final class SilverBronzeIntegrationJob {
    private static final ObjectMapper JSON = new ObjectMapper();
    private SilverBronzeIntegrationJob() { }

    public static void main(String[] args) throws Exception {
        if (args.length != 1 || Files.size(Path.of(args[0])) > 65536) { throw new IOException("EXACT_REQUEST_REQUIRED"); }
        Path requestPath = Path.of(args[0]).toAbsolutePath();
        JsonNode request = JSON.readTree(Files.readAllBytes(requestPath));
        validate(request);
        Instant processed = Instant.parse(request.path("processed_at_utc").asText());
        String runId = request.path("run_id").asText();
        var observations = new ArrayList<SilverObservation>();
        var rejects = new ArrayList<SilverRejectRecord>();
        var coverage = JSON.createArrayNode();
        var verifiedIngestRuns = new LinkedHashMap<String, String>();
        try (var bronze = MinioBronzeObjectStore.fromEnvironment()) {
            for (var pin : request.path("bronze_inputs")) {
                String uri = pin.path("manifest_uri").asText();
                if (!bronze.uriForKey(key(uri)).equals(uri)) { throw new IOException("INPUT_BUCKET_MISMATCH"); }
                byte[] bytes = bronze.read(key(uri));
                if (bytes.length > 65536 || !SilverBundlePublisher.sha(bytes).equals(pin.path("manifest_sha256").asText())) {
                    throw new IOException("INPUT_PIN_MISMATCH");
                }
                var manifest = JSON.readTree(bytes);
                if (manifest.path("content_length_bytes").asLong(Long.MAX_VALUE) > 32L * 1024 * 1024
                        || ("USGS".equals(pin.path("source_system").asText())
                        && manifest.path("record_count_estimate").asLong(Long.MAX_VALUE) > 10000)) {
                    throw new IOException("BOUNDED_INPUT_REQUIRED");
                }
            }
            // Fresh full raw SHA/count and full ZIP CRC before sampling; no source HTTP calls.
            JsonNode verified = BronzeReuseVerifier.verify(request, bronze);
            for (var pin : verified.path("observability").path("bronze_inputs")) {
                byte[] bytes = bronze.read(key(pin.path("manifest_uri").asText()));
                if (!SilverBundlePublisher.sha(bytes).equals(pin.path("manifest_sha256").asText())) {
                    throw new IOException("INPUT_CHANGED");
                }
                var manifest = JSON.readTree(bytes);
                String rawUri = manifest.path("raw_object_uri").asText();
                String manifestId = manifest.path("manifest_id").asText();
                String ingestRun = manifest.path("run_id").asText();
                if (manifestId.isBlank() || ingestRun.isBlank()
                        || verifiedIngestRuns.putIfAbsent(manifestId, ingestRun) != null) {
                    throw new IOException("UNIQUE_BRONZE_LINEAGE_REQUIRED");
                }
                byte[] raw = bronze.read(key(rawUri));
                if (raw.length != manifest.path("content_length_bytes").asLong(-1)
                        || !SilverBundlePublisher.sha(raw).equals(pin.path("sha256").asText())) {
                    throw new IOException("INPUT_CHANGED");
                }
                int selected, parsed, rejected;
                if ("USGS".equals(pin.path("source_system").asText())) {
                    var result = new UsgsGeoJsonParser().parse(raw, new UsgsParseContext(
                            manifest.path("manifest_id").asText(), rawUri, pin.path("sha256").asText(),
                            manifest.path("run_id").asText(), processed));
                    observations.addAll(result.observations()); rejects.addAll(result.rejects());
                    selected = parsed = result.parsedCount(); rejected = result.rejects().size();
                } else {
                    String member = manifest.path("provenance").path("member_name").asText();
                    try (var zip = new ZipInputStream(new ByteArrayInputStream(raw))) {
                        var entry = zip.getNextEntry();
                        if (entry == null || !member.equals(entry.getName())) { throw new IOException("MEMBER_CHANGED"); }
                        byte[] sample = ResourcePilotJob.firstLines(zip, request.path("jma_records_per_archive").asInt());
                        selected = (int) new String(sample, java.nio.charset.StandardCharsets.US_ASCII)
                                .chars().filter(value -> value == '\n').count();
                        var result = new JmaFixedWidthParser().parse(sample, new JmaParseContext(
                                manifest.path("manifest_id").asText(), rawUri, pin.path("sha256").asText(),
                                manifest.path("run_id").asText(), pin.path("catalog_release").asText(), null,
                                manifest.path("provenance").path("source_url").textValue(), member, processed));
                        observations.addAll(result.observations()); rejects.addAll(result.rejects());
                        parsed = result.parsedCount(); rejected = result.rejects().size();
                    }
                }
                var row = coverage.addObject(); row.put("manifest_uri", pin.path("manifest_uri").asText());
                row.put("source_system", pin.path("source_system").asText());
                row.put("bronze_manifest_id", manifestId); row.put("ingest_run_id", ingestRun);
                row.put("full_bronze_records", pin.path("record_count_estimate").asLong());
                row.put("selected_records", selected); row.put("parsed", parsed); row.put("parser_rejects", rejected);
                row.put("ignored", selected - parsed); row.put("selection", "USGS".equals(pin.path("source_system").asText())
                        ? "all-records" : "first-n-physical-lines-v1");
            }
        }
        var config = SilverLinkConfig.defaultConfig();
        for (var observation : observations) {
            if (observation.eventTimeUtc().isBefore(Instant.parse(request.path("window_start_utc").asText()))
                    || !observation.eventTimeUtc().isBefore(Instant.parse(request.path("window_end_utc").asText()))) {
                throw new IOException("OBSERVATION_OUTSIDE_RESOLVED_SCOPE");
            }
        }
        var result = new SilverMultiSourceIntegrationRunner().run(runId, observations, rejects, config, null, false,
                processed, verifiedIngestRuns);
        if (!result.isPublishable()) {
            var failed = JSON.createObjectNode(); failed.put("task", "SLV-09"); failed.put("run_id", runId);
            failed.put("status", "QUALITY_OR_RECONCILIATION_BLOCKED"); failed.set("sources", coverage);
            failed.set("reconciliation", counts(result));
            failed.set("quality_reason_counts", JSON.valueToTree(result.qualityResult().reasonCounts()));
            System.out.println(JSON.writeValueAsString(failed));
            throw new IOException("SILVER_QUALITY_OR_RECONCILIATION_BLOCKED");
        }
        var context = JSON.createObjectNode();
        context.put("quality_passed", true); context.put("reconciliation_balanced", true);
        context.set("request", request.deepCopy()); context.set("coverage", coverage);
        context.set("link_config", JSON.valueToTree(config));
        var counts = counts(result); context.set("reconciliation", counts);
        SparkSession spark = SparkSession.builder().appName("slv-09-bronze-integration")
                .config("spark.sql.session.timeZone", "UTC").getOrCreate();
        try (var silver = MinioSilverObjectStore.fromEnvironment()) {
            var publisher = new SilverBundlePublisher(silver);
            var written = publisher.publish(new SilverWriteRequest(runId, result.allObservations(),
                    result.qualityResult().rejectedRecords(), result.sourceLinks(), result.canonicalMemberships(), processed, false), context);
            JsonNode bundle = publisher.verify(runId);
            // Shared scoped staging volume: worker reads the SAME verified bytes, not driver-only /tmp.
            Path readbackRoot = Files.createTempDirectory(requestPath.getParent(), "gold-readback-");
            // Client uses AIRFLOW_UID, worker uses Spark UID. Temp directories default
            // to 0700; expose only public earthquake readback bytes to the worker.
            Files.setPosixFilePermissions(readbackRoot, java.nio.file.attribute.PosixFilePermissions.fromString("rwxr-xr-x"));
            var observationData = readDataset(spark, silver, bundle, "source_observation", readbackRoot);
            var membershipData = readDataset(spark, silver, bundle, "canonical_membership", readbackRoot);
            var linkData = readDataset(spark, silver, bundle, "source_link", readbackRoot);
            var regions = spark.createDataFrame(List.<Row>of(), GoldEventTransformer.REGION_SCHEMA);
            var manifests = new ArrayList<String>(); request.path("bronze_inputs").forEach(pin ->
                    manifests.add(pin.path("manifest_uri").asText()));
            var gold = new GoldEventTransformer().transform(observationData, membershipData, linkData, regions,
                    new GoldRunContext(runId, Instant.parse(request.path("window_start_utc").asText()),
                            Instant.parse(request.path("window_end_utc").asText()),
                            LocalDate.parse(request.path("processing_date").asText()), request.path("is_backfill").asBoolean(),
                            request.path("config_version").asText(), manifests), processed);
            if (gold.currentObservationCount() != result.reconciliationReport().totalCurrentCount()
                    || gold.canonicalEventCount() != result.reconciliationReport().canonicalEventsCount()
                    || gold.bridgeRowCount() != result.canonicalMemberships().size()
                    || observationData.count() != written.totalObservations()
                    || membershipData.count() != written.totalMemberships() || linkData.count() != written.totalLinks()) {
                throw new IOException("GOLD_HANDOFF_COUNT_MISMATCH");
            }
            // GoldEventTransformer already executes distributed output counts,
            // uniqueness/quality checks and event/bridge balance (not lazy-only construction).
            // Do not recompute the expensive event join graph a second time here.
            var report = JSON.createObjectNode(); report.put("task", "SLV-09"); report.put("run_id", runId);
            report.put("scope_sha256", request.path("scope_sha256").asText()); report.set("sources", coverage);
            report.set("run_context", request.deepCopy());
            report.set("reconciliation", counts); report.set("datasets", bundle.path("datasets"));
            report.put("bundle_manifest_uri", silver.uriForKey(written.bundleManifestKey()));
            report.put("bundle_manifest_sha256", SilverBundlePublisher.sha(silver.read(written.bundleManifestKey())));
            report.put("identity_sha256", bundle.path("identity_sha256").asText());
            report.put("idempotent_reuse", written.idempotentReuse()); report.put("silver_status", "SilverReady");
            report.put("gold_handoff_verified", true); report.put("gold_published", false);
            report.put("gold_canonical_rows", gold.canonicalEventCount()); report.put("gold_bridge_rows", gold.bridgeRowCount());
            report.put("java_version", System.getProperty("java.version")); report.put("spark_version", spark.version());
            report.put("application_id", spark.sparkContext().applicationId()); report.put("master", spark.sparkContext().master());
            System.out.println(JSON.writeValueAsString(report));
        } finally { spark.stop(); }
    }

    static void validate(JsonNode request) throws IOException {
        if (!"slv09-live-v1".equals(request.path("integration_version").asText())
                || !"orc-04-v1".equals(request.path("observability_version").asText())
                || !request.path("jma_records_per_archive").isIntegralNumber()
                || request.path("jma_records_per_archive").asInt() < 1 || request.path("jma_records_per_archive").asInt() > 512
                || request.path("config_version").asText().isBlank() || request.path("bronze_inputs").size() != 3) {
            throw new IOException("BOUNDED_EXACT_SCOPE_REQUIRED");
        }
        SilverBundlePublisher.root(request.path("run_id").asText());
        Instant.parse(request.path("processed_at_utc").asText());
        if (!request.path("is_backfill").isBoolean() || !request.path("is_backfill").asBoolean()
                || !Instant.parse(request.path("window_start_utc").asText())
                    .isBefore(Instant.parse(request.path("window_end_utc").asText()))
                || !LocalDate.parse(request.path("processing_date").asText()).equals(LocalDate.ofInstant(
                    Instant.parse(request.path("processed_at_utc").asText()), ZoneOffset.UTC))) {
            throw new IOException("RESOLVED_BACKFILL_CONTEXT_REQUIRED");
        }
        int usgs = 0; var years = new HashSet<Integer>();
        for (JsonNode pin : request.path("bronze_inputs")) {
            if ("USGS".equals(pin.path("source_system").asText())) { usgs++; }
            else if ("JMA_BULLETIN".equals(pin.path("source_system").asText())) { years.add(pin.path("year").asInt()); }
            else { throw new IOException("TWO_SOURCES_REQUIRED"); }
        }
        if (usgs != 1 || !years.equals(Set.of(2000, 2023))) { throw new IOException("JMA_2000_AND_2023_REQUIRED"); }
    }

    private static ObjectNode counts(SilverIntegrationResult result) {
        var r = result.reconciliationReport(); var out = JSON.createObjectNode();
        out.put("parsed", r.totalParsedCount()); out.put("valid", r.totalValidCount()); out.put("rejected", r.totalRejectCount());
        out.put("current", r.totalCurrentCount()); out.put("duplicate", r.totalDuplicateCount()); out.put("superseded", r.totalSupersededCount());
        out.put("usgs_parsed", r.usgsParsedCount()); out.put("jma_parsed", r.jmaParsedCount());
        out.put("candidate_pairs", r.candidatePairsEvaluated()); out.put("accepted_links", r.acceptedLinksCount());
        out.put("rejected_links", r.rejectedLinksCount()); out.put("ambiguous_links", r.ambiguousLinksCount());
        out.put("canonical", r.canonicalEventsCount()); out.put("matched", r.matchedEventsCount());
        out.put("usgs_only", r.usgsOnlyEventsCount()); out.put("jma_only", r.jmaOnlyEventsCount());
        out.put("memberships", result.canonicalMemberships().size()); out.put("balanced", r.isReconciliationBalanced());
        return out;
    }

    private static Dataset<Row> readDataset(SparkSession spark, SilverObjectStore store, JsonNode bundle,
            String name, Path readbackRoot) throws IOException {
        String uri = bundle.path("datasets").path(name).path("manifest_uri").asText();
        String manifestKey = SilverBundlePublisher.root(bundle.path("run_id").asText()) + "/" + name + "/manifest.json";
        if (!store.uriForKey(manifestKey).equals(uri)) { throw new IOException("EXACT_DATASET_REQUIRED"); }
        byte[] manifestBytes = store.read(manifestKey);
        var manifest = JSON.readTree(manifestBytes);
        if (!SilverBundlePublisher.sha(manifestBytes).equals(bundle.path("datasets").path(name).path("sha256").asText())) {
            throw new IOException("READBACK_MANIFEST_CHANGED");
        }
        var paths = new ArrayList<String>(); int index = 0;
        for (var file : manifest.path("files")) {
            byte[] bytes = store.read(file.path("relative_path").asText());
            if (!SilverBundlePublisher.sha(bytes).equals(file.path("sha256").asText())) { throw new IOException("READBACK_CHANGED"); }
            Path path = readbackRoot.resolve(name + "-" + index++ + ".parquet");
            Files.write(path, bytes, StandardOpenOption.CREATE_NEW); paths.add(path.toString());
            Files.setPosixFilePermissions(path, java.nio.file.attribute.PosixFilePermissions.fromString("rw-r--r--"));
        }
        return spark.read().parquet(paths.toArray(String[]::new));
    }

    private static String key(String uri) { return URI.create(uri).getPath().substring(1); }
}
