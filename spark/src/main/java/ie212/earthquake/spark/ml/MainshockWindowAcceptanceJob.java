package ie212.earthquake.spark.ml;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.GoldCommitJob;
import ie212.earthquake.spark.bronze.BronzeReuseVerifier;
import ie212.earthquake.spark.gold.*;
import ie212.earthquake.spark.silver.*;
import ie212.earthquake.spark.usgs.MinioBronzeObjectStore;
import java.net.URI;
import java.nio.file.*;
import java.time.*;
import java.util.*;

/** Isolated live QA only. Real pinned archives -> bounded Silver -> fresh Published Gold -> windows. */
public final class MainshockWindowAcceptanceJob {
    private static final ObjectMapper JSON = new ObjectMapper();
    private MainshockWindowAcceptanceJob() { }
    public static void validate(JsonNode request, JsonNode pins) {
        String suffix = request.path("run_id").asText().replaceFirst("^mld03-qa-", "");
        String gold = request.path("gold_namespace").asText();
        if (!suffix.matches("[a-f0-9]{32}") || !request.path("run_id").asText().equals("mld03-qa-" + suffix)
                || !"mainshock_window_qa".equals(request.path("dag_id").asText())
                || !gold.matches("[a-z][a-z0-9_]*\\.gold_qa_[a-f0-9]{32}") || !gold.endsWith(suffix)
                || !gold.replace(".gold_qa_", ".ml_qa_").equals(request.path("ml_namespace").asText())
                || !request.path("code_version").asText().matches("artifact_[a-f0-9]{64}")
                || !request.path("config_version").asText().matches("[A-Za-z0-9._-]{1,128}")
                || !pins.path("bronze_inputs").isArray() || pins.path("bronze_inputs").size() != 3)
            throw new IllegalArgumentException("ML_WINDOW_FRESH_QA_SCOPE_REQUIRED");
        var sources = new ArrayList<String>(); var years = new TreeSet<Integer>();
        for (var pin : pins.path("bronze_inputs")) {
            sources.add(pin.path("source_system").asText());
            if ("JMA_BULLETIN".equals(pin.path("source_system").asText())) years.add(pin.path("year").asInt());
        }
        if (Collections.frequency(sources, "USGS") != 1 || Collections.frequency(sources, "JMA_BULLETIN") != 2
                || !years.equals(new TreeSet<>(List.of(2000, 2023)))) throw new IllegalArgumentException("ML_WINDOW_TWO_JMA_YEARS_REQUIRED");
    }
    public static void main(String[] args) throws Exception {
        boolean resume = args.length == 3 && "--resume-windows".equals(args[2]);
        if ((!resume && args.length != 2) || Files.size(Path.of(args[0])) > 65536 || Files.size(Path.of(args[1])) > 65536)
            throw new IllegalArgumentException("ML_WINDOW_QA_BOUNDED_REQUEST_REQUIRED");
        var request = JSON.readTree(Files.readAllBytes(Path.of(args[0])));
        var pins = JSON.readTree(Files.readAllBytes(Path.of(args[1]))); validate(request, pins);
        Path root = Path.of(args[0]).toAbsolutePath().getParent();
        String run = request.path("run_id").asText(), goldNamespace = request.path("gold_namespace").asText(), mlNamespace = request.path("ml_namespace").asText();
        Runnable lease = () -> { try {
            var owner = JSON.readTree(Files.readAllBytes(Path.of(System.getenv("SOURCE_GUARD_ROOT"), "owner.json")));
            if (!run.equals(owner.path("run_id").asText()) || !"mainshock_window_qa".equals(owner.path("dag_id").asText())) throw new IllegalStateException();
        } catch (Exception error) { throw new IllegalStateException("ML_WINDOW_QA_LEASE_NOT_HELD"); } };
        String host = System.getenv("TRINO_HOST"), port = System.getenv("TRINO_INTERNAL_PORT");
        if (host == null || !host.matches("[a-z0-9][a-z0-9.-]*") || port == null || !port.matches("[0-9]{1,5}")) throw new IllegalArgumentException("ML_TRINO_CONFIG_REQUIRED");
        var sql = new TrinoSqlClient(URI.create("http://" + host + ":" + port)); lease.run();
        if (resume) { resumeWindows(request, pins, root, sql, lease); return; }
        String catalog = goldNamespace.split("\\.")[0];
        for (String namespace : List.of(goldNamespace, mlNamespace)) {
            if (!sql.execute("SELECT schema_name FROM " + catalog + ".information_schema.schemata WHERE schema_name="
                + GoldVerificationPlan.quote(namespace.split("\\.")[1])).rows().isEmpty()) throw new IllegalArgumentException("ML_FRESH_QA_NAMESPACE_REQUIRED");
        }
        var processed = Instant.now(); String silverRun = run + "-silver";
        var observations = new ArrayList<SilverObservation>(); var rejects = new ArrayList<SilverRejectRecord>();
        var lineage = new LinkedHashMap<String, String>();
        var report = JSON.createObjectNode().put("task", "MLD-03").put("scope", "BOUNDED_QA_ONLY").put("research_ready", false)
            .put("gold_namespace", goldNamespace).put("ml_namespace", mlNamespace).put("run_id", run)
            .put("code_version", request.path("code_version").asText()).put("sampling_version", JmaMainshockPilotSampler.VERSION);
        // Trino date_diff has no microsecond unit; MySQL date_format truncates to milliseconds.
        String microGate = "WITH m AS (SELECT CAST(from_iso8601_timestamp_nanos('2023-01-01T23:59:59.999999Z') AS timestamp(6) with time zone) event_time_utc),"
            + " c AS (SELECT CAST(from_iso8601_timestamp_nanos('2023-01-02T00:00:00.000000Z') AS timestamp(6) with time zone) event_time_utc)"
            + " SELECT " + oracleMicros("c.event_time_utc") + "-" + oracleMicros("m.event_time_utc")
            + "," + oracleMicros("m.event_time_utc") + "-" + oracleMicros("c.event_time_utc") + " FROM m CROSS JOIN c";
        var precision = sql.execute(microGate);
        if (precision.rows().size() != 1 || precision.rows().get(0).get(0).asLong() != 1 || precision.rows().get(0).get(1).asLong() != -1)
            throw new IllegalStateException("ML_PILOT_TRINO_MICROSECOND_GATE_FAILED");
        report.put("trino_microsecond_gate_passed", true);
        var coverage = report.putArray("sources");
        try (var bronze = MinioBronzeObjectStore.fromEnvironment()) {
            for (var pin : pins.path("bronze_inputs")) {
                var manifest = JSON.readTree(bronze.read(key(pin.path("manifest_uri").asText())));
                if (manifest.path("content_length_bytes").asLong(Long.MAX_VALUE) > 32L * 1024 * 1024)
                    throw new IllegalArgumentException("ML_PILOT_ARCHIVE_SIZE_LIMIT");
            }
            var reuse = JSON.createObjectNode().put("contract_version", "orc-03-v1").put("observability_version", "orc-04-v1")
                .put("scope_sha256", DatasetIdentityPlan.sha(DatasetIdentityPlan.canonical(pins).toString()));
            reuse.set("bronze_inputs", pins.path("bronze_inputs"));
            var verified = BronzeReuseVerifier.verify(reuse, bronze); report.set("bronze_readback", verified);
            for (var pin : pins.path("bronze_inputs")) {
                lease.run(); byte[] manifestBytes = bronze.read(key(pin.path("manifest_uri").asText()));
                if (!SilverBundlePublisher.sha(manifestBytes).equals(pin.path("manifest_sha256").asText())) throw new IllegalStateException("ML_BRONZE_CHANGED");
                var manifest = JSON.readTree(manifestBytes); String rawUri = manifest.path("raw_object_uri").asText();
                byte[] raw = bronze.read(key(rawUri));
                if (!SilverBundlePublisher.sha(raw).equals(pin.path("sha256").asText())) throw new IllegalStateException("ML_BRONZE_CHANGED");
                lineage.put(manifest.path("manifest_id").asText(), manifest.path("run_id").asText());
                var item = coverage.addObject(); item.set("pin", pin); item.put("full_bronze_records", manifest.path("record_count_estimate").asLong());
                if ("USGS".equals(pin.path("source_system").asText())) {
                    var parsed = new UsgsGeoJsonParser().parse(raw, new UsgsParseContext(manifest.path("manifest_id").asText(), rawUri,
                        pin.path("sha256").asText(), manifest.path("run_id").asText(), processed));
                    observations.addAll(parsed.observations()); rejects.addAll(parsed.rejects());
                    item.put("selected", parsed.parsedCount()).put("selection", "all-records");
                } else {
                    var context = new JmaParseContext(manifest.path("manifest_id").asText(), rawUri, pin.path("sha256").asText(),
                        manifest.path("run_id").asText(), pin.path("catalog_release").asText(), null,
                        manifest.path("provenance").path("source_url").textValue(), manifest.path("provenance").path("member_name").asText(), processed);
                    var sample = JmaMainshockPilotSampler.select(raw, context, pin.path("year").asInt());
                    if (sample.physicalCount() != manifest.path("record_count_estimate").asLong()) throw new IllegalStateException("ML_PILOT_ARCHIVE_COUNT_CONFLICT");
                    observations.addAll(sample.observations()); rejects.addAll(sample.rejects());
                    item.put("selected", sample.observations().size()).put("selected_or_unassigned_time_parse_rejects", sample.rejects().size())
                        .put("selection", JmaMainshockPilotSampler.VERSION).put("sample_start_utc", sample.start().toString()).put("sample_end_utc_exclusive", sample.end().toString())
                        .put("centre_source_record_key", sample.centre().sourceRecordKey()).put("centre_raw_locator", sample.centre().rawRecordLocator())
                        .put("centre_magnitude", sample.centre().magnitude()).put("centre_depth_km", sample.centre().depthKm());
                }
            }
        }
        if (observations.size() + rejects.size() > 30_016) throw new IllegalStateException("ML_PILOT_TOTAL_LIMIT_EXCEEDED");
        System.out.println("task=MLD-03 phase=sampled observations=" + observations.size());
        var integration = new SilverMultiSourceIntegrationRunner().run(silverRun, observations, rejects, SilverLinkConfig.defaultConfig(), null, false, processed, lineage);
        if (!integration.isPublishable()) {
            report.set("silver_gate", blockedDiagnostics(integration));
            report.put("status", "BLOCKED_BEFORE_PUBLICATION");
            Files.write(root.resolve("silver-blocked-report.json"), JSON.writerWithDefaultPrettyPrinter().writeValueAsBytes(report));
            throw new IllegalStateException("ML_PILOT_SILVER_BLOCKED");
        }
        var spark = GoldCommitJob.configured(System.getenv()).appName("mld03-mainshock-pilot").getOrCreate(); spark.sparkContext().setLogLevel("ERROR");
        JsonNode publication;
        report.put("java_version", System.getProperty("java.version")).put("spark_version", spark.version());
        try (var store = MinioSilverObjectStore.fromEnvironment()) {
            var silverContext = JSON.createObjectNode().put("quality_passed", true).put("reconciliation_balanced", true).put("coverage", "bounded-mainshock-centred-pilot-not-complete-months");
            silverContext.set("sources", coverage);
            var publisher = new SilverBundlePublisher(store);
            var write = new SilverWriteRequest(silverRun, integration.allObservations(), integration.qualityResult().rejectedRecords(),
                integration.sourceLinks(), integration.canonicalMemberships(), processed, false);
            publisher.publish(write, silverContext); publisher.publish(write, silverContext); var bundle = publisher.verify(silverRun);
            report.set("silver_bundle", bundle); String silverSha = SilverBundlePublisher.sha(store.read(SilverBundlePublisher.root(silverRun) + "/manifest.json"));
            report.put("silver_bundle_sha256", silverSha).put("silver_run_id", silverRun);
            var context = new GoldRunContext(run, Instant.parse("1999-12-31T15:00:00Z"), Instant.parse("2024-01-01T00:00:00Z"),
                LocalDate.ofInstant(processed, ZoneOffset.UTC), true, request.path("config_version").asText(), List.of(store.uriForKey(SilverBundlePublisher.root(silverRun) + "/manifest.json")));
            var gold = GoldInputReader.read(spark, store, silverRun, silverSha, context, processed, root.resolve("readback"));
            var baseline = new TreeMap<String, Long>(); GoldIcebergWriter.TABLES.forEach(name -> baseline.put(name, 0L));
            var months = new ArrayList<String>(); for (int year : List.of(2000, 2023)) for (int month = 1; month <= 12; month++) months.add(String.format(Locale.ROOT, "%d-%02d", year, month));
            var writer = new GoldIcebergWriter(spark, goldNamespace, store, lease);
            var scope = new GoldCommitScope(context, silverSha, request.path("code_version").asText(), months, baseline);
            var commit = writer.commit(gold, scope);
            if (!commit.equals(writer.commit(gold, scope))) throw new IllegalStateException("ML_PILOT_COMMIT_RERUN_CONFLICT");
            report.set("commit", commit);
            var goldPublisher = new GoldPublisher(sql, store, lease);
            publication = goldPublisher.publish(run, goldNamespace, commit.path("identity_sha256").asText());
            if (!publication.equals(goldPublisher.publish(run, goldNamespace, commit.path("identity_sha256").asText()))) throw new IllegalStateException("ML_PILOT_PUBLICATION_RERUN_CONFLICT");
            report.set("publication", publication);
            System.out.println("task=MLD-03 phase=published");
        } finally { spark.stop(); }
        finishWindows(request, report, root, sql, lease, processed);
    }

    /** Explicit QA-only continuation: never seed or modify Silver/Gold on this path. */
    private static void resumeWindows(JsonNode request, JsonNode pins, Path root, GoldPublisher.Sql sql, Runnable lease) throws Exception {
        String run = request.path("run_id").asText(), gold = request.path("gold_namespace").asText();
        var report = JSON.createObjectNode().put("task", "MLD-03").put("scope", "BOUNDED_QA_ONLY").put("research_ready", false)
            .put("run_id", run).put("gold_namespace", gold).put("ml_namespace", request.path("ml_namespace").asText())
            .put("code_version", request.path("code_version").asText()).put("sampling_version", JmaMainshockPilotSampler.VERSION)
            .put("resumed_from_published_qa", true);
        try (var store = MinioSilverObjectStore.fromEnvironment(); var bronze = MinioBronzeObjectStore.fromEnvironment()) {
            lease.run();
            var commit = JSON.readTree(store.read("gold-commits/" + run + "/commit.json"));
            if (!store.exists("gold-verification/" + run + "/publication.json")) throw new IllegalArgumentException("ML_RESUME_PUBLISHED_RECEIPT_REQUIRED");
            var silver = new SilverBundlePublisher(store).verify(run + "-silver");
            String silverSha = SilverBundlePublisher.sha(store.read(SilverBundlePublisher.root(run + "-silver") + "/manifest.json"));
            var publication = new GoldPublisher(sql, store, lease).publish(run, gold, commit.path("identity_sha256").asText());
            validateResumeSource(request, pins, commit, silver, silverSha, publication);
            var reuse = JSON.createObjectNode().put("contract_version", "orc-03-v1").put("observability_version", "orc-04-v1")
                .put("scope_sha256", DatasetIdentityPlan.sha(DatasetIdentityPlan.canonical(pins).toString()));
            reuse.set("bronze_inputs", pins.path("bronze_inputs")); report.set("bronze_readback", BronzeReuseVerifier.verify(reuse, bronze));
            report.set("sources", silver.path("context").path("sources")); report.set("silver_bundle", silver);
            report.put("silver_run_id", run + "-silver").put("silver_bundle_sha256", silverSha);
            report.set("commit", commit); report.set("publication", publication);
        }
        String gate = "WITH m AS (SELECT CAST(from_iso8601_timestamp_nanos('2023-01-01T23:59:59.999999Z') AS timestamp(6) with time zone) event_time_utc),"
            + " c AS (SELECT CAST(from_iso8601_timestamp_nanos('2023-01-02T00:00:00.000000Z') AS timestamp(6) with time zone) event_time_utc) SELECT "
            + oracleMicros("c.event_time_utc") + "-" + oracleMicros("m.event_time_utc") + "," + oracleMicros("m.event_time_utc") + "-" + oracleMicros("c.event_time_utc") + " FROM m CROSS JOIN c";
        var precision = sql.execute(gate);
        if (precision.rows().size() != 1 || precision.rows().get(0).get(0).asLong() != 1 || precision.rows().get(0).get(1).asLong() != -1)
            throw new IllegalStateException("ML_PILOT_TRINO_MICROSECOND_GATE_FAILED");
        report.put("trino_microsecond_gate_passed", true);
        System.out.println("task=MLD-03 phase=resume-published-verified");
        finishWindows(request, report, root, sql, lease, Instant.now());
    }

    static void validateResumeSource(JsonNode request, JsonNode pins, JsonNode commit, JsonNode silver, String silverSha, JsonNode publication) {
        String run = request.path("run_id").asText();
        if (!run.equals(commit.path("gold_run_id").asText()) || !request.path("gold_namespace").equals(commit.path("namespace"))
                || !request.path("code_version").equals(commit.path("scope").path("code_version"))
                || !silverSha.equals(commit.path("scope").path("silver_bundle_sha256").asText())
                || !silver.path("context").path("quality_passed").asBoolean(false)
                || !silver.path("context").path("reconciliation_balanced").asBoolean(false)
                || !publication.path("published").asBoolean(false) || !"PASSED".equals(publication.path("verify_status").asText())
                || !run.equals(publication.path("gold_run_id").asText()) || !request.path("gold_namespace").equals(publication.path("namespace")))
            throw new IllegalArgumentException("ML_RESUME_SOURCE_PIN_CONFLICT");
        var recorded = JSON.createArrayNode(); silver.path("context").path("sources").forEach(source -> recorded.add(source.path("pin")));
        if (!DatasetIdentityPlan.canonical(recorded).toString().equals(DatasetIdentityPlan.canonical(pins.path("bronze_inputs")).toString()))
            throw new IllegalArgumentException("ML_RESUME_BRONZE_PINS_CHANGED");
    }

    private static void finishWindows(JsonNode request, ObjectNode report, Path root, GoldPublisher.Sql sql, Runnable lease, Instant processed) throws Exception {
        String run = request.path("run_id").asText(), goldNamespace = request.path("gold_namespace").asText(), mlNamespace = request.path("ml_namespace").asText();
        var publication = report.path("publication");
        Path jar = Path.of(MainshockWindowAcceptanceJob.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        if (!Files.isRegularFile(jar) || Files.size(jar) > 134217728) throw new IllegalArgumentException("ML_WINDOW_RUNTIME_ARTIFACT_REQUIRED");
        String windowCode = "artifact_" + SilverBundlePublisher.sha(Files.readAllBytes(jar));
        if (!windowCode.equals(request.path("window_code_version").asText(request.path("code_version").asText())))
            throw new IllegalArgumentException("ML_WINDOW_RUNTIME_ARTIFACT_CHANGED");
        report.put("window_code_version", windowCode).put("java_version", System.getProperty("java.version"));
        var datasets = report.putArray("datasets"); var cutoff = Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS);
        for (String split : List.of("REPRODUCTION", "EXTENSION")) {
            var config = GoldAuditAcceptanceJob.datasetConfig(split, cutoff, windowCode);
            config.put("dataset_config_version", "mld03-centred-pilot-v1").put("window_model_version", WindowModelConfig.expandedV1().windowModelVersion());
            var filters = (ObjectNode) config.path("filter_config"); filters.put("coverage_scope", "bounded-14d-mainshock-centred-2000-2023-not-full-window");
            filters.set("window_config", WindowModelConfig.expandedV1().toJsonNode());
            filters.set("mainshock_selection_config", ("REPRODUCTION".equals(split) ? MainshockSelectionConfig.reproductionBaseline() : MainshockSelectionConfig.extensionBaseline()).toJsonNode());
            var manifests = new DatasetManifestWriter(sql, lease);
            var manifest = manifests.build(goldNamespace, publication.path("publication_id").asText(), mlNamespace, config, run, Instant.now());
            if (!manifest.equals(manifests.build(goldNamespace, publication.path("publication_id").asText(), mlNamespace, config, run, Instant.now()))) throw new IllegalStateException("ML_PILOT_IDENTITY_RERUN_CONFLICT");
            Path splitRoot = root.resolve(split.toLowerCase(Locale.ROOT)), auditRoot = splitRoot.resolve("audit"); Files.createDirectories(auditRoot);
            var input = JSON.createObjectNode().put("ml_namespace", mlNamespace).put("dataset_id", manifest.path("dataset_id").asText()).put("run_id", run).put("dag_id", "mainshock_window_qa");
            Files.write(auditRoot.resolve("request.json"), JSON.writeValueAsBytes(input)); GoldInputAuditJob.main(new String[]{auditRoot.resolve("request.json").toString()});
            input.put("audit_handoff_path", "audit/audited-manifest.json").put("audit_handoff_sha256", SilverBundlePublisher.sha(Files.readAllBytes(auditRoot.resolve("audited-manifest.json"))))
                .put("created_at_utc", processed.toString());
            Path windowRequest = splitRoot.resolve("request.json"); Files.write(windowRequest, JSON.writeValueAsBytes(input));
            MainshockWindowJob.main(new String[]{windowRequest.toString()});
            var first = JSON.readTree(Files.readAllBytes(splitRoot.resolve("window-runtime-report.json")));
            MainshockWindowJob.main(new String[]{windowRequest.toString()});
            var second = JSON.readTree(Files.readAllBytes(splitRoot.resolve("window-runtime-report.json")));
            report.put("spark_version", second.path("spark_version").asText());
            for (String field : List.of("mainshock_count", "total_candidate_row_count", "windows", "role_counts", "mc_config_sha256", "mainshock_content_sha256", "candidate_content_sha256"))
                if (!first.path(field).equals(second.path(field))) throw new IllegalStateException("ML_PILOT_WINDOW_RERUN_CONFLICT");
            if (second.path("mainshock_count").asLong() < 1 || second.path("role_counts").path("PRE").asLong() < 1
                    || second.path("role_counts").path("POST").asLong() < 1 || !second.path("bucket_join_verified").asBoolean(false)) throw new IllegalStateException("ML_PILOT_NONEMPTY_PRE_POST_REQUIRED");
            var oracle = oracle(sql, manifest, second.path("mc_applied").asDouble());
            var windows = second.path("windows");
            if (oracle.size() != windows.size()) throw new IllegalStateException("ML_PILOT_TRINO_WINDOWS_CONFLICT");
            long total = 0, pre = 0, post = 0, self = 0;
            for (var row : oracle) {
                if (row.path("candidate_count").asLong() != windows.path(row.path("mainshock_event_id").asText()).path("candidate_count").asLong(-1)) throw new IllegalStateException("ML_PILOT_TRINO_COUNTS_CONFLICT");
                total += row.path("candidate_count").asLong(); pre += row.path("pre_count").asLong(); post += row.path("post_count").asLong(); self += row.path("self_count").asLong();
            }
            if (total != second.path("total_candidate_row_count").asLong() || pre != second.path("role_counts").path("PRE").asLong()
                    || post != second.path("role_counts").path("POST").asLong() || self != second.path("mainshock_count").asLong()) throw new IllegalStateException("ML_PILOT_TRINO_ROLES_CONFLICT");
            var item = datasets.addObject(); item.set("manifest", manifest); item.set("audit", JSON.readTree(Files.readAllBytes(auditRoot.resolve("audit-report.json"))));
            item.set("window_manifest", JSON.readTree(Files.readAllBytes(splitRoot.resolve("window-manifest.json")))); item.set("window", second);
            item.set("trino_oracle", JSON.valueToTree(oracle)); item.put("rerun_unchanged", true);
        }
        lease.run(); report.put("status", "VERIFIED_PUBLISHED_JMA_WINDOWS").put("rerun_unchanged", true);
        Files.write(root.resolve("mld-03-report.json"), JSON.writerWithDefaultPrettyPrinter().writeValueAsBytes(report));
        System.out.println("task=MLD-03 status=VERIFIED_PUBLISHED_JMA_WINDOWS");
    }
    /** Bounded metadata only: a failed gate is never a successful acceptance receipt. */
    static ObjectNode blockedDiagnostics(SilverIntegrationResult integration) {
        var quality = integration.qualityResult(); var reconciliation = integration.reconciliationReport();
        var result = JSON.createObjectNode().put("publishable", integration.isPublishable())
            .put("quality_blocked", quality.publishBlocked()).put("parsed", quality.parsedCount())
            .put("valid", quality.validCount()).put("rejected", quality.rejectedCount())
            .put("parsed_balanced", reconciliation.isParsedBalanced()).put("dedup_balanced", reconciliation.isDedupBalanced())
            .put("canonical_balanced", reconciliation.isCanonicalBalanced()).put("bridge_balanced", reconciliation.isBridgeCountBalanced());
        result.set("reason_counts", JSON.valueToTree(new TreeMap<>(quality.reasonCounts())));
        var samples = result.putArray("reject_samples");
        quality.rejectedRecords().stream().limit(8).forEach(reject -> {
            var item = samples.addObject().put("source_system", reject.sourceSystem())
                .put("bronze_manifest_id", reject.bronzeManifestId()).put("raw_record_locator", reject.rawRecordLocator())
                .put("raw_record_hash", reject.rawRecordHash()).put("reject_stage", reject.rejectStage());
            item.set("reason_codes", JSON.valueToTree(reject.rejectReasonCodes()));
        });
        return result;
    }
    private static String key(String uri) { return URI.create(uri).getPath().substring(1); }
    static String oracleMicros(String column) {
        if (!Set.of("m.event_time_utc", "c.event_time_utc").contains(column)) throw new IllegalArgumentException("ML_ORACLE_COLUMN_REQUIRED");
        return "(date_diff('second',from_iso8601_timestamp('1970-01-01T00:00:00Z'),date_trunc('second'," + column + "))*1000000"
            + "+CAST(rpad(coalesce(regexp_extract(to_iso8601(" + column + "),'[.]([0-9]+)',1),''),6,'0') AS bigint))";
    }
    static List<JsonNode> oracle(GoldPublisher.Sql sql, JsonNode manifest, double mc) throws Exception {
        String pin = DatasetManifestWriter.pinnedGoldSql(manifest);
        String source = "canonical_source_system='JMA_BULLETIN' AND catalog_era='UNIFIED' AND is_natural_earthquake AND is_in_study_area AND event_type_code='EARTHQUAKE'"
            + " AND latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180 AND is_finite(depth_km) AND is_finite(magnitude) AND magnitude >= " + mc
            + " AND event_time_utc >= from_iso8601_timestamp(" + GoldVerificationPlan.quote(manifest.path("period_start_utc").asText()) + ")"
            + " AND event_time_utc < from_iso8601_timestamp(" + GoldVerificationPlan.quote(manifest.path("period_end_utc_exclusive").asText()) + ")";
        String post = "least(17520.0,24.0*0.0567*exp(1.235*m.magnitude))", pre = "least(720.0,greatest(168.0,0.15*(" + post + ")))";
        String radius = "least(250.0,0.359*exp(0.804*m.magnitude))";
        String deltaMicros = "(" + oracleMicros("c.event_time_utc") + "-" + oracleMicros("m.event_time_utc") + ")";
        return DatasetManifestWriter.records(sql.execute("WITH events AS (SELECT * FROM (" + pin + ") t WHERE " + source + ")"
            + " SELECT m.canonical_event_id AS mainshock_event_id,count(*) AS candidate_count,"
            + "count_if(c.canonical_event_id=m.canonical_event_id) AS self_count,"
            + "count_if(c.canonical_event_id<>m.canonical_event_id AND c.event_time_utc<m.event_time_utc) AS pre_count,"
            + "count_if(c.canonical_event_id<>m.canonical_event_id AND c.event_time_utc>=m.event_time_utc) AS post_count"
            + " FROM events m JOIN events c ON " + deltaMicros + " BETWEEN -CAST(round((" + pre + ")*3600000000) AS bigint) AND CAST(round((" + post + ")*3600000000) AS bigint)"
            + " AND c.latitude BETWEEN m.latitude-(" + radius + ")/110.57 AND m.latitude+(" + radius + ")/110.57"
            + " AND c.longitude BETWEEN m.longitude-(" + radius + ")/(111.32*cos(radians(m.latitude))) AND m.longitude+(" + radius + ")/(111.32*cos(radians(m.latitude)))"
            + " AND sqrt(pow(111.32*cos(radians(m.latitude))*(c.longitude-m.longitude),2)+pow(110.57*(c.latitude-m.latitude),2)) <= (" + radius + ")+0.00001"
            + " WHERE m.magnitude>5.5 AND m.depth_km BETWEEN 50 AND 200 GROUP BY m.canonical_event_id ORDER BY m.canonical_event_id"));
    }
}
