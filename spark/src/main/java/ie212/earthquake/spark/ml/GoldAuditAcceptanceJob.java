package ie212.earthquake.spark.ml;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.GoldCommitJob;
import ie212.earthquake.spark.gold.*;
import ie212.earthquake.spark.silver.*;
import java.net.URI;
import java.nio.file.*;
import java.time.*;
import java.util.*;

/** Opt-in bounded JMA acceptance. Never substitutes sampled months into production Gold. */
public final class GoldAuditAcceptanceJob {
    private static final ObjectMapper JSON = new ObjectMapper();
    public static final List<String> QA_MONTHS = List.of("1999-12", "2000-01", "2022-12", "2023-01");
    private GoldAuditAcceptanceJob() { }

    public static void validateRequest(JsonNode request, JsonNode reference) {
        String gold = request.path("gold_namespace").asText();
        if (!gold.matches("[a-z][a-z0-9_]*\\.gold_qa_[a-f0-9]{32}")
                || !gold.replace(".gold_qa_", ".ml_qa_").equals(request.path("ml_namespace").asText())
                || !request.path("run_id").asText().matches("mld02-qa-[a-f0-9]{32}")
                || !"gold_input_audit_qa".equals(request.path("dag_id").asText())
                || !request.path("code_version").asText().matches("artifact_[a-f0-9]{64}")
                || !request.path("config_version").asText().matches("[A-Za-z0-9._-]{1,128}"))
            throw new IllegalArgumentException("ML_BOUNDED_QA_REQUEST_REQUIRED");
        var attempt = reference.path("attempts").path(0);
        long count = attempt.path("datasets").path("source_observation").path("record_count").asLong(-1);
        if (!"VERIFIED_BOUNDED_SAMPLE".equals(reference.path("status").asText())
                || !reference.path("rerun_unchanged").asBoolean(false)
                || !"SilverReady".equals(attempt.path("silver_status").asText())
                || !attempt.path("gold_handoff_verified").asBoolean(false)
                || !reference.path("run_id").equals(attempt.path("run_id"))
                || !attempt.path("bundle_manifest_sha256").asText().matches("[a-f0-9]{64}")
                || count < 100 || count > 20_000
                || !"1999-12-31T15:00:00Z".equals(attempt.path("run_context").path("window_start_utc").asText())
                || !"2024-01-01T00:00:00Z".equals(attempt.path("run_context").path("window_end_utc").asText()))
            throw new IllegalArgumentException("ML_VERIFIED_BOUNDED_SILVER_REQUIRED");
        SilverBundlePublisher.root(reference.path("run_id").asText());
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2 || Files.size(Path.of(args[0])) > 65536 || Files.size(Path.of(args[1])) > 1_048_576)
            throw new IllegalArgumentException("ML_BOUNDED_QA_INPUT_REQUIRED");
        var request = JSON.readTree(Files.readAllBytes(Path.of(args[0])));
        var reference = JSON.readTree(Files.readAllBytes(Path.of(args[1]))); validateRequest(request, reference);
        String run = request.path("run_id").asText(), goldNamespace = request.path("gold_namespace").asText();
        String mlNamespace = request.path("ml_namespace").asText();
        Path root = Path.of(args[0]).toAbsolutePath().getParent();
        Runnable lease = () -> { try {
            var owner = JSON.readTree(Files.readAllBytes(Path.of(System.getenv("SOURCE_GUARD_ROOT"), "owner.json")));
            if (!run.equals(owner.path("run_id").asText()) || !"gold_input_audit_qa".equals(owner.path("dag_id").asText()))
                throw new IllegalStateException();
        } catch (Exception error) { throw new IllegalStateException("ML_QA_LEASE_NOT_HELD"); } };
        String host = System.getenv("TRINO_HOST"), port = System.getenv("TRINO_INTERNAL_PORT");
        if (host == null || !host.matches("[a-z0-9][a-z0-9.-]*") || port == null || !port.matches("[0-9]{1,5}"))
            throw new IllegalArgumentException("ML_TRINO_CONFIG_REQUIRED");
        var sql = new TrinoSqlClient(URI.create("http://" + host + ":" + port)); lease.run();
        // Empty baselines are permitted ONLY for this newly generated, absent QA schema.
        var schemas = sql.execute("SELECT schema_name FROM " + goldNamespace.split("\\.")[0]
            + ".information_schema.schemata WHERE schema_name=" + GoldVerificationPlan.quote(goldNamespace.split("\\.")[1]));
        if (!schemas.rows().isEmpty()) throw new IllegalArgumentException("ML_FRESH_QA_NAMESPACE_REQUIRED");
        var attempt = reference.path("attempts").path(0);
        String silverRun = reference.path("run_id").asText(), silverSha = attempt.path("bundle_manifest_sha256").asText();
        var report = JSON.createObjectNode().put("task", "MLD-02").put("scope", "BOUNDED_QA_ONLY")
            .put("research_ready", false).put("gold_namespace", goldNamespace).put("ml_namespace", mlNamespace)
            .put("code_version", request.path("code_version").asText()).put("silver_run_id", silverRun)
            .put("silver_bundle_sha256", silverSha).put("java_version", System.getProperty("java.version"));
        JsonNode publication;
        var spark = GoldCommitJob.configured(System.getenv()).appName("mld02-bounded-jma-acceptance").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR"); report.put("spark_version", spark.version());
        try (var store = MinioSilverObjectStore.fromEnvironment()) {
            var processed = Instant.now();
            var context = new GoldRunContext(run, Instant.parse("1999-12-31T15:00:00Z"), Instant.parse("2024-01-01T00:00:00Z"),
                LocalDate.ofInstant(processed, ZoneOffset.UTC), true, request.path("config_version").asText(),
                List.of(store.uriForKey(SilverBundlePublisher.root(silverRun) + "/manifest.json")));
            var gold = GoldInputReader.read(spark, store, silverRun, silverSha, context, processed, root.resolve("readback"));
            if (gold.currentObservationCount() != attempt.path("datasets").path("source_observation").path("record_count").asLong())
                throw new IllegalStateException("ML_SILVER_COUNT_CONFLICT");
            var baseline = new TreeMap<String, Long>(); GoldIcebergWriter.TABLES.forEach(name -> baseline.put(name, 0L));
            var scope = new GoldCommitScope(context, silverSha, request.path("code_version").asText(), QA_MONTHS, baseline);
            var writer = new GoldIcebergWriter(spark, goldNamespace, store, lease);
            var commit = writer.commit(gold, scope);
            if (!commit.equals(writer.commit(gold, scope))) throw new IllegalStateException("ML_GOLD_RERUN_CHANGED");
            report.set("commit", commit);
            var publisher = new GoldPublisher(sql, store, lease);
            publication = publisher.publish(run, goldNamespace, commit.path("identity_sha256").asText());
            if (!publication.equals(publisher.publish(run, goldNamespace, commit.path("identity_sha256").asText())))
                throw new IllegalStateException("ML_PUBLICATION_RERUN_CHANGED");
            report.set("publication", publication);
        } finally { spark.stop(); }
        var entries = report.putArray("datasets");
        Instant cutoff = Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS); // Trino timestamp(6) round-trip safe.
        for (String split : List.of("REPRODUCTION", "EXTENSION")) {
            lease.run(); var config = datasetConfig(split, cutoff, request.path("code_version").asText());
            var manifests = new DatasetManifestWriter(sql, lease);
            var manifest = manifests.build(goldNamespace, publication.path("publication_id").asText(), mlNamespace, config, run, Instant.now());
            if (!manifest.equals(manifests.build(goldNamespace, publication.path("publication_id").asText(), mlNamespace, config, run, Instant.now())))
                throw new IllegalStateException("ML_MANIFEST_RERUN_CHANGED");
            Path auditRoot = root.resolve(split.toLowerCase(Locale.ROOT)); Files.createDirectories(auditRoot);
            var auditRequest = JSON.createObjectNode().put("ml_namespace", mlNamespace).put("dataset_id", manifest.path("dataset_id").asText())
                .put("dag_id", "gold_input_audit_qa").put("run_id", run);
            Path input = auditRoot.resolve("request.json"); Files.write(input, JSON.writeValueAsBytes(auditRequest));
            GoldInputAuditJob.main(new String[]{input.toString()});
            var first = (ObjectNode) JSON.readTree(Files.readAllBytes(auditRoot.resolve("audit-report.json")));
            GoldInputAuditJob.main(new String[]{input.toString()});
            var second = (ObjectNode) JSON.readTree(Files.readAllBytes(auditRoot.resolve("audit-report.json")));
            var stableFirst = first.deepCopy(); stableFirst.remove("audited_at_utc");
            var stableSecond = second.deepCopy(); stableSecond.remove("audited_at_utc");
            if (!stableFirst.equals(stableSecond)) throw new IllegalStateException("ML_AUDIT_RERUN_CHANGED");
            var handoff = JSON.readTree(Files.readAllBytes(auditRoot.resolve("audited-manifest.json")));
            var counts = first.path("counts");
            long expected = publication.path("snapshot_bundle").path("event_current").path("count").asLong(-1);
            if (expected < 1 || counts.path("input_event_count").asLong() != expected
                    || !first.path("candidate_build_allowed").asBoolean(false)
                    || !first.path("completeness_overall").path("is_reliable").asBoolean(false)
                    || first.path("completeness_overall").path("sample_count").asLong() < 50)
                throw new IllegalStateException("ML_JMA_PILOT_ACCEPTANCE_BLOCKED");
            String pinned = DatasetManifestWriter.pinnedGoldSql(manifest);
            long liveCount = sql.execute("SELECT count(*) FROM (" + pinned + ") t").rows().get(0).get(0).asLong();
            if (liveCount != counts.path("input_event_count").asLong()) throw new IllegalStateException("ML_TRINO_AUDIT_COUNT_CONFLICT");
            String naturalJma = "canonical_source_system='JMA_BULLETIN' AND catalog_era='UNIFIED' AND is_natural_earthquake AND is_in_study_area"
                + " AND event_time_utc >= from_iso8601_timestamp(" + GoldVerificationPlan.quote(config.path("period_start_utc").asText()) + ")"
                + " AND event_time_utc < from_iso8601_timestamp(" + GoldVerificationPlan.quote(config.path("period_end_utc_exclusive").asText()) + ")"
                + " AND latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180"
                + " AND event_type_code='EARTHQUAKE' AND is_finite(depth_km) AND is_finite(magnitude) AND magnitude >= " + first.path("mc_applied").asText();
            long eligible = sql.execute("SELECT count(*) FROM (" + pinned + ") t WHERE " + naturalJma).rows().get(0).get(0).asLong();
            if (eligible != counts.path("eligible_event_count").asLong()) throw new IllegalStateException("ML_TRINO_ELIGIBLE_COUNT_CONFLICT");
            var entry = entries.addObject(); entry.set("manifest", manifest); entry.set("audit", first); entry.set("audited_manifest", handoff);
            entry.put("rerun_unchanged", true).put("trino_input_count", liveCount).put("trino_eligible_count", eligible);
        }
        lease.run(); report.put("rerun_unchanged", true).put("status", "VERIFIED_BOUNDED_JMA_PILOT");
        Files.write(root.resolve("mld-02-report.json"), JSON.writerWithDefaultPrettyPrinter().writeValueAsBytes(report));
        System.out.println("task=MLD-02 status=VERIFIED_BOUNDED_JMA_PILOT research_ready=false");
    }

    public static ObjectNode datasetConfig(String split, Instant cutoff, String code) {
        var config = JSON.createObjectNode().put("dataset_split", split).put("observation_cutoff_utc", cutoff.toString());
        boolean reproduction = "REPRODUCTION".equals(split);
        if (!reproduction && !"EXTENSION".equals(split)) throw new IllegalArgumentException("DS_INVALID_PERIOD");
        config.put("period_start_utc", reproduction ? GoldAuditConfig.REPRODUCTION_START : GoldAuditConfig.EXTENSION_START)
            .put("period_end_utc_exclusive", reproduction ? GoldAuditConfig.REPRODUCTION_END : GoldAuditConfig.EXTENSION_END);
        var filter = config.putObject("filter_config").put("is_natural_earthquake", true).put("is_in_study_area", true)
            .put("canonical_source_system", "JMA_BULLETIN").put("catalog_era", "UNIFIED")
            .put("coverage_scope", "slv09-first256-jma-2000-2023-not-full-period");
        filter.putObject("completeness_config").put("bin_width", 0.1).put("sensitivity_step", 0.2).put("min_events", 50)
            .put("region_version", "overall-bounded-pilot-v1");
        config.put("dataset_config_version", "mld02-bounded-pilot-v1").put("audit_rule_version", "gold-audit-v1")
            .put("mc_method_version", "maximum-curvature-v1").put("window_model_version", "window-model-v1")
            .put("feature_version", "relative-4d-v1").put("scaling_config_version", "scaling-v1").put("code_version", code);
        return config;
    }
}
