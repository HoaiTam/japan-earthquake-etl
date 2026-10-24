package ie212.earthquake.spark.ml;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.GoldCommitJob;
import ie212.earthquake.spark.gold.TrinoSqlClient;
import ie212.earthquake.spark.silver.SilverBundlePublisher;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.net.URI;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

/**
 * Entry point job for mainshock selection and candidate window generation (MLD-03).
 */
public final class MainshockWindowJob {
    private static final ObjectMapper JSON = new ObjectMapper();

    /** Runtime never accepts arbitrary Gold rows, latest aliases or a fixture Mc. */
    public static void main(String[] args) throws Exception {
        if (args.length != 1 || Files.size(Path.of(args[0])) > 65536) throw new IllegalArgumentException("ML_WINDOW_REQUEST_REQUIRED");
        Path root = Path.of(args[0]).toAbsolutePath().getParent();
        var request = JSON.readTree(Files.readAllBytes(Path.of(args[0])));
        String run = request.path("run_id").asText(), dag = request.path("dag_id").asText();
        if (!run.matches("[A-Za-z0-9][A-Za-z0-9_-]{0,127}") || !dag.matches("[A-Za-z0-9][A-Za-z0-9_-]{0,127}"))
            throw new IllegalArgumentException("ML_WINDOW_RUN_REQUIRED");
        Runnable lease = () -> { try {
            var owner = JSON.readTree(Files.readAllBytes(Path.of(System.getenv("SOURCE_GUARD_ROOT"), "owner.json")));
            if (!run.equals(owner.path("run_id").asText()) || !dag.equals(owner.path("dag_id").asText())) throw new IllegalStateException();
        } catch (Exception error) { throw new IllegalStateException("ML_WINDOW_LEASE_NOT_HELD"); } };
        lease.run(); Files.deleteIfExists(root.resolve("window-runtime-report.json"));
        String host = System.getenv("TRINO_HOST"), port = System.getenv("TRINO_INTERNAL_PORT");
        if (host == null || !host.matches("[a-z0-9][a-z0-9.-]*") || port == null || !port.matches("[0-9]{1,5}"))
            throw new IllegalArgumentException("ML_TRINO_CONFIG_REQUIRED");
        var sql = new TrinoSqlClient(URI.create("http://" + host + ":" + port));
        var manifest = PinnedAuditResolver.resolve(request.path("ml_namespace").asText(), request.path("dataset_id").asText(), run, sql, lease);
        Path handoffPath = root.resolve(request.path("audit_handoff_path").asText()).normalize();
        if (!handoffPath.startsWith(root) || Files.size(handoffPath) > 65536) throw new IllegalArgumentException("ML_WINDOW_BOUNDED_HANDOFF_REQUIRED");
        byte[] handoffBytes = Files.readAllBytes(handoffPath);
        if (!SilverBundlePublisher.sha(handoffBytes).equals(request.path("audit_handoff_sha256").asText()))
            throw new IllegalArgumentException("ML_WINDOW_HANDOFF_CHANGED");
        var handoff = JSON.readTree(handoffBytes);
        var window = windowConfig(manifest);
        var selection = selectionConfig(manifest);
        var spark = GoldCommitJob.configured(System.getenv()).appName("mld03-pinned-windows").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        try {
            lease.run();
            var audit = GoldInputAuditJob.runPinnedAudit(spark, manifest, GoldAuditConfig.fromManifest(manifest, run, "audit-report.json"), Instant.now());
            validateHandoff(manifest, handoff, audit);
            var result = run(audit.auditedEvents(), selection, window, audit.mcApplied(), manifest.path("dataset_id").asText(),
                Instant.parse(request.path("created_at_utc").asText()), root.resolve("windows").toString(),WindowCoveragePolicy.fromManifest(manifest));
            var main = spark.read().parquet(root.resolve("windows/mainshock_candidate_snapshot.parquet").toString());
            var candidates = spark.read().parquet(root.resolve("windows/sequence_candidate_snapshot.parquet").toString());
            // Validate READBACK content, not only lazy in-memory outputs or file counts.
            var readback = new MainshockWindowResult(main, candidates, result.mainshockCount(), result.totalCandidateRowCount(),
                result.distinctCandidateEventCount(), result.candidatesPerWindow(), result.nestedMainshocksPerWindow(),
                result.resourceGuardStatusPerWindow(), result.flaggedWindowCount(), result.rejectedWindowCount(), result.datasetId(),
                selection, window, result.mcApplied(), result.createdAtUtc()).withCoverage(result.windowCoverageReport());
            readback.assertInvariants();
            String mainSha = fingerprint(main), candidateSha = fingerprint(candidates);
            if (!mainSha.equals(fingerprint(result.mainshockSnapshot())) || !candidateSha.equals(fingerprint(result.candidateSnapshot())))
                throw new IOException("ML_WINDOW_PARQUET_CONTENT_CONFLICT");
            var report = result.toJsonNode();
            report.put("task", "MLD-03").put("input_mode", "PUBLISHED_SNAPSHOT").put("dataset_status", "BUILDING")
                .put("spark_version", spark.version()).put("java_version", System.getProperty("java.version"))
                .put("research_ready", false).put("gold_snapshot_id", manifest.path("gold_snapshot_id").asLong())
                .put("gold_publication_id", manifest.path("gold_publication_id").asText())
                .put("physical_gold_table_name", manifest.path("physical_gold_table_name").asText())
                .put("mc_config_sha256", audit.mcConfigSha256()).put("mainshock_content_sha256", mainSha)
                .put("candidate_content_sha256", candidateSha).put("parquet_readback_verified", true);
            var roles = report.putObject("role_counts");
            for (Row row : candidates.groupBy("relative_time_role").count().collectAsList()) roles.put(row.getString(0), row.getLong(1));
            String plan = result.candidateSnapshot().queryExecution().executedPlan().toString();
            boolean bucket = (plan.contains("HashJoin") || plan.contains("SortMergeJoin"))
                && !plan.contains("CartesianProduct") && !plan.contains("BroadcastNestedLoopJoin");
            if (result.mainshockCount() > 0 && !bucket) throw new IllegalStateException("ML_WINDOW_BUCKET_PLAN_REQUIRED");
            report.put("bucket_join_verified", result.mainshockCount() > 0 && bucket);
            var files = report.putArray("parquet_files");
            try (var paths = Files.walk(root.resolve("windows"))) {
                for (Path file : paths.filter(Files::isRegularFile).filter(p -> p.toString().endsWith(".parquet")).sorted().toList()) {
                    var entry = files.addObject().put("path", root.relativize(file).toString()).put("bytes", Files.size(file));
                    try (var input = Files.newInputStream(file)) {
                        var digest = MessageDigest.getInstance("SHA-256"); byte[] buffer = new byte[262144]; int n;
                        while ((n = input.read(buffer)) != -1) digest.update(buffer, 0, n);
                        entry.put("sha256", HexFormat.of().formatHex(digest.digest()));
                    }
                }
            }
            var enriched = (ObjectNode) handoff.deepCopy(); result.enrichManifest(enriched);
            lease.run(); Files.write(root.resolve("window-manifest.json"), JSON.writeValueAsBytes(enriched));
            Files.write(root.resolve("window-runtime-report.json"), JSON.writeValueAsBytes(report));
            System.out.println("task=MLD-03 dataset_id=" + result.datasetId() + " mainshocks=" + result.mainshockCount() + " candidates=" + result.totalCandidateRowCount());
        } finally { spark.stop(); }
    }

    public static WindowModelConfig windowConfig(JsonNode manifest) throws Exception {
        var model = switch (manifest.path("window_model_version").asText()) {
            case "wm_expanded_v1.0" -> WindowModelConfig.expandedV1();
            case "wm_uhrhammer_v1.0" -> WindowModelConfig.uhrhammerV1();
            case "wm_gardner_knopoff_v1.0" -> WindowModelConfig.gardnerKnopoffV1();
            default -> throw new IllegalArgumentException("ML_WINDOW_VERSION_UNSUPPORTED");
        };
        var filter = JSON.readTree(manifest.path("filter_config_json").asText());
        if (!DatasetIdentityPlan.canonical(model.toJsonNode()).toString().equals(DatasetIdentityPlan.canonical(filter.path("window_config")).toString()))
            throw new IllegalArgumentException("ML_WINDOW_CONFIG_NOT_PINNED");
        return model;
    }

    public static MainshockSelectionConfig selectionConfig(JsonNode manifest) throws Exception {
        var selection = switch (manifest.path("dataset_split").asText()) {
            case "REPRODUCTION" -> MainshockSelectionConfig.reproductionBaseline();
            case "EXTENSION" -> MainshockSelectionConfig.extensionBaseline();
            default -> throw new IllegalArgumentException("ML_WINDOW_SPLIT_UNSUPPORTED");
        };
        var filter = JSON.readTree(manifest.path("filter_config_json").asText());
        // JSON round-trip narrows small LongNode to IntNode; compare canonical JSON values,
        // not Jackson's JVM number-node classes. Unknown/changed parameters still fail.
        if (!DatasetIdentityPlan.canonical(selection.toJsonNode()).toString().equals(
                DatasetIdentityPlan.canonical(filter.path("mainshock_selection_config")).toString()))
            throw new IllegalArgumentException("ML_WINDOW_SELECTION_NOT_PINNED");
        return selection;
    }

    public static void validateHandoff(JsonNode manifest, JsonNode handoff, GoldAuditResult audit) {
        for (String key : List.of("input_event_count", "eligible_event_count", "excluded_event_count"))
            if (!handoff.path(key).isIntegralNumber()) throw new IllegalArgumentException("ML_WINDOW_INTEGRAL_COUNTS_REQUIRED");
        for (var names = manifest.fieldNames(); names.hasNext();) {
            String key = names.next();
            if (handoff.get(key) == null || (!manifest.path(key).isNull() && !manifest.path(key).equals(handoff.path(key))))
                throw new IllegalArgumentException("ML_WINDOW_MANIFEST_CONFLICT");
        }
        if (!Double.isFinite(audit.mcApplied()) || !audit.completenessResult().isReliable()
                || !handoff.path("mc_value").isNumber() || handoff.path("mc_value").doubleValue() != audit.mcApplied()
                || !audit.mcConfigJson().equals(handoff.path("mc_config_json").asText())
                || !audit.mcConfigSha256().equals(handoff.path("mc_config_sha256").asText())
                || handoff.path("input_event_count").asLong(-1) != audit.inputEventCount()
                || handoff.path("eligible_event_count").asLong(-1) != audit.eligibleEventCount()
                || handoff.path("excluded_event_count").asLong(-1) != audit.excludedEventCount())
            throw new IllegalArgumentException("ML_WINDOW_ACCEPTED_AUDIT_REQUIRED");
    }

    /** Sorted row hashes stream through the driver, bounded memory; clock is not business identity. */
    public static String fingerprint(Dataset<Row> data) throws Exception {
        var names = Arrays.stream(data.columns()).filter(n -> !"created_at_utc".equals(n)).sorted().toArray(String[]::new);
        var columns = Arrays.stream(names).map(org.apache.spark.sql.functions::col).toArray(org.apache.spark.sql.Column[]::new);
        var hashes = data.select(org.apache.spark.sql.functions.sha2(org.apache.spark.sql.functions.to_json(org.apache.spark.sql.functions.struct(columns)), 256).alias("hash"));
        var digest = MessageDigest.getInstance("SHA-256");
        var rows = hashes.orderBy("hash").toLocalIterator();
        while (rows.hasNext()) digest.update((rows.next().getString(0) + "\n").getBytes(StandardCharsets.UTF_8));
        return HexFormat.of().formatHex(digest.digest());
    }

    public static MainshockWindowResult run(
            Dataset<Row> auditedEvents,
            MainshockSelectionConfig selectionConfig,
            WindowModelConfig windowConfig,
            double mcValue,
            String datasetId,
            Instant createdAtUtc,
            String outputDir
    ) throws IOException {
        return run(auditedEvents,selectionConfig,windowConfig,mcValue,datasetId,createdAtUtc,outputDir,WindowCoveragePolicy.LEGACY);
    }
    public static MainshockWindowResult run(Dataset<Row> auditedEvents,MainshockSelectionConfig selectionConfig,
            WindowModelConfig windowConfig,double mcValue,String datasetId,Instant createdAtUtc,String outputDir,String coveragePolicy) throws IOException {
        File out = outputDir == null || outputDir.isBlank() ? null : new File(outputDir);
        if (out != null) {
            Files.createDirectories(out.toPath());
            // Invalidate before computation too: a validation failure on rerun is not success.
            Files.deleteIfExists(new File(out, "window_audit_report.json").toPath());
            Files.deleteIfExists(new File(out, "window_audit_summary.md").toPath());
        }
        MainshockWindowEngine engine = new MainshockWindowEngine();
        MainshockWindowResult result = engine.execute(
                auditedEvents, selectionConfig, windowConfig, mcValue, datasetId, createdAtUtc,coveragePolicy
        );

        // Verify all invariants programmatically
        result.assertInvariants();

        if (out != null) {
            File jsonReport = new File(out, "window_audit_report.json");
            File mdSummary = new File(out, "window_audit_summary.md");
            // A stale success report must not survive a failed rerun. Snapshots are not a
            // multi-table atomic publication: consumers wait for the completed audit report.
            if (result.rejectedWindowCount() > 0) {
                throw new IllegalStateException("ML_RESOURCE_LIMIT_EXCEEDED: rejected windows cannot be exported");
            }
            File mainshockParquet = new File(out, "mainshock_candidate_snapshot.parquet");
            result.mainshockSnapshot().write().mode(SaveMode.Overwrite).parquet(mainshockParquet.getAbsolutePath());
            File candidateParquet = new File(out, "sequence_candidate_snapshot.parquet");
            result.candidateSnapshot().write().mode(SaveMode.Overwrite).parquet(candidateParquet.getAbsolutePath());
            // Propagate any storage/configuration failure; no implicit JSON fallback.
            if (auditedEvents.sparkSession().read().parquet(mainshockParquet.getAbsolutePath()).count() != result.mainshockCount()
                    || auditedEvents.sparkSession().read().parquet(candidateParquet.getAbsolutePath()).count() != result.totalCandidateRowCount()) {
                throw new IOException("Parquet readback count mismatch");
            }
            Files.writeString(mdSummary.toPath(), result.toMarkdownSummary(), StandardCharsets.UTF_8);
            Files.writeString(jsonReport.toPath(), result.toJsonReport(), StandardCharsets.UTF_8);
        }

        return result;
    }
}
