package ie212.earthquake.spark.ml;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.GoldCommitJob;
import ie212.earthquake.spark.gold.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Objects;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Entry point for running Gold Input Audit and Completeness Evaluation.
 */
public final class GoldInputAuditJob {

    /** Exact persisted dataset ID; never takes a fixture snapshot or latest alias from the CLI. */
    public static void main(String[] args) throws Exception {
        var json = new ObjectMapper();
        if (args.length != 1 || Files.size(Path.of(args[0])) > 65536) throw new IllegalArgumentException("ML_EXACT_AUDIT_REQUEST_REQUIRED");
        var request = json.readTree(Files.readAllBytes(Path.of(args[0])));
        String namespace = request.path("ml_namespace").asText(), id = request.path("dataset_id").asText();
        String run = request.path("run_id").asText(), dag = request.path("dag_id").asText();
        if (!namespace.matches("[a-z][a-z0-9_]*\\.[a-z][a-z0-9_]*") || !id.matches("ds_[a-f0-9]{64}")
                || !run.matches("[A-Za-z0-9][A-Za-z0-9_-]{0,127}") || !dag.matches("[A-Za-z0-9][A-Za-z0-9_-]{0,127}"))
            throw new IllegalArgumentException("ML_EXACT_DATASET_AND_RUN_REQUIRED");
        Path owner = Path.of(Objects.requireNonNull(System.getenv("SOURCE_GUARD_ROOT")), "owner.json");
        Runnable lease = () -> { try {
            var current = json.readTree(Files.readAllBytes(owner));
            if (!run.equals(current.path("run_id").asText()) || !dag.equals(current.path("dag_id").asText())) throw new IllegalStateException();
        } catch (Exception e) { throw new IllegalStateException("ML_AUDIT_LEASE_NOT_HELD"); } };
        String host = System.getenv("TRINO_HOST"), port = System.getenv("TRINO_INTERNAL_PORT");
        if (host == null || !host.matches("[a-z0-9][a-z0-9.-]*") || port == null || !port.matches("[0-9]{1,5}"))
            throw new IllegalArgumentException("ML_TRINO_CONFIG_REQUIRED");
        var sql = new TrinoSqlClient(URI.create("http://" + host + ":" + port)); lease.run();
        JsonNode manifest = PinnedAuditResolver.resolve(namespace, id, run, sql, lease);
        SparkSession spark = GoldCommitJob.configured(System.getenv()).appName("mld-02-pinned-audit").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        try {
            lease.run();
            var auditConfig = GoldAuditConfig.fromManifest(manifest, run, "audit-report.json");
            var result = runPinnedAudit(spark, manifest, auditConfig, Instant.now());
            Path root = Path.of(args[0]).toAbsolutePath().getParent();
            ObjectNode report = result.toJsonReport(); report.put("dataset_id", id); report.put("input_mode", "PUBLISHED_SNAPSHOT");
            report.put("candidate_build_allowed", Double.isFinite(result.mcApplied()));
            Files.write(root.resolve("audit-report.json"), json.writeValueAsBytes(report));
            Files.writeString(root.resolve("audit-report.md"), result.toMarkdownReport());
            // Output manifest is a handoff; lifecycle remains BUILDING until feature gates pass.
            Files.write(root.resolve("audited-manifest.json"), json.writeValueAsBytes(result.enrichManifest((ObjectNode)manifest.deepCopy())));
            System.out.println("task=MLD-02 dataset_id=" + id + " input=" + result.inputEventCount() + " eligible="
                + result.eligibleEventCount() + " excluded=" + result.excludedEventCount() + " candidate_build_allowed=" + Double.isFinite(result.mcApplied()));
        } finally { spark.stop(); }
    }

    public static GoldAuditResult runPinnedAudit(SparkSession spark, JsonNode manifest, GoldAuditConfig config, Instant clock) {
        if (config.goldSnapshotId() != manifest.path("gold_snapshot_id").asLong()
                || !config.goldPublicationId().equals(manifest.path("gold_publication_id").asText()))
            throw new IllegalArgumentException("ML_AUDIT_PIN_CONFLICT");
        DatasetManifestWriter.pinnedGoldSql(manifest);
        Dataset<Row> events = spark.read().option("snapshot-id", Long.toString(config.goldSnapshotId()))
            .table(manifest.path("physical_gold_table_name").asText());
        return runAudit(spark, events, config, clock);
    }

    public static GoldAuditResult runAudit(
            SparkSession spark,
            Dataset<Row> goldEvents,
            GoldAuditConfig config,
            Instant auditedAtUtc
    ) {
        Objects.requireNonNull(spark, "spark");
        Objects.requireNonNull(goldEvents, "goldEvents");
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(auditedAtUtc, "auditedAtUtc");

        GoldInputAuditEngine engine = new GoldInputAuditEngine();
        return engine.audit(goldEvents, config, auditedAtUtc);
    }

    public static void writeReports(GoldAuditResult result, Path reportJsonPath, Path reportMdPath) {
        try {
            if (reportJsonPath != null) {
                if (reportJsonPath.getParent() != null) {
                    Files.createDirectories(reportJsonPath.getParent());
                }
                Files.writeString(reportJsonPath, result.toJsonReport().toPrettyString());
            }
            if (reportMdPath != null) {
                if (reportMdPath.getParent() != null) {
                    Files.createDirectories(reportMdPath.getParent());
                }
                Files.writeString(reportMdPath, result.toMarkdownReport());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to write audit reports", e);
        }
    }
}
