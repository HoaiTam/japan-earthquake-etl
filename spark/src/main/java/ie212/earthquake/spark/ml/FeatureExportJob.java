package ie212.earthquake.spark.ml;

import static ie212.earthquake.spark.ml.FeatureExportBundle.*;
import static ie212.earthquake.spark.gold.GoldVerificationPlan.quote;
import static org.apache.spark.sql.functions.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.GoldCommitJob;
import ie212.earthquake.spark.gold.*;
import ie212.earthquake.spark.silver.*;
import java.net.URI;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.*;

/** MLD-05 exact persisted dataset -> validated snapshots -> portable export. Caller owns whole-run lease. */
public final class FeatureExportJob {
    private FeatureExportJob() { }
    static JsonNode manifest(GoldPublisher.Sql sql, String ns, String id) throws Exception {
        var rows = DatasetManifestWriter.records(sql.execute("SELECT * FROM " + ns + ".dataset_manifest WHERE dataset_id=" + quote(id)));
        require(rows.size() == 1 && Set.of("BUILDING", "VALIDATED", "EXPORTED").contains(rows.get(0).path("dataset_status").asText()), "DS_EXPORT_ACTIVE_MANIFEST_REQUIRED");
        return rows.get(0);
    }
    static long scalar(GoldPublisher.Sql sql, String query) throws Exception {
        var r = sql.execute(query); require(r.rows().size() == 1 && r.rows().get(0).size() == 1
            && r.rows().get(0).get(0).isIntegralNumber(), "DS_EXPORT_SQL_SHAPE"); return r.rows().get(0).get(0).asLong();
    }
    static void verifyPin(GoldPublisher.Sql sql, String table, long pin, String id, String identity, long expected) throws Exception {
        String ns = table.substring(0, table.lastIndexOf('.')), name = table.substring(table.lastIndexOf('.') + 1);
        require(scalar(sql, "SELECT count(*) FROM " + table + " FOR VERSION AS OF " + pin + " WHERE dataset_id=" + quote(id)) == expected
            && scalar(sql, "SELECT count(*) FROM " + ns + ".\"" + name + "$snapshots\" WHERE snapshot_id=" + pin
                + " AND summary['ml.dataset_id']=" + quote(id) + " AND summary['ml.export_identity_sha256']=" + quote(identity)) == 1,
            "DS_EXPORT_TRINO_PIN_READBACK_FAILED");
    }
    static void update(GoldPublisher.Sql sql, String ns, String id, String from, Map<String,JsonNode> values, Runnable lease) throws Exception {
        var assignments = new ArrayList<String>();
        for (var e : values.entrySet()) {
            String key = e.getKey(); require(key.matches("[a-z][a-z0-9_]*"), "DS_EXPORT_SAFE_FIELD_REQUIRED");
            JsonNode v = e.getValue(); String literal = v.isNull() ? "NULL" : v.isNumber() ? v.asText() : quote(v.asText());
            if (key.endsWith("_utc") && !v.isNull()) literal = "from_iso8601_timestamp(" + quote(v.asText()) + ")";
            assignments.add(key + "=" + literal);
        }
        lease.run(); sql.execute("UPDATE " + ns + ".dataset_manifest SET " + String.join(",", assignments)
            + " WHERE dataset_id=" + quote(id) + " AND dataset_status=" + quote(from));
        JsonNode after = manifest(sql, ns, id);
        for (var e : values.entrySet()) require(e.getValue().toString().equals(after.path(e.getKey()).toString()), "DS_EXPORT_LIFECYCLE_READBACK_FAILED:" + e.getKey());
    }
    public static ObjectNode export(SparkSession spark, GoldPublisher.Sql sql, SilverObjectStore store,
            String ns, String id, String run, Path output, Path registry, Runnable lease) throws Exception {
        require(ns.matches("[a-z][a-z0-9_]*\\.[a-z][a-z0-9_]*") && id.matches("ds_[a-f0-9]{64}")
            && run.matches("[A-Za-z0-9][A-Za-z0-9_-]{0,127}"), "DS_EXPORT_EXACT_REQUEST_REQUIRED");
        lease.run(); JsonNode original = manifest(sql, ns, id); DatasetManifestWriter.pinnedGoldSql(original);
        String physical = original.path("physical_gold_table_name").asText(); String gold = physical.substring(0, physical.lastIndexOf('.'));
        require(!ns.equals(gold) && ns.split("\\.")[0].equals(gold.split("\\.")[0]), "DS_EXPORT_NAMESPACE_CONFLICT");
        var pubs = DatasetManifestWriter.records(sql.execute("SELECT * FROM " + gold + ".publication_status WHERE publication_id=" + quote(original.path("gold_publication_id").asText())));
        require(pubs.size() == 1 && "PASSED".equals(pubs.get(0).path("verify_status").asText())
            && original.path("gold_snapshot_id").equals(pubs.get(0).path("snapshot_id")), "DS_GOLD_NOT_PUBLISHED");
        var coverage = FeatureExportCoverage.resolve(store, pubs.get(0), original); // No lifecycle/data writes before coverage gate.
        var filter = JSON.readTree(original.path("filter_config_json").asText()); var scaling = FeatureScalingConfig.fromJson(filter.path("feature_scaling_config"));
        var config = JSON.createObjectNode();
        for (String key : List.of("dataset_split", "period_start_utc", "period_end_utc_exclusive", "observation_cutoff_utc")) config.set(key, original.path(key));
        DatasetIdentityPlan.VERSIONS.forEach(key -> config.set(key, original.path(key))); config.set("filter_config", filter);
        var resolved = new DatasetManifestWriter(sql, lease).build(gold, original.path("gold_publication_id").asText(), ns, config, run, Instant.now());
        require(resolved.path("dataset_id").equals(original.path("dataset_id")), "DS_IDENTITY_CONFLICT");
        require(!Instant.parse(original.path("observation_cutoff_utc").asText()).isBefore(Instant.parse(pubs.get(0).path("published_at_utc").asText())), "ML_HISTORICAL_CUTOFF_UNPROVEN");
        Path root = safeRoot(output), trustedRoot = safeRoot(registry), target = root.resolve("dataset_id=" + id);
        if (!"EXPORTED".equals(original.path("dataset_status").asText())) {
            lease.run(); var audit = GoldInputAuditJob.runPinnedAudit(spark, original, GoldAuditConfig.fromManifest(original, run, "audit-report.json"), Instant.now());
            audit.assertReconciled(); require(audit.completenessResult().isReliable() && Double.isFinite(audit.mcApplied()), "DS_EXPORT_MC_UNRELIABLE");
            var windows = new MainshockWindowEngine().execute(audit.auditedEvents(), MainshockWindowJob.selectionConfig(original),
                MainshockWindowJob.windowConfig(original), audit.mcApplied(), id, Instant.parse(original.path("created_at_utc").asText()));
            windows.assertInvariants();
            var main = windows.mainshockSnapshot().cache();
            var features = new SpacetimeFeatureEngine().execute(main, windows.candidateSnapshot(), scaling); features.assertValid();
            var selected = columns(features.features()).cache();
            try {
                var quality = validate(selected, main, original, scaling);
                // Windows clipped by the dataset period are not silently declared complete.
                Instant start = Instant.parse(original.path("period_start_utc").asText()), end = Instant.parse(original.path("period_end_utc_exclusive").asText());
                for (var row : main.select("event_time_utc", "pre_window_hours", "post_window_hours").collectAsList()) {
                    Instant t = row.getTimestamp(0).toInstant();
                    require(!t.minusNanos(Math.round(row.getDouble(1) * 3_600_000_000.0) * 1000).isBefore(start)
                        && t.plusNanos(Math.round(row.getDouble(2) * 3_600_000_000.0) * 1000).isBefore(end), "DS_EXPORT_WINDOW_COVERAGE_INCOMPLETE");
                }
                String identity = digest(JSON.createObjectNode().put("dataset_id", id).put("identity_payload_sha256", original.path("identity_payload_sha256").asText())
                    .put("scaling_sha256", digest(filter.path("feature_scaling_config"))).put("coverage_sha256", digest(coverage))
                    .put("feature_content_sha256", quality.path("feature_content_sha256").asText()).put("feature_schema_sha256", quality.path("feature_schema_sha256").asText()));
                long mainPin = FeatureSnapshotWriter.commit(main, ns + ".mainshock_candidate_snapshot", id, identity, lease);
                long candidatePin = FeatureSnapshotWriter.commit(selected, ns + ".sequence_candidate_snapshot", id, identity, lease);
                verifyPin(sql, ns + ".mainshock_candidate_snapshot", mainPin, id, identity, main.count());
                verifyPin(sql, ns + ".sequence_candidate_snapshot", candidatePin, id, identity, selected.count());
                for (var entry : Map.of("mainshock_snapshot_id", "bigint", "candidate_snapshot_id", "bigint", "export_identity_sha256", "varchar").entrySet()) {
                    lease.run(); sql.execute("ALTER TABLE " + ns + ".dataset_manifest ADD COLUMN IF NOT EXISTS " + entry.getKey() + " " + entry.getValue());
                }
                if ("BUILDING".equals(original.path("dataset_status").asText())) {
                    var enriched = audit.enrichManifest((ObjectNode)original.deepCopy());
                    var changes = new LinkedHashMap<String,JsonNode>();
                    for (String key : List.of("input_event_count", "eligible_event_count", "excluded_event_count", "exclusion_counts_json", "mc_method", "mc_value", "mc_region_version", "mc_config_json", "mc_config_sha256")) changes.put(key, enriched.path(key));
                    var additions = JSON.createObjectNode().put("mainshock_count", main.count()).put("candidate_row_count", selected.count()).put("feature_row_count", selected.count())
                        .put("feature_schema_sha256", quality.path("feature_schema_sha256").asText()).put("mainshock_snapshot_id", mainPin).put("candidate_snapshot_id", candidatePin)
                        .put("export_identity_sha256", identity).put("dataset_status", "VALIDATED").put("updated_at_utc", Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS).toString());
                    additions.fields().forEachRemaining(e -> changes.put(e.getKey(), e.getValue())); update(sql, ns, id, "BUILDING", changes, lease);
                }
                var validated = (ObjectNode)manifest(sql, ns, id).deepCopy();
                require(identity.equals(validated.path("export_identity_sha256").asText()) && validated.path("candidate_snapshot_id").asLong() == candidatePin
                    && validated.path("mainshock_snapshot_id").asLong() == mainPin, "DS_EXPORT_VALIDATED_PIN_CONFLICT");
                validated.set("feature_schema", quality.path("feature_schema")); validated.set("vector_columns", JSON.valueToTree(SpacetimeFeatureEngine.VECTOR));
                validated.set("coverage", coverage); validated.set("scaling_config", filter.path("feature_scaling_config"));
                validated.set("feature_content_sha256", quality.path("feature_content_sha256"));
                validated.put("export_artifact_uri", "ml-export/dataset_id=" + id + "/");
                var candidates = validated.putArray("candidates");
                var pinned = FeatureSnapshotWriter.read(spark, ns + ".sequence_candidate_snapshot", candidatePin, id);
                for (var row : pinned.select("mainshock_event_id", "candidate_event_id", "relative_time_role").orderBy("mainshock_event_id", "candidate_event_id").collectAsList())
                    candidates.addObject().put("mainshock_event_id", row.getString(0)).put("candidate_event_id", row.getString(1)).put("role", row.getString(2));
                // Import handoff uses decimal strings for exact long pins; no JSON floating-point round trips.
                validated.put("mainshock_snapshot_id", Long.toString(mainPin)).put("candidate_snapshot_id", Long.toString(candidatePin));
                var bundle = materialize(pinned, validated, root); lease.run();
                var changes = new LinkedHashMap<String,JsonNode>();
                for (String key : List.of("export_artifact_uri", "export_manifest_sha256")) changes.put(key, bundle.path(key));
                changes.put("dataset_status", JSON.getNodeFactory().textNode("EXPORTED"));
                changes.put("updated_at_utc", JSON.getNodeFactory().textNode(Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS).toString()));
                update(sql, ns, id, "VALIDATED", changes, lease);
            } finally { main.unpersist(); selected.unpersist(); }
        }
        lease.run(); var bundle = verify(spark, target); var current = manifest(sql, ns, id);
        for (String key : List.of("dataset_id", "identity_payload_sha256", "gold_snapshot_id", "gold_publication_id", "filter_config_json", "feature_version", "scaling_config_version"))
            require(bundle.path(key).toString().equals(current.path(key).toString()), "DS_EXPORT_PUBLISHED_BUNDLE_CONFLICT");
        for (String key : List.of("export_identity_sha256", "export_manifest_sha256", "feature_schema_sha256", "feature_row_count"))
            require(bundle.path(key).toString().equals(current.path(key).toString()), "DS_EXPORT_PUBLISHED_BUNDLE_CONFLICT");
        require("EXPORTED".equals(current.path("dataset_status").asText()), "DS_EXPORT_NOT_PUBLISHED");
        long mainPin = Long.parseLong(bundle.path("mainshock_snapshot_id").asText()), candidatePin = Long.parseLong(bundle.path("candidate_snapshot_id").asText());
        require(mainPin == current.path("mainshock_snapshot_id").asLong() && candidatePin == current.path("candidate_snapshot_id").asLong(), "DS_EXPORT_PUBLISHED_BUNDLE_CONFLICT");
        verifyPin(sql, ns + ".mainshock_candidate_snapshot", mainPin, id, current.path("export_identity_sha256").asText(), current.path("mainshock_count").asLong());
        verifyPin(sql, ns + ".sequence_candidate_snapshot", candidatePin, id, current.path("export_identity_sha256").asText(), current.path("feature_row_count").asLong());
        var exactFeatures = columns(FeatureSnapshotWriter.read(spark, ns + ".sequence_candidate_snapshot", candidatePin, id));
        require(MainshockWindowJob.fingerprint(exactFeatures).equals(bundle.path("feature_content_sha256").asText())
            && digest(LocalFeatureJob.schema(exactFeatures)).equals(current.path("feature_schema_sha256").asText()), "DS_EXPORT_PINNED_CONTENT_CONFLICT");
        var expectedCandidates = JSON.createArrayNode();
        for (var row : exactFeatures.select("mainshock_event_id", "candidate_event_id", "relative_time_role").orderBy("mainshock_event_id", "candidate_event_id").collectAsList())
            expectedCandidates.addObject().put("mainshock_event_id", row.getString(0)).put("candidate_event_id", row.getString(1)).put("role", row.getString(2));
        require(expectedCandidates.equals(bundle.path("candidates")), "DS_EXPORT_CANDIDATE_LINEAGE_CONFLICT");
        var table = Spark3Util.loadIcebergTable(spark, ns + ".dataset_manifest"); table.refresh(); long registryPin = 0;
        var snapshots = new ArrayList<org.apache.iceberg.Snapshot>(); table.snapshots().forEach(snapshots::add); snapshots.sort(Comparator.comparingLong(org.apache.iceberg.Snapshot::timestampMillis));
        for (var s : snapshots) if (FeatureSnapshotWriter.read(spark, ns + ".dataset_manifest", s.snapshotId(), id)
                .filter(col("dataset_status").equalTo("EXPORTED").and(col("export_manifest_sha256").equalTo(current.path("export_manifest_sha256").asText()))).count() == 1) { registryPin = s.snapshotId(); break; }
        require(registryPin > 0 && scalar(sql, "SELECT count(*) FROM " + ns + ".dataset_manifest FOR VERSION AS OF " + registryPin
            + " WHERE dataset_id=" + quote(id) + " AND dataset_status='EXPORTED'") == 1, "DS_EXPORT_REGISTRY_PIN_REQUIRED");
        var trusted = bundle.deepCopy().put("registry_snapshot_id", Long.toString(registryPin));
        Path indexDir = safeRoot(trustedRoot.resolve("exports").resolve(id));
        immutable(indexDir.resolve("export-checksums.sha256"), Files.readAllBytes(target.resolve("export-checksums.sha256")));
        immutable(trustedRoot.resolve(id + ".json"), bytes(trusted));
        return JSON.createObjectNode().put("task", "MLD-05").put("status", "DATASET_EXPORTED").put("dataset_id", id)
            .put("feature_row_count", current.path("feature_row_count").asLong()).put("mainshock_count", current.path("mainshock_count").asLong())
            .put("registry_snapshot_id", Long.toString(registryPin)).put("candidate_snapshot_id", Long.toString(candidatePin)).put("mainshock_snapshot_id", Long.toString(mainPin))
            .put("dataset_manifest_ref", id + ".json").put("dataset_manifest_sha256", WindowArtifactInput.sha(trustedRoot.resolve(id + ".json")))
            .put("bundle_manifest_sha256", WindowArtifactInput.sha(target.resolve("dataset_manifest.json")))
            .put("checksums_sha256", WindowArtifactInput.sha(target.resolve("checksums.sha256"))).put("reused", "EXPORTED".equals(original.path("dataset_status").asText()));
    }
    public static void main(String[] args) throws Exception {
        require(args.length == 1 && Files.size(Path.of(args[0])) <= 65536, "DS_EXPORT_REQUEST_REQUIRED");
        Path root = Path.of(args[0]).toAbsolutePath().normalize().getParent(); var request = JSON.readTree(Files.readAllBytes(Path.of(args[0])));
        String run = request.path("run_id").asText(), dag = request.path("dag_id").asText();
        require(run.matches("[A-Za-z0-9][A-Za-z0-9_-]{0,127}") && dag.matches("[A-Za-z0-9][A-Za-z0-9_-]{0,127}"), "DS_EXPORT_OWNER_REQUIRED");
        Runnable lease = () -> { try { var owner = JSON.readTree(Files.readAllBytes(Path.of(System.getenv("SOURCE_GUARD_ROOT"), "owner.json")));
            require(run.equals(owner.path("run_id").asText()) && dag.equals(owner.path("dag_id").asText()), "DS_EXPORT_LEASE_NOT_HELD");
        } catch (Exception e) { throw new IllegalStateException("DS_EXPORT_LEASE_NOT_HELD"); } };
        lease.run(); var spark = GoldCommitJob.configured(System.getenv()).appName("mld05-feature-export").getOrCreate(); spark.sparkContext().setLogLevel("ERROR");
        try (var store = MinioSilverObjectStore.fromEnvironment()) {
            var sql = new TrinoSqlClient(URI.create("http://" + System.getenv("TRINO_HOST") + ":" + System.getenv("TRINO_INTERNAL_PORT")));
            var report = export(spark, sql, store, request.path("ml_namespace").asText(), request.path("dataset_id").asText(), run,
                Path.of(System.getenv().getOrDefault("MLD05_EXPORT_ROOT", "/opt/pipeline/staging/ml-export")),
                Path.of(System.getenv().getOrDefault("MLI_DATASET_REGISTRY_ROOT", "/opt/pipeline/staging/ml-artifacts")), lease);
            immutable(root.resolve("export-report.json"), bytes(report)); System.out.println(report);
        } finally { spark.stop(); }
    }
}
