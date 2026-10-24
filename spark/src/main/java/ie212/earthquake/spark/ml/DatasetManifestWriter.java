package ie212.earthquake.spark.ml;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.gold.GoldIcebergWriter;
import ie212.earthquake.spark.gold.GoldPublisher;
import java.io.IOException;
import java.time.Instant;
import java.util.*;
import static ie212.earthquake.spark.gold.GoldVerificationPlan.quote;

/** Exact PASSED publication -> Iceberg metadata. Caller holds the shared whole-run lease. */
public final class DatasetManifestWriter {
    private static final ObjectMapper JSON = new ObjectMapper();
    private final GoldPublisher.Sql sql;
    private final Runnable lease;
    public DatasetManifestWriter(GoldPublisher.Sql sql, Runnable lease) {
        this.sql = Objects.requireNonNull(sql); this.lease = Objects.requireNonNull(lease);
    }
    public JsonNode build(String goldNamespace, String publicationId, String mlNamespace, JsonNode config,
                          String run, Instant now) throws Exception {
        namespace(goldNamespace); namespace(mlNamespace);
        ie212.earthquake.spark.gold.OperationalGoldPolicy.researchOnly(goldNamespace);
        if (!goldNamespace.split("\\.")[0].equals(mlNamespace.split("\\.")[0]) || goldNamespace.equals(mlNamespace)
                || publicationId == null || !publicationId.matches("pub_[a-f0-9]{64}"))
            throw new IllegalArgumentException("DS_EXACT_PUBLICATION_REQUIRED");
        var publication = resolvePublication(goldNamespace, publicationId);
        var plan = new DatasetIdentityPlan(publication, config, run, now);
        var manifest = plan.manifest(); String table = mlNamespace + ".dataset_manifest";
        lease.run(); sql.execute("CREATE SCHEMA IF NOT EXISTS " + mlNamespace);
        lease.run(); sql.execute("CREATE TABLE IF NOT EXISTS " + table + " (" + ddl() + ") WITH (format='PARQUET',format_version=2)");
        var existing = records(sql.execute("SELECT * FROM " + table + " WHERE dataset_id=" + quote(manifest.path("dataset_id").asText())));
        if (existing.size() > 1) throw new IOException("DS_DUPLICATE_IDENTITY");
        if (existing.isEmpty()) {
            var columns = new ArrayList<String>(); var values = new ArrayList<String>();
            manifest.fields().forEachRemaining(field -> {
                columns.add(field.getKey()); JsonNode value = field.getValue();
                values.add(value.isIntegralNumber() ? value.asText() : field.getKey().endsWith("_utc")
                    || field.getKey().equals("period_end_utc_exclusive") ? "from_iso8601_timestamp(" + quote(value.asText()) + ")" : quote(value.asText()));
            });
            lease.run(); sql.execute("INSERT INTO " + table + " (" + String.join(",", columns) + ") VALUES (" + String.join(",", values) + ")");
            existing = records(sql.execute("SELECT * FROM " + table + " WHERE dataset_id=" + quote(manifest.path("dataset_id").asText())));
        }
        if (existing.size() != 1) throw new IOException("DS_MANIFEST_READBACK_FAILED");
        plan.assertRerun(existing.get(0));
        return existing.get(0); // Preserve original run/timestamps and advanced lifecycle on rerun.
    }

    /** Read-only publication/physical-snapshot gate, also used before a build DAG creates its manifest. */
    public ObjectNode resolvePublication(String goldNamespace, String publicationId) throws Exception {
        namespace(goldNamespace);
        ie212.earthquake.spark.gold.OperationalGoldPolicy.researchOnly(goldNamespace);
        if (publicationId == null || !publicationId.matches("pub_[a-f0-9]{64}"))
            throw new IllegalArgumentException("DS_EXACT_PUBLICATION_REQUIRED");
        lease.run();
        var rows = records(sql.execute("SELECT snapshot_id,gold_table_name,physical_gold_table_name,publication_id,"
            + "verify_status,bundle_sha256,snapshot_bundle_json FROM " + goldNamespace
            + ".publication_status WHERE publication_id=" + quote(publicationId)));
        if (rows.size() != 1) throw new IOException("DS_GOLD_NOT_PUBLISHED");
        JsonNode row = rows.get(0);
        if (!"PASSED".equals(row.path("verify_status").asText()) || !publicationId.equals(row.path("publication_id").asText())
                || !goldNamespace.concat(".earthquake_event_current").equals(row.path("gold_table_name").asText())
                || !goldNamespace.concat(".event_current").equals(row.path("physical_gold_table_name").asText())
                || !publicationId.substring(4).equals(row.path("bundle_sha256").asText()))
            throw new IOException("DS_PUBLICATION_CONFLICT");
        var bundle = JSON.readTree(row.path("snapshot_bundle_json").asText());
        var names = new HashSet<String>(); bundle.fieldNames().forEachRemaining(names::add);
        if (!names.equals(new HashSet<>(GoldIcebergWriter.TABLES))) throw new IOException("DS_INCOMPLETE_PUBLICATION_BUNDLE");
        for (String name : GoldIcebergWriter.TABLES) {
            var pin = bundle.path(name);
            if (!pin.path("snapshot_id").isIntegralNumber() || pin.path("snapshot_id").asLong() <= 0
                    || !goldNamespace.concat("." + name).equals(pin.path("table").asText()))
                throw new IOException("DS_PUBLICATION_PIN_CONFLICT");
        }
        long snapshot = row.path("snapshot_id").asLong();
        if (!row.path("snapshot_id").isIntegralNumber() || snapshot != bundle.path("event_current").path("snapshot_id").asLong())
            throw new IOException("DS_PUBLICATION_PIN_CONFLICT");
        lease.run();
        var available = sql.execute("SELECT count(*) FROM " + goldNamespace + ".\"event_current$snapshots\" WHERE snapshot_id=" + snapshot
            +ie212.earthquake.spark.gold.OperationalGoldPolicy.researchSnapshotPredicate());
        if (available.rows().size() != 1 || available.rows().get(0).get(0).asLong(-1) != 1)
            throw new IOException("DS_PINNED_SNAPSHOT_UNAVAILABLE");
        var publication = JSON.createObjectNode();
        publication.put("verify_status", "PASSED"); publication.put("gold_snapshot_id", snapshot);
        publication.set("gold_table_name", row.path("gold_table_name"));
        publication.set("physical_gold_table_name", row.path("physical_gold_table_name"));
        publication.put("gold_publication_id", publicationId); publication.set("publication_bundle_sha256", row.path("bundle_sha256"));
        return publication;
    }
    public static String pinnedGoldSql(JsonNode manifest) {
        String table = manifest.path("physical_gold_table_name").asText();
        ie212.earthquake.spark.gold.OperationalGoldPolicy.researchOnly(table);
        if (!table.matches("[a-z][a-z0-9_]*\\.[a-z][a-z0-9_]*\\.event_current")
                || manifest.path("gold_snapshot_id").asLong() <= 0) throw new IllegalArgumentException("DS_INVALID_PIN");
        return "SELECT * FROM " + table + " FOR VERSION AS OF " + manifest.path("gold_snapshot_id").asLong();
    }
    private static void namespace(String value) {
        if (value == null || !value.matches("[a-z][a-z0-9_]*\\.[a-z][a-z0-9_]*")) throw new IllegalArgumentException("DS_INVALID_NAMESPACE");
    }
    static List<JsonNode> records(GoldPublisher.Result result) throws IOException {
        var records = new ArrayList<JsonNode>();
        for (JsonNode row : result.rows()) {
            if (row.size() != result.columns().size()) throw new IOException("DS_SQL_SHAPE_MISMATCH");
            ObjectNode record = JSON.createObjectNode();
            for (int i = 0; i < row.size(); i++) {
                String name = result.columns().get(i).path("name").asText(); JsonNode value = row.get(i);
                if (value.isTextual() && (name.endsWith("_utc") || name.equals("period_end_utc_exclusive"))) {
                    String text = value.asText();
                    try { text = Instant.parse(text).toString(); }
                    catch (java.time.format.DateTimeParseException e) {
                        var format = new java.time.format.DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss")
                            .optionalStart().appendFraction(java.time.temporal.ChronoField.NANO_OF_SECOND, 0, 9, true).optionalEnd()
                            .appendLiteral(' ').appendZoneId().toFormatter();
                        text = java.time.ZonedDateTime.parse(text, format).toInstant().toString();
                    }
                    record.put(name, text);
                } else record.set(name, value);
            }
            records.add(record);
        }
        return records;
    }
    private static String ddl() {
        var fields = new LinkedHashMap<String, String>();
        for (String key : List.of("schema_version", "dataset_id", "dataset_split", "gold_table_name", "physical_gold_table_name",
                "gold_publication_id", "publication_bundle_sha256", "identity_payload_sha256", "filter_config_json", "mc_method",
                "mc_region_version", "mc_config_json", "mc_config_sha256", "exclusion_counts_json", "audit_report_uri",
                "feature_schema_sha256", "export_artifact_uri", "export_manifest_sha256", "dataset_status", "status_reason_code", "build_run_id"))
            fields.put(key, "varchar");
        DatasetIdentityPlan.VERSIONS.forEach(key -> fields.put(key, "varchar"));
        for (String key : List.of("period_start_utc", "period_end_utc_exclusive", "observation_cutoff_utc", "created_at_utc", "updated_at_utc"))
            fields.put(key, "timestamp(6) with time zone");
        for (String key : List.of("gold_snapshot_id", "input_event_count", "eligible_event_count", "excluded_event_count",
                "mainshock_count", "candidate_row_count", "feature_row_count")) fields.put(key, "bigint");
        fields.put("mc_value", "double");
        return String.join(",", fields.entrySet().stream().map(e -> e.getKey() + " " + e.getValue()).toList());
    }
}
