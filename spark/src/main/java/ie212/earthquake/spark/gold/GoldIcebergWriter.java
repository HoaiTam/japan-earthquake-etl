package ie212.earthquake.spark.gold;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.silver.SilverBundlePublisher;
import ie212.earthquake.spark.silver.SilverObjectStore;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.util.*;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;

/** Snapshot-bundle writer. Caller holds the shared whole-run lease until completion/recovery. */
public final class GoldIcebergWriter {
    public static final List<String> TABLES = List.of("event_current", "event_source_bridge", "dim_date",
            "dim_region", "dim_magnitude_band", "dim_depth_band");
    private static final ObjectMapper JSON = new ObjectMapper();
    private final SparkSession spark;
    private final String namespace;
    private final SilverObjectStore journal;
    private final Runnable assertLease;

    public GoldIcebergWriter(SparkSession spark, String namespace, SilverObjectStore journal, Runnable assertLease) {
        if (namespace == null || !namespace.matches("[a-z][a-z0-9_]*\\.[a-z][a-z0-9_]*"))
            throw new IllegalArgumentException("SAFE_GOLD_NAMESPACE_REQUIRED");
        this.spark = Objects.requireNonNull(spark); this.namespace = namespace;
        this.journal = Objects.requireNonNull(journal); this.assertLease = Objects.requireNonNull(assertLease);
        if (!"UTC".equals(spark.conf().get("spark.sql.session.timeZone")))
            throw new IllegalArgumentException("UTC_SESSION_REQUIRED");
    }

    /** May resume only this operation. Never replay a committed table or silently change its baseline. */
    public JsonNode commit(GoldTransformationResult gold, GoldCommitScope scope) throws Exception {
        return commit(gold, scope, ignored -> { });
    }

    // Failure injection is limited to tests; production uses the overload above.
    JsonNode commit(GoldTransformationResult gold, GoldCommitScope scope,
            java.util.function.Consumer<String> afterTable) throws Exception {
        assertLease.run();
        if (!scope.baselineSnapshots().keySet().equals(new HashSet<>(TABLES)))
            throw new IllegalArgumentException("ALL_TABLE_BASELINES_REQUIRED");
        LinkedHashMap<String, Dataset<Row>> inputs = inputs(gold);
        inputs.replaceAll((name, data) -> data.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK()));
        try {
        Column eventScope = monthPredicate(scope.affectedMonths());
        requireEmpty(gold.eventCurrent().filter(col("event_time_utc").isNull().or(not(eventScope))), "EVENT_OUTSIDE_AFFECTED_MONTHS");
        requireEmpty(gold.eventCurrent().filter(col("quality_status").isNull()
                .or(not(col("quality_status").isin("VALID", "WARNING")))), "GOLD_QUALITY_BLOCKED");
        unique(gold.eventCurrent(), "canonical_event_id"); unique(gold.eventSourceBridge(), "source_observation_id");
        if (gold.eventCurrent().count() != gold.canonicalEventCount()
                || gold.eventSourceBridge().count() != gold.currentObservationCount()
                || gold.bridgeRowCount() != gold.currentObservationCount()
                || inputs.get("event_source_bridge").count() != gold.bridgeRowCount()) throw new IOException("GOLD_COUNTS_UNBALANCED");
        ObjectNode identity = JSON.createObjectNode();
        identity.put("commit_version", "gld-03-v1"); identity.put("namespace", namespace);
        identity.set("scope", scopeJson(scope));
        var fingerprints = identity.putObject("data");
        for (var entry : inputs.entrySet()) {
            var info = fingerprints.putObject(entry.getKey());
            info.put("schema", entry.getValue().schema().json());
            info.put("sha256", fingerprint(entry.getValue())); info.put("count", entry.getValue().count());
        }
        String identitySha = SilverBundlePublisher.sha(JSON.writeValueAsBytes(identity));
        identity.put("identity_sha256", identitySha);
        byte[] claimBytes = JSON.writeValueAsBytes(identity);
        String root = "gold-commits/" + scope.context().runId();
        String claimKey = root + "/identity.json", receiptKey = root + "/commit.json";
        if (journal.exists(claimKey)) {
            if (!Arrays.equals(journal.read(claimKey), claimBytes))
                throw new IOException("GOLD_OPERATION_CONFLICT");
        } else {
            journal.put(claimKey, claimBytes, "application/json");
            if (!Arrays.equals(journal.read(claimKey), claimBytes)) throw new IOException("CLAIM_READBACK_FAILED");
        }
        if (journal.exists(receiptKey)) return verify(JSON.readTree(journal.read(receiptKey)), scope, identitySha);

        spark.sql("CREATE NAMESPACE IF NOT EXISTS " + namespace);
        ObjectNode receipt = JSON.createObjectNode();
        receipt.put("commit_version", "gld-03-v1"); receipt.put("gold_run_id", scope.context().runId());
        receipt.put("identity_sha256", identitySha); receipt.put("namespace", namespace);
        receipt.set("scope", scopeJson(scope)); receipt.put("status", "COMMITTED");
        receipt.put("published", false); var committed = receipt.putObject("tables");
        for (var entry : inputs.entrySet()) {
            assertLease.run(); String name = entry.getKey(), tableName = namespace + "." + name;
            Dataset<Row> incoming = entry.getValue();
            if (!spark.catalog().tableExists(tableName)) {
                if (scope.baselineSnapshots().get(name) != 0) throw new IOException("MISSING_BASELINE_TABLE");
                String columns = Arrays.stream(incoming.schema().fields()).map(field -> {
                    if (!field.name().matches("[a-z][a-z0-9_]*")) throw new IllegalArgumentException("SAFE_COLUMN_REQUIRED");
                    return "`"+field.name()+"` "+field.dataType().sql();
                }).collect(java.util.stream.Collectors.joining(", "));
                String partition = name.equals("event_current") ? " PARTITIONED BY (months(event_time_utc))"
                    : name.equals("event_source_bridge") ? " PARTITIONED BY (event_month_utc)" : "";
                spark.sql("CREATE TABLE "+tableName+" ("+columns+") USING iceberg"+partition+
                    " TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet')");
            }
            Table table = Spark3Util.loadIcebergTable(spark, tableName); table.refresh();
            Snapshot done = findOperation(table, scope.context().runId(), identitySha);
            long current = table.currentSnapshot() == null ? 0 : table.currentSnapshot().snapshotId();
            if (done == null) {
                if (current != scope.baselineSnapshots().get(name)) throw new IOException("GOLD_BASELINE_CHANGED");
                Dataset<Row> previous = current == 0 ? incoming.limit(0) :
                        spark.read().format("iceberg").option("snapshot-id", Long.toString(current)).load(tableName);
                Column predicate;
                Dataset<Row> replacement;
                if (name.equals("event_current")) { predicate = eventScope; replacement = incoming; }
                else if (name.equals("event_source_bridge")) {
                    predicate = col("event_month_utc").isin(scope.affectedMonths().toArray()); replacement = incoming;
                } else {
                    String key = dimensionKey(name); unique(incoming, key);
                    // Fixed dimension members are immutable. Dates/regions may be appended, never silently relabeled.
                    Dataset<Row> overlap = previous.join(incoming.select(key), key, "inner");
                    if (overlap.except(incoming).limit(1).count() != 0) throw new IOException("DIMENSION_VALUE_CONFLICT");
                    replacement = previous.join(incoming.select(key), key, "left_anti").unionByName(incoming);
                    predicate = lit(true);
                }
                long expected = previous.filter(not(predicate)).count() + replacement.count();
                String contentSha = fingerprint(previous.filter(not(predicate)).unionByName(replacement));
                // Iceberg rejects files partly outside an overwrite filter; month-aligned predicates prevent broad overwrite.
                replacement.writeTo(tableName).option("snapshot-property.gold.operation_id", scope.context().runId())
                        .option("snapshot-property.gold.identity_sha256", identitySha)
                        .option("snapshot-property.gold.baseline_snapshot", Long.toString(current))
                        .option("snapshot-property.gold.expected_count", Long.toString(expected))
                        .option("snapshot-property.gold.content_sha256", contentSha).overwrite(predicate);
                table.refresh(); done = findOperation(table, scope.context().runId(), identitySha);
                if (done == null) throw new IOException("COMMIT_OUTCOME_UNKNOWN");
                afterTable.accept(name);
            }
            var tableReceipt = committed.putObject(name);
            tableReceipt.put("table", tableName); tableReceipt.put("snapshot_id", done.snapshotId());
            tableReceipt.put("count", Long.parseLong(done.summary().get("gold.expected_count")));
            tableReceipt.put("committed_at_utc", java.time.Instant.ofEpochMilli(done.timestampMillis()).toString());
            tableReceipt.put("schema_json", incoming.schema().json());
        }
        verify(receipt, scope, identitySha); assertLease.run();
        byte[] receiptBytes = JSON.writeValueAsBytes(receipt);
        journal.put(receiptKey, receiptBytes, "application/json");
        byte[] readbackBytes = journal.read(receiptKey);
        if (!Arrays.equals(readbackBytes, receiptBytes)) throw new IOException("COMMIT_RECEIPT_READBACK_FAILED");
        JsonNode readback = JSON.readTree(readbackBytes);
        return readback;
        } finally { inputs.values().forEach(Dataset::unpersist); }
    }

    private JsonNode verify(JsonNode receipt, GoldCommitScope scope, String sha) throws Exception {
        if (!receipt.path("identity_sha256").asText().equals(sha) || receipt.path("published").asBoolean(true)
                || !"COMMITTED".equals(receipt.path("status").asText())
                || !receipt.path("namespace").asText().equals(namespace)
                || receipt.path("tables").size() != TABLES.size()) throw new IOException("INVALID_COMMIT_RECEIPT");
        for (String name : TABLES) {
            JsonNode pin = receipt.path("tables").path(name); String tableName = namespace + "." + name;
            Table table = Spark3Util.loadIcebergTable(spark, tableName); table.refresh();
            Snapshot snapshot = table.snapshot(pin.path("snapshot_id").asLong());
            if (snapshot == null || !scope.context().runId().equals(snapshot.summary().get("gold.operation_id"))
                    || !sha.equals(snapshot.summary().get("gold.identity_sha256"))
                    || !tableName.equals(pin.path("table").asText())) throw new IOException("SNAPSHOT_IDENTITY_MISMATCH");
            Dataset<Row> data = spark.read().format("iceberg").option("snapshot-id", Long.toString(snapshot.snapshotId())).load(tableName);
            if (data.count() != pin.path("count").asLong(-1)) throw new IOException("SNAPSHOT_READBACK_COUNT_MISMATCH");
            String key = name.equals("event_current") ? "canonical_event_id" : name.equals("event_source_bridge")
                    ? "source_observation_id" : dimensionKey(name);
            unique(data, key);
            if (!fingerprint(data).equals(snapshot.summary().get("gold.content_sha256")))
                throw new IOException("SNAPSHOT_CONTENT_READBACK_MISMATCH");
        }
        return receipt;
    }

    private LinkedHashMap<String, Dataset<Row>> inputs(GoldTransformationResult result) {
        var out = new LinkedHashMap<String, Dataset<Row>>();
        Dataset<Row> events = result.eventCurrent();
        if (Arrays.asList(events.columns()).contains("event_time_jst"))
            events = events.withColumn("event_time_jst", col("event_time_jst").cast("timestamp_ntz"));
        out.put("event_current", events);
        out.put("event_source_bridge", result.eventSourceBridge().join(result.eventCurrent()
                .select(col("canonical_event_id"), date_format(col("event_time_utc"), "yyyy-MM").alias("event_month_utc")),
                "canonical_event_id"));
        out.put("dim_date", result.dimDate()); out.put("dim_region", result.dimRegion());
        out.put("dim_magnitude_band", result.dimMagnitudeBand()); out.put("dim_depth_band", result.dimDepthBand()); return out;
    }
    private static Snapshot findOperation(Table table, String run, String sha) throws IOException {
        Snapshot found = null;
        for (Snapshot snapshot : table.snapshots()) if (run.equals(snapshot.summary().get("gold.operation_id"))) {
            if (!sha.equals(snapshot.summary().get("gold.identity_sha256")) || found != null)
                throw new IOException("AMBIGUOUS_GOLD_OPERATION");
            found = snapshot;
        }
        return found;
    }
    public Map<String, Long> baselines() throws Exception {
        var out = new TreeMap<String, Long>();
        for (String name : TABLES) {
            if (!spark.catalog().tableExists(namespace + "." + name)) out.put(name, 0L);
            else { Table table = Spark3Util.loadIcebergTable(spark, namespace + "." + name); table.refresh();
                out.put(name, table.currentSnapshot() == null ? 0L : table.currentSnapshot().snapshotId()); }
        }
        return out;
    }
    public static Column monthPredicate(List<String> months) {
        Column result = lit(false);
        for (String month : months) {
            var start = YearMonth.parse(month).atDay(1).atStartOfDay(ZoneOffset.UTC).toInstant();
            var end = YearMonth.parse(month).plusMonths(1).atDay(1).atStartOfDay(ZoneOffset.UTC).toInstant();
            result = result.or(col("event_time_utc").geq(java.sql.Timestamp.from(start))
                    .and(col("event_time_utc").lt(java.sql.Timestamp.from(end))));
        }
        return result;
    }
    public static ObjectNode scopeJson(GoldCommitScope scope) {
        ObjectNode out = JSON.createObjectNode(); var context = out.putObject("context");
        context.put("run_id", scope.context().runId()); context.put("window_start_utc", scope.context().windowStartUtc().toString());
        context.put("window_end_utc", scope.context().windowEndUtc().toString());
        context.put("processing_date", scope.context().processingDate().toString()); context.put("is_backfill", scope.context().isBackfill());
        context.put("config_version", scope.context().configVersion());
        context.set("input_manifest_uris", JSON.valueToTree(scope.context().inputManifestUris()));
        out.put("silver_bundle_sha256", scope.silverBundleSha256()); out.put("code_version", scope.codeVersion());
        out.set("affected_months", JSON.valueToTree(scope.affectedMonths()));
        out.set("baseline_snapshots", JSON.valueToTree(new TreeMap<>(scope.baselineSnapshots()))); return out;
    }
    private static String dimensionKey(String name) {
        return name.equals("dim_date") ? "date_key" : name.equals("dim_region") ? "region_key" : "band_code";
    }
    private static String fingerprint(Dataset<Row> data) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        String[] names = data.columns().clone(); Arrays.sort(names);
        var hashes = data.select(sha2(to_json(struct(Arrays.stream(names).map(org.apache.spark.sql.functions::col)
                .toArray(Column[]::new)), Map.of("ignoreNullFields", "false")), 256).alias("hash")).orderBy("hash").toLocalIterator();
        while (hashes.hasNext()) digest.update(hashes.next().getString(0).getBytes(StandardCharsets.US_ASCII));
        return HexFormat.of().formatHex(digest.digest());
    }
    private static void unique(Dataset<Row> data, String key) throws IOException {
        requireEmpty(data.filter(col(key).isNull().or(length(trim(col(key))).equalTo(0))), "NULL_LOGICAL_KEY");
        requireEmpty(data.groupBy(key).count().filter(col("count").gt(1)), "DUPLICATE_LOGICAL_KEY");
    }
    private static void requireEmpty(Dataset<Row> data, String reason) throws IOException {
        if (data.limit(1).count() != 0) throw new IOException(reason);
    }
}
