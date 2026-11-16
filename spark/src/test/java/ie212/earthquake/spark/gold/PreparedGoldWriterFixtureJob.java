package ie212.earthquake.spark.gold;

import java.nio.file.*;
import java.sql.Timestamp;
import java.time.*;
import java.util.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import ie212.earthquake.spark.silver.FileSilverObjectStore;
import static org.apache.spark.sql.functions.*;

/** Synthetic local-filesystem component QA; never configures REST/MinIO or certifies research coverage. */
public final class PreparedGoldWriterFixtureJob {
    @FunctionalInterface interface Action { void run() throws Exception; }
    private static void check(boolean ok, String reason) { if (!ok) throw new AssertionError(reason); }
    private static void blocked(Action action) throws Exception {
        boolean failed = false; try { action.run(); } catch (Exception expected) { failed = true; } check(failed, "EXPECTED_FAIL_CLOSED");
    }
    static GoldCommitScope scope(String run, String digest) {
        var zero = new TreeMap<String,Long>(); GoldIcebergWriter.TABLES.forEach(n -> zero.put(n, 0L));
        var ctx = new GoldRunContext(run, Instant.parse("2000-01-01T00:00:00Z"), Instant.parse("2024-01-01T00:00:00Z"),
                LocalDate.parse("2026-11-15"), true, PreparedGoldProof.VERSION, List.of("file:///synthetic/catalog.json"));
        return new GoldCommitScope(ctx, null, PreparedGoldProof.VERSION, PortableImportContract.ALL_MONTHS, zero,
                new PreparedGoldProof(PreparedGoldProof.NAMESPACE, digest, "b".repeat(64), "c".repeat(64), "d".repeat(64), "2026-10-25T00:00:00Z"));
    }
    static GoldTransformationResult data(SparkSession spark) {
        var e = spark.createDataFrame(List.of(RowFactory.create("c", Timestamp.from(Instant.parse("2023-01-01T00:00:00Z")), "VALID")),
                new StructType().add("canonical_event_id", "string").add("event_time_utc", "timestamp").add("quality_status", "string"));
        var b = spark.createDataFrame(List.of(RowFactory.create("c", "o", "2023-01")),
                new StructType().add("canonical_event_id", "string").add("source_observation_id", "string").add("event_month_utc", "string"));
        // PREPARED Parquet is alphabetically projected: DATE precedes the INT key.
        var d = spark.createDataFrame(List.of(RowFactory.create(java.sql.Date.valueOf("2023-01-01"), 20230101)),
                new StructType().add("calendar_date", "date").add("date_key", "integer"));
        var r = spark.createDataFrame(List.of(RowFactory.create("UNKNOWN")), new StructType().add("region_key", "string"));
        var band = spark.createDataFrame(List.of(RowFactory.create("UNKNOWN")), new StructType().add("band_code", "string"));
        return new GoldTransformationResult(e, e, b, d, r, band, band, 1, 1, 1);
    }
    public static int runAll(SparkSession spark, Path root) throws Exception {
        Files.createDirectories(root); int cases = 0;
        var journal = new FileSilverObjectStore(root.resolve("journal")); var input = data(spark);
        var writer = new GoldIcebergWriter(spark, PreparedGoldProof.NAMESPACE, journal, () -> {}); var scope = scope("prepared-fixture", "a".repeat(64));
        blocked(() -> writer.commit(input, scope, table -> { if (table.equals("event_source_bridge")) throw new IllegalStateException("INJECTED_PARTIAL_COMMIT"); })); cases++;
        long factPin = spark.sql("SELECT snapshot_id FROM " + PreparedGoldProof.NAMESPACE + ".event_current.snapshots").first().getLong(0);
        var first = writer.commit(input, scope); check(!first.path("published").asBoolean(true), "COMMIT_IS_NOT_PUBLISH"); cases++;
        check(first.path("tables").path("event_current").path("snapshot_id").asLong() == factPin, "PARTIAL_FACT_NOT_REWRITTEN"); cases++;
        check(first.path("scope").has("prepared_input") && !first.path("scope").has("silver_bundle_sha256"), "NO_FAKE_SILVER_PROOF"); cases++;
        var second = writer.commit(input, scope); check(first.equals(second), "IMMUTABLE_RERUN"); cases++;
        for (String table : GoldIcebergWriter.TABLES) check(spark.table(PreparedGoldProof.NAMESPACE + "." + table + ".snapshots").count() == 1, "NO_NEW_SNAPSHOT_ON_RERUN"); cases++;
        blocked(() -> writer.commit(input, scope("prepared-fixture", "f".repeat(64)))); cases++;
        var bad = new GoldTransformationResult(input.eventCurrent(), input.earthquakeEventCurrent(), input.eventSourceBridge().drop("event_month_utc"),
                input.dimDate(), input.dimRegion(), input.dimMagnitudeBand(), input.dimDepthBand(), 1, 1, 1);
        blocked(() -> writer.commit(bad, scope)); cases++;
        var guarded = new GoldIcebergWriter(spark, PreparedGoldProof.NAMESPACE, journal, () -> {throw new IllegalStateException("LEASE_LOST");});
        blocked(() -> guarded.commit(input, scope)); cases++;
        var legacy = new GoldIcebergWriter(spark, "iceberg.legacy_fixture", new FileSilverObjectStore(root.resolve("legacy-journal")), () -> {});
        var old = new GoldCommitScope(scope.context(), "a".repeat(64), "legacy-v1", List.of("2023-01"), scope.baselineSnapshots());
        var legacyInput = new GoldTransformationResult(input.eventCurrent(), input.earthquakeEventCurrent(), input.eventSourceBridge().drop("event_month_utc"),
                input.dimDate(), input.dimRegion(), input.dimMagnitudeBand(), input.dimDepthBand(), 1, 1, 1);
        var oldCommit = legacy.commit(legacyInput, old); check(oldCommit.path("scope").has("silver_bundle_sha256") && !oldCommit.path("scope").has("prepared_input"), "LEGACY_UNCHANGED"); cases++;
        return cases;
    }
    public static void main(String[] args) throws Exception {
        if (args.length != 1 || Files.exists(Path.of(args[0]))) throw new IllegalArgumentException("NEW_LOCAL_FIXTURE_ROOT_REQUIRED");
        Path root = Path.of(args[0]); Files.createDirectories(root);
        var spark = SparkSession.builder().master("local[1]").appName("dat03-prepared-gold-component-qa")
                .config("spark.ui.enabled", "false").config("spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", "1")
                .config("spark.sql.adaptive.enabled", "false").config("spark.sql.codegen.wholeStage", "false")
                .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog").config("spark.sql.catalog.iceberg.type", "hadoop")
                .config("spark.sql.catalog.iceberg.warehouse", root.resolve("warehouse").toUri().toString()).getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        try { System.out.println("event=prepared_gold_writer_fixtures_passed cases=" + runAll(spark, root) + " synthetic_component_qa=true gold_published=false"); }
        finally { spark.stop(); }
    }
}
