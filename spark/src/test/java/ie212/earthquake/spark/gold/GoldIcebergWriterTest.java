package ie212.earthquake.spark.gold;

import static org.junit.jupiter.api.Assertions.*;
import static org.apache.spark.sql.functions.*;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.*;
import java.util.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import ie212.earthquake.spark.silver.FileSilverObjectStore;

/** Actual Iceberg tables/snapshots, not a mocked commit adapter. */
class GoldIcebergWriterTest {
    @TempDir static Path root;
    static SparkSession spark;
    @BeforeAll static void start() {
        spark = SparkSession.builder().master("local[1]").appName("gld-03-real-iceberg")
            .config("spark.ui.enabled", "false").config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1").config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "1").config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.codegen.wholeStage", "false")
            .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lake.type", "hadoop")
            .config("spark.sql.catalog.lake.warehouse", root.resolve("warehouse").toUri().toString()).getOrCreate();
        if (System.getProperty("os.name").startsWith("Windows")) {
            spark.sparkContext().hadoopConfiguration().set("fs.file.impl", NioTestFileSystem.class.getName());
            spark.sparkContext().hadoopConfiguration().setBoolean("fs.file.impl.disable.cache", true);
        }
        spark.sparkContext().setLogLevel("ERROR");
    }
    @AfterAll static void stop() { if (spark != null) spark.stop(); }

    /** Windows test adapter: NIO directories/ACL defaults; production S3 and Linux FS are unchanged. */
    public static class NioTestFileSystem extends org.apache.hadoop.fs.RawLocalFileSystem {
        @Override public String getScheme() { return "file"; }
        @Override public boolean mkdirs(org.apache.hadoop.fs.Path path, org.apache.hadoop.fs.permission.FsPermission mode)
                throws java.io.IOException {
            java.nio.file.Files.createDirectories(pathToFile(path).toPath()); return true;
        }
        @Override public void setPermission(org.apache.hadoop.fs.Path path, org.apache.hadoop.fs.permission.FsPermission mode) {
            // POSIX chmod is not applicable to the test's Windows NTFS temp directory.
        }
    }

    @Test void commitRerunRevisionEmptyAndOutsideMonthRetention() throws Exception {
        var writer = writer("retention");
        var first = writer.commit(data("a", "2023-01-02T00:00:00Z", 4), scope("first", List.of("2023-01"), writer.baselines()));
        assertEquals("COMMITTED", first.path("status").asText()); assertFalse(first.path("published").asBoolean());
        var same = writer.commit(data("a", "2023-01-02T00:00:00Z", 4), scope("first", List.of("2023-01"), zero()));
        assertEquals(first, same);
        var next = writer.commit(data("b", "2023-02-02T00:00:00Z", 5), scope("second", List.of("2023-02"), writer.baselines()));
        assertEquals(2, next.path("tables").path("event_current").path("count").asLong());
        var revised = writer.commit(data("a", "2023-01-02T00:00:00Z", 6), scope("revision", List.of("2023-01"), writer.baselines()));
        assertEquals(2, revised.path("tables").path("event_current").path("count").asLong());
        assertEquals(5d, spark.table("lake.retention.event_current").filter(col("canonical_event_id").equalTo("b")).head().getAs("magnitude"));
        assertEquals(4d, spark.read().format("iceberg").option("snapshot-id", first.path("tables").path("event_current").path("snapshot_id").asText())
                .load("lake.retention.event_current").head().getAs("magnitude"));
        var empty = data("a", "2023-01-02T00:00:00Z", 6);
        empty = new GoldTransformationResult(empty.eventCurrent().limit(0), empty.earthquakeEventCurrent().limit(0),
                empty.eventSourceBridge().limit(0), empty.dimDate().limit(0), empty.dimRegion().limit(0),
                empty.dimMagnitudeBand().limit(0), empty.dimDepthBand().limit(0), 0, 0, 0);
        writer.commit(empty, scope("delete-january", List.of("2023-01"), writer.baselines()));
        assertEquals(1, spark.table("lake.retention.event_current").count());
        assertEquals("b", spark.table("lake.retention.event_current").head().getAs("canonical_event_id"));
    }
    @Test void partialCommitResumesWithoutRecommittingAndConflictsFailClosed() throws Exception {
        var writer = writer("partial"); var input = data("x", "2023-01-02T00:00:00Z", 4);
        var context = scope("partial", List.of("2023-01"), zero());
        assertThrows(IllegalStateException.class, () -> writer.commit(input, context, name -> { throw new IllegalStateException("injected"); }));
        assertEquals(1, spark.table("lake.partial.event_current.snapshots").count());
        assertFalse(new FileSilverObjectStore(root.resolve("partial-journal")).exists("gold-commits/partial/commit.json"));
        var resumed = writer.commit(input, context);
        assertEquals(1, spark.table("lake.partial.event_current.snapshots").count());
        assertFalse(resumed.path("published").asBoolean());
        assertThrows(Exception.class, () -> writer.commit(data("x", "2023-01-02T00:00:00Z", 5), context));
        assertThrows(Exception.class, () -> writer.commit(input, scope("stale", List.of("2023-01"), zero())));
    }
    @Test void invalidScopeAndBlockedQualityDoNotWrite() throws Exception {
        var writer = writer("blocked"); var good = data("x", "2023-02-02T00:00:00Z", 4);
        assertThrows(Exception.class, () -> writer.commit(good, scope("outside", List.of("2023-01"), zero())));
        var badEvents = good.eventCurrent().withColumn("quality_status", lit("REJECTED"));
        var bad = new GoldTransformationResult(badEvents, badEvents, good.eventSourceBridge(), good.dimDate(), good.dimRegion(),
            good.dimMagnitudeBand(), good.dimDepthBand(), 1, 1, 1);
        assertThrows(Exception.class, () -> writer.commit(bad, scope("quality", List.of("2023-02"), zero())));
        var nullQuality = good.eventCurrent().withColumn("quality_status", lit(null).cast("string"));
        var invalid = new GoldTransformationResult(nullQuality, nullQuality, good.eventSourceBridge(), good.dimDate(), good.dimRegion(),
            good.dimMagnitudeBand(), good.dimDepthBand(), 1, 1, 1);
        assertThrows(Exception.class, () -> writer.commit(invalid, scope("null-quality", List.of("2023-02"), zero())));
        assertFalse(spark.catalog().tableExists("lake.blocked.event_current"));
    }
    @Test void emptyInitialCommitAndLostLeaseFailClosed() throws Exception {
        var populated = data("x", "2023-01-02T00:00:00Z", 4);
        var empty = new GoldTransformationResult(populated.eventCurrent().limit(0), populated.earthquakeEventCurrent().limit(0),
            populated.eventSourceBridge().limit(0), populated.dimDate().limit(0), populated.dimRegion().limit(0),
            populated.dimMagnitudeBand().limit(0), populated.dimDepthBand().limit(0), 0, 0, 0);
        var receipt = writer("empty_initial").commit(empty, scope("empty-initial", List.of("2023-01"), zero()));
        for (String name : GoldIcebergWriter.TABLES) {
            assertTrue(receipt.path("tables").path(name).path("snapshot_id").asLong() > 0);
            assertEquals(0, receipt.path("tables").path(name).path("count").asLong());
        }
        var guarded = new GoldIcebergWriter(spark, "lake.lost_lease", new FileSilverObjectStore(root.resolve("lost-lease")),
                () -> { throw new IllegalStateException("lease lost"); });
        assertThrows(IllegalStateException.class, () -> guarded.commit(populated, scope("lost", List.of("2023-01"), zero())));
        assertFalse(spark.catalog().tableExists("lake.lost_lease.event_current"));
    }
    static GoldIcebergWriter writer(String schema) { return new GoldIcebergWriter(spark, "lake."+schema,
            new FileSilverObjectStore(root.resolve(schema+"-journal")), () -> { }); }
    static Map<String, Long> zero() { var out = new HashMap<String, Long>(); GoldIcebergWriter.TABLES.forEach(name -> out.put(name, 0L)); return out; }
    static GoldCommitScope scope(String run, List<String> months, Map<String, Long> bases) {
        return new GoldCommitScope(new GoldRunContext(run, Instant.parse("2023-01-01T00:00:00Z"),
            Instant.parse("2023-03-01T00:00:00Z"), LocalDate.parse("2023-03-01"), true, "test-v1", List.of("s3://fixture/silver/manifest.json")),
            "a".repeat(64), "test-v1", months, bases);
    }
    static GoldTransformationResult data(String id, String time, double magnitude) {
        StructType es = new StructType().add("canonical_event_id", "string", false).add("event_time_utc", "timestamp", false)
            .add("magnitude", "double", true).add("quality_status", "string", false);
        var e = spark.createDataFrame(List.of(RowFactory.create(id, Timestamp.from(Instant.parse(time)), magnitude, "VALID")), es);
        var b = spark.createDataFrame(List.of(RowFactory.create(id, "obs-"+id)), new StructType().add("canonical_event_id", "string", false)
            .add("source_observation_id", "string", false));
        var d = spark.createDataFrame(List.of(RowFactory.create(20230102)), new StructType().add("date_key", "integer", false));
        var r = spark.createDataFrame(List.of(RowFactory.create("UNKNOWN")), new StructType().add("region_key", "string", false));
        var band = spark.createDataFrame(List.of(RowFactory.create("UNKNOWN")), new StructType().add("band_code", "string", false));
        return new GoldTransformationResult(e, e, b, d, r, band, band, 1, 1, 1);
    }
}
