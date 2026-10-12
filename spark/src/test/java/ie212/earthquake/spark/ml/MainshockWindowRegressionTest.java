package ie212.earthquake.spark.ml;

import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MainshockWindowRegressionTest {
    private static SparkSession spark;
    private static final Instant NOW = Instant.parse("2026-10-11T00:00:00Z");
    private static final MainshockSelectionConfig SELECTION = MainshockSelectionConfig.reproductionBaseline();

    @BeforeAll
    static void start() {
        spark = SparkSession.builder().master("local[1]").appName("MLD-03-regression")
                .config("spark.ui.enabled", "false").config("spark.driver.host", "127.0.0.1")
                .config("spark.driver.bindAddress", "127.0.0.1").config("spark.sql.session.timeZone", "UTC")
                .config("spark.sql.shuffle.partitions", "2").config("spark.sql.adaptive.enabled", "false")
                .config("spark.sql.codegen.wholeStage", "false").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
    }

    @AfterAll
    static void stop() { if (spark != null) spark.stop(); }

    private static Row event(String id, String time, double lat, double lon, double magnitude) {
        return RowFactory.create(id, Timestamp.from(Instant.parse(time)), lat, lon, 70.0, magnitude, true);
    }

    private static Dataset<Row> data(List<Row> rows) {
        return spark.createDataFrame(rows, MainshockWindowEngineTest.AUDITED_SCHEMA);
    }

    private static Dataset<Row> sample() {
        return data(List.of(event("M", "2005-06-01T00:00:00.900Z", 35, 139, 6),
                event("PRE", "2005-06-01T00:00:00.100Z", 35, 139, 3),
                event("POST", "2005-06-01T00:00:01.100Z", 35, 139, 3)));
    }

    private static MainshockWindowResult execute(Dataset<Row> rows, WindowModelConfig window) {
        return new MainshockWindowEngine().execute(rows, SELECTION, window, 2, "ds_regression", NOW);
    }

    @Test
    void derivedEligibilityWithNullableGoldFieldsDoesNotCrashWindowUdfs() {
        var schema = DataTypes.createStructType(Arrays.stream(MainshockWindowEngineTest.AUDITED_SCHEMA.fields())
            .map(f -> DataTypes.createStructField(f.name(), f.dataType(), true)).toArray(StructField[]::new));
        var rows = new ArrayList<Row>(sample().collectAsList());
        Timestamp time = Timestamp.from(Instant.parse("2005-06-01T00:00:00Z"));
        rows.add(RowFactory.create("NO_MAG", time, 35.0, 139.0, 70.0, null, false));
        rows.add(RowFactory.create("NO_LAT", time, null, 139.0, 70.0, 6.0, false));
        rows.add(RowFactory.create("BAD_LAT", time, 91.0, 139.0, 70.0, 6.0, false));
        rows.add(RowFactory.create("INF_MAG", time, 35.0, 139.0, 70.0, Double.POSITIVE_INFINITY, false));
        var audited = spark.createDataFrame(rows, schema).withColumn("is_eligible",
            col("magnitude").isNotNull().and(abs(col("magnitude")).leq(Double.MAX_VALUE))
                .and(col("latitude").isNotNull()).and(abs(col("latitude")).leq(90)));
        var result = execute(audited, WindowModelConfig.expandedV1());
        assertEquals(1, result.mainshockCount()); assertEquals(3, result.totalCandidateRowCount());
        result.assertInvariants();
    }

    @Test
    void preservesFractionalSecondsAndRejectsCorruptVectors() {
        MainshockWindowResult result = execute(sample(), WindowModelConfig.uhrhammerV1());
        Row pre = result.candidateSnapshot().filter("candidate_event_id='PRE'").first();
        Row post = result.candidateSnapshot().filter("candidate_event_id='POST'").first();
        assertEquals(-0.8 / 3600, (Double) pre.getAs("delta_time_hours"), 1e-14);
        assertEquals(0.2 / 3600, (Double) post.getAs("delta_time_hours"), 1e-14);
        result.assertInvariants();
        Dataset<Row> corrupt = result.candidateSnapshot().withColumn("delta_time_hours", lit(0.0));
        assertThrows(AssertionError.class, () -> withCandidates(result, corrupt).assertInvariants());
        Dataset<Row> nan = result.candidateSnapshot().withColumn("dx_km", lit(Double.NaN));
        assertThrows(AssertionError.class, () -> withCandidates(result, nan).assertInvariants());
    }

    @Test
    void keepsSimultaneousNonSelfEventAsPostWithoutInventingDelta() {
        MainshockWindowResult result = execute(data(List.of(
                event("M", "2005-06-01T00:00:00.900Z", 35, 139, 6),
                event("TIE", "2005-06-01T00:00:00.900Z", 35, 139, 3))), WindowModelConfig.uhrhammerV1());
        Row tie = result.candidateSnapshot().filter("candidate_event_id='TIE'").first();
        assertEquals("POST", tie.getAs("relative_time_role"));
        assertFalse((Boolean) tie.getAs("is_mainshock"));
        assertEquals(0.0, (Double) tie.getAs("delta_time_hours"));
        result.assertInvariants();
    }

    @Test
    void includesExactMicrosecondWindowEdgesAcrossDayAndSpatialBuckets() {
        // 1-hour pre/post caps make exact edges easy to assert, independently of the model formula.
        WindowModelConfig window = new WindowModelConfig(WindowModelType.UHRHAMMER, "wm_boundary_fixture",
                0.1, 168, 1.0, 1.0, null, null, null, null, null);
        MainshockWindowResult result = execute(data(List.of(
                event("M", "2005-06-01T00:00:00.900Z", 35, 139, 6),
                event("PRE_EDGE", "2005-05-31T23:00:00.900Z", 34.999, 138.999, 3),
                event("POST_EDGE", "2005-06-01T01:00:00.900Z", 35.001, 139.001, 3),
                event("BEFORE", "2005-05-31T23:00:00.899999Z", 35, 139, 3),
                event("AFTER", "2005-06-01T01:00:00.900001Z", 35, 139, 3))), window);
        assertEquals(3, result.totalCandidateRowCount());
        assertEquals(1, result.candidateSnapshot().filter("candidate_event_id='PRE_EDGE'").count());
        assertEquals(1, result.candidateSnapshot().filter("candidate_event_id='POST_EDGE'").count());
        result.assertInvariants();
    }

    @Test
    void usesEquiJoinWhenBroadcastIsDisabledAndPreservesInputOrderIndependence() {
        String previous = spark.conf().get("spark.sql.autoBroadcastJoinThreshold");
        spark.conf().set("spark.sql.autoBroadcastJoinThreshold", "-1");
        try {
            List<Row> rows = new ArrayList<>(List.of(event("M", "2005-06-01T00:00:00Z", 35, 139, 6)));
            for (int i = 0; i < 60; i++) {
                rows.add(event("C" + i, "2005-06-02T00:00:00Z", 35.01, 139.01, 3));
                rows.add(event("FAR" + i, "2005-06-02T00:00:00Z", 40, 145, 3));
            }
            MainshockWindowResult result = execute(data(rows).repartition(2), WindowModelConfig.uhrhammerV1());
            String plan = result.candidateSnapshot().queryExecution().executedPlan().toString();
            assertTrue(plan.contains("SortMergeJoin") || plan.contains("ShuffledHashJoin"), plan);
            assertFalse(plan.contains("BroadcastNestedLoopJoin") || plan.contains("CartesianProduct"), plan);
            assertEquals(61, result.totalCandidateRowCount());
            result.assertInvariants();
            java.util.Collections.reverse(rows);
            MainshockWindowResult reversed = execute(data(rows), WindowModelConfig.uhrhammerV1());
            assertEquals(0, result.candidateSnapshot().exceptAll(reversed.candidateSnapshot()).count());
            assertEquals(0, reversed.candidateSnapshot().exceptAll(result.candidateSnapshot()).count());
        } finally {
            spark.conf().set("spark.sql.autoBroadcastJoinThreshold", previous);
        }
    }

    @Test
    void distributedInvariantsRejectWrongCountDuplicateAndMissingSelf() {
        MainshockWindowResult result = execute(sample(), WindowModelConfig.uhrhammerV1());
        Dataset<Row> wrong = result.mainshockSnapshot().withColumn("candidate_count", lit(999L));
        assertThrows(AssertionError.class, () -> new MainshockWindowResult(wrong, result.candidateSnapshot(),
                result.mainshockCount(), result.totalCandidateRowCount(), result.distinctCandidateEventCount(),
                result.candidatesPerWindow(), result.nestedMainshocksPerWindow(), result.resourceGuardStatusPerWindow(),
                result.flaggedWindowCount(), result.rejectedWindowCount(), result.datasetId(), result.selectionConfig(),
                result.windowConfig(), result.mcApplied(), result.createdAtUtc()).assertInvariants());
        assertThrows(AssertionError.class, () -> withCandidates(result,
                result.candidateSnapshot().union(result.candidateSnapshot())).assertInvariants());
        assertThrows(AssertionError.class, () -> withCandidates(result,
                result.candidateSnapshot().filter(not(col("is_mainshock")))).assertInvariants());
    }

    @Test
    void parquetFailurePropagatesAndInvalidatesStaleReports(@TempDir Path output) throws Exception {
        Files.writeString(output.resolve("window_audit_report.json"), "stale success");
        Files.writeString(output.resolve("window_audit_summary.md"), "stale success");
        String key = "spark.sql.parquet.output.committer.class";
        String previous = spark.conf().get(key, "org.apache.parquet.hadoop.ParquetOutputCommitter");
        spark.conf().set(key, "review.DoesNotExist");
        try {
            assertThrows(Exception.class, () -> MainshockWindowJob.run(sample(), SELECTION,
                    WindowModelConfig.uhrhammerV1(), 2, "ds_export", NOW, output.toString()));
            assertFalse(Files.exists(output.resolve("window_audit_report.json")));
            assertFalse(Files.exists(output.resolve("window_audit_summary.md")));
            assertFalse(Files.exists(output.resolve("sequence_candidate_snapshot.json")));
        } finally {
            spark.conf().set(key, previous);
        }
        Files.writeString(output.resolve("window_audit_report.json"), "stale success");
        Files.writeString(output.resolve("window_audit_summary.md"), "stale success");
        assertThrows(IllegalArgumentException.class, () -> MainshockWindowJob.run(sample(), SELECTION,
                WindowModelConfig.uhrhammerV1(), Double.NaN, "ds_export", NOW, output.toString()));
        assertFalse(Files.exists(output.resolve("window_audit_report.json")));
        assertFalse(Files.exists(output.resolve("window_audit_summary.md")));
    }

    @Test
    void rejectedWindowCannotBeExported(@TempDir Path output) {
        MainshockSelectionConfig strict = new MainshockSelectionConfig("strict_fixture", 50, 200, 5.5,
                "REPRODUCTION", SELECTION.periodStartUtc(), SELECTION.periodEndUtcExclusive(), 1, true);
        assertThrows(IllegalStateException.class, () -> MainshockWindowJob.run(sample(), strict,
                WindowModelConfig.uhrhammerV1(), 2, "ds_rejected", NOW, output.toString()));
        assertFalse(Files.exists(output.resolve("window_audit_report.json")));
    }

    @Test
    void emptyInputAndUnavailableMcAreExplicit() {
        MainshockWindowResult empty = execute(data(List.of()), WindowModelConfig.uhrhammerV1());
        assertEquals(0, empty.totalCandidateRowCount());
        empty.assertInvariants();
        assertThrows(IllegalArgumentException.class, () -> new MainshockWindowEngine().execute(sample(),
                SELECTION, WindowModelConfig.uhrhammerV1(), Double.NaN, "ds_invalid", NOW));
    }

    private static MainshockWindowResult withCandidates(MainshockWindowResult result, Dataset<Row> candidates) {
        return new MainshockWindowResult(result.mainshockSnapshot(), candidates, result.mainshockCount(), candidates.count(),
                result.distinctCandidateEventCount(), result.candidatesPerWindow(), result.nestedMainshocksPerWindow(),
                result.resourceGuardStatusPerWindow(), result.flaggedWindowCount(), result.rejectedWindowCount(),
                result.datasetId(), result.selectionConfig(), result.windowConfig(), result.mcApplied(), result.createdAtUtc());
    }
}
