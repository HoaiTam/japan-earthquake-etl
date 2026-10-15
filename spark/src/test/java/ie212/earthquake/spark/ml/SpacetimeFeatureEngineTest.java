package ie212.earthquake.spark.ml;

import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;

class SpacetimeFeatureEngineTest {
    @org.junit.jupiter.api.io.TempDir java.nio.file.Path temporary;
    static SparkSession spark;
    static Dataset<Row> main, candidates;
    static final SpacetimeFeatureEngine ENGINE = new SpacetimeFeatureEngine();
    @BeforeAll static void start() {
        spark = SparkSession.builder().master("local[1]").appName("MLD-04-features-test")
            .config("spark.ui.enabled", "false").config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1").config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "1").config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.codegen.wholeStage", "false").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        var schema = new StructType().add("canonical_event_id", "string", false).add("event_time_utc", "timestamp", false)
            .add("latitude", "double", true).add("longitude", "double", true).add("depth_km", "double", true)
            .add("magnitude", "double", true).add("is_eligible", "boolean", false);
        var base = Instant.parse("2000-07-01T00:00:00Z");
        var rows = List.of(row("main", base, 60, 140, 80, 6), row("post", base.plusSeconds(7200), 60.1, 140.1, 82, 3),
            row("pre", base.minusSeconds(3600), 59.9, 139.9, 78, 3), row("tie", base, 60.01, 140, 80, 3),
            row("micro-pre", base.minusNanos(1000), 60, 140, 80, 3), row("micro-post", base.plusNanos(1000), 60, 140, 80, 3));
        var result = new MainshockWindowEngine().execute(spark.createDataFrame(rows, schema),
            MainshockSelectionConfig.reproductionBaseline(), WindowModelConfig.expandedV1(), 1,
            "ds_feature_fixture", Instant.parse("2026-01-01T00:00:00Z"));
        main = result.mainshockSnapshot().localCheckpoint(true);
        candidates = result.candidateSnapshot().localCheckpoint(true);
    }
    static Row row(String id, Instant time, double lat, double lon, double depth, double mag) {
        return RowFactory.create(id, Timestamp.from(time), lat, lon, depth, mag, true);
    }
    @AfterAll static void stop() { if (spark != null) spark.stop(); }
    static FeatureScalingConfig cfg(String mode) { return FeatureScalingConfigTest.config(mode, 50, 10000); }
    @Test void calculatesKilometresRadiansSignedTimeAndExactlyFourScaledFeatures() {
        var result = ENGINE.execute(main, candidates, cfg("GLOBAL")); result.assertValid();
        assertEquals(6, result.inputCount()); assertEquals(0, result.invalidCount());
        Row post = result.features().filter(col("candidate_event_id").equalTo("post")).first();
        assertEquals(5.566, post.<Double>getAs("dx_km"), 1e-10);
        assertEquals(11.057, post.<Double>getAs("dy_km"), 1e-10);
        assertEquals(2, post.<Double>getAs("dz_km"), 0);
        assertEquals(2, post.<Double>getAs("delta_time_hours"), 0);
        assertEquals(5.566 / 50, post.<Double>getAs("x_scaled"), 1e-10);
        assertEquals(2.0 / 30, post.<Double>getAs("z_scaled"), 1e-12);
        assertEquals(2.0 / 720, post.<Double>getAs("time_scaled"), 1e-12);
        assertEquals(List.of("x_scaled", "y_scaled", "z_scaled", "time_scaled"), SpacetimeFeatureEngine.VECTOR);
        assertFalse(SpacetimeFeatureEngine.VECTOR.contains("candidate_magnitude"));
        for (String name : SpacetimeFeatureEngine.VECTOR)
            assertEquals(0, result.features().filter(col("is_mainshock")).first().<Double>getAs(name), 0);
        assertTrue(result.features().filter(col("candidate_event_id").equalTo("micro-pre")).first().<Double>getAs("time_scaled") < 0);
        assertTrue(result.features().filter(col("candidate_event_id").equalTo("micro-post")).first().<Double>getAs("time_scaled") > 0);
        Row tie = result.features().filter(col("candidate_event_id").equalTo("tie")).first();
        assertEquals("POST", tie.getAs("relative_time_role")); assertEquals(0, tie.<Double>getAs("time_scaled"), 0);
    }
    @Test void adaptiveUsesVersionedWindowScalesNotCandidateFit() {
        var result = ENGINE.execute(main, candidates, cfg("ADAPTIVE_WINDOW")); result.assertValid();
        Row m = main.first(), r = result.features().filter(col("candidate_event_id").equalTo("post")).first();
        assertEquals(m.<Double>getAs("search_radius_km"), r.<Double>getAs("space_scale_km"));
        assertEquals(m.<Double>getAs("post_window_hours"), r.<Double>getAs("time_scale_hours"));
        assertEquals("test-scaling-v1", r.getAs("scaling_config_version"));
    }
    @Test void parquetReadbackAndIndependentWritesHaveIdenticalBytes() throws Exception {
        var result = ENGINE.execute(main, candidates, cfg("GLOBAL")); result.assertValid();
        LocalFeatureJob.parquet(result.features(), temporary.resolve("first.parquet"), "dataset_id", "mainshock_event_id", "candidate_event_id");
        LocalFeatureJob.parquet(result.features().repartition(2), temporary.resolve("second.parquet"), "dataset_id", "mainshock_event_id", "candidate_event_id");
        assertEquals(WindowArtifactInput.sha(temporary.resolve("first.parquet")), WindowArtifactInput.sha(temporary.resolve("second.parquet")));
        assertThrows(Exception.class, () -> LocalFeatureJob.parquet(result.features(), temporary.resolve("first.parquet"), "dataset_id"));
    }
    @Test void nullNanInfinityRemainCountedAndBlockPublication() {
        for (Column bad : List.of(lit(null).cast("double"), lit(Double.NaN), lit(Double.POSITIVE_INFINITY))) {
            var changed = candidates.withColumn("candidate_latitude", when(col("candidate_event_id").equalTo("post"), bad).otherwise(col("candidate_latitude")));
            var result = ENGINE.execute(main, changed, cfg("GLOBAL"));
            assertEquals(6, result.inputCount()); assertEquals(1, result.invalidCount());
            assertTrue(result.reasonCounts().containsKey("ML_NON_FINITE_FEATURE"));
            assertThrows(IllegalStateException.class, result::assertValid);
        }
    }
    @Test void duplicateAndMissingSelfNeverGetDeduplicatedOrFilled() {
        var duplicate = ENGINE.execute(main, candidates.union(candidates.filter(col("is_mainshock"))), cfg("GLOBAL"));
        assertEquals(7, duplicate.inputCount()); assertTrue(duplicate.reasonCounts().containsKey("ML_DUPLICATE_GRAIN"));
        assertThrows(IllegalStateException.class, duplicate::assertValid);
        var missing = ENGINE.execute(main, candidates.filter(not(col("is_mainshock"))), cfg("GLOBAL"));
        assertEquals(5, missing.invalidCount()); assertThrows(IllegalStateException.class, missing::assertValid);
    }
    @Test void rejectsWrongRelativeUnitsRoleAndSelfSource() {
        var wrong = ENGINE.execute(main, candidates.withColumn("dx_km", lit(999.0)), cfg("GLOBAL"));
        assertTrue(wrong.reasonCounts().containsKey("ML_RELATIVE_COORDINATE_CONFLICT"));
        assertThrows(IllegalStateException.class, wrong::assertValid);
        var role = ENGINE.execute(main, candidates.withColumn("relative_time_role", lit("POST")), cfg("GLOBAL"));
        assertTrue(role.reasonCounts().containsKey("ML_EVENT_ROLE_MISMATCH"));
        var self = ENGINE.execute(main, candidates.withColumn("candidate_latitude",
            when(col("is_mainshock"), lit(61.0)).otherwise(col("candidate_latitude"))), cfg("GLOBAL"));
        assertTrue(self.reasonCounts().containsKey("ML_SELF_SOURCE_MISMATCH"));
    }
    @Test void resourceOverflowAndNumericOverflowRejectWithoutTruncation() {
        var result = ENGINE.execute(main, candidates, FeatureScalingConfigTest.config("GLOBAL", 50, 2));
        assertEquals(6, result.invalidCount()); assertEquals(6L, result.reasonCounts().get("ML_RESOURCE_LIMIT_EXCEEDED"));
        assertThrows(IllegalStateException.class, result::assertValid);
        var overflow = ENGINE.execute(main, candidates, FeatureScalingConfigTest.config("GLOBAL", Double.MIN_VALUE, 10000));
        assertTrue(overflow.invalidCount() > 0); assertThrows(IllegalStateException.class, overflow::assertValid);
    }
    @Test void schemaTimezoneOrphansAndEmptyWindowsFailClosed() {
        assertThrows(IllegalArgumentException.class, () -> ENGINE.execute(main, candidates.withColumn("candidate_latitude", lit("60")), cfg("GLOBAL")));
        assertThrows(IllegalArgumentException.class, () -> ENGINE.execute(main, candidates.limit(0), cfg("GLOBAL")));
        var orphan = ENGINE.execute(main.limit(0), candidates, cfg("GLOBAL"));
        assertEquals(6, orphan.invalidCount()); assertTrue(orphan.reasonCounts().containsKey("ML_MISSING_MAINSHOCK"));
        spark.conf().set("spark.sql.session.timeZone", "Asia/Tokyo");
        try { assertThrows(IllegalArgumentException.class, () -> ENGINE.execute(main, candidates, cfg("GLOBAL"))); }
        finally { spark.conf().set("spark.sql.session.timeZone", "UTC"); }
    }
}
