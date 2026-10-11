package ie212.earthquake.spark.ml;

import static org.junit.jupiter.api.Assertions.*;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;

class MagnitudeHistogramSparkTest {
    private static SparkSession spark;
    @BeforeAll static void start() {
        spark = SparkSession.builder().master("local[2]").appName("mld02-weighted-histogram")
            .config("spark.ui.enabled", "false").config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1").config("spark.driver.maxResultSize", "1m")
            .config("spark.sql.shuffle.partitions", "2").config("spark.sql.adaptive.enabled", "false").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
    }
    @AfterAll static void stop() { if (spark != null) spark.stop(); }
    @Test void millionEventsFitOneMegabyteDriverResultLimit() {
        var events = spark.range(1_000_000).select(when(col("id").mod(3).equalTo(0), lit(2.5)).otherwise(lit(2.0)).alias("magnitude"));
        var result = new MagnitudeCompletenessEstimator().estimate(events, "ALL");
        assertEquals(1_000_000, result.sampleCount());
        assertEquals(2.0, result.centralMc());
        assertEquals(666_666, result.maxCurvatureBinCount());
        assertEquals(1_000_000, result.eventsAboveMc());
    }
    @Test void depthGroupsUseExactBoundariesAndIgnoreNonFiniteValues() {
        var events = spark.sql("SELECT * FROM VALUES (2.0D, 69.9D), (2.1D, 70.0D), (3.0D, 299.9D),"
            + "(4.0D, 300.0D), (5.0D, CAST('Infinity' AS DOUBLE)), (CAST('NaN' AS DOUBLE), 10.0D),"
            + "(6.0D, CAST(NULL AS DOUBLE)), (7.0D, -1.0D) AS t(magnitude, depth_km)");
        var groups = new MagnitudeCompletenessEstimator().estimateStratifiedByDepth(events);
        assertEquals(1, groups.get("SHALLOW").sampleCount());
        assertEquals(2, groups.get("INTERMEDIATE").sampleCount());
        assertEquals(1, groups.get("DEEP").sampleCount());
        assertFalse(groups.get("SHALLOW").isReliable());
    }
    @Test void excessiveDistinctBinsFailInsteadOfTruncating() {
        var events = spark.range(10_002).select(col("id").multiply(0.1).alias("magnitude"));
        var error = assertThrows(IllegalArgumentException.class, () -> new MagnitudeCompletenessEstimator().estimate(events, "ALL"));
        assertEquals("ML_MC_HISTOGRAM_LIMIT_EXCEEDED", error.getMessage());
    }
}
