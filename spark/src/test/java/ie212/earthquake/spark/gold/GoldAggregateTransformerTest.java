package ie212.earthquake.spark.gold;

import static org.junit.jupiter.api.Assertions.*;
import static org.apache.spark.sql.functions.*;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ie212.earthquake.spark.silver.SilverSchemas;

class GoldAggregateTransformerTest {
    private static SparkSession spark;
    private static final Instant TIME = Instant.parse("2023-01-01T16:00:00Z");
    private static final GoldRunContext CONTEXT = new GoldRunContext("gold-agg-fixture", TIME.minusSeconds(86400),
            TIME.plusSeconds(86400), LocalDate.parse("2023-01-02"), false, "fixture-v1",
            List.of("mock://silver/current", "mock://silver/membership"));

    @BeforeAll
    static void start() {
        spark = SparkSession.builder().master("local[1]").appName("GLD-02-offline")
                .config("spark.ui.enabled", "false").config("spark.driver.host", "127.0.0.1")
                .config("spark.driver.bindAddress", "127.0.0.1").config("spark.sql.session.timeZone", "UTC")
                .config("spark.sql.shuffle.partitions", "1").config("spark.sql.adaptive.enabled", "false")
                .config("spark.sql.codegen.wholeStage", "false").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
    }

    @AfterAll
    static void stop() {
        if (spark != null) spark.stop();
    }

    private static Dataset<Row> observations(String... json) {
        return spark.read().schema(SilverSchemas.OBSERVATION_SCHEMA)
                .json(spark.createDataset(List.of(json), Encoders.STRING()));
    }

    private static String observation(String id, String source, String time, String extra) {
        return "{\"schema_version\":\"1.0\",\"source_observation_id\":\"" + id + "\",\"source_system\":\"" + source
                + "\",\"source_record_key\":\"key-" + id + "\",\"is_current_source_revision\":true,"
                + "\"event_time_utc\":\"" + time + "\",\"latitude\":35.0,\"longitude\":139.0,"
                + "\"event_type_code\":\"EARTHQUAKE\",\"is_in_study_area\":true,\"quality_status\":\"VALID\","
                + "\"raw_object_uri\":\"mock://bronze/" + id + "\",\"bronze_manifest_id\":\"manifest-" + id + "\"" + extra + "}";
    }

    private static Dataset<Row> memberships(Row... rows) {
        return spark.createDataFrame(List.of(rows), GoldEventTransformer.MEMBERSHIP_SCHEMA);
    }

    private static Row member(String event, String id, String role, String link) {
        return RowFactory.create(event, id, role, link, "fixture-v1", Timestamp.from(TIME));
    }

    private static GoldTransformationResult transformGold(Dataset<Row> observations, Dataset<Row> membership, Dataset<Row> regions) {
        return new GoldEventTransformer().transform(observations, membership,
                spark.createDataFrame(List.of(), GoldEventTransformer.LINK_SCHEMA),
                regions, CONTEXT, TIME);
    }

    @Test
    void dailyAggregateReconcilesExactCountsAndAveragesWithFactTable() {
        // Event 1: Tokyo, M4.2, depth 10km, tsunami false, JMA intensity 2
        String obs1 = observation("o1", "JMA_BULLETIN", "2023-01-01T02:00:00Z",
                ",\"magnitude\":4.2,\"depth_km\":10.0,\"max_intensity_code\":\"2\",\"determining_agency_code\":\"J\",\"catalog_era\":\"UNIFIED\"");
        // Event 2: Tokyo, M5.6, depth 30km, tsunami true, alert yellow, USGS
        String obs2 = observation("o2", "USGS", "2023-01-01T04:00:00Z",
                ",\"magnitude\":5.6,\"depth_km\":30.0,\"tsunami_flag\":true,\"alert_level\":\"yellow\"");
        // Event 3: Offshore, M6.5, depth 80km, tsunami false, alert green, USGS
        String obs3 = observation("o3", "USGS", "2023-01-01T10:00:00Z",
                ",\"magnitude\":6.5,\"depth_km\":80.0,\"tsunami_flag\":false,\"alert_level\":\"green\"");
        // Event 4: Tokyo (next JST day: UTC 2023-01-01T16:00 is JST 2023-01-02 01:00), M3.5, depth 15km
        String obs4 = observation("o4", "JMA_BULLETIN", "2023-01-01T16:00:00Z",
                ",\"magnitude\":3.5,\"depth_km\":15.0,\"determining_agency_code\":\"J\",\"catalog_era\":\"UNIFIED\"");

        Dataset<Row> input = observations(obs1, obs2, obs3, obs4);
        Dataset<Row> mem = memberships(
                member("c1", "o1", "PRIMARY", null),
                member("c2", "o2", "PRIMARY", null),
                member("c3", "o3", "PRIMARY", null),
                member("c4", "o4", "PRIMARY", null)
        );
        Dataset<Row> regions = spark.createDataFrame(List.of(
                RowFactory.create("o1", "PREFECTURE:13", "Tokyo", "PREFECTURE"),
                RowFactory.create("o2", "PREFECTURE:13", "Tokyo", "PREFECTURE"),
                RowFactory.create("o3", "OFFSHORE", "Offshore", "OFFSHORE"),
                RowFactory.create("o4", "PREFECTURE:13", "Tokyo", "PREFECTURE")
        ), GoldEventTransformer.REGION_SCHEMA);

        GoldTransformationResult gold = transformGold(input, mem, regions);
        GoldAggregateTransformer transformer = new GoldAggregateTransformer();
        Dataset<Row> daily = transformer.transformDaily(gold.earthquakeEventCurrent(), CONTEXT, TIME);

        // Check total fact count matches aggregate sum of event_count
        long totalFact = gold.earthquakeEventCurrent().count();
        assertEquals(4, totalFact);
        long totalAgg = daily.agg(sum("event_count")).head().getLong(0);
        assertEquals(totalFact, totalAgg);

        // JST Date verification: obs1, obs2, obs3 are 20230101; obs4 is 20230102
        long countDay1 = daily.filter(col("event_date_key_jst").equalTo(20230101)).agg(sum("event_count")).head().getLong(0);
        long countDay2 = daily.filter(col("event_date_key_jst").equalTo(20230102)).agg(sum("event_count")).head().getLong(0);
        assertEquals(3, countDay1);
        assertEquals(1, countDay2);

        // Strong earthquakes (magnitude >= 5.0): obs2 (5.6) and obs3 (6.5) -> 2
        long strongCount = daily.agg(sum("strong_earthquake_count")).head().getLong(0);
        assertEquals(2, strongCount);

        // Tsunami flagged count: obs2 -> 1
        long tsunamiCount = daily.agg(sum("tsunami_flagged_count")).head().getLong(0);
        assertEquals(1, tsunamiCount);

        // Alerted event count: obs2 ("yellow") and obs3 ("green") -> 2
        long alertCount = daily.agg(sum("alerted_event_count")).head().getLong(0);
        assertEquals(2, alertCount);

        // Intensity reported: obs1 ("2") -> 1
        long intensityCount = daily.agg(sum("intensity_reported_count")).head().getLong(0);
        assertEquals(1, intensityCount);

        // Offshore or unknown: obs3 ("OFFSHORE") -> 1
        long offshoreCount = daily.agg(sum("offshore_or_unknown_count")).head().getLong(0);
        assertEquals(1, offshoreCount);

        // Magnitude max: 6.5
        double maxMag = daily.agg(max("magnitude_max")).head().getDouble(0);
        assertEquals(6.5, maxMag, 0.001);

        // Magnitude sum: 4.2 + 5.6 + 6.5 + 3.5 = 19.8
        double sumMag = daily.agg(sum("magnitude_sum")).head().getDouble(0);
        assertEquals(19.8, sumMag, 0.001);

        // Depth sum: 10 + 30 + 80 + 15 = 135.0
        double sumDepth = daily.agg(sum("depth_sum")).head().getDouble(0);
        assertEquals(135.0, sumDepth, 0.001);
    }

    @Test
    void sourceLinkProvenanceDoesNotDoubleCountInAggregates() {
        // Linked event: 2 observations (JMA primary + USGS supporting), 1 canonical event
        String jmaObs = observation("jma1", "JMA_BULLETIN", "2023-01-01T05:00:00Z",
                ",\"magnitude\":5.2,\"depth_km\":20.0,\"determining_agency_code\":\"J\",\"catalog_era\":\"UNIFIED\"");
        String usgsObs = observation("usgs1", "USGS", "2023-01-01T05:00:00Z",
                ",\"magnitude\":5.3,\"depth_km\":22.0,\"alert_level\":\"green\",\"tsunami_flag\":true");

        Dataset<Row> input = observations(jmaObs, usgsObs);
        Dataset<Row> mem = memberships(
                member("c-linked", "jma1", "PRIMARY", "link1"),
                member("c-linked", "usgs1", "SUPPORTING", "link1")
        );
        Dataset<Row> regions = spark.createDataFrame(List.of(
                RowFactory.create("jma1", "PREFECTURE:13", "Tokyo", "PREFECTURE")
        ), GoldEventTransformer.REGION_SCHEMA);

        GoldTransformationResult gold = transformGold(input, mem, regions);

        // Verify fact table has 1 canonical event and bridge has 2 rows
        assertEquals(1, gold.canonicalEventCount());
        assertEquals(2, gold.bridgeRowCount());

        GoldAggregateTransformer transformer = new GoldAggregateTransformer();
        Dataset<Row> daily = transformer.transformDaily(gold.earthquakeEventCurrent(), CONTEXT, TIME);

        // Aggregates must have exactly 1 event count, NOT 2
        assertEquals(1, daily.count());
        Row row = daily.head();
        assertEquals(1L, (Long) row.getAs("event_count"));
        assertEquals("USGS_JMA", row.getAs("source_coverage_code"));
        assertEquals(5.2, (Double) row.getAs("magnitude_max"), 0.001);
        assertEquals(Boolean.TRUE, gold.earthquakeEventCurrent().head().getAs("tsunami_flag"));
        assertEquals(1L, (Long) row.getAs("tsunami_flagged_count"));
    }

    @Test
    void nullMagnitudeAndNegativeDepthPoliciesPreserved() {
        // Event with null magnitude and negative depth (-2.0 km)
        String obs = observation("null-mag", "JMA_BULLETIN", "2023-01-01T08:00:00Z",
                ",\"magnitude\":null,\"depth_km\":-2.0,\"determining_agency_code\":\"J\",\"catalog_era\":\"UNIFIED\"");

        Dataset<Row> input = observations(obs);
        Dataset<Row> mem = memberships(member("c-null", "null-mag", "PRIMARY", null));
        Dataset<Row> regions = spark.createDataFrame(List.of(), GoldEventTransformer.REGION_SCHEMA);

        GoldTransformationResult gold = transformGold(input, mem, regions);
        GoldAggregateTransformer transformer = new GoldAggregateTransformer();
        Dataset<Row> daily = transformer.transformDaily(gold.earthquakeEventCurrent(), CONTEXT, TIME);

        assertEquals(1, daily.count());
        Row row = daily.head();
        assertEquals(1L, (Long) row.getAs("event_count"));
        assertEquals("UNKNOWN", row.getAs("magnitude_band_code"));
        assertEquals("NEGATIVE", row.getAs("depth_band_code"));
        assertEquals(0L, (Long) row.getAs("magnitude_non_null_count"));
        assertNull(row.getAs("magnitude_sum"));
        assertNull(row.getAs("magnitude_avg"));
        assertNull(row.getAs("magnitude_max"));
        assertEquals(1L, (Long) row.getAs("depth_non_null_count"));
        assertEquals(-2.0, (Double) row.getAs("depth_sum"), 0.001);
        assertEquals(-2.0, (Double) row.getAs("depth_avg"), 0.001);
    }

    @Test
    void nonNaturalAndOutOfStudyAreaEventsExcludedFromAggregates() {
        // Natural earthquake in study area
        String natural = observation("nat", "USGS", "2023-01-01T01:00:00Z", ",\"magnitude\":4.0,\"depth_km\":10.0");
        // Artificial explosion in study area
        String artificial = observation("art", "USGS", "2023-01-01T02:00:00Z", ",\"magnitude\":3.0,\"depth_km\":0.0")
                .replace("\"event_type_code\":\"EARTHQUAKE\"", "\"event_type_code\":\"ARTIFICIAL\"");
        // Earthquake OUTSIDE study area
        String outside = observation("out", "USGS", "2023-01-01T03:00:00Z", ",\"magnitude\":5.0,\"depth_km\":20.0")
                .replace("\"is_in_study_area\":true", "\"is_in_study_area\":false");

        Dataset<Row> input = observations(natural, artificial, outside);
        Dataset<Row> mem = memberships(
                member("c-nat", "nat", "PRIMARY", null),
                member("c-art", "art", "PRIMARY", null),
                member("c-out", "out", "PRIMARY", null)
        );
        Dataset<Row> regions = spark.createDataFrame(List.of(), GoldEventTransformer.REGION_SCHEMA);

        GoldTransformationResult gold = transformGold(input, mem, regions);

        // Full event_current has 3 events, but earthquakeEventCurrent has only 1
        assertEquals(3, gold.eventCurrent().count());
        assertEquals(1, gold.earthquakeEventCurrent().count());

        GoldAggregateTransformer transformer = new GoldAggregateTransformer();
        // Even if caller passes eventCurrent (all events), transformer filters to natural & study area
        Dataset<Row> daily = transformer.transformDaily(gold.eventCurrent(), CONTEXT, TIME);
        assertEquals(1, daily.count());
        assertEquals(1L, (Long) daily.agg(sum("event_count")).head().getLong(0));
    }

    @Test
    void monthlyRollupMatchesDailyRollupExactly() {
        // Both obs1 and obs2 occur in Jan 2023 and fall into M4_TO_LT5, SHALLOW, USGS_ONLY, OFFSHORE
        String obs1 = observation("m1", "USGS", "2023-01-05T01:00:00Z", ",\"magnitude\":4.2,\"depth_km\":12.0");
        String obs2 = observation("m2", "USGS", "2023-01-10T02:00:00Z", ",\"magnitude\":4.8,\"depth_km\":26.0,\"tsunami_flag\":true");
        // obs3 occurs in Feb 2023, M3_TO_LT4, SHALLOW
        String obs3 = observation("m3", "USGS", "2023-02-01T03:00:00Z", ",\"magnitude\":3.8,\"depth_km\":8.0");

        Dataset<Row> input = observations(obs1, obs2, obs3);
        Dataset<Row> mem = memberships(
                member("c-m1", "m1", "PRIMARY", null),
                member("c-m2", "m2", "PRIMARY", null),
                member("c-m3", "m3", "PRIMARY", null)
        );
        Dataset<Row> regions = spark.createDataFrame(List.of(), GoldEventTransformer.REGION_SCHEMA);

        GoldTransformationResult gold = transformGold(input, mem, regions);
        GoldAggregateTransformer transformer = new GoldAggregateTransformer();
        Dataset<Row> daily = transformer.transformDaily(gold.earthquakeEventCurrent(), CONTEXT, TIME);
        Dataset<Row> monthly = transformer.transformMonthly(daily, TIME);

        // Daily has 3 dates (Jan 5, Jan 10, Feb 1), Monthly has 2 months (202301 and 202302)
        assertEquals(3, daily.select("event_date_key_jst").distinct().count());
        assertEquals(2, monthly.select("year_month_jst").distinct().count());

        // Sum of event_count across all monthly rows must equal daily total
        long dailyTotal = daily.agg(sum("event_count")).head().getLong(0);
        long monthlyTotal = monthly.agg(sum("event_count")).head().getLong(0);
        assertEquals(3, monthlyTotal);
        assertEquals(dailyTotal, monthlyTotal);

        // Jan 2023 monthly has 1 row with 2 events rolled up across the two Jan dates
        Dataset<Row> janRows = monthly.filter(col("year_month_jst").equalTo(202301));
        assertEquals(1, janRows.count());
        Row janRow = janRows.head();
        assertEquals(2L, (Long) janRow.getAs("event_count"));
        assertEquals(9.0, janRow.getAs("magnitude_sum"), 0.001);
        assertEquals(4.5, janRow.getAs("magnitude_avg"), 0.001);
        assertEquals(4.8, janRow.getAs("magnitude_max"), 0.001);
        assertEquals(38.0, janRow.getAs("depth_sum"), 0.001);
        assertEquals(19.0, janRow.getAs("depth_avg"), 0.001);
        assertEquals(1L, (Long) janRow.getAs("tsunami_flagged_count"));

        // Feb 2023 monthly has 1 event
        long febCount = monthly.filter(col("year_month_jst").equalTo(202302)).agg(sum("event_count")).head().getLong(0);
        assertEquals(1, febCount);
    }

    @Test
    void emptyInputProducesEmptyDatasetWithCorrectSchema() {
        Dataset<Row> emptyInput = observations();
        Dataset<Row> emptyMem = memberships();
        Dataset<Row> emptyRegions = spark.createDataFrame(List.of(), GoldEventTransformer.REGION_SCHEMA);

        GoldTransformationResult gold = transformGold(emptyInput, emptyMem, emptyRegions);
        GoldAggregateTransformer transformer = new GoldAggregateTransformer();
        Dataset<Row> daily = transformer.transformDaily(gold.earthquakeEventCurrent(), CONTEXT, TIME);
        Dataset<Row> monthly = transformer.transformMonthly(daily, TIME);

        assertEquals(0, daily.count());
        assertEquals(0, monthly.count());
        assertEquals(GoldAggregateTransformer.EVENT_DAILY_SCHEMA.fieldNames().length, daily.schema().fieldNames().length);
        assertEquals(GoldAggregateTransformer.EVENT_MONTHLY_SCHEMA.fieldNames().length, monthly.schema().fieldNames().length);
    }
}
