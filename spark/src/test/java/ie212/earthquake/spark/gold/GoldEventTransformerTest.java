package ie212.earthquake.spark.gold;

import static org.junit.jupiter.api.Assertions.*;
import static org.apache.spark.sql.functions.col;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ie212.earthquake.spark.silver.SilverSchemas;

class GoldEventTransformerTest {
    private static SparkSession spark;
    private static final Instant TIME = Instant.parse("2023-01-01T16:00:00Z");
    private static final GoldRunContext CONTEXT = new GoldRunContext("gold-fixture", TIME.minusSeconds(86400),
            TIME.plusSeconds(86400), LocalDate.parse("2023-01-02"), false, "fixture-v1", List.of("mock://silver/current", "mock://silver/membership"));

    @BeforeAll static void start() {
        spark = SparkSession.builder().master("local[1]").appName("GLD-01-offline")
                .config("spark.ui.enabled", "false").config("spark.driver.host", "127.0.0.1")
                .config("spark.driver.bindAddress", "127.0.0.1").config("spark.sql.session.timeZone", "UTC")
                .config("spark.sql.shuffle.partitions", "1").config("spark.sql.adaptive.enabled", "false")
                .config("spark.sql.codegen.wholeStage", "false").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
    }
    @AfterAll static void stop() { if (spark != null) spark.stop(); }

    private static Dataset<Row> observations(String... json) {
        return spark.read().schema(SilverSchemas.OBSERVATION_SCHEMA).json(spark.createDataset(List.of(json), Encoders.STRING()));
    }
    private static String observation(String id, String source, String extra) {
        return "{\"schema_version\":\"1.0\",\"source_observation_id\":\"" + id + "\",\"source_system\":\"" + source
                + "\",\"source_record_key\":\"key-" + id + "\",\"is_current_source_revision\":true,"
                + "\"event_time_utc\":\"2023-01-01T16:00:00Z\",\"latitude\":35.0,\"longitude\":139.0,"
                + "\"event_type_code\":\"EARTHQUAKE\",\"is_in_study_area\":true,\"quality_status\":\"VALID\","
                + "\"raw_object_uri\":\"mock://bronze/" + id + "\",\"bronze_manifest_id\":\"manifest-" + id + "\"" + extra + "}";
    }
    private static Dataset<Row> memberships(Row... rows) {
        return spark.createDataFrame(List.of(rows), GoldEventTransformer.MEMBERSHIP_SCHEMA);
    }
    private static Row member(String event, String id, String role, String link) {
        return RowFactory.create(event, id, role, link, "fixture-v1", Timestamp.from(TIME));
    }
    private static GoldTransformationResult transform(Dataset<Row> observations, Dataset<Row> membership) {
        return new GoldEventTransformer().transform(observations, membership,
                spark.createDataFrame(List.of(), GoldEventTransformer.LINK_SCHEMA),
                spark.createDataFrame(List.of(), GoldEventTransformer.REGION_SCHEMA), CONTEXT, TIME);
    }

    @Test void linkedSourcesPreservePrimaryNullAndSupportingProvenanceWithoutDoubleCount() {
        Dataset<Row> input = observations(observation("jma", "JMA_BULLETIN", ",\"magnitude\":null,\"depth_km\":-1.0,\"catalog_era\":\"UNIFIED\",\"determining_agency_code\":\"J\",\"max_intensity_code\":\"3\""),
                observation("usgs", "USGS", ",\"magnitude\":6.0,\"depth_km\":100.0,\"alert_level\":\"green\",\"significance\":100,\"tsunami_flag\":true"));
        GoldTransformationResult result = transform(input, memberships(member("canonical-existing", "jma", "PRIMARY", "link1"),
                member("canonical-existing", "usgs", "SUPPORTING", "link1")));
        assertEquals(2, result.currentObservationCount());
        assertEquals(1, result.canonicalEventCount());
        assertEquals(2, result.bridgeRowCount());
        Row event = result.eventCurrent().head();
        assertEquals("canonical-existing", event.getAs("canonical_event_id"));
        assertEquals("jma", event.getAs("primary_observation_id"));
        assertNull(event.getAs("magnitude"));
        assertEquals("UNKNOWN", event.getAs("magnitude_band_code"));
        assertEquals("NEGATIVE", event.getAs("depth_band_code"));
        assertEquals("UNIFIED", event.getAs("catalog_era"));
        assertEquals("green", event.getAs("alert_level"));
        assertEquals(Boolean.TRUE, event.getAs("tsunami_flag"));
        assertEquals("USGS_JMA", event.getAs("source_coverage_code"));
        assertEquals("MATCHED", event.getAs("link_status"));
        assertEquals("UNKNOWN", event.getAs("region_category"));
        assertEquals(20230101, (Integer) event.getAs("event_date_key_utc"));
        assertEquals(20230102, (Integer) event.getAs("event_date_key_jst"));
        assertEquals(1, result.earthquakeEventCurrent().count());
        assertEquals(2, result.dimDate().count());
        assertEquals(1, result.eventSourceBridge().filter(col("is_primary")).count());
        GoldTransformationResult rerun = transform(input, memberships(member("canonical-existing", "usgs", "SUPPORTING", "link1"),
                member("canonical-existing", "jma", "PRIMARY", "link1")));
        assertEquals(event, rerun.eventCurrent().head());
    }

    @Test void ambiguousAndNonNaturalEventsRemainInCoreWithOffshoreRegion() {
        Dataset<Row> input = observations(observation("u", "USGS", ""), observation("j", "JMA_BULLETIN", "")
                .replace("EARTHQUAKE", "ARTIFICIAL"));
        Dataset<Row> membership = memberships(member("c-u", "u", "PRIMARY", null), member("c-j", "j", "PRIMARY", null));
        GoldTransformationResult result = new GoldEventTransformer().transform(input, membership,
                spark.createDataFrame(List.of(RowFactory.create("u", "j", "AMBIGUOUS")), GoldEventTransformer.LINK_SCHEMA),
                spark.createDataFrame(List.of(RowFactory.create("u", "OFFSHORE", "Offshore", "OFFSHORE")), GoldEventTransformer.REGION_SCHEMA), CONTEXT, TIME);
        assertEquals(2, result.eventCurrent().count());
        assertEquals(1, result.earthquakeEventCurrent().count());
        assertEquals(2, result.eventCurrent().filter(col("link_status").equalTo("AMBIGUOUS")).count());
        assertEquals("OFFSHORE", result.eventCurrent().filter(col("canonical_event_id").equalTo("c-u")).head().getAs("region_key"));
        assertNull(result.eventCurrent().head().getAs("tsunami_flag"));
    }

    @Test void bandsHaveExactHalfOpenBoundaries() {
        StructType schema = new StructType().add("value", DataTypes.DoubleType, true);
        List<Row> rows = java.util.Arrays.asList(RowFactory.create((Object) null), RowFactory.create(-1.0), RowFactory.create(0.0),
                RowFactory.create(2.999), RowFactory.create(3.0), RowFactory.create(4.0), RowFactory.create(5.0), RowFactory.create(6.0),
                RowFactory.create(7.0), RowFactory.create(69.999), RowFactory.create(70.0), RowFactory.create(299.999), RowFactory.create(300.0));
        List<Row> output = spark.createDataFrame(rows, schema).select(GoldEventTransformer.magnitudeBand(col("value")).alias("m"),
                GoldEventTransformer.depthBand(col("value")).alias("d")).collectAsList();
        assertEquals(List.of("UNKNOWN", "LT_3", "LT_3", "LT_3", "M3_TO_LT4", "M4_TO_LT5", "M5_TO_LT6", "M6_TO_LT7", "GE_7", "GE_7", "GE_7", "GE_7", "GE_7"),
                output.stream().map(row -> row.getString(0)).toList());
        assertEquals(List.of("UNKNOWN", "NEGATIVE", "SHALLOW", "SHALLOW", "SHALLOW", "SHALLOW", "SHALLOW", "SHALLOW", "SHALLOW", "SHALLOW", "INTERMEDIATE", "INTERMEDIATE", "DEEP"),
                output.stream().map(row -> row.getString(1)).toList());
    }

    @Test void emptyValidInputProducesEmptyGoldWithBandDimensions() {
        GoldTransformationResult result = transform(observations(), memberships());
        assertEquals(0, result.canonicalEventCount());
        assertEquals(0, result.bridgeRowCount());
        assertEquals(7, result.dimMagnitudeBand().count());
        assertEquals(5, result.dimDepthBand().count());
    }

    @Test void invalidMembershipAndMissingCurrentInputFailClosed() {
        Dataset<Row> input = observations(observation("u", "USGS", ""));
        assertThrows(IllegalArgumentException.class, () -> transform(input, memberships()));
        assertThrows(IllegalArgumentException.class, () -> transform(input, memberships(member("c", "u", "SUPPORTING", null))));
        assertThrows(IllegalArgumentException.class, () -> transform(input, memberships(member("c", "u", "PRIMARY", null), member("c", "u", "PRIMARY", null))));
        assertThrows(IllegalArgumentException.class, () -> transform(input, memberships(member("c", "old", "PRIMARY", null))));
        assertThrows(IllegalArgumentException.class, () -> new GoldRunContext("run", TIME, TIME, LocalDate.now(), false, "v1", List.of("mock://input")));
    }

    @Test void oldRevisionIsNotCountedAndInvalidCoordinatesAreBlocked() {
        Dataset<Row> input = observations(observation("u", "USGS", ""), observation("old", "USGS", "")
                .replace("\"is_current_source_revision\":true", "\"is_current_source_revision\":false"));
        GoldTransformationResult result = transform(input, memberships(member("stable", "u", "PRIMARY", null)));
        assertEquals(1, result.currentObservationCount());
        assertEquals(1, result.canonicalEventCount());
        assertThrows(IllegalArgumentException.class, () -> transform(observations(observation("u", "USGS", "")
                .replace("\"latitude\":35.0", "\"latitude\":95.0")), memberships(member("stable", "u", "PRIMARY", null))));
    }
}
