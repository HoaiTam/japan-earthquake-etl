package ie212.earthquake.spark.ml;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class GoldInputAuditEngineTest {
    private static SparkSession spark;
    private static final ObjectMapper JSON = new ObjectMapper();

    public static final StructType GOLD_SCHEMA = new StructType()
            .add("canonical_event_id", DataTypes.StringType, false)
            .add("primary_observation_id", DataTypes.StringType, false)
            .add("canonical_source_system", DataTypes.StringType, false)
            .add("event_time_utc", DataTypes.TimestampType, false)
            .add("event_time_jst", DataTypes.TimestampType, false)
            .add("event_date_utc", DataTypes.DateType, false)
            .add("event_date_jst", DataTypes.DateType, false)
            .add("event_date_key_utc", DataTypes.IntegerType, false)
            .add("event_date_key_jst", DataTypes.IntegerType, false)
            .add("latitude", DataTypes.DoubleType, true)
            .add("longitude", DataTypes.DoubleType, true)
            .add("depth_km", DataTypes.DoubleType, true)
            .add("magnitude", DataTypes.DoubleType, true)
            .add("magnitude_type", DataTypes.StringType, true)
            .add("event_type_code", DataTypes.StringType, false)
            .add("is_natural_earthquake", DataTypes.BooleanType, false)
            .add("place_name", DataTypes.StringType, true)
            .add("is_in_study_area", DataTypes.BooleanType, false)
            .add("region_key", DataTypes.StringType, false)
            .add("region_name", DataTypes.StringType, false)
            .add("region_category", DataTypes.StringType, false)
            .add("magnitude_band_code", DataTypes.StringType, false)
            .add("depth_band_code", DataTypes.StringType, false)
            .add("tsunami_flag", DataTypes.BooleanType, true)
            .add("alert_level", DataTypes.StringType, true)
            .add("significance", DataTypes.IntegerType, true)
            .add("max_intensity_code", DataTypes.StringType, true)
            .add("determining_agency_code", DataTypes.StringType, true)
            .add("catalog_era", DataTypes.StringType, true)
            .add("source_count", DataTypes.IntegerType, false)
            .add("has_usgs", DataTypes.BooleanType, false)
            .add("has_jma", DataTypes.BooleanType, false)
            .add("source_coverage_code", DataTypes.StringType, false)
            .add("link_status", DataTypes.StringType, false)
            .add("quality_status", DataTypes.StringType, false)
            .add("canonical_model_version", DataTypes.StringType, false)
            .add("gold_run_id", DataTypes.StringType, false)
            .add("record_updated_at_utc", DataTypes.TimestampType, false);

    @BeforeAll
    static void start() {
        spark = SparkSession.builder()
                .master("local[1]")
                .appName("MLD-02-offline")
                .config("spark.ui.enabled", "false")
                .config("spark.driver.host", "127.0.0.1")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.sql.session.timeZone", "UTC")
                .config("spark.sql.shuffle.partitions", "1")
                .config("spark.sql.adaptive.enabled", "false")
                .config("spark.sql.codegen.wholeStage", "false")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
    }

    @AfterAll
    static void stop() {
        if (spark != null) spark.stop();
    }

    private static Row createGoldRow(
            String id,
            String source,
            String timeIso,
            Double lat,
            Double lon,
            Double depth,
            Double mag,
            String eventType,
            boolean isNatural,
            boolean inStudyArea,
            String era
    ) {
        Timestamp time = timeIso != null ? Timestamp.from(Instant.parse(timeIso)) : null;
        java.sql.Date date = time != null ? new java.sql.Date(time.getTime()) : null;
        int dateKey = time != null ? 20050101 : 0;
        return RowFactory.create(
                id, // canonical_event_id
                "obs-" + id, // primary_observation_id
                source, // canonical_source_system
                time, // event_time_utc
                time, // event_time_jst
                date, // event_date_utc
                date, // event_date_jst
                dateKey, // event_date_key_utc
                dateKey, // event_date_key_jst
                lat, // latitude
                lon, // longitude
                depth, // depth_km
                mag, // magnitude
                "M", // magnitude_type
                eventType, // event_type_code
                isNatural, // is_natural_earthquake
                "Japan", // place_name
                inStudyArea, // is_in_study_area
                "PREFECTURE:13", // region_key
                "Tokyo", // region_name
                "PREFECTURE", // region_category
                mag != null && mag >= 5.0 ? "M5_TO_LT6" : (mag != null && mag >= 2.0 ? "LT_3" : "UNKNOWN"), // magnitude_band_code
                depth != null && depth < 70 ? "SHALLOW" : (depth != null && depth < 300 ? "INTERMEDIATE" : "DEEP"), // depth_band_code
                null, // tsunami_flag
                null, // alert_level
                null, // significance
                null, // max_intensity_code
                "J", // determining_agency_code
                era, // catalog_era
                1, // source_count
                "USGS".equals(source), // has_usgs
                "JMA_BULLETIN".equals(source), // has_jma
                "JMA_BULLETIN".equals(source) ? "JMA_ONLY" : "USGS_ONLY", // source_coverage_code
                "SINGLE_SOURCE", // link_status
                "VALID", // quality_status
                "1.0", // canonical_model_version
                "run-1", // gold_run_id
                Timestamp.from(Instant.parse("2026-01-01T00:00:00Z")) // record_updated_at_utc
        );
    }

    @Test
    void auditRejectsDuplicateCanonicalEventId() {
        Row row1 = createGoldRow("ev-dup", "JMA_BULLETIN", "2005-06-01T12:00:00Z", 35.0, 139.0, 50.0, 3.0, "EARTHQUAKE", true, true, "UNIFIED");
        Row row2 = createGoldRow("ev-dup", "JMA_BULLETIN", "2005-06-01T12:00:00Z", 35.0, 139.0, 50.0, 3.0, "EARTHQUAKE", true, true, "UNIFIED");
        Dataset<Row> dataset = spark.createDataFrame(List.of(row1, row2), GOLD_SCHEMA);

        GoldAuditConfig config = GoldAuditConfig.forReproduction("run-audit-1", 2.0);
        GoldInputAuditEngine engine = new GoldInputAuditEngine();

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                engine.audit(dataset, config, Instant.now()));
        assertTrue(ex.getMessage().contains("Duplicate canonical_event_id"));
    }

    @Test
    void auditExcludesAllNonCompliantEventsWithExactReasonCodes() {
        List<Row> rows = new ArrayList<>();

        // 1. Missing coordinate
        rows.add(createGoldRow("e-coord-null", "JMA_BULLETIN", "2005-06-01T12:00:00Z", null, 139.0, 50.0, 3.0, "EARTHQUAKE", true, true, "UNIFIED"));
        rows.add(createGoldRow("e-coord-nan", "JMA_BULLETIN", "2005-06-01T12:00:00Z", Double.NaN, 139.0, 50.0, 3.0, "EARTHQUAKE", true, true, "UNIFIED"));
        rows.add(createGoldRow("e-coord-out", "JMA_BULLETIN", "2005-06-01T12:00:00Z", 95.0, 139.0, 50.0, 3.0, "EARTHQUAKE", true, true, "UNIFIED"));

        // 2. Missing depth
        rows.add(createGoldRow("e-depth-null", "JMA_BULLETIN", "2005-06-01T12:00:00Z", 35.0, 139.0, null, 3.0, "EARTHQUAKE", true, true, "UNIFIED"));
        rows.add(createGoldRow("e-depth-nan", "JMA_BULLETIN", "2005-06-01T12:00:00Z", 35.0, 139.0, Double.NaN, 3.0, "EARTHQUAKE", true, true, "UNIFIED"));

        // 3. Missing magnitude
        rows.add(createGoldRow("e-mag-null", "JMA_BULLETIN", "2005-06-01T12:00:00Z", 35.0, 139.0, 50.0, null, "EARTHQUAKE", true, true, "UNIFIED"));
        rows.add(createGoldRow("e-mag-nan", "JMA_BULLETIN", "2005-06-01T12:00:00Z", 35.0, 139.0, 50.0, Double.NaN, "EARTHQUAKE", true, true, "UNIFIED"));

        // 4. Non-natural event
        rows.add(createGoldRow("e-non-natural", "JMA_BULLETIN", "2005-06-01T12:00:00Z", 35.0, 139.0, 50.0, 3.0, "ARTIFICIAL", false, true, "UNIFIED"));

        // 5. Outside study area
        rows.add(createGoldRow("e-outside-area", "JMA_BULLETIN", "2005-06-01T12:00:00Z", 35.0, 139.0, 50.0, 3.0, "EARTHQUAKE", true, false, "UNIFIED"));

        // 6. Non-comparable source (USGS)
        rows.add(createGoldRow("e-usgs-source", "USGS", "2005-06-01T12:00:00Z", 35.0, 139.0, 50.0, 3.0, "EARTHQUAKE", true, true, "UNIFIED"));

        // 7. Legacy catalog era
        rows.add(createGoldRow("e-legacy-era", "JMA_BULLETIN", "2005-06-01T12:00:00Z", 35.0, 139.0, 50.0, 3.0, "EARTHQUAKE", true, true, "LEGACY"));

        // 8. Outside time range (before 2000-01-01)
        rows.add(createGoldRow("e-too-early", "JMA_BULLETIN", "1999-12-31T23:59:59Z", 35.0, 139.0, 50.0, 3.0, "EARTHQUAKE", true, true, "UNIFIED"));

        // 9. Outside time range (after 2018-10-01)
        rows.add(createGoldRow("e-too-late", "JMA_BULLETIN", "2020-01-01T00:00:00Z", 35.0, 139.0, 50.0, 3.0, "EARTHQUAKE", true, true, "UNIFIED"));

        // 10. Below completeness (Mc = 2.0, mag = 1.8)
        rows.add(createGoldRow("e-below-mc", "JMA_BULLETIN", "2005-06-01T12:00:00Z", 35.0, 139.0, 50.0, 1.8, "EARTHQUAKE", true, true, "UNIFIED"));

        // 11. Eligible event (mag = 3.5 >= Mc)
        rows.add(createGoldRow("e-eligible-1", "JMA_BULLETIN", "2005-06-01T12:00:00Z", 35.0, 139.0, 50.0, 3.5, "EARTHQUAKE", true, true, "UNIFIED"));

        // 12. Boundary: mag == Mc (mag = 2.0) -> MUST be eligible!
        rows.add(createGoldRow("e-eligible-exact-mc", "JMA_BULLETIN", "2005-06-01T12:00:00Z", 35.0, 139.0, 50.0, 2.0, "EARTHQUAKE", true, true, "UNIFIED"));

        // 13. Boundary: time == 2000-01-01T00:00:00Z (inclusive start) -> MUST be eligible!
        rows.add(createGoldRow("e-eligible-exact-start", "JMA_BULLETIN", "2000-01-01T00:00:00Z", 35.0, 139.0, 50.0, 2.5, "EARTHQUAKE", true, true, "UNIFIED"));

        // 14. Boundary: time == 2018-10-01T00:00:00Z (exclusive end) -> MUST be excluded ML_OUTSIDE_TIME_RANGE!
        rows.add(createGoldRow("e-boundary-exact-end", "JMA_BULLETIN", "2018-10-01T00:00:00Z", 35.0, 139.0, 50.0, 2.5, "EARTHQUAKE", true, true, "UNIFIED"));

        Dataset<Row> dataset = spark.createDataFrame(rows, GOLD_SCHEMA);

        GoldAuditConfig config = GoldAuditConfig.forReproduction("run-audit-all", 2.0);
        GoldInputAuditEngine engine = new GoldInputAuditEngine();
        GoldAuditResult result = engine.audit(dataset, config, Instant.parse("2026-10-10T12:00:00Z"));

        // Count reconciliation
        assertEquals(rows.size(), result.inputEventCount());
        assertEquals(3, result.eligibleEventCount()); // e-eligible-1, e-eligible-exact-mc, e-eligible-exact-start
        assertEquals(rows.size() - 3, result.excludedEventCount());
        result.assertReconciled();

        // Check specific exclusion codes
        Map<String, Long> reasons = result.exclusionCountsByReason();
        assertEquals(3, reasons.get("ML_MISSING_COORDINATE"));
        assertEquals(2, reasons.get("ML_MISSING_DEPTH"));
        assertEquals(2, reasons.get("ML_MISSING_MAGNITUDE"));
        assertEquals(1, reasons.get("ML_NON_NATURAL_EVENT"));
        assertEquals(1, reasons.get("ML_OUTSIDE_STUDY_AREA"));
        assertEquals(1, reasons.get("ML_SOURCE_NOT_COMPARABLE"));
        assertEquals(1, reasons.get("ML_LEGACY_CATALOG_ERA"));
        assertEquals(3, reasons.get("ML_OUTSIDE_TIME_RANGE")); // e-too-early, e-too-late, e-boundary-exact-end
        assertEquals(1, reasons.get("ML_BELOW_COMPLETENESS")); // e-below-mc

        // Verify Gold immutability: all columns from GOLD_SCHEMA remain intact
        for (String col : GOLD_SCHEMA.fieldNames()) {
            assertTrue(Arrays.asList(result.auditedEvents().columns()).contains(col));
        }

        // Verify eligible rows are intact
        List<String> eligibleIds = result.eligibleEvents().select("canonical_event_id")
                .as(org.apache.spark.sql.Encoders.STRING()).collectAsList();
        assertTrue(eligibleIds.contains("e-eligible-1"));
        assertTrue(eligibleIds.contains("e-eligible-exact-mc"));
        assertTrue(eligibleIds.contains("e-eligible-exact-start"));
        assertFalse(eligibleIds.contains("e-below-mc"));
        assertFalse(eligibleIds.contains("e-boundary-exact-end"));
    }

    @Test
    void auditEnrichesDatasetManifestWithExactContractFields() throws Exception {
        List<Row> rows = new ArrayList<>();
        // Add 60 events so estimator can run reliably if needed
        for (int i = 0; i < 60; i++) {
            rows.add(createGoldRow("ev-" + i, "JMA_BULLETIN", "2005-01-01T00:00:00Z", 35.0, 139.0, 50.0, 2.5, "EARTHQUAKE", true, true, "UNIFIED"));
        }
        rows.add(createGoldRow("ev-ex-coord", "JMA_BULLETIN", "2005-01-01T00:00:00Z", null, 139.0, 50.0, 2.5, "EARTHQUAKE", true, true, "UNIFIED"));

        Dataset<Row> dataset = spark.createDataFrame(rows, GOLD_SCHEMA);
        GoldAuditConfig config = GoldAuditConfig.forReproduction("test-run-123", 2.0);
        GoldInputAuditEngine engine = new GoldInputAuditEngine();
        GoldAuditResult result = engine.audit(dataset, config, Instant.parse("2026-10-10T12:00:00Z"));

        ObjectNode manifest = JSON.createObjectNode();
        manifest.put("dataset_id", "ds_fixture");
        manifest.put("gold_snapshot_id", 1001L);
        manifest.put("dataset_status", "BUILDING");

        ObjectNode enriched = result.enrichManifest(manifest);

        assertEquals(61, enriched.get("input_event_count").asLong());
        assertEquals(60, enriched.get("eligible_event_count").asLong());
        assertEquals(1, enriched.get("excluded_event_count").asLong());
        assertEquals("MAXIMUM_CURVATURE", enriched.get("mc_method").asText());
        assertEquals("1.0", enriched.get("mc_method_version").asText());
        assertEquals(2.0, enriched.get("mc_value").asDouble());
        assertEquals("overall-v1", enriched.get("mc_region_version").asText());
        assertNotNull(enriched.get("mc_config_json").asText());
        assertEquals(64, enriched.get("mc_config_sha256").asText().length());
        assertEquals("staging/audit/reproduction_audit_report.json", enriched.get("audit_report_uri").asText());

        // Validate exclusion_counts_json can be parsed as JSON object
        var exclusionsNode = JSON.readTree(enriched.get("exclusion_counts_json").asText());
        assertTrue(exclusionsNode.isObject());
        assertEquals(1, exclusionsNode.get("ML_MISSING_COORDINATE").asLong());
    }

    @Test
    void auditCannotUseExtensionPeriodForReproductionSplit() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                new GoldAuditConfig(
                        "REPRODUCTION",
                        Instant.parse("2018-10-01T00:00:00Z"), // Wrong! This is extension start
                        Instant.parse("2024-01-01T00:00:00Z"), // Wrong!
                        Instant.parse("2024-01-01T00:00:00Z"),
                        "JMA_BULLETIN",
                        "UNIFIED",
                        true,
                        true,
                        "1.0",
                        "1.0",
                        "overall-v1",
                        2.0,
                        0.2,
                        0.1,
                        50,
                        "1.0",
                        1L,
                        "pub_1",
                        "run-1",
                        null
                ));
        assertTrue(ex.getMessage().contains("DS_INVALID_PERIOD"));
    }

    @Test
    void auditGeneratesValidJsonAndMarkdownReports() throws Exception {
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < 55; i++) {
            rows.add(createGoldRow("ev-" + i, "JMA_BULLETIN", "2005-01-01T00:00:00Z", 35.0, 139.0, 50.0, 2.5, "EARTHQUAKE", true, true, "UNIFIED"));
        }
        rows.add(createGoldRow("ev-ex", "JMA_BULLETIN", "2005-01-01T00:00:00Z", null, 139.0, 50.0, 2.5, "EARTHQUAKE", true, true, "UNIFIED"));

        Dataset<Row> dataset = spark.createDataFrame(rows, GOLD_SCHEMA);
        GoldAuditConfig config = GoldAuditConfig.forReproduction("run-rep-test", 2.0);
        GoldInputAuditEngine engine = new GoldInputAuditEngine();
        GoldAuditResult result = engine.audit(dataset, config, Instant.parse("2026-10-10T12:00:00Z"));

        ObjectNode jsonReport = result.toJsonReport();
        assertNotNull(jsonReport);
        assertEquals("GOLD_INPUT_AUDIT_AND_COMPLETENESS", jsonReport.get("report_type").asText());
        assertEquals("REPRODUCTION", jsonReport.get("dataset_split").asText());
        assertEquals(56, jsonReport.get("counts").get("input_event_count").asLong());
        assertEquals(55, jsonReport.get("counts").get("eligible_event_count").asLong());
        assertEquals(1, jsonReport.get("counts").get("excluded_event_count").asLong());

        String mdReport = result.toMarkdownReport();
        assertNotNull(mdReport);
        assertTrue(mdReport.contains("# Gold Input Audit & Completeness Report"));
        assertTrue(mdReport.contains("Executive Summary & Counts Reconciliation"));
        assertTrue(mdReport.contains("Exclusion Breakdown by Primary Reason"));
        assertTrue(mdReport.contains("ML_MISSING_COORDINATE"));
    }
}
