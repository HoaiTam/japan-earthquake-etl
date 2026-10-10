package ie212.earthquake.spark.ml;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class GoldInputAuditIntegrationTest {
    private static SparkSession spark;
    private static final ObjectMapper JSON = new ObjectMapper();

    @TempDir
    Path tempDir;

    @BeforeAll
    static void initSpark() {
        spark = SparkSession.builder()
                .master("local[1]")
                .appName("MLD-02-Integration-Pilot")
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
    static void stopSpark() {
        if (spark != null) {
            spark.stop();
        }
    }

    private static Row createRow(
            String id,
            String source,
            String timeIso,
            Double lat,
            Double lon,
            Double depth,
            Double mag,
            String era
    ) {
        Timestamp time = Timestamp.from(Instant.parse(timeIso));
        java.sql.Date date = new java.sql.Date(time.getTime());
        int dateKey = Integer.parseInt(timeIso.substring(0, 4) + timeIso.substring(5, 7) + timeIso.substring(8, 10));
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
                "EARTHQUAKE", // event_type_code
                true, // is_natural_earthquake
                "Japan Region", // place_name
                true, // is_in_study_area
                "PREFECTURE:01", // region_key
                "Hokkaido", // region_name
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
                "gold-pilot-1", // gold_run_id
                Timestamp.from(Instant.parse("2026-10-10T00:00:00Z")) // record_updated_at_utc
        );
    }

    @Test
    void runReproductionAndExtensionAuditPilotsWithReportGeneration() throws Exception {
        List<Row> rows = new ArrayList<>();

        // Generate JMA 2000 reproduction pilot sample events (within 2000-01-01 .. 2018-10-01)
        // Distribution has peak at M=2.1 (mode)
        double[] repMags = {1.8, 1.9, 2.0, 2.1, 2.1, 2.1, 2.1, 2.2, 2.3, 2.5, 2.8, 3.2, 3.8, 5.2, 5.6};
        for (int i = 0; i < 70; i++) {
            double mag = repMags[i % repMags.length];
            double depth = 10.0 + (i % 15) * 10.0;
            rows.add(createRow("jma-2000-" + i, "JMA_BULLETIN", "2000-05-15T10:00:00Z", 36.0, 140.0, depth, mag, "UNIFIED"));
        }

        // Generate JMA 2023 extension sample events (within 2018-10-01 .. 2024-01-01)
        double[] extMags = {1.5, 1.7, 1.8, 1.8, 1.8, 2.0, 2.2, 2.6, 3.0, 5.5};
        for (int i = 0; i < 60; i++) {
            double mag = extMags[i % extMags.length];
            double depth = 15.0 + (i % 10) * 12.0;
            rows.add(createRow("jma-2023-" + i, "JMA_BULLETIN", "2023-01-02T12:00:00Z", 35.5, 139.5, depth, mag, "UNIFIED"));
        }

        // Generate USGS 2023 events
        for (int i = 0; i < 10; i++) {
            rows.add(createRow("usgs-2023-" + i, "USGS", "2023-01-01T08:00:00Z", 35.0, 138.0, 25.0, 4.5, "UNIFIED"));
        }

        Dataset<Row> goldEvents = spark.createDataFrame(rows, GoldInputAuditEngineTest.GOLD_SCHEMA);
        assertEquals(140, goldEvents.count());

        // 1. Run Reproduction Audit
        GoldAuditConfig repConfig = GoldAuditConfig.forReproduction("run-mld02-rep-pilot");
        GoldAuditResult repResult = GoldInputAuditJob.runAudit(spark, goldEvents, repConfig, Instant.parse("2026-10-10T15:00:00Z"));

        repResult.assertReconciled();
        assertEquals(140, repResult.inputEventCount());
        // JMA 2023 (60) has primary reason ML_OUTSIDE_TIME_RANGE; USGS 2023 (10) has primary reason ML_SOURCE_NOT_COMPARABLE
        assertEquals(60, repResult.exclusionCountsByReason().get("ML_OUTSIDE_TIME_RANGE"));
        assertEquals(10, repResult.exclusionCountsByReason().get("ML_SOURCE_NOT_COMPARABLE"));
        // In allExclusionCounts, all 70 events in 2023 have ML_OUTSIDE_TIME_RANGE
        assertEquals(70, repResult.allExclusionCounts().get("ML_OUTSIDE_TIME_RANGE"));

        // All 70 events of JMA 2000 are in period and primary JMA UNIFIED; evaluated for Mc
        CompletenessResult repCompleteness = repResult.completenessResult();
        assertTrue(repCompleteness.isReliable());
        assertEquals("MAXIMUM_CURVATURE", repCompleteness.method());
        assertEquals(2.1, repCompleteness.centralMc(), 1e-4);
        assertEquals(1.9, repCompleteness.sensitivityLower(), 1e-4);
        assertEquals(2.3, repCompleteness.sensitivityUpper(), 1e-4);

        // Eligible events in reproduction: events with mag >= 2.1
        assertTrue(repResult.eligibleEventCount() > 0);
        assertEquals(repResult.eligibleEventCount() + repResult.excludedEventCount(), repResult.inputEventCount());

        // 2. Run Extension Audit
        GoldAuditConfig extConfig = GoldAuditConfig.forExtension("run-mld02-ext-pilot");
        GoldAuditResult extResult = GoldInputAuditJob.runAudit(spark, goldEvents, extConfig, Instant.parse("2026-10-10T15:00:00Z"));

        extResult.assertReconciled();
        assertEquals(140, extResult.inputEventCount());
        // JMA 2000 (70) excluded as ML_OUTSIDE_TIME_RANGE
        assertEquals(70, extResult.exclusionCountsByReason().get("ML_OUTSIDE_TIME_RANGE"));
        // USGS 2023 (10) excluded as ML_SOURCE_NOT_COMPARABLE
        assertEquals(10, extResult.exclusionCountsByReason().get("ML_SOURCE_NOT_COMPARABLE"));

        // Extension Mc is estimated independently from JMA 2023
        CompletenessResult extCompleteness = extResult.completenessResult();
        assertTrue(extCompleteness.isReliable());
        assertEquals(1.8, extCompleteness.centralMc(), 1e-4);

        // 3. Write reports to files
        Path repJsonPath = tempDir.resolve("reproduction_audit_report.json");
        Path repMdPath = tempDir.resolve("reproduction_audit_report.md");
        GoldInputAuditJob.writeReports(repResult, repJsonPath, repMdPath);

        assertTrue(Files.exists(repJsonPath));
        assertTrue(Files.exists(repMdPath));
        String jsonContent = Files.readString(repJsonPath);
        assertTrue(jsonContent.contains("\"dataset_split\" : \"REPRODUCTION\""));
        assertTrue(jsonContent.contains("\"mc_method\" : \"MAXIMUM_CURVATURE\""));

        String mdContent = Files.readString(repMdPath);
        assertTrue(mdContent.contains("# Gold Input Audit & Completeness Report"));
        assertTrue(mdContent.contains("**Central Mc:** `2.1`"));

        // 4. Enrich Building Manifest
        ObjectNode manifest = JSON.createObjectNode();
        manifest.put("dataset_id", "ds_pilot_rep");
        manifest.put("dataset_status", "BUILDING");
        ObjectNode enrichedManifest = repResult.enrichManifest(manifest);

        assertEquals(140, enrichedManifest.get("input_event_count").asLong());
        assertEquals(repResult.eligibleEventCount(), enrichedManifest.get("eligible_event_count").asLong());
        assertEquals(repResult.excludedEventCount(), enrichedManifest.get("excluded_event_count").asLong());
        assertEquals(2.1, enrichedManifest.get("mc_value").asDouble());
        assertNotNull(enrichedManifest.get("mc_config_json"));
        assertEquals(64, enrichedManifest.get("mc_config_sha256").asText().length());
    }
}
