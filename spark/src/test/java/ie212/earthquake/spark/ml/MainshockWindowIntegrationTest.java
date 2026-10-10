package ie212.earthquake.spark.ml;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.nio.file.Files;
import java.sql.Date;
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

class MainshockWindowIntegrationTest {
    private static SparkSession spark;
    private static final ObjectMapper JSON = new ObjectMapper();

    @BeforeAll
    static void start() {
        spark = SparkSession.builder()
                .master("local[1]")
                .appName("MLD-03-integration")
                .config("spark.ui.enabled", "false")
                .config("spark.driver.host", "127.0.0.1")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.sql.session.timeZone", "UTC")
                .config("spark.sql.shuffle.partitions", "1")
                .config("spark.sql.adaptive.enabled", "false")
                .config("spark.sql.codegen.wholeStage", "false")
                .getOrCreate();
    }

    @AfterAll
    static void stop() {
        if (spark != null) {
            spark.stop();
        }
    }

    private static Row createGoldRow(
            String canonicalId,
            String source,
            String isoTimestamp,
            double lat,
            double lon,
            double depth,
            double mag,
            String eventType,
            boolean isNatural,
            boolean inStudyArea,
            String era
    ) {
        Instant inst = Instant.parse(isoTimestamp);
        Timestamp tsUtc = Timestamp.from(inst);
        Date dUtc = new Date(tsUtc.getTime());
        return RowFactory.create(
                canonicalId,
                "obs_" + canonicalId,
                source,
                tsUtc,
                tsUtc,
                dUtc,
                dUtc,
                20000101,
                20000101,
                lat,
                lon,
                depth,
                mag,
                "M",
                eventType,
                isNatural,
                "Honshu",
                inStudyArea,
                "jp_reg",
                "Kanto",
                "ONSHORE",
                "M_MED",
                "INTERMEDIATE",
                false,
                "GREEN",
                100,
                "4",
                source,
                era,
                1,
                "USGS".equals(source),
                "JMA".equals(source),
                "SINGLE_SOURCE",
                "RESOLVED",
                "VALID",
                "1.0",
                "run_gld_test",
                tsUtc
        );
    }

    @Test
    void testEndToEndAuditToWindowPipelineWithReportExport(@TempDir File tempDir) throws Exception {
        // Construct realistic Gold dataset:
        // Mainshock in 2000: M = 6.4, depth = 80 km (intermediate) in Izu islands region
        // Foreshocks (-12h, -6h), Aftershocks (+2h, +10h, +30d), and Background noise
        List<Row> goldRows = new ArrayList<>();

        // Mainshock
        goldRows.add(createGoldRow("JMA_2000_MAIN", "JMA_BULLETIN", "2000-07-01T00:00:00Z", 34.2, 139.2, 80.0, 6.4, "EARTHQUAKE", true, true, "UNIFIED"));

        // Foreshocks
        goldRows.add(createGoldRow("JMA_2000_FORE_1", "JMA_BULLETIN", "2000-06-30T12:00:00Z", 34.22, 139.21, 78.0, 4.5, "EARTHQUAKE", true, true, "UNIFIED"));
        goldRows.add(createGoldRow("JMA_2000_FORE_2", "JMA_BULLETIN", "2000-06-30T18:00:00Z", 34.19, 139.19, 82.0, 3.8, "EARTHQUAKE", true, true, "UNIFIED"));

        // Aftershocks
        goldRows.add(createGoldRow("JMA_2000_AFTER_1", "JMA_BULLETIN", "2000-07-01T02:00:00Z", 34.21, 139.22, 79.0, 5.2, "EARTHQUAKE", true, true, "UNIFIED"));
        goldRows.add(createGoldRow("JMA_2000_AFTER_2", "JMA_BULLETIN", "2000-07-01T10:00:00Z", 34.18, 139.18, 81.0, 4.1, "EARTHQUAKE", true, true, "UNIFIED"));
        goldRows.add(createGoldRow("JMA_2000_AFTER_3", "JMA_BULLETIN", "2000-07-15T00:00:00Z", 34.25, 139.25, 80.0, 3.9, "EARTHQUAKE", true, true, "UNIFIED"));

        // Additional events for completeness estimation (M 2.0 to 3.5)
        for (int i = 0; i < 50; i++) {
            double mag = 2.0 + (i % 15) * 0.1;
            goldRows.add(createGoldRow("JMA_BG_" + i, "JMA_BULLETIN", "2000-07-02T00:00:00Z", 34.2 + (i % 5) * 0.01, 139.2 + (i % 5) * 0.01, 75.0, mag, "EARTHQUAKE", true, true, "UNIFIED"));
        }

        // Non-comparable event (USGS)
        goldRows.add(createGoldRow("USGS_EVENT", "USGS", "2000-07-01T01:00:00Z", 34.2, 139.2, 80.0, 5.0, "EARTHQUAKE", true, true, "UNIFIED"));

        Dataset<Row> goldDf = spark.createDataFrame(goldRows, GoldInputAuditEngineTest.GOLD_SCHEMA);

        // Step 1: Run MLD-02 Audit Engine
        GoldAuditConfig auditConfig = GoldAuditConfig.forReproduction("build_run_pilot");
        GoldInputAuditEngine auditEngine = new GoldInputAuditEngine();
        Instant now = Instant.parse("2026-10-10T12:00:00Z");
        GoldAuditResult auditResult = auditEngine.audit(goldDf, auditConfig, now);

        auditResult.assertReconciled();
        assertTrue(auditResult.completenessResult().isReliable());
        double estimatedMc = auditResult.completenessResult().centralMc();

        // Step 2: Run MLD-03 Window Job with real audited output and Mc
        MainshockSelectionConfig selConfig = MainshockSelectionConfig.reproductionBaseline();
        WindowModelConfig winConfig = WindowModelConfig.uhrhammerV1();

        File outputDir = new File(tempDir, "window_artifacts");
        MainshockWindowResult windowResult = MainshockWindowJob.run(
                auditResult.auditedEvents(),
                selConfig,
                winConfig,
                estimatedMc,
                "ds_reproduction_pilot",
                now,
                outputDir.getAbsolutePath()
        );

        // Invariant verification
        assertDoesNotThrow(windowResult::assertInvariants);

        assertEquals(1, windowResult.mainshockCount());
        assertTrue(windowResult.totalCandidateRowCount() >= 6); // Mainshock + 2 foreshocks + 3 aftershocks + BG events >= Mc
        assertEquals(0, windowResult.flaggedWindowCount());
        assertEquals(0, windowResult.rejectedWindowCount());

        // Step 3: Verify exported artifacts
        File reportJsonFile = new File(outputDir, "window_audit_report.json");
        File reportMdFile = new File(outputDir, "window_audit_summary.md");
        File mainshockParquet = new File(outputDir, "mainshock_candidate_snapshot.parquet");
        File mainshockJson = new File(outputDir, "mainshock_candidate_snapshot.json");
        File candidateParquet = new File(outputDir, "sequence_candidate_snapshot.parquet");
        File candidateJson = new File(outputDir, "sequence_candidate_snapshot.json");

        assertTrue(reportJsonFile.exists() && reportJsonFile.length() > 0);
        assertTrue(reportMdFile.exists() && reportMdFile.length() > 0);
        assertTrue((mainshockParquet.exists() && mainshockParquet.isDirectory()) || (mainshockJson.exists() && mainshockJson.length() > 0));
        assertTrue((candidateParquet.exists() && candidateParquet.isDirectory()) || (candidateJson.exists() && candidateJson.length() > 0));

        // Verify JSON contents
        String jsonContent = Files.readString(reportJsonFile.toPath());
        ObjectNode parsedReport = (ObjectNode) JSON.readTree(jsonContent);
        assertEquals("1.0", parsedReport.get("schema_version").asText());
        assertEquals("ds_reproduction_pilot", parsedReport.get("dataset_id").asText());
        assertEquals(1, parsedReport.get("mainshock_count").asInt());

        // Step 4: Verify manifest enrichment
        ObjectNode manifest = JSON.createObjectNode();
        windowResult.enrichManifest(manifest);
        assertEquals(1, manifest.get("mainshock_count").asInt());
        assertEquals(windowResult.totalCandidateRowCount(), manifest.get("candidate_row_count").asLong());
        assertEquals("wm_uhrhammer_v1.0", manifest.get("window_model_version").asText());
    }
}
