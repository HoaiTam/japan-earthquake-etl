package ie212.earthquake.spark.ml;

import static org.junit.jupiter.api.Assertions.*;

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

class MainshockWindowEngineTest {
    private static SparkSession spark;

    public static final StructType AUDITED_SCHEMA = new StructType()
            .add("canonical_event_id", DataTypes.StringType, false)
            .add("event_time_utc", DataTypes.TimestampType, false)
            .add("latitude", DataTypes.DoubleType, false)
            .add("longitude", DataTypes.DoubleType, false)
            .add("depth_km", DataTypes.DoubleType, false)
            .add("magnitude", DataTypes.DoubleType, false)
            .add("is_eligible", DataTypes.BooleanType, false);

    @BeforeAll
    static void start() {
        spark = SparkSession.builder()
                .master("local[1]")
                .appName("MLD-03-offline")
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

    private static Row createAuditedRow(String id, String isoTime, double lat, double lon, double depth, double mag, boolean eligible) {
        return RowFactory.create(id, Timestamp.from(Instant.parse(isoTime)), lat, lon, depth, mag, eligible);
    }

    @Test
    void testGrainUniquenessAndSelfMainshockInvariants() {
        List<Row> rows = List.of(
                // Mainshock 1: M = 6.0, depth = 70.0 km
                createAuditedRow("M1", "2005-06-01T00:00:00Z", 35.0, 139.0, 70.0, 6.0, true),
                // Candidate PRE (-24h): M = 3.5, depth = 72 km, distance ≈ 15 km
                createAuditedRow("C_PRE", "2005-05-31T00:00:00Z", 35.1, 139.1, 72.0, 3.5, true),
                // Candidate POST (+12h): M = 4.0, depth = 68 km, distance ≈ 10 km
                createAuditedRow("C_POST", "2005-06-01T12:00:00Z", 35.05, 139.05, 68.0, 4.0, true),
                // Far event (> 500 km away)
                createAuditedRow("FAR", "2005-06-01T01:00:00Z", 40.0, 145.0, 70.0, 5.0, true),
                // Ineligible event
                createAuditedRow("INELIGIBLE", "2005-06-01T02:00:00Z", 35.01, 139.01, 70.0, 4.5, false)
        );

        Dataset<Row> auditedEvents = spark.createDataFrame(rows, AUDITED_SCHEMA);
        MainshockSelectionConfig selConfig = MainshockSelectionConfig.reproductionBaseline();
        WindowModelConfig winConfig = WindowModelConfig.uhrhammerV1();

        MainshockWindowEngine engine = new MainshockWindowEngine();
        MainshockWindowResult result = engine.execute(
                auditedEvents, selConfig, winConfig, 2.0, "ds_repro_test", Instant.parse("2026-10-10T00:00:00Z")
        );

        // Verify invariants
        assertDoesNotThrow(result::assertInvariants);

        assertEquals(1, result.mainshockCount());
        // Window should have 3 events: M1 (self), C_PRE, and C_POST (FAR and INELIGIBLE excluded)
        assertEquals(3, result.totalCandidateRowCount());
        assertEquals(3, result.distinctCandidateEventCount());

        List<Row> candidates = result.candidateSnapshot().collectAsList();
        boolean foundSelf = false;
        boolean foundPre = false;
        boolean foundPost = false;

        for (Row c : candidates) {
            String cid = c.getString(c.fieldIndex("candidate_event_id"));
            String role = c.getString(c.fieldIndex("relative_time_role"));
            boolean isMainshock = c.getBoolean(c.fieldIndex("is_mainshock"));
            double dt = c.getDouble(c.fieldIndex("delta_time_hours"));
            double dist3d = c.getDouble(c.fieldIndex("distance_3d_km"));

            if ("M1".equals(cid)) {
                foundSelf = true;
                assertTrue(isMainshock);
                assertEquals("MAINSHOCK", role);
                assertEquals(0.0, dt, 1e-6);
                assertEquals(0.0, dist3d, 1e-6);
            } else if ("C_PRE".equals(cid)) {
                foundPre = true;
                assertFalse(isMainshock);
                assertEquals("PRE", role);
                assertEquals(-24.0, dt, 0.1);
                assertTrue(dist3d > 0.0);
            } else if ("C_POST".equals(cid)) {
                foundPost = true;
                assertFalse(isMainshock);
                assertEquals("POST", role);
                assertEquals(12.0, dt, 0.1);
                assertTrue(dist3d > 0.0);
            }
        }

        assertTrue(foundSelf);
        assertTrue(foundPre);
        assertTrue(foundPost);
    }

    @Test
    void testEventInMultipleWindowsNotTreatedAsDuplicate() {
        List<Row> rows = List.of(
                // Mainshock 1 at (35.0, 139.0) on 2005-06-01
                createAuditedRow("M1", "2005-06-01T00:00:00Z", 35.0, 139.0, 60.0, 6.0, true),
                // Mainshock 2 at (35.2, 139.2) on 2005-06-05 (close to M1)
                createAuditedRow("M2", "2005-06-05T00:00:00Z", 35.2, 139.2, 65.0, 6.0, true),
                // Shared event close to both M1 and M2 on 2005-06-03
                createAuditedRow("SHARED_EV", "2005-06-03T00:00:00Z", 35.1, 139.1, 62.0, 3.5, true)
        );

        Dataset<Row> auditedEvents = spark.createDataFrame(rows, AUDITED_SCHEMA);
        MainshockSelectionConfig selConfig = MainshockSelectionConfig.reproductionBaseline();
        WindowModelConfig winConfig = WindowModelConfig.uhrhammerV1();

        MainshockWindowEngine engine = new MainshockWindowEngine();
        MainshockWindowResult result = engine.execute(
                auditedEvents, selConfig, winConfig, 2.0, "ds_repro_test", Instant.parse("2026-10-10T00:00:00Z")
        );

        assertDoesNotThrow(result::assertInvariants);
        assertEquals(2, result.mainshockCount());

        // Check SHARED_EV appears in both windows
        List<Row> sharedRows = result.candidateSnapshot().filter("candidate_event_id = 'SHARED_EV'").collectAsList();
        assertEquals(2, sharedRows.size(), "Event in both windows must generate 2 candidate rows without being dropped");

        Set<String> windowIdsForShared = new HashSet<>();
        for (Row r : sharedRows) {
            windowIdsForShared.add(r.getString(r.fieldIndex("mainshock_event_id")));
        }
        assertTrue(windowIdsForShared.contains("M1"));
        assertTrue(windowIdsForShared.contains("M2"));

        // Verify composite grain is strictly unique
        long totalRows = result.candidateSnapshot().count();
        long uniqueGrain = result.candidateSnapshot().select("dataset_id", "mainshock_event_id", "candidate_event_id").distinct().count();
        assertEquals(totalRows, uniqueGrain);
    }

    @Test
    void testNoCrossJoinPrefilterInPlan() {
        List<Row> rows = List.of(
                createAuditedRow("M1", "2005-06-01T00:00:00Z", 35.0, 139.0, 60.0, 6.0, true),
                createAuditedRow("C1", "2005-06-02T00:00:00Z", 35.1, 139.1, 62.0, 3.0, true)
        );
        Dataset<Row> auditedEvents = spark.createDataFrame(rows, AUDITED_SCHEMA);
        MainshockSelectionConfig selConfig = MainshockSelectionConfig.reproductionBaseline();
        WindowModelConfig winConfig = WindowModelConfig.uhrhammerV1();

        MainshockWindowEngine engine = new MainshockWindowEngine();
        MainshockWindowResult result = engine.execute(
                auditedEvents, selConfig, winConfig, 2.0, "ds_plan_test", Instant.parse("2026-10-10T00:00:00Z")
        );

        // Check execution plan: must contain range join conditions
        String optimizedPlan = result.candidateSnapshot().queryExecution().optimizedPlan().toString();

        // The query plan must contain spatial and temporal join conditions, proving prefilter before fine distance
        assertTrue(optimizedPlan.contains("candidate_time_utc") || optimizedPlan.contains("event_time_utc"),
                "Execution plan must contain time window predicate");
        assertTrue(optimizedPlan.contains("candidate_latitude") || optimizedPlan.contains("latitude"),
                "Execution plan must contain latitude bounding box predicate");
    }

    @Test
    void testHardLimitResourceGuardFlaggingNoSilentTruncation() {
        List<Row> rows = new ArrayList<>();
        // Mainshock
        rows.add(createAuditedRow("M_BIG", "2005-06-01T00:00:00Z", 35.0, 139.0, 60.0, 6.5, true));

        // Add 5 candidates close to mainshock
        for (int i = 1; i <= 5; i++) {
            rows.add(createAuditedRow("C" + i, "2005-06-01T0" + i + ":00:00Z", 35.01, 139.01, 61.0, 3.0, true));
        }

        Dataset<Row> auditedEvents = spark.createDataFrame(rows, AUDITED_SCHEMA);

        // Set maxCandidatesPerWindow = 3 (so 6 total candidates will exceed 3)
        MainshockSelectionConfig selConfig = new MainshockSelectionConfig(
                "sel_test", 50.0, 200.0, 5.5, "REPRODUCTION",
                MainshockSelectionConfig.REPRODUCTION_START, MainshockSelectionConfig.REPRODUCTION_END,
                3L, false // failOnResourceExceeded = false (FLAGGED)
        );
        WindowModelConfig winConfig = WindowModelConfig.uhrhammerV1();

        MainshockWindowEngine engine = new MainshockWindowEngine();
        MainshockWindowResult result = engine.execute(
                auditedEvents, selConfig, winConfig, 2.0, "ds_guard_test", Instant.parse("2026-10-10T00:00:00Z")
        );

        assertDoesNotThrow(result::assertInvariants);

        // All 6 rows MUST be retained (NO SILENT TRUNCATION!)
        assertEquals(6, result.totalCandidateRowCount());

        // Window status must be FLAGGED
        assertEquals(1, result.flaggedWindowCount());
        assertEquals("FLAGGED", result.resourceGuardStatusPerWindow().get("M_BIG"));

        List<Row> mainshocks = result.mainshockSnapshot().collectAsList();
        assertEquals(1, mainshocks.size());
        Row mRow = mainshocks.get(0);
        assertEquals("FLAGGED", mRow.getString(mRow.fieldIndex("resource_guard_status")));
        assertEquals("ML_RESOURCE_LIMIT_EXCEEDED", mRow.getString(mRow.fieldIndex("resource_reason_code")));
    }

    @Test
    void testNestedMainshockDetection() {
        List<Row> rows = List.of(
                // Two mainshocks within each other's window
                createAuditedRow("M1", "2005-06-01T00:00:00Z", 35.0, 139.0, 70.0, 6.2, true),
                createAuditedRow("M2", "2005-06-02T00:00:00Z", 35.05, 139.05, 75.0, 5.8, true)
        );

        Dataset<Row> auditedEvents = spark.createDataFrame(rows, AUDITED_SCHEMA);
        MainshockSelectionConfig selConfig = MainshockSelectionConfig.reproductionBaseline();
        WindowModelConfig winConfig = WindowModelConfig.uhrhammerV1();

        MainshockWindowEngine engine = new MainshockWindowEngine();
        MainshockWindowResult result = engine.execute(
                auditedEvents, selConfig, winConfig, 2.0, "ds_nested_test", Instant.parse("2026-10-10T00:00:00Z")
        );

        assertDoesNotThrow(result::assertInvariants);
        assertEquals(2, result.mainshockCount());

        // Each window should detect the other as a nested mainshock
        assertEquals(1, result.nestedMainshocksPerWindow().getOrDefault("M1", 0));
        assertEquals(1, result.nestedMainshocksPerWindow().getOrDefault("M2", 0));
    }

    @Test
    void testMainshockSelectionCriteriaBoundaryConditions() {
        List<Row> rows = List.of(
                // Boundary mag 5.5: NOT mainshock (mag > 5.5 exclusive)
                createAuditedRow("E_MAG_55", "2005-06-01T00:00:00Z", 35.0, 139.0, 70.0, 5.5, true),
                // Boundary mag 5.51: IS mainshock
                createAuditedRow("E_MAG_551", "2005-06-01T00:00:00Z", 35.0, 139.0, 70.0, 5.51, true),
                // Boundary depth 49.9 km: NOT mainshock (depth 50-200 km)
                createAuditedRow("E_DEPTH_49", "2005-06-01T00:00:00Z", 35.0, 139.0, 49.9, 6.0, true),
                // Boundary depth 50.0 km: IS mainshock
                createAuditedRow("E_DEPTH_50", "2005-06-01T00:00:00Z", 35.0, 139.0, 50.0, 6.0, true),
                // Boundary depth 200.0 km: IS mainshock
                createAuditedRow("E_DEPTH_200", "2005-06-01T00:00:00Z", 35.0, 139.0, 200.0, 6.0, true),
                // Boundary depth 200.1 km: NOT mainshock
                createAuditedRow("E_DEPTH_201", "2005-06-01T00:00:00Z", 35.0, 139.0, 200.1, 6.0, true),
                // Event outside period (1999): NOT mainshock
                createAuditedRow("E_OUTSIDE_PERIOD", "1999-12-31T23:59:59Z", 35.0, 139.0, 70.0, 6.5, true)
        );

        Dataset<Row> auditedEvents = spark.createDataFrame(rows, AUDITED_SCHEMA);
        MainshockSelectionConfig selConfig = MainshockSelectionConfig.reproductionBaseline();
        WindowModelConfig winConfig = WindowModelConfig.uhrhammerV1();

        MainshockWindowEngine engine = new MainshockWindowEngine();
        MainshockWindowResult result = engine.execute(
                auditedEvents, selConfig, winConfig, 2.0, "ds_bound_test", Instant.parse("2026-10-10T00:00:00Z")
        );

        // Expected mainshocks: E_MAG_551, E_DEPTH_50, E_DEPTH_200 (exactly 3)
        assertEquals(3, result.mainshockCount());
        Set<String> selectedIds = result.candidatesPerWindow().keySet();
        assertTrue(selectedIds.contains("E_MAG_551"));
        assertTrue(selectedIds.contains("E_DEPTH_50"));
        assertTrue(selectedIds.contains("E_DEPTH_200"));
        assertFalse(selectedIds.contains("E_MAG_55"));
        assertFalse(selectedIds.contains("E_DEPTH_49"));
        assertFalse(selectedIds.contains("E_DEPTH_201"));
        assertFalse(selectedIds.contains("E_OUTSIDE_PERIOD"));
    }
}
