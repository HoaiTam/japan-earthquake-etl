package ie212.earthquake.spark.silver;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end integration and reconciliation test suite for Silver multi-source processing (SLV-09).
 * Verifies that:
 *   1. Logic and counts remain completely deterministic across reruns (AC 1).
 *   2. Parsed, valid, rejected, duplicate, superseded, and canonical events reconcile perfectly (AC 2).
 */
class SilverMultiSourceIntegrationTest {

    private static final Instant EXEC_TIME = Instant.parse("2026-10-09T14:30:00Z");
    private SilverMultiSourceIntegrationRunner runner;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        runner = new SilverMultiSourceIntegrationRunner();
    }

    private static Path repositoryRoot() {
        Path candidate = Path.of(System.getProperty("user.dir")).toAbsolutePath().normalize();
        while (candidate != null) {
            if (Files.isRegularFile(candidate.resolve("tests/fixtures/cases.json"))) {
                return candidate;
            }
            candidate = candidate.getParent();
        }
        throw new IllegalStateException("Could not locate repository tests/fixtures");
    }

    private static byte[] loadFixture(String relativePath) throws IOException {
        return Files.readAllBytes(repositoryRoot().resolve("tests/fixtures").resolve(relativePath));
    }

    private SilverObservation createObservation(
            String sourceSystem,
            String sourceRecordKey,
            String revisionKey,
            Instant eventTimeUtc,
            double latitude,
            double longitude,
            Double depthKm,
            Double magnitude,
            String qualityStatus,
            String agencyCode,
            String runId) {

        boolean isJma = "JMA_BULLETIN".equalsIgnoreCase(sourceSystem);
        String obsId = SourceKeyGenerator.observationId(sourceSystem, sourceRecordKey, revisionKey);

        return new SilverObservation(
                "1.0",
                obsId,
                sourceSystem,
                sourceRecordKey,
                revisionKey,
                isJma ? null : eventTimeUtc.plusSeconds(60),
                isJma ? "release-v1" : null,
                isJma ? EXEC_TIME : null,
                false,
                eventTimeUtc,
                LocalDateTime.of(2023, 9, 1, 12, 0, 0),
                LocalDate.of(2023, 9, 1),
                LocalDate.of(2023, 9, 1),
                2023,
                9,
                latitude,
                longitude,
                depthKm,
                magnitude,
                "mw",
                "EARTHQUAKE",
                "Test Place",
                false,
                null,
                null,
                null,
                isJma ? (agencyCode != null ? agencyCode : "J") : null,
                isJma ? "UNIFIED" : null,
                isJma ? "K" : "reviewed",
                "https://example.invalid",
                true,
                qualityStatus != null ? qualityStatus : "VALID",
                List.of(),
                "manifest-" + runId,
                "s3://bucket/raw",
                "0".repeat(64),
                "line=1",
                "hash-" + revisionKey,
                runId,
                "parser",
                "1.0",
                EXEC_TIME);
    }

    @Test
    void testFxLink02AcceptedMatchEndToEndIntegration() throws Exception {
        byte[] usgsBytes = loadFixture("usgs/success.geojson");
        byte[] jmaBytes = loadFixture("jma/fixed-width/success.hyp");

        String dummySha = "0".repeat(64);
        UsgsParseContext usgsCtx = new UsgsParseContext("m-suc-u", "s3://b/usgs/suc.geojson", dummySha, "run-slv09-02", EXEC_TIME);
        JmaParseContext jmaCtx = new JmaParseContext("m-suc-j", "s3://b/jma/suc.hyp", dummySha, "run-slv09-02", "rel-1", null, "https://example.invalid", "success.hyp", EXEC_TIME);

        FileSilverObjectStore store = new FileSilverObjectStore(tempDir.resolve("silver-store"));

        SilverIntegrationRequest request = new SilverIntegrationRequest(
                "run-slv09-02",
                usgsBytes,
                usgsCtx,
                jmaBytes,
                jmaCtx,
                SilverLinkConfig.defaultConfig(),
                store,
                true,
                EXEC_TIME);

        SilverIntegrationResult result = runner.run(request);

        // Reconciliation assertions
        SilverRunReconciliationReport report = result.reconciliationReport();
        assertTrue(report.isReconciliationBalanced(), "Run reconciliation must balance across all dimensions");
        assertTrue(report.isParsedBalanced());
        assertTrue(report.isDedupBalanced());
        assertTrue(report.isCanonicalBalanced());
        assertTrue(report.isBridgeCountBalanced());

        assertEquals(3, report.totalParsedCount());
        assertEquals(1, report.usgsParsedCount());
        assertEquals(2, report.jmaParsedCount());

        assertEquals(3, report.totalValidCount());
        assertEquals(0, report.totalRejectCount());

        assertEquals(3, report.totalCurrentCount());
        assertEquals(0, report.totalDuplicateCount());
        assertEquals(0, report.totalSupersededCount());

        assertEquals(1, report.acceptedLinksCount());
        assertEquals(0, report.ambiguousLinksCount());
        assertEquals(2, report.canonicalEventsCount());
        assertEquals(1, report.matchedEventsCount());
        assertEquals(0, report.usgsOnlyEventsCount());
        assertEquals(1, report.jmaOnlyEventsCount()); // artificial quarry blast remains single source

        // Storage verification
        assertNotNull(result.writeResult());
        assertEquals(3, result.writeResult().totalObservations());
        assertEquals(0, result.writeResult().totalRejects());
        assertTrue(result.writeResult().publishedPartitions().size() >= 1);
    }

    @Test
    void testFxLink01AmbiguousEndToEndIntegration() throws Exception {
        byte[] usgsBytes = loadFixture("usgs/ambiguous.geojson");
        byte[] jmaBytes = loadFixture("jma/fixed-width/ambiguous.hyp");

        String dummySha = "0".repeat(64);
        UsgsParseContext usgsCtx = new UsgsParseContext("m-amb-u", "s3://b/usgs/amb.geojson", dummySha, "run-slv09-01", EXEC_TIME);
        JmaParseContext jmaCtx = new JmaParseContext("m-amb-j", "s3://b/jma/amb.hyp", dummySha, "run-slv09-01", "rel-1", null, "https://example.invalid", "ambiguous.hyp", EXEC_TIME);

        FileSilverObjectStore store = new FileSilverObjectStore(tempDir.resolve("silver-amb-store"));

        SilverIntegrationRequest request = new SilverIntegrationRequest(
                "run-slv09-01",
                usgsBytes,
                usgsCtx,
                jmaBytes,
                jmaCtx,
                SilverLinkConfig.defaultConfig(),
                store,
                true,
                EXEC_TIME);

        SilverIntegrationResult result = runner.run(request);

        SilverRunReconciliationReport report = result.reconciliationReport();
        assertTrue(report.isReconciliationBalanced());

        assertEquals(3, report.totalParsedCount());
        assertEquals(3, report.totalValidCount());
        assertEquals(3, report.totalCurrentCount());

        // Ambiguous match assertions: strictly no auto-merge
        assertEquals(2, report.candidatePairsEvaluated());
        assertEquals(2, report.ambiguousLinksCount());
        assertEquals(0, report.acceptedLinksCount());
        assertEquals(3, report.canonicalEventsCount()); // 1 USGS + 2 JMA = 3 single-source canonical events
        assertEquals(0, report.matchedEventsCount());
        assertEquals(1, report.usgsOnlyEventsCount());
        assertEquals(2, report.jmaOnlyEventsCount());
    }

    @Test
    void testRerunIdempotencyProducesIdenticalLogicAndOverwritesCleanly() throws Exception {
        byte[] usgsBytes = loadFixture("usgs/success.geojson");
        byte[] jmaBytes = loadFixture("jma/fixed-width/success.hyp");

        String dummySha = "0".repeat(64);
        UsgsParseContext usgsCtx = new UsgsParseContext("m-rerun-u", "s3://b/usgs/suc.geojson", dummySha, "run-rerun", EXEC_TIME);
        JmaParseContext jmaCtx = new JmaParseContext("m-rerun-j", "s3://b/jma/suc.hyp", dummySha, "run-rerun", "rel-1", null, "https://example.invalid", "success.hyp", EXEC_TIME);

        FileSilverObjectStore store = new FileSilverObjectStore(tempDir.resolve("silver-rerun-store"));

        SilverIntegrationRequest request = new SilverIntegrationRequest(
                "run-rerun",
                usgsBytes,
                usgsCtx,
                jmaBytes,
                jmaCtx,
                SilverLinkConfig.defaultConfig(),
                store,
                true,
                EXEC_TIME);

        // Run 1
        SilverIntegrationResult run1 = runner.run(request);
        // Run 2 (Rerun with identical input and same storage)
        SilverIntegrationResult run2 = runner.run(request);

        // AC 1: Logic and counts are strictly identical across reruns
        assertEquals(run1.reconciliationReport().totalParsedCount(), run2.reconciliationReport().totalParsedCount());
        assertEquals(run1.reconciliationReport().totalValidCount(), run2.reconciliationReport().totalValidCount());
        assertEquals(run1.reconciliationReport().totalCurrentCount(), run2.reconciliationReport().totalCurrentCount());
        assertEquals(run1.reconciliationReport().canonicalEventsCount(), run2.reconciliationReport().canonicalEventsCount());
        assertEquals(run1.reconciliationReport().acceptedLinksCount(), run2.reconciliationReport().acceptedLinksCount());
        assertEquals(run1.reconciliationReport().toSummaryString(), run2.reconciliationReport().toSummaryString());

        // Verify deterministic canonical event IDs
        assertEquals(
                run1.resolutionResult().canonicalEventIds(),
                run2.resolutionResult().canonicalEventIds());

        // Verify storage row counts: partition overwrite ensures no duplicate rows appended
        assertEquals(3, run2.writeResult().totalObservations());
    }

    @Test
    void testReconciliationAccountingWithRejectsAndRevisions() throws Exception {
        String runId = "run-reconcile-complex";
        Instant time = Instant.parse("2023-09-01T12:00:00.000Z");

        // 1. USGS observations: 1 newer current, 1 older superseded revision, 1 exact duplicate
        SilverObservation usgsNewer = createObservation("USGS", "usgs-event-1", "rev-2", time, 35.0, 139.0, 10.0, 5.0, "VALID", null, runId);
        SilverObservation usgsOlder = createObservation("USGS", "usgs-event-1", "rev-1", time, 35.0, 139.0, 10.0, 4.8, "VALID", null, runId);
        SilverObservation usgsDup = createObservation("USGS", "usgs-event-1", "rev-2", time, 35.0, 139.0, 10.0, 5.0, "VALID", null, runId);

        // 2. JMA observations: 1 matching usgs-event-1, 1 distinct solo event
        SilverObservation jmaMatch = createObservation("JMA_BULLETIN", "jma-event-1", "rel-1", time, 35.0, 139.0, 10.0, 5.0, "VALID", "J", runId);
        SilverObservation jmaSolo = createObservation("JMA_BULLETIN", "jma-event-2", "rel-1", time.plusSeconds(500), 38.0, 141.0, 20.0, 4.0, "VALID", "J", runId);

        // 3. Parser Reject records: 1 USGS reject, 1 JMA reject
        SilverRejectRecord usgsReject = new SilverRejectRecord(
                "1.0", "USGS", "bad-key", "m-1", "s3://b/raw", "0".repeat(64),
                "loc-1", "hash-1", "PARSE", List.of("INVALID_LATITUDE"), runId, "1.0", EXEC_TIME);
        SilverRejectRecord jmaReject = new SilverRejectRecord(
                "1.0", "JMA_BULLETIN", "bad-jma", "m-2", "s3://b/raw", "0".repeat(64),
                "loc-2", "hash-2", "PARSE", List.of("INVALID_EVENT_TIME"), runId, "1.0", EXEC_TIME);

        List<SilverObservation> observations = List.of(usgsNewer, usgsOlder, usgsDup, jmaMatch, jmaSolo);
        List<SilverRejectRecord> parserRejects = List.of(usgsReject, jmaReject);

        FileSilverObjectStore store = new FileSilverObjectStore(tempDir.resolve("silver-complex-store"));

        SilverIntegrationResult result = runner.run(
                runId,
                observations,
                parserRejects,
                SilverLinkConfig.defaultConfig(),
                store,
                true,
                EXEC_TIME);

        SilverRunReconciliationReport report = result.reconciliationReport();

        // AC 2: parsed/valid/rejected/duplicate/canonical đối soát được
        assertTrue(report.isParsedBalanced(), "Parsed balance failed");
        assertTrue(report.isDedupBalanced(), "Dedup balance failed");
        assertTrue(report.isCanonicalBalanced(), "Canonical balance failed");
        assertTrue(report.isBridgeCountBalanced(), "Bridge count balance failed");
        assertTrue(report.isReconciliationBalanced(), "Overall reconciliation failed");

        assertEquals(7, report.totalParsedCount()); // 5 obs + 2 rejects
        assertEquals(4, report.usgsParsedCount());  // 3 obs + 1 reject
        assertEquals(3, report.jmaParsedCount());   // 2 obs + 1 reject

        assertEquals(5, report.totalValidCount());
        assertEquals(2, report.totalRejectCount());

        assertEquals(3, report.totalCurrentCount());     // 1 usgs + 2 jma
        assertEquals(1, report.totalDuplicateCount());   // 1 usgsDup
        assertEquals(1, report.totalSupersededCount());  // 1 usgsOlder

        assertEquals(1, report.acceptedLinksCount());     // usgs-event-1 linked to jma-event-1
        assertEquals(2, report.canonicalEventsCount());   // 1 matched + 1 jma solo
        assertEquals(1, report.matchedEventsCount());
        assertEquals(1, report.jmaOnlyEventsCount());
        assertEquals(0, report.usgsOnlyEventsCount());
    }
}
