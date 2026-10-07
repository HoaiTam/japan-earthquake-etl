package ie212.earthquake.spark.silver;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.*;

class SilverQualityValidatorTest {
    private static final Instant EVENT_TIME = Instant.parse("2023-09-01T03:34:56.780Z");
    private static final Instant VALIDATED_AT = Instant.parse("2026-10-07T03:00:00Z");
    private static final SilverQualityValidator VALIDATOR = new SilverQualityValidator(Clock.fixed(VALIDATED_AT, ZoneOffset.UTC));

    @Test
    void validAndRejectedCountsReconcileAndRejectKeepsAllContractLineage() {
        var valid = usgs("usgs-1", 35.0, 140.0, 10.0, 4.2);
        var invalid = usgs("", 95.0, 140.0, null, null);
        var result = VALIDATOR.validate(List.of(valid, invalid), "run-1");
        assertEquals(2, result.parsedCount());
        assertEquals(1, result.validCount());
        assertEquals(1, result.rejectedCount());
        assertTrue(result.publishBlocked());
        assertEquals(1L, result.reasonCounts().get("MISSING_SOURCE_KEY"));
        assertEquals(1L, result.reasonCounts().get("INVALID_LATITUDE"));
        var reject = result.rejectedRecords().get(0);
        assertEquals("1.0", reject.schemaVersion());
        assertEquals("USGS", reject.sourceSystem());
        assertEquals(invalid.bronzeManifestId(), reject.bronzeManifestId());
        assertEquals(invalid.rawObjectUri(), reject.rawObjectUri());
        assertEquals(invalid.rawSha256(), reject.rawSha256());
        assertEquals(invalid.rawRecordLocator(), reject.rawRecordLocator());
        assertEquals(invalid.rawRecordHash(), reject.rawRecordHash());
        assertEquals(invalid.ingestRunId(), reject.ingestRunId());
        assertEquals(invalid.parserVersion(), reject.parserVersion());
        assertEquals(VALIDATED_AT, reject.rejectedAtUtc());
        assertEquals("VALIDATE", reject.rejectStage());
        assertThrows(IllegalStateException.class, result::requirePublishable);
        assertEquals(13, reject.toRow().size());
    }

    @Test
    void nullableNumbersNegativeDepthAndWarningsAreNotBlockers() {
        var observation = usgs("usgs-2", 35.0, 130.0, -10.0, null);
        var missing = usgs("usgs-3", 35.0, 130.0, null, null);
        var result = VALIDATOR.validate(List.of(observation, missing), "run-1");
        assertEquals(2, result.validCount());
        assertFalse(result.publishBlocked());
        assertDoesNotThrow(result::requirePublishable);
        assertSame(observation, result.validObservations().get(0));
        assertEquals(-10.0, result.validObservations().get(0).depthKm());
        assertNull(result.validObservations().get(1).depthKm());
        assertNull(result.validObservations().get(1).magnitude());
    }

    @Test
    void rejectsNonFiniteNumbersAndOutOfRangeCoordinates() {
        var result = VALIDATOR.validate(List.of(
                usgs("nan-lat", Double.NaN, 139.0, null, null),
                usgs("bad-lon", 35.0, 181.0, null, null),
                usgs("infinite-depth", 35.0, 139.0, Double.POSITIVE_INFINITY, null),
                usgs("nan-mag", 35.0, 139.0, null, Double.NaN)), "run-1");
        assertEquals(4, result.rejectedCount());
        assertEquals(1L, result.reasonCounts().get("INVALID_LATITUDE"));
        assertEquals(1L, result.reasonCounts().get("INVALID_LONGITUDE"));
        assertEquals(2L, result.reasonCounts().get("NON_FINITE_NUMBER"));
    }

    @Test
    void jmaUsesSourceStatusForNativeFlagAndPreservesAgencyWithoutFiltering() {
        for (String flag : new String[] {"K", "S", "k", "s", "A", "a", "N", "F"}) {
            var observation = jma("release-1", "UNIFIED", flag);
            var result = VALIDATOR.validate(List.of(observation), "run-1");
            assertEquals(1, result.validCount(), flag);
            assertEquals("U", result.validObservations().get(0).determiningAgencyCode());
            assertEquals(flag, result.validObservations().get(0).sourceStatus());
        }
        assertEquals(1, VALIDATOR.validate(List.of(jma("release-1", "LEGACY", null)), "run-1").validCount());
        for (var observation : List.of(jma(null, "UNIFIED", "K"), jma("release-1", "INVALID", "K"), jma("release-1", "UNIFIED", "X"))) {
            assertEquals(1L, VALIDATOR.validate(List.of(observation), "run-1").reasonCounts().get("CONTRACT_MISMATCH"));
        }
    }

    @Test
    void rejectsForeignRunAndInvalidContextWithoutInventingLineage() {
        var observation = usgs("usgs-1", 35.0, 139.0, null, null);
        assertTrue(VALIDATOR.validate(List.of(observation), "other-run").publishBlocked());
        assertThrows(IllegalArgumentException.class, () -> VALIDATOR.validate(List.of(observation), " "));
        assertThrows(IllegalArgumentException.class, () -> VALIDATOR.validate(Arrays.asList(observation, null), "run-1"));
        assertThrows(IllegalArgumentException.class, () -> VALIDATOR.validate((List<SilverObservation>) null, "run-1"));
    }

    @Test
    void parserRejectsAreIncludedInRunGateAndNeverLostByValidatingOnlySuccessfulRows() throws Exception {
        var parsed = parse("invalid-fields.geojson");
        var result = VALIDATOR.validate(parsed, "run-1");
        assertEquals(parsed.parsedCount(), result.parsedCount());
        assertEquals(3, result.rejectedCount());
        assertEquals(0, result.validCount());
        assertEquals(parsed.rejects(), result.rejectedRecords());
        assertTrue(result.reasonCounts().containsKey("INVALID_EVENT_TIME"));
        assertTrue(result.publishBlocked());
        assertThrows(IllegalArgumentException.class, () -> VALIDATOR.validate(parsed, "other-run"));
    }

    @Test
    void reconcilesMixedParserAndQualityRejectsAndCountsEachReasonOncePerRow() throws Exception {
        var parserReject = parse("invalid-fields.geojson").rejects().get(0);
        var repeatedReason = new SilverRejectRecord(parserReject.schemaVersion(), parserReject.sourceSystem(),
                parserReject.sourceRecordKeyCandidate(), parserReject.bronzeManifestId(), parserReject.rawObjectUri(),
                parserReject.rawSha256(), parserReject.rawRecordLocator(), parserReject.rawRecordHash(),
                parserReject.rejectStage(), List.of("MISSING_SOURCE_KEY", "MISSING_SOURCE_KEY"),
                parserReject.ingestRunId(), parserReject.parserVersion(), parserReject.rejectedAtUtc());
        var parsed = new UsgsParseResult(List.of(usgs("good", 35.0, 139.0, null, null),
                usgs("bad", 91.0, 139.0, null, null)), List.of(repeatedReason));
        var result = VALIDATOR.validate(parsed, "run-1");
        assertEquals(3, result.parsedCount());
        assertEquals(1, result.validCount());
        assertEquals(2, result.rejectedCount());
        assertEquals(1L, result.reasonCounts().get("MISSING_SOURCE_KEY"));
        assertEquals(1L, result.reasonCounts().get("INVALID_LATITUDE"));
        assertEquals(repeatedReason, result.rejectedRecords().get(0));
        assertTrue(result.publishBlocked());
    }

    @Test
    void emptyInputAndRerunPreserveCountsAndDoNotDeduplicateSourceRecords() throws Exception {
        var empty = VALIDATOR.validate(parse("empty.geojson"), "run-1");
        assertEquals(0, empty.parsedCount());
        assertFalse(empty.publishBlocked());
        var duplicate = parse("duplicate.geojson");
        var first = VALIDATOR.validate(duplicate, "run-1");
        var rerun = VALIDATOR.validate(duplicate, "run-1");
        assertEquals(duplicate.parsedCount(), first.validCount());
        assertEquals(first, rerun);
    }

    @Test
    void writesFullRejectSummaryDeterministicallyAndRefusesWrongSource(@TempDir Path temp) throws Exception {
        var quality = VALIDATOR.validate(List.of(usgs("bad", 91.0, 10.0, null, null)), "run-1");
        var writer = new SilverQualitySummaryWriter();
        Path path = writer.write(temp.resolve("quality-summary.json"), quality, "USGS");
        byte[] first = Files.readAllBytes(path);
        JsonNode json = new ObjectMapper().readTree(first);
        assertTrue(json.path("publish_blocked").asBoolean());
        JsonNode reject = json.path("rejected_records").get(0);
        assertEquals("a".repeat(64), reject.path("raw_sha256").asText());
        assertEquals("b".repeat(64), reject.path("raw_record_hash").asText());
        assertEquals("run-1", reject.path("ingest_run_id").asText());
        assertEquals("slv-02-v1", reject.path("parser_version").asText());
        assertEquals(VALIDATED_AT.toString(), reject.path("rejected_at_utc").asText());
        assertEquals("INVALID_LATITUDE", reject.path("reject_reason_codes").get(0).asText());
        writer.write(path, quality, "USGS");
        assertArrayEquals(first, Files.readAllBytes(path));
        assertThrows(IllegalArgumentException.class, () -> writer.write(path, quality, "JMA_BULLETIN"));
    }

    @Test
    void parserQualityAndPr35WriterPublishVerifiedParquetTogether(@TempDir Path temp) throws Exception {
        var quality = VALIDATOR.validate(parse("success.geojson"), "run-1");
        assertEquals(1, quality.validCount());
        var request = new SilverWriteRequest("run-1", quality.validObservations(), quality.rejectedRecords(), VALIDATED_AT, true);
        var store = new FileSilverObjectStore(temp);
        var result = new SilverParquetWriter(store).write(request, quality);
        assertEquals("SilverReady", result.silverStatus());
        assertEquals(1, result.totalObservations());
        var file = result.publishedPartitions().get(0).files().get(0);
        byte[] parquet = store.read(file.relativePath());
        assertEquals(file.sha256(), SilverParquetWriter.sha256(parquet));
        SilverParquetSerializer.verifyParquet(parquet, 1, SilverParquetSerializer.OBSERVATION_PARQUET_SCHEMA);
    }

    @Test
    void blockedOrMismatchedGateTouchesNoPr35Storage(@TempDir Path temp) throws Exception {
        var store = new FileSilverObjectStore(temp);
        var writer = new SilverParquetWriter(store);
        var blocked = VALIDATOR.validate(parse("invalid-fields.geojson"), "run-1");
        var blockedRequest = new SilverWriteRequest("run-1", blocked.validObservations(), blocked.rejectedRecords(), VALIDATED_AT, true);
        assertThrows(IllegalStateException.class, () -> writer.write(blockedRequest, blocked));
        var valid = VALIDATOR.validate(parse("success.geojson"), "run-1");
        assertThrows(IllegalArgumentException.class, () -> writer.write(
                new SilverWriteRequest("other-run", valid.validObservations(), List.of(), VALIDATED_AT, true), valid));
        assertThrows(IllegalArgumentException.class, () -> writer.write(
                new SilverWriteRequest("run-1", List.of(), List.of(), VALIDATED_AT, true), valid));
        try (var files = Files.walk(temp)) {
            assertEquals(1L, files.count());
        }
    }

    @Test
    void resultCannotClaimCountsThatDisagreeWithDatasets() {
        assertThrows(IllegalArgumentException.class, () -> new SilverQualityResult("run-1", 1, 1, 0, false, List.of(), List.of(), Map.of()));
    }

    private static UsgsParseResult parse(String fixture) throws Exception {
        byte[] bytes = Files.readAllBytes(Path.of("../tests/fixtures/usgs").resolve(fixture));
        var context = new UsgsParseContext("manifest-1", "s3://bucket/bronze/usgs/response.geojson", "a".repeat(64), "run-1", VALIDATED_AT);
        return new UsgsGeoJsonParser().parse(bytes, context);
    }

    private static SilverObservation usgs(String key, double latitude, double longitude, Double depth, Double magnitude) {
        return observation("USGS", key, latitude, longitude, depth, magnitude, null, null, "reviewed");
    }

    private static SilverObservation jma(String release, String era, String flag) {
        return observation("JMA_BULLETIN", "jma-1", 35.0, 139.0, 10.0, 4.2, release, era, flag);
    }

    private static SilverObservation observation(String source, String key, double latitude, double longitude,
            Double depth, Double magnitude, String release, String era, String status) {
        var utc = EVENT_TIME.atZone(ZoneOffset.UTC);
        var jst = EVENT_TIME.atZone(ZoneId.of("Asia/Tokyo"));
        boolean jma = "JMA_BULLETIN".equals(source);
        boolean negativeDepth = depth != null && depth < 0;
        return new SilverObservation(
                "1.0", "obs_" + key, source, key, "revision-1", jma ? null : EVENT_TIME.plusSeconds(60),
                release, jma && release != null ? VALIDATED_AT : null, false, EVENT_TIME, jst.toLocalDateTime(),
                utc.toLocalDate(), jst.toLocalDate(), utc.getYear(), utc.getMonthValue(), latitude, longitude,
                depth, magnitude, null, "EARTHQUAKE", null, null, null, null, null, jma ? "U" : null, era,
                status, null, true, negativeDepth ? "WARNING" : "VALID", negativeDepth ? List.of("NEGATIVE_DEPTH") : List.of(),
                "manifest-1", "s3://bucket/bronze/raw", "a".repeat(64), "features[0]", "b".repeat(64), "run-1",
                jma ? "jma-hypocenter-fixed-width" : "usgs-geojson", "slv-02-v1", VALIDATED_AT);
    }
}
