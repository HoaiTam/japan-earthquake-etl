package ie212.earthquake.spark.silver;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class UsgsGeoJsonParserTest {
    private UsgsGeoJsonParser parser;

    @BeforeEach
    void setUp() {
        parser = new UsgsGeoJsonParser();
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

    @Test
    void parsesSuccessFixtureWithCompleteStandardMapping() throws Exception {
        byte[] payload = loadFixture("usgs/success.geojson");
        UsgsParseContext context = new UsgsParseContext(
                "manifest-success-01",
                "s3://bucket/bronze/usgs/success.geojson",
                "sha256-dummy",
                "run-success-01",
                Instant.parse("2023-09-16T00:15:08Z"));

        UsgsParseResult result = parser.parse(payload, context);

        assertEquals(1, result.parsedCount());
        assertEquals(1, result.validCount());
        assertEquals(0, result.rejectedCount());

        SilverObservation obs = result.observations().get(0);
        assertEquals("1.0", obs.schemaVersion());
        assertEquals("USGS", obs.sourceSystem());
        assertEquals("fx-usgs-success-001", obs.sourceRecordKey());
        assertTrue(obs.sourceObservationId().startsWith("obs_"));
        assertEquals(Instant.parse("2023-09-01T03:34:56.780Z"), obs.eventTimeUtc());
        assertEquals(LocalDateTime.of(2023, 9, 1, 12, 34, 56, 780_000_000), obs.eventTimeJst());
        assertEquals(LocalDate.of(2023, 9, 1), obs.eventDateUtc());
        assertEquals(LocalDate.of(2023, 9, 1), obs.eventDateJst());
        assertEquals(2023, obs.eventYearUtc());
        assertEquals(9, obs.eventMonthUtc());
        assertEquals(35.67, obs.latitude(), 1e-6);
        assertEquals(139.76, obs.longitude(), 1e-6);
        assertEquals(10.0, obs.depthKm(), 1e-6);
        assertEquals(5.2, obs.magnitude(), 1e-6);
        assertEquals("mww", obs.magnitudeType());
        assertEquals("EARTHQUAKE", obs.eventTypeCode());
        assertEquals("Synthetic Tokyo Bay", obs.placeName());
        assertEquals(Boolean.FALSE, obs.tsunamiFlag());
        assertEquals("green", obs.alertLevel());
        assertEquals(416, obs.significance());
        assertEquals("reviewed", obs.sourceStatus());
        assertEquals("https://example.invalid/usgs/event/fx-usgs-success-001", obs.sourceUrl());
        assertTrue(obs.isInStudyArea());
        assertEquals("VALID", obs.qualityStatus());
        assertTrue(obs.qualityFlags().isEmpty());
        assertEquals("manifest-success-01", obs.bronzeManifestId());
        assertEquals("s3://bucket/bronze/usgs/success.geojson", obs.rawObjectUri());
        assertEquals("sha256-dummy", obs.rawSha256());
        assertEquals("features[0]", obs.rawRecordLocator());
        assertEquals("run-success-01", obs.ingestRunId());
        assertEquals("usgs-geojson", obs.parserName());
        assertEquals("slv-02-v1", obs.parserVersion());
        assertEquals(Instant.parse("2023-09-16T00:15:08Z"), obs.processedAtUtc());
        assertFalse(obs.isCurrentSourceRevision());
        assertNull(obs.catalogRelease());
        assertNull(obs.catalogReleaseAtUtc());
    }

    @Test
    void parsesEmptyFixtureWithZeroRecords() throws Exception {
        byte[] payload = loadFixture("usgs/empty.geojson");
        UsgsParseContext context = UsgsParseContext.synthetic("run-empty-01");

        UsgsParseResult result = parser.parse(payload, context);

        assertEquals(0, result.parsedCount());
        assertEquals(0, result.validCount());
        assertEquals(0, result.rejectedCount());
        assertTrue(result.observations().isEmpty());
        assertTrue(result.rejects().isEmpty());
    }

    @Test
    void rejectsInvalidFieldsFixtureWithSpecificReasonCodes() throws Exception {
        byte[] payload = loadFixture("usgs/invalid-fields.geojson");
        UsgsParseContext context = UsgsParseContext.synthetic("run-invalid-01");

        UsgsParseResult result = parser.parse(payload, context);

        assertEquals(3, result.parsedCount());
        assertEquals(0, result.validCount());
        assertEquals(3, result.rejectedCount());

        List<SilverRejectRecord> rejects = result.rejects();

        // Feature 0: Missing source key (empty string id)
        SilverRejectRecord r0 = rejects.get(0);
        assertEquals("features[0]", r0.rawRecordLocator());
        assertNull(r0.sourceRecordKeyCandidate());
        assertEquals("VALIDATE", r0.rejectStage());
        assertTrue(r0.rejectReasonCodes().contains("MISSING_SOURCE_KEY"));

        // Feature 1: Invalid event time ("not-an-epoch-millis")
        SilverRejectRecord r1 = rejects.get(1);
        assertEquals("features[1]", r1.rawRecordLocator());
        assertEquals("fx-usgs-invalid-time", r1.sourceRecordKeyCandidate());
        assertEquals("VALIDATE", r1.rejectStage());
        assertTrue(r1.rejectReasonCodes().contains("INVALID_EVENT_TIME"));

        // Feature 2: Invalid coordinates ([181.0, 91.0, "not-a-number"])
        SilverRejectRecord r2 = rejects.get(2);
        assertEquals("features[2]", r2.rawRecordLocator());
        assertEquals("fx-usgs-invalid-coordinate", r2.sourceRecordKeyCandidate());
        assertEquals("VALIDATE", r2.rejectStage());
        assertTrue(r2.rejectReasonCodes().contains("INVALID_LONGITUDE"));
        assertTrue(r2.rejectReasonCodes().contains("INVALID_LATITUDE"));
        assertTrue(r2.rejectReasonCodes().contains("INVALID_NUMBER"));

        Set<String> allCodes = new HashSet<>();
        for (SilverRejectRecord r : rejects) {
            allCodes.addAll(r.rejectReasonCodes());
        }
        assertTrue(allCodes.contains("MISSING_SOURCE_KEY"));
        assertTrue(allCodes.contains("INVALID_EVENT_TIME"));
        assertTrue(allCodes.contains("INVALID_LATITUDE"));
        assertTrue(allCodes.contains("INVALID_LONGITUDE"));
        assertTrue(allCodes.contains("INVALID_NUMBER"));
    }

    @Test
    void parsesDuplicateFixtureWithoutDroppingRevisionsAtParserStage() throws Exception {
        byte[] payload = loadFixture("usgs/duplicate.geojson");
        UsgsParseContext context = UsgsParseContext.synthetic("run-dup-01");

        UsgsParseResult result = parser.parse(payload, context);

        assertEquals(2, result.parsedCount());
        assertEquals(2, result.validCount());
        assertEquals(0, result.rejectedCount());

        assertEquals("fx-usgs-duplicate-001", result.observations().get(0).sourceRecordKey());
        assertEquals("fx-usgs-duplicate-001", result.observations().get(1).sourceRecordKey());
    }

    @Test
    void parsesRevisionFixturesIndividually() throws Exception {
        byte[] v1 = loadFixture("usgs/revision-v1.geojson");
        byte[] v2 = loadFixture("usgs/revision-v2.geojson");

        UsgsParseResult r1 = parser.parse(v1, UsgsParseContext.synthetic("run-v1"));
        UsgsParseResult r2 = parser.parse(v2, UsgsParseContext.synthetic("run-v2"));

        assertEquals(1, r1.validCount());
        assertEquals(1, r2.validCount());

        SilverObservation o1 = r1.observations().get(0);
        SilverObservation o2 = r2.observations().get(0);

        assertEquals("fx-usgs-revision-001", o1.sourceRecordKey());
        assertEquals("fx-usgs-revision-001", o2.sourceRecordKey());
        assertEquals(4.0, o1.magnitude());
        assertEquals(4.2, o2.magnitude());
        assertEquals(Instant.ofEpochMilli(1696443000000L), o1.sourceUpdatedAtUtc());
        assertEquals(Instant.ofEpochMilli(1696446600000L), o2.sourceUpdatedAtUtc());
    }

    @Test
    void parsesTimezoneBoundaryFixtureWithCorrectHalfOpenDates() throws Exception {
        byte[] payload = loadFixture("usgs/timezone-boundary.geojson");
        UsgsParseContext context = UsgsParseContext.synthetic("run-tz-01");

        UsgsParseResult result = parser.parse(payload, context);

        assertEquals(2, result.parsedCount());
        assertEquals(2, result.validCount());

        SilverObservation event0 = result.observations().get(0);
        assertEquals(Instant.parse("2023-12-31T23:59:59.999Z"), event0.eventTimeUtc());
        assertEquals(LocalDate.of(2023, 12, 31), event0.eventDateUtc());
        assertEquals(LocalDate.of(2024, 1, 1), event0.eventDateJst());
        assertTrue(event0.isInStudyArea());

        SilverObservation event1 = result.observations().get(1);
        assertEquals(Instant.parse("2024-01-01T00:00:00Z"), event1.eventTimeUtc());
        assertEquals(LocalDate.of(2024, 1, 1), event1.eventDateUtc());
        assertEquals(LocalDate.of(2024, 1, 1), event1.eventDateJst());
        assertTrue(event1.isInStudyArea());
    }

    @Test
    void preservesNullValuesWithoutCoercingToZero() throws Exception {
        String json = """
                {
                  "type": "FeatureCollection",
                  "features": [
                    {
                      "type": "Feature",
                      "id": "fx-null-test",
                      "geometry": {
                        "type": "Point",
                        "coordinates": [135.0, 35.0]
                      },
                      "properties": {
                        "time": 1693539296780,
                        "mag": null,
                        "place": null,
                        "tsunami": null,
                        "alert": null,
                        "sig": null,
                        "magType": null
                      }
                    }
                  ]
                }
                """;
        UsgsParseResult result = parser.parse(json.getBytes(StandardCharsets.UTF_8), UsgsParseContext.synthetic("run-nulls"));

        assertEquals(1, result.validCount());
        SilverObservation obs = result.observations().get(0);
        assertNull(obs.magnitude(), "magnitude must stay null, not coerced to 0");
        assertNull(obs.depthKm(), "depth must stay null when absent from coordinates, not coerced to 0");
        assertNull(obs.tsunamiFlag(), "tsunami must stay null, not coerced to false");
        assertNull(obs.alertLevel(), "alert must stay null");
        assertNull(obs.significance(), "sig must stay null, not coerced to 0");
        assertNull(obs.placeName(), "place must stay null");
        assertNull(obs.magnitudeType(), "magType must stay null");
    }

    @Test
    void flagsNegativeDepthWithWarningAndDoesNotCoerceToZero() throws Exception {
        String json = """
                {
                  "type": "FeatureCollection",
                  "features": [
                    {
                      "type": "Feature",
                      "id": "fx-neg-depth",
                      "geometry": {
                        "type": "Point",
                        "coordinates": [135.0, 35.0, -3.5]
                      },
                      "properties": {
                        "time": 1693539296780,
                        "mag": 2.1
                      }
                    }
                  ]
                }
                """;
        UsgsParseResult result = parser.parse(json.getBytes(StandardCharsets.UTF_8), UsgsParseContext.synthetic("run-neg-depth"));

        assertEquals(1, result.validCount());
        SilverObservation obs = result.observations().get(0);
        assertEquals(-3.5, obs.depthKm(), 1e-6, "negative depth must be preserved");
        assertEquals("WARNING", obs.qualityStatus());
        assertTrue(obs.qualityFlags().contains("NEGATIVE_DEPTH"));
    }

    @Test
    void mapsEventTypesAccurately() {
        assertEquals("EARTHQUAKE", UsgsGeoJsonParser.mapEventType("earthquake"));
        assertEquals("ARTIFICIAL", UsgsGeoJsonParser.mapEventType("quarry blast"));
        assertEquals("ARTIFICIAL", UsgsGeoJsonParser.mapEventType("nuclear explosion"));
        assertEquals("ERUPTION", UsgsGeoJsonParser.mapEventType("volcanic eruption"));
        assertEquals("ERUPTION", UsgsGeoJsonParser.mapEventType("volcano"));
        assertEquals("OTHER", UsgsGeoJsonParser.mapEventType("sonic boom unknown meteorite"));
        assertEquals("UNKNOWN", UsgsGeoJsonParser.mapEventType(null));
        assertEquals("UNKNOWN", UsgsGeoJsonParser.mapEventType("  "));
    }

    @Test
    void convertsObservationAndRejectToSparkRowsAndMatchesSchema() throws Exception {
        byte[] payload = loadFixture("usgs/success.geojson");
        UsgsParseResult result = parser.parse(payload, UsgsParseContext.synthetic("run-rows"));

        SilverObservation obs = result.observations().get(0);
        Row obsRow = obs.toRow();
        assertEquals(SilverSchemas.OBSERVATION_SCHEMA.length(), obsRow.length());
        assertEquals("fx-usgs-success-001", obsRow.getString(3));

        SilverRejectRecord reject = new SilverRejectRecord(
                "1.0",
                "USGS",
                "candidate-1",
                "manifest-1",
                "uri-1",
                "sha-1",
                "features[0]",
                "hash-1",
                "VALIDATE",
                List.of("INVALID_LATITUDE"),
                "run-1",
                "slv-02-v1",
                Instant.now());
        Row rejectRow = reject.toRow();
        assertEquals(SilverSchemas.REJECT_SCHEMA.length(), rejectRow.length());
        assertEquals("USGS", rejectRow.getString(1));
    }

    @Test
    void parsesStagedBronzeInputResolvedBySlv01(@TempDir Path temp) throws Exception {
        byte[] payload = loadFixture("usgs/success.geojson");
        Path stagedObject = temp.resolve("success.geojson");
        Files.write(stagedObject, payload);

        ResolvedBronzeInput input = new ResolvedBronzeInput(
                "manifest-slv01-test",
                "USGS",
                null,
                "run-slv01-test",
                "s3://bucket/bronze/usgs/success.geojson",
                "usgs/success.geojson",
                UsgsGeoJsonParser.sha256(payload),
                payload.length,
                stagedObject,
                temp.resolve("staging-manifest.json"),
                false);

        UsgsParseResult result = parser.parse(input);

        assertEquals(1, result.validCount());
        SilverObservation obs = result.observations().get(0);
        assertEquals("manifest-slv01-test", obs.bronzeManifestId());
        assertEquals("run-slv01-test", obs.ingestRunId());
        assertEquals("s3://bucket/bronze/usgs/success.geojson", obs.rawObjectUri());
    }

    @Test
    void rejectsMalformedJsonEnvelope() {
        assertThrows(IOException.class, () -> parser.parse(
                "{\"type\": \"NotAFeatureCollection\"}".getBytes(StandardCharsets.UTF_8),
                UsgsParseContext.synthetic("run-bad")));
    }
}
