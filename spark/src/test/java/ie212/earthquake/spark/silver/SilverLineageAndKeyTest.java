package ie212.earthquake.spark.silver;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Verification test suite for multi-source key generation, stable identity,
 * and raw record lineage (SLV-04).
 */
class SilverLineageAndKeyTest {

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
    void eachObservationTracesBackToExactRawRecordAndObject() throws Exception {
        byte[] payload = loadFixture("usgs/success.geojson");
        UsgsGeoJsonParser parser = new UsgsGeoJsonParser();
        UsgsParseContext context = new UsgsParseContext(
                "manifest-trace-01",
                "s3://bucket/bronze/usgs/success.geojson",
                SourceKeyGenerator.sha256(payload),
                "run-trace-01",
                Instant.parse("2023-09-16T00:15:08Z"));

        UsgsParseResult result = parser.parse(payload, context);
        assertEquals(1, result.validCount());
        SilverObservation obs = result.observations().get(0);

        SilverLineage lineage = SilverLineage.of(obs);
        assertEquals("USGS", lineage.sourceSystem());
        assertEquals("manifest-trace-01", lineage.bronzeManifestId());
        assertEquals("s3://bucket/bronze/usgs/success.geojson", lineage.rawObjectUri());
        assertEquals(SourceKeyGenerator.sha256(payload), lineage.rawSha256());
        assertEquals("features[0]", lineage.rawRecordLocator());
        assertEquals("run-trace-01", lineage.ingestRunId());
        assertEquals("usgs-geojson", lineage.parserName());
        assertEquals("slv-02-v1", lineage.parserVersion());

        // Verify that raw object hash matches
        assertTrue(lineage.verifyRawObject(payload));
        byte[] tamperedObject = payload.clone();
        tamperedObject[0] ^= 0x01;
        assertFalse(lineage.verifyRawObject(tamperedObject));

        // Reconstruct feature[0] bytes and verify record-level lineage hash
        String featureJson = new com.fasterxml.jackson.databind.ObjectMapper()
                .readTree(payload)
                .get("features")
                .get(0)
                .toString();
        byte[] featureBytes = new com.fasterxml.jackson.databind.ObjectMapper()
                .writeValueAsBytes(new com.fasterxml.jackson.databind.ObjectMapper().readTree(payload).get("features").get(0));
        assertTrue(lineage.verifyRawRecord(featureBytes));

        byte[] tamperedFeature = featureBytes.clone();
        tamperedFeature[0] ^= 0x01;
        assertFalse(lineage.verifyRawRecord(tamperedFeature));
    }

    @Test
    void keysAreDeterministicAndStableAcrossReruns() {
        String featureId = "fx-usgs-test-123";
        Long updatedMillis = 1693542896780L;
        String rawHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

        String firstRecordKey = SourceKeyGenerator.usgsRecordKey(featureId);
        String firstRevisionKey = SourceKeyGenerator.usgsRevisionKey(updatedMillis, rawHash);
        String firstObservationId = SourceKeyGenerator.observationId("USGS", firstRecordKey, firstRevisionKey);

        for (int i = 0; i < 1000; i++) {
            assertEquals(firstRecordKey, SourceKeyGenerator.usgsRecordKey(featureId));
            assertEquals(firstRevisionKey, SourceKeyGenerator.usgsRevisionKey(updatedMillis, rawHash));
            assertEquals(firstObservationId, SourceKeyGenerator.observationId("USGS", firstRecordKey, firstRevisionKey));
        }
    }

    @Test
    void jmaKeysAreStableAcrossRevisionsAndReruns() throws Exception {
        List<String> v1Lines = Files.readAllLines(
                repositoryRoot().resolve("tests/fixtures/jma/fixed-width/revision-v1.hyp"),
                StandardCharsets.US_ASCII);
        List<String> v2Lines = Files.readAllLines(
                repositoryRoot().resolve("tests/fixtures/jma/fixed-width/revision-v2.hyp"),
                StandardCharsets.US_ASCII);

        assertFalse(v1Lines.isEmpty());
        assertFalse(v2Lines.isEmpty());

        String v1Line = v1Lines.get(0);
        String v2Line = v2Lines.get(0);

        // Raw records differ in depth and magnitude
        assertNotEquals(v1Line, v2Line);

        // However, official hypocenter identity columns are identical
        String v1RecordKey = SourceKeyGenerator.jmaRecordKeyFromLine(v1Line);
        String v2RecordKey = SourceKeyGenerator.jmaRecordKeyFromLine(v2Line);

        // Acceptance criterion: same_source_record_key = true across revisions
        assertEquals(v1RecordKey, v2RecordKey, "JMA revisions must share the exact same source_record_key");
        assertTrue(v1RecordKey.startsWith("jma_k1_"));

        // Revision keys differ because catalog releases and raw hashes differ
        String v1RevisionKey = SourceKeyGenerator.jmaRevisionKey("rel-2023-v1", SourceKeyGenerator.sha256(v1Line));
        String v2RevisionKey = SourceKeyGenerator.jmaRevisionKey("rel-2023-v2", SourceKeyGenerator.sha256(v2Line));
        assertNotEquals(v1RevisionKey, v2RevisionKey);

        // Consequently, observation IDs are distinct revisions of the same source record key
        String obs1 = SourceKeyGenerator.observationId("JMA_BULLETIN", v1RecordKey, v1RevisionKey);
        String obs2 = SourceKeyGenerator.observationId("JMA_BULLETIN", v2RecordKey, v2RevisionKey);
        assertNotEquals(obs1, obs2);
        assertTrue(obs1.startsWith("obs_"));
        assertTrue(obs2.startsWith("obs_"));
    }

    @Test
    void jmaDuplicateRecordsYieldIdenticalRecordKeys() throws Exception {
        List<String> dupLines = Files.readAllLines(
                repositoryRoot().resolve("tests/fixtures/jma/fixed-width/duplicate.hyp"),
                StandardCharsets.US_ASCII);
        assertEquals(2, dupLines.size());

        String key1 = SourceKeyGenerator.jmaRecordKeyFromLine(dupLines.get(0));
        String key2 = SourceKeyGenerator.jmaRecordKeyFromLine(dupLines.get(1));
        assertEquals(key1, key2, "Duplicate lines must produce identical source record key");
    }

    @Test
    void jmaDistinctEventsYieldDistinctKeys() throws Exception {
        List<String> lines = Files.readAllLines(
                repositoryRoot().resolve("tests/fixtures/jma/fixed-width/success.hyp"),
                StandardCharsets.US_ASCII);
        assertEquals(2, lines.size());

        String naturalKey = SourceKeyGenerator.jmaRecordKeyFromLine(lines.get(0));
        String artificialKey = SourceKeyGenerator.jmaRecordKeyFromLine(lines.get(1));

        assertNotEquals(naturalKey, artificialKey, "Different events must produce distinct source keys");
    }

    @Test
    void canonicalEventIdNeverUsesTimeOrCoordinates() {
        String sourceSystem = "USGS";
        String sourceRecordKey = "fx-usgs-success-001";

        String canonicalId = SourceKeyGenerator.canonicalEventId(sourceSystem, sourceRecordKey);

        // 1. Must be opaque with "evt_" prefix and hexadecimal hash
        assertTrue(canonicalId.startsWith("evt_"), "Canonical event ID must start with evt_");
        String hashPart = canonicalId.substring(4);
        assertEquals(32, hashPart.length());
        assertTrue(hashPart.matches("[0-9a-fA-F]{32}"), "Canonical ID hash part must be hexadecimal opaque");

        // 2. Acceptance criterion: DO NOT use time or coordinate strings
        assertFalse(canonicalId.contains(":"), "Canonical event ID must not contain time delimiter ':'");
        assertFalse(canonicalId.contains("-"), "Canonical event ID must not contain date delimiter '-' (except evt_ prefix)");
        assertFalse(canonicalId.contains("."), "Canonical event ID must not contain decimal point from coordinates");
        assertFalse(canonicalId.contains("2023"), "Canonical event ID must not embed year string");
        assertFalse(canonicalId.contains("0901"), "Canonical event ID must not embed date string");
        assertFalse(canonicalId.contains("35.67"), "Canonical event ID must not embed latitude string");
        assertFalse(canonicalId.contains("139.76"), "Canonical event ID must not embed longitude string");

        // 3. Stability check: Even if origin time or coordinates update later,
        // canonical ID remains 100% invariant because it is seeded from stable source identity
        String recomputedId = SourceKeyGenerator.canonicalEventId(sourceSystem, sourceRecordKey);
        assertEquals(canonicalId, recomputedId);

        // 4. Distinct events produce distinct canonical IDs
        String otherCanonicalId = SourceKeyGenerator.canonicalEventId(sourceSystem, "fx-usgs-other-999");
        assertNotEquals(canonicalId, otherCanonicalId);
    }

    @Test
    void rejectsInvalidInputForRecordKeyGenerators() {
        assertThrows(IllegalArgumentException.class, () -> SourceKeyGenerator.usgsRecordKey(null));
        assertThrows(IllegalArgumentException.class, () -> SourceKeyGenerator.usgsRecordKey("   "));
        assertThrows(IllegalArgumentException.class, () -> SourceKeyGenerator.jmaRecordKeyFromLine("short line"));
        assertThrows(NullPointerException.class, () -> SourceKeyGenerator.canonicalEventId(null, "k1"));
        assertThrows(NullPointerException.class, () -> SourceKeyGenerator.canonicalEventId("USGS", null));
    }
}
