package ie212.earthquake.spark.silver;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SourceDedupTransformerTest {

    private SourceDedupTransformer transformer;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        transformer = new SourceDedupTransformer();
    }

    @Test
    void emptyObservationsProducesZeroCounts() {
        SourceDedupResult result = transformer.deduplicate(List.of());
        assertEquals(0, result.currentCount());
        assertEquals(0, result.duplicateCount());
        assertEquals(0, result.supersededCount());
        assertEquals(0, result.inputCount());
        assertTrue(result.currentObservations().isEmpty());
        assertTrue(result.historyObservations().isEmpty());
        assertTrue(result.allObservations().isEmpty());
    }

    @Test
    void singleObservationProducesOneCurrentZeroHistory() {
        SilverObservation obs = createUsgsObservation("usgs-01", 1000L, 2000L, "hash-a", 5.0);
        SourceDedupResult result = transformer.deduplicate(List.of(obs));

        assertEquals(1, result.currentCount());
        assertEquals(0, result.duplicateCount());
        assertEquals(0, result.supersededCount());
        assertEquals(1, result.inputCount());

        SilverObservation current = result.currentObservations().get(0);
        assertTrue(current.isCurrentSourceRevision());
        assertEquals("usgs-01", current.sourceRecordKey());
        assertTrue(result.historyObservations().isEmpty());
    }

    @Test
    void nullInputOrNullElementsThrowIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> transformer.deduplicate(null));
        List<SilverObservation> listWithNull = new ArrayList<>();
        listWithNull.add(createUsgsObservation("usgs-01", 1000L, 2000L, "hash-a", 5.0));
        listWithNull.add(null);
        assertThrows(IllegalArgumentException.class, () -> transformer.deduplicate(listWithNull));
    }

    /**
     * Acceptance criterion 1: Rerun cùng input cho cùng kết quả (idempotency, permutation invariance).
     */
    @Test
    void rerunWithSameInputOrPermutationsProducesIdenticalResults() {
        SilverObservation o1 = createUsgsObservation("usgs-shared-1", 1000L, 2000L, "hash-01", 4.0);
        SilverObservation o2 = createUsgsObservation("usgs-shared-1", 2000L, 2100L, "hash-02", 4.2);
        SilverObservation o3 = createUsgsObservation("usgs-shared-1", 2000L, 2100L, "hash-02", 4.2); // exact dup of o2
        SilverObservation o4 = createJmaObservation("jma-shared-1", "release-v1", null, 1000L, "hash-j1", 3.5);
        SilverObservation o5 = createJmaObservation("jma-shared-1", "release-v2", null, 2000L, "hash-j2", 3.8);

        List<SilverObservation> baseList = List.of(o1, o2, o3, o4, o5);
        SourceDedupResult baseline = transformer.deduplicate(baseList);

        assertEquals(2, baseline.currentCount());
        assertEquals(1, baseline.duplicateCount());
        assertEquals(2, baseline.supersededCount());
        assertEquals(5, baseline.inputCount());

        // Test 10 random permutations of input
        Random random = new Random(42L);
        for (int i = 0; i < 10; i++) {
            List<SilverObservation> shuffled = new ArrayList<>(baseList);
            Collections.shuffle(shuffled, random);
            SourceDedupResult permutedResult = transformer.deduplicate(shuffled);

            assertEquals(baseline.currentCount(), permutedResult.currentCount());
            assertEquals(baseline.duplicateCount(), permutedResult.duplicateCount());
            assertEquals(baseline.supersededCount(), permutedResult.supersededCount());
            assertEquals(baseline.inputCount(), permutedResult.inputCount());

            // Output observations must be in identical deterministic order
            for (int j = 0; j < baseline.currentObservations().size(); j++) {
                assertEquals(
                        baseline.currentObservations().get(j).sourceObservationId(),
                        permutedResult.currentObservations().get(j).sourceObservationId());
            }
            for (int j = 0; j < baseline.historyObservations().size(); j++) {
                assertEquals(
                        baseline.historyObservations().get(j).sourceObservationId(),
                        permutedResult.historyObservations().get(j).sourceObservationId());
            }
        }
    }

    /**
     * Acceptance criterion 2: Revision mới thay đúng bản cũ (USGS).
     */
    @Test
    void usgsNewerUpdateReplacesOlderRevisionAndPreservesHistory() {
        SilverObservation v1 = createUsgsObservation("fx-usgs-001", 1000L, 2000L, "hash-1", 4.0);
        SilverObservation v2 = createUsgsObservation("fx-usgs-001", 3000L, 3500L, "hash-2", 4.2);

        // Input v1 then v2
        SourceDedupResult result = transformer.deduplicate(List.of(v1, v2));
        assertEquals(1, result.currentCount());
        assertEquals(1, result.supersededCount());
        assertEquals(0, result.duplicateCount());
        assertEquals(2, result.inputCount());

        SilverObservation current = result.currentObservations().get(0);
        assertTrue(current.isCurrentSourceRevision());
        assertEquals(4.2, current.magnitude());
        assertEquals("hash-2", current.rawRecordHash());

        SilverObservation history = result.historyObservations().get(0);
        assertFalse(history.isCurrentSourceRevision());
        assertEquals(4.0, history.magnitude());
        assertEquals("hash-1", history.rawRecordHash());

        // Out-of-order arrival: input v2 then v1
        SourceDedupResult outOfOrderResult = transformer.deduplicate(List.of(v2, v1));
        assertEquals(1, outOfOrderResult.currentCount());
        assertEquals(1, outOfOrderResult.supersededCount());
        assertEquals(4.2, outOfOrderResult.currentObservations().get(0).magnitude());
    }

    /**
     * Acceptance criterion 2: Revision mới thay đúng bản cũ (JMA multi-release).
     */
    @Test
    void jmaNewerReleaseReplacesOlderRevisionAndPreservesHistory() {
        SilverObservation v1 = createJmaObservation("fx-jma-001", "release-v1", null, 1000L, "hash-j1", 4.0);
        SilverObservation v2 = createJmaObservation("fx-jma-001", "release-v2", null, 1500L, "hash-j2", 4.2);

        SourceDedupResult result = transformer.deduplicate(List.of(v1, v2));
        assertEquals(1, result.currentCount());
        assertEquals(1, result.supersededCount());
        assertEquals(0, result.duplicateCount());

        SilverObservation current = result.currentObservations().get(0);
        assertTrue(current.isCurrentSourceRevision());
        assertEquals("release-v2", current.catalogRelease());
        assertEquals(4.2, current.magnitude());

        SilverObservation history = result.historyObservations().get(0);
        assertFalse(history.isCurrentSourceRevision());
        assertEquals("release-v1", history.catalogRelease());
        assertEquals(4.0, history.magnitude());

        // Out-of-order delivery
        SourceDedupResult outOfOrder = transformer.deduplicate(List.of(v2, v1));
        assertEquals(1, outOfOrder.currentCount());
        assertEquals(4.2, outOfOrder.currentObservations().get(0).magnitude());
    }

    @Test
    void jmaReleaseTimestampTakesPrecedenceWhenPresent() {
        Instant t1 = Instant.parse("2023-01-01T00:00:00Z");
        Instant t2 = Instant.parse("2023-06-01T00:00:00Z");

        SilverObservation o1 = createJmaObservation("jma-key-ts", "rel-a", t1, 1000L, "hash-a", 3.0);
        SilverObservation o2 = createJmaObservation("jma-key-ts", "rel-b", t2, 1000L, "hash-b", 3.5);

        SourceDedupResult result = transformer.deduplicate(List.of(o1, o2));
        assertEquals(1, result.currentCount());
        assertEquals(3.5, result.currentObservations().get(0).magnitude());
    }

    /**
     * Acceptance criterion 3: Không dedup mơ hồ giữa hai nguồn (USGS vs JMA).
     */
    @Test
    void crossSourceIsolationNoAmbiguousDedupBetweenUsgsAndJma() {
        String sharedKey = "shared-earthquake-id";
        Instant originTime = Instant.parse("2023-09-01T03:34:56.780Z");

        SilverObservation usgs = createObservation(
                "USGS", sharedKey, "rev-usgs", Instant.ofEpochMilli(1000L), null, null,
                originTime, 35.67, 139.76, 25.0, 5.2, "hash-usgs", Instant.ofEpochMilli(2000L));

        SilverObservation jma = createObservation(
                "JMA_BULLETIN", sharedKey, "rev-jma", null, "release-v1", null,
                originTime, 35.67, 139.76, 25.0, 5.2, "hash-jma", Instant.ofEpochMilli(2000L));

        SourceDedupResult result = transformer.deduplicate(List.of(usgs, jma));

        // Both must be selected as current, never deduplicating each other
        assertEquals(2, result.currentCount());
        assertEquals(0, result.duplicateCount());
        assertEquals(0, result.supersededCount());
        assertEquals(2, result.inputCount());

        boolean hasUsgs = result.currentObservations().stream().anyMatch(o -> "USGS".equals(o.sourceSystem()));
        boolean hasJma = result.currentObservations().stream().anyMatch(o -> "JMA_BULLETIN".equals(o.sourceSystem()));
        assertTrue(hasUsgs);
        assertTrue(hasJma);

        // Verify per-source metrics
        SourceDedupMetrics usgsMetrics = result.metrics().bySourceSystem().get("USGS");
        assertNotNull(usgsMetrics);
        assertEquals(1, usgsMetrics.currentCount());
        assertEquals(0, usgsMetrics.duplicateCount());
        assertEquals(0, usgsMetrics.supersededCount());

        SourceDedupMetrics jmaMetrics = result.metrics().bySourceSystem().get("JMA_BULLETIN");
        assertNotNull(jmaMetrics);
        assertEquals(1, jmaMetrics.currentCount());
        assertEquals(0, jmaMetrics.duplicateCount());
        assertEquals(0, jmaMetrics.supersededCount());
    }

    @Test
    void exactDuplicatesClassifiedCorrectly() {
        SilverObservation o1 = createUsgsObservation("usgs-dup-01", 1000L, 2000L, "hash-same", 4.5);
        SilverObservation o2 = createUsgsObservation("usgs-dup-01", 1000L, 2000L, "hash-same", 4.5);

        SourceDedupResult result = transformer.deduplicate(List.of(o1, o2));
        assertEquals(1, result.currentCount());
        assertEquals(1, result.duplicateCount());
        assertEquals(0, result.supersededCount());
        assertEquals(2, result.inputCount());
    }

    @Test
    void combinedMultiRevisionAndDuplicates() {
        SilverObservation v1a = createUsgsObservation("usgs-multi", 1000L, 2000L, "hash-v1", 4.0);
        SilverObservation v1b = createUsgsObservation("usgs-multi", 1000L, 2000L, "hash-v1", 4.0); // dup of v1
        SilverObservation v2 = createUsgsObservation("usgs-multi", 2000L, 2500L, "hash-v2", 4.2);  // winner

        SourceDedupResult result = transformer.deduplicate(List.of(v1a, v1b, v2));
        assertEquals(1, result.currentCount());
        assertEquals(1, result.duplicateCount());
        assertEquals(1, result.supersededCount());
        assertEquals(3, result.inputCount());

        assertEquals(4.2, result.currentObservations().get(0).magnitude());
    }

    @Test
    void tieBreakRulesAreDeterministic() {
        // USGS: same update time, different processed time -> newer processed wins
        SilverObservation u1 = createUsgsObservation("tie-u1", 1000L, 2000L, "hash-a", 4.0);
        SilverObservation u2 = createUsgsObservation("tie-u1", 1000L, 3000L, "hash-b", 4.5);
        SourceDedupResult r1 = transformer.deduplicate(List.of(u1, u2));
        assertEquals(4.5, r1.currentObservations().get(0).magnitude());

        // USGS: same update and processed time, different hash -> smaller hash (ASC) wins
        SilverObservation u3 = createUsgsObservation("tie-u2", 1000L, 2000L, "hash-z", 4.0);
        SilverObservation u4 = createUsgsObservation("tie-u2", 1000L, 2000L, "hash-a", 4.5);
        SourceDedupResult r2 = transformer.deduplicate(List.of(u3, u4));
        assertEquals("hash-a", r2.currentObservations().get(0).rawRecordHash());
        assertEquals(4.5, r2.currentObservations().get(0).magnitude());

        // JMA: same release, different processed time -> newer processed wins
        SilverObservation j1 = createJmaObservation("tie-j1", "release-v1", null, 1000L, "hash-a", 3.0);
        SilverObservation j2 = createJmaObservation("tie-j1", "release-v1", null, 2000L, "hash-b", 3.5);
        SourceDedupResult r3 = transformer.deduplicate(List.of(j1, j2));
        assertEquals(3.5, r3.currentObservations().get(0).magnitude());

        // JMA: same release and processed time, different hash -> smaller hash (ASC) wins
        SilverObservation j3 = createJmaObservation("tie-j2", "release-v1", null, 1000L, "hash-z", 3.0);
        SilverObservation j4 = createJmaObservation("tie-j2", "release-v1", null, 1000L, "hash-a", 3.5);
        SourceDedupResult r4 = transformer.deduplicate(List.of(j3, j4));
        assertEquals("hash-a", r4.currentObservations().get(0).rawRecordHash());
        assertEquals(3.5, r4.currentObservations().get(0).magnitude());
    }

    /**
     * Acceptance testing with synthetic late-revision fixture chains.
     */
    @Test
    void verifiesSyntheticLateRevisionFixtureChains() throws IOException {
        Path usgsFixturePath = Path.of("src/test/resources/fixtures/late_revision/usgs_multirevision_chain.json");
        if (Files.isRegularFile(usgsFixturePath)) {
            JsonNode root = objectMapper.readTree(Files.readAllBytes(usgsFixturePath));
            JsonNode events = root.path("events");
            List<SilverObservation> observations = new ArrayList<>();
            for (JsonNode ev : events) {
                String id = ev.path("feature_id").asText();
                double mag = ev.path("mag").asDouble();
                long updated = ev.path("updated_millis").asLong();
                long time = ev.path("time_millis").asLong();
                String hash = SourceKeyGenerator.sha256((id + "|" + updated + "|" + mag).getBytes());
                observations.add(createObservation("USGS", id, id + ":" + hash, Instant.ofEpochMilli(updated),
                        null, null, Instant.ofEpochMilli(time), 36.5, 140.25, 24.0, mag, hash, Instant.ofEpochMilli(updated)));
            }
            SourceDedupResult result = transformer.deduplicate(observations);
            assertEquals(2, result.currentCount()); // fx-usgs-rev-chain-01 and fx-usgs-single-02
            assertEquals(1, result.duplicateCount());
            assertEquals(2, result.supersededCount()); // revisions 1 and 2
            assertEquals(5, result.inputCount());

            SilverObservation chainCurrent = result.currentObservations().stream()
                    .filter(o -> "fx-usgs-rev-chain-01".equals(o.sourceRecordKey()))
                    .findFirst().orElseThrow();
            assertEquals(4.3, chainCurrent.magnitude()); // latest revision order 3
        }

        Path jmaFixturePath = Path.of("src/test/resources/fixtures/late_revision/jma_multirevision_chain.json");
        if (Files.isRegularFile(jmaFixturePath)) {
            JsonNode root = objectMapper.readTree(Files.readAllBytes(jmaFixturePath));
            JsonNode events = root.path("events");
            List<SilverObservation> observations = new ArrayList<>();
            for (JsonNode ev : events) {
                String release = ev.path("catalog_release").asText();
                String key = SourceKeyGenerator.jmaRecordKey(
                        ev.path("agency").asText(), ev.path("origin_time_jst").asText(),
                        ev.path("lat_deg_min").asText(), ev.path("lon_deg_min").asText());
                double mag = ev.path("magnitude").asDouble();
                String hash = ev.path("raw_record_hash").asText();
                observations.add(createObservation("JMA_BULLETIN", key, release + ":" + hash, null,
                        release, null, Instant.parse("2023-09-01T03:34:56.780Z"), 35.67, 139.75, 24.0, mag, hash, Instant.now()));
            }
            SourceDedupResult result = transformer.deduplicate(observations);
            assertEquals(1, result.currentCount());
            assertEquals(1, result.duplicateCount());
            assertEquals(1, result.supersededCount());
            assertEquals(3, result.inputCount());
            assertEquals("release-v2", result.currentObservations().get(0).catalogRelease());
            assertEquals(4.2, result.currentObservations().get(0).magnitude());
        }
    }

    /**
     * Acceptance testing on contract cases.json (FX-USGS-05, FX-USGS-06, FX-JMA-04, FX-JMA-05).
     */
    @Test
    void verifiesContractCasesFromCasesJson() throws Exception {
        Path casesPath = findRepositoryRoot().resolve("tests/fixtures/cases.json");
        if (!Files.isRegularFile(casesPath)) {
            return;
        }
        JsonNode root = objectMapper.readTree(Files.readAllBytes(casesPath));
        JsonNode cases = root.path("cases");

        // Verify FX-USGS-05 (duplicate)
        JsonNode fxUsgs05 = findCase(cases, "FX-USGS-05");
        assertNotNull(fxUsgs05);
        byte[] usgsDupBytes = readFixturePath(fxUsgs05.path("input_paths").get(0).asText());
        var usgsParser = new UsgsGeoJsonParser();
        var usgsParsed = usgsParser.parse(usgsDupBytes, new UsgsParseContext("m-dup", "s3://b/dup", "hash-d", "run-1", Instant.now()));
        SourceDedupResult usgsDupResult = transformer.deduplicate(usgsParsed.observations());
        assertEquals(1, usgsDupResult.currentCount());
        assertEquals(1, usgsDupResult.duplicateCount());
        assertEquals(0, usgsDupResult.supersededCount());
        assertEquals(2, usgsDupResult.inputCount());

        // Verify FX-USGS-06 (revised)
        JsonNode fxUsgs06 = findCase(cases, "FX-USGS-06");
        assertNotNull(fxUsgs06);
        byte[] v1Bytes = readFixturePath(fxUsgs06.path("input_paths").get(0).asText());
        byte[] v2Bytes = readFixturePath(fxUsgs06.path("input_paths").get(1).asText());
        var parsedV1 = usgsParser.parse(v1Bytes, new UsgsParseContext("m-v1", "s3://b/v1", "hash-v1", "run-1", Instant.now()));
        var parsedV2 = usgsParser.parse(v2Bytes, new UsgsParseContext("m-v2", "s3://b/v2", "hash-v2", "run-2", Instant.now()));
        List<SilverObservation> revisedUsgs = new ArrayList<>(parsedV1.observations());
        revisedUsgs.addAll(parsedV2.observations());
        SourceDedupResult usgsRevResult = transformer.deduplicate(revisedUsgs);
        assertEquals(1, usgsRevResult.currentCount());
        assertEquals(0, usgsRevResult.duplicateCount());
        assertEquals(1, usgsRevResult.supersededCount());
        assertEquals(2, usgsRevResult.inputCount());
        assertEquals(4.2, usgsRevResult.currentObservations().get(0).magnitude());

        // Verify FX-JMA-04 (duplicate)
        JsonNode fxJma04 = findCase(cases, "FX-JMA-04");
        assertNotNull(fxJma04);
        byte[] jmaDupBytes = readFixturePath(fxJma04.path("input_paths").get(0).asText());
        var jmaParser = new JmaFixedWidthParser();
        String jmaDupSha = SourceKeyGenerator.sha256(jmaDupBytes);
        var jmaParsed = jmaParser.parse(jmaDupBytes, new JmaParseContext("m-jdup", "s3://b/jdup", jmaDupSha, "run-1", "release-v1", null, "https://url", "h", Instant.now()));
        SourceDedupResult jmaDupResult = transformer.deduplicate(jmaParsed.observations());
        assertEquals(1, jmaDupResult.currentCount());
        assertEquals(1, jmaDupResult.duplicateCount());
        assertEquals(0, jmaDupResult.supersededCount());
        assertEquals(2, jmaDupResult.inputCount());

        // Verify FX-JMA-05 (revised archive v1 vs v2)
        JsonNode fxJma05 = findCase(cases, "FX-JMA-05");
        assertNotNull(fxJma05);
        byte[] jmaV1Zip = readFixturePath(fxJma05.path("input_paths").get(0).asText());
        byte[] jmaV2Zip = readFixturePath(fxJma05.path("input_paths").get(1).asText());
        var jmaV1Parsed = jmaParser.parseArchive(jmaV1Zip, new JmaParseContext("m-jv1", "s3://b/jv1", SourceKeyGenerator.sha256(jmaV1Zip), "run-1", "release-v1", null, "https://url", "hypo.dat", Instant.now()));
        var jmaV2Parsed = jmaParser.parseArchive(jmaV2Zip, new JmaParseContext("m-jv2", "s3://b/jv2", SourceKeyGenerator.sha256(jmaV2Zip), "run-2", "release-v2", null, "https://url", "hypo.dat", Instant.now()));
        List<SilverObservation> revisedJma = new ArrayList<>(jmaV1Parsed.observations());
        revisedJma.addAll(jmaV2Parsed.observations());
        SourceDedupResult jmaRevResult = transformer.deduplicate(revisedJma);
        assertEquals(1, jmaRevResult.currentCount());
        assertEquals(0, jmaRevResult.duplicateCount());
        assertEquals(1, jmaRevResult.supersededCount());
        assertEquals(2, jmaRevResult.inputCount());
        assertEquals(4.2, jmaRevResult.currentObservations().get(0).magnitude());
        assertEquals("release-v2", jmaRevResult.currentObservations().get(0).catalogRelease());
    }

    private static JsonNode findCase(JsonNode cases, String caseId) {
        for (JsonNode item : cases) {
            if (caseId.equals(item.path("case_id").asText())) {
                return item;
            }
        }
        return null;
    }

    private static byte[] readFixturePath(String relative) throws IOException {
        return Files.readAllBytes(findRepositoryRoot().resolve("tests/fixtures").resolve(relative));
    }

    private static Path findRepositoryRoot() {
        Path candidate = Path.of("").toAbsolutePath();
        while (candidate != null) {
            if (Files.isRegularFile(candidate.resolve("tests/fixtures/cases.json"))) {
                return candidate;
            }
            candidate = candidate.getParent();
        }
        throw new IllegalStateException("Could not find repository root with tests/fixtures/cases.json");
    }

    private SilverObservation createUsgsObservation(String key, Long updatedMillis, long processedMillis, String hash, double mag) {
        return createObservation(
                "USGS", key, key + ":" + hash, updatedMillis != null ? Instant.ofEpochMilli(updatedMillis) : null,
                null, null, Instant.parse("2023-09-01T03:34:56.780Z"), 35.67, 139.76, 25.0, mag, hash, Instant.ofEpochMilli(processedMillis));
    }

    private SilverObservation createJmaObservation(String key, String catalogRelease, Instant releaseAtUtc, long processedMillis, String hash, double mag) {
        return createObservation(
                "JMA_BULLETIN", key, catalogRelease + ":" + hash, null,
                catalogRelease, releaseAtUtc, Instant.parse("2023-09-01T03:34:56.780Z"), 35.67, 139.76, 25.0, mag, hash, Instant.ofEpochMilli(processedMillis));
    }

    private SilverObservation createObservation(
            String sourceSystem,
            String sourceRecordKey,
            String sourceRevisionKey,
            Instant sourceUpdatedAtUtc,
            String catalogRelease,
            Instant catalogReleaseAtUtc,
            Instant eventTimeUtc,
            double lat,
            double lon,
            Double depthKm,
            Double mag,
            String hash,
            Instant processedAtUtc) {

        String obsId = SourceKeyGenerator.observationId(sourceSystem, sourceRecordKey, sourceRevisionKey);
        return new SilverObservation(
                "1.0",
                obsId,
                sourceSystem,
                sourceRecordKey,
                sourceRevisionKey,
                sourceUpdatedAtUtc,
                catalogRelease,
                catalogReleaseAtUtc,
                false,
                eventTimeUtc,
                LocalDateTime.of(2023, 9, 1, 12, 34, 56),
                LocalDate.of(2023, 9, 1),
                LocalDate.of(2023, 9, 1),
                2023,
                9,
                lat,
                lon,
                depthKm,
                mag,
                "mw",
                "EARTHQUAKE",
                "Tokyo Bay",
                false,
                null,
                null,
                null,
                null,
                "JMA_BULLETIN".equals(sourceSystem) ? "UNIFIED" : null,
                "reviewed",
                "https://example.invalid",
                true,
                "VALID",
                List.of(),
                "manifest-01",
                "s3://bucket/test",
                "sha256-mock",
                "loc-01",
                hash,
                "run-01",
                "parser-mock",
                "1.0",
                processedAtUtc);
    }
}
