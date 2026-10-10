package ie212.earthquake.spark.silver;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SilverEntityResolverTest {

    private static final Instant EVAL_TIME = Instant.parse("2026-10-09T14:00:00Z");
    private SilverLinkConfig defaultConfig;

    @BeforeEach
    void setUp() {
        defaultConfig = SilverLinkConfig.defaultConfig();
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
            String id,
            String sourceSystem,
            String sourceRecordKey,
            Instant eventTimeUtc,
            double latitude,
            double longitude,
            Double depthKm,
            Double magnitude,
            String qualityStatus,
            String agencyCode,
            String eventTypeCode) {

        String revisionKey = "rev-01:hash";
        String obsId = SourceKeyGenerator.observationId(sourceSystem, sourceRecordKey, revisionKey);

        return new SilverObservation(
                "1.0",
                obsId,
                sourceSystem,
                sourceRecordKey,
                revisionKey,
                null,
                "release-v1",
                null,
                true,
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
                eventTypeCode != null ? eventTypeCode : "EARTHQUAKE",
                "Test Location",
                false,
                null,
                null,
                null,
                agencyCode,
                "UNIFIED",
                "reviewed",
                "https://example.invalid/event",
                true,
                qualityStatus != null ? qualityStatus : "VALID",
                List.of(),
                "manifest-01",
                "s3://bucket/raw",
                "sha256-dummy",
                "line=1",
                "hash-dummy",
                "run-01",
                "parser",
                "v1",
                EVAL_TIME);
    }

    @Test
    void testBoundaryTimeDeltaThreshold() {
        Instant baseTime = Instant.parse("2023-09-01T12:00:00.000Z");
        SilverObservation usgs = createObservation("u1", "USGS", "usgs-t1", baseTime, 35.0, 139.0, 10.0, 5.0, "VALID", null, "EARTHQUAKE");

        // Boundary: exactly 16.0s (allowed)
        Instant exactBoundaryTime = baseTime.plusMillis(16_000);
        SilverObservation jmaExact = createObservation("j1", "JMA_BULLETIN", "jma-t1", exactBoundaryTime, 35.0, 139.0, 10.0, 5.0, "VALID", "J", "EARTHQUAKE");
        SilverSourceLink linkExact = SilverEntityResolver.evaluatePair(usgs, jmaExact, defaultConfig, EVAL_TIME);
        assertEquals(SilverEntityResolver.DECISION_ACCEPTED, linkExact.linkDecision());
        assertEquals(16.0, linkExact.timeDeltaSeconds(), 1e-6);
        assertFalse(linkExact.decisionReasonCodes().contains(SilverEntityResolver.REASON_TIME_DELTA_EXCEEDED));

        // Just beyond boundary: 16.001s (rejected)
        Instant exceededTime = baseTime.plusMillis(16_001);
        SilverObservation jmaExceeded = createObservation("j2", "JMA_BULLETIN", "jma-t2", exceededTime, 35.0, 139.0, 10.0, 5.0, "VALID", "J", "EARTHQUAKE");
        SilverSourceLink linkExceeded = SilverEntityResolver.evaluatePair(usgs, jmaExceeded, defaultConfig, EVAL_TIME);
        assertEquals(SilverEntityResolver.DECISION_REJECTED, linkExceeded.linkDecision());
        assertTrue(linkExceeded.timeDeltaSeconds() > 16.0);
        assertTrue(linkExceeded.decisionReasonCodes().contains(SilverEntityResolver.REASON_TIME_DELTA_EXCEEDED));
    }

    @Test
    void testBoundaryDistanceThreshold() {
        Instant time = Instant.parse("2023-09-01T12:00:00.000Z");
        // Calculate latitude offset for exactly 100.0 km
        // Earth radius = 6371.0 km, dLat = 100.0 / 6371.0 rad = 0.015696123 rad = 0.8993216 degrees
        double lat1 = 35.0;
        double lon1 = 139.0;
        double deltaLatBoundary = Math.toDegrees(100.0 / 6371.0);
        double distAtBoundary = SilverEntityResolver.haversineDistanceKm(lat1, lon1, lat1 + deltaLatBoundary, lon1);
        assertEquals(100.0, distAtBoundary, 1e-4);

        SilverObservation usgs = createObservation("u1", "USGS", "usgs-d1", time, lat1, lon1, 10.0, 5.0, "VALID", null, "EARTHQUAKE");

        // Boundary: distance <= 100.0 km (accepted)
        SilverObservation jmaExact = createObservation("j1", "JMA_BULLETIN", "jma-d1", time, lat1 + deltaLatBoundary, lon1, 10.0, 5.0, "VALID", "J", "EARTHQUAKE");
        SilverSourceLink linkExact = SilverEntityResolver.evaluatePair(usgs, jmaExact, defaultConfig, EVAL_TIME);
        assertEquals(SilverEntityResolver.DECISION_ACCEPTED, linkExact.linkDecision());
        assertFalse(linkExact.decisionReasonCodes().contains(SilverEntityResolver.REASON_DISTANCE_EXCEEDED));

        // Beyond boundary: distance > 100.0 km (e.g. + 0.005 degrees extra)
        SilverObservation jmaExceeded = createObservation("j2", "JMA_BULLETIN", "jma-d2", time, lat1 + deltaLatBoundary + 0.005, lon1, 10.0, 5.0, "VALID", "J", "EARTHQUAKE");
        SilverSourceLink linkExceeded = SilverEntityResolver.evaluatePair(usgs, jmaExceeded, defaultConfig, EVAL_TIME);
        assertEquals(SilverEntityResolver.DECISION_REJECTED, linkExceeded.linkDecision());
        assertTrue(linkExceeded.distanceKm() > 100.0);
        assertTrue(linkExceeded.decisionReasonCodes().contains(SilverEntityResolver.REASON_DISTANCE_EXCEEDED));
    }

    @Test
    void testBoundaryDepthThreshold() {
        Instant time = Instant.parse("2023-09-01T12:00:00.000Z");
        SilverObservation usgs = createObservation("u1", "USGS", "usgs-dp1", time, 35.0, 139.0, 10.0, 5.0, "VALID", null, "EARTHQUAKE");

        // Boundary: depth delta exactly 50.0 km (allowed)
        SilverObservation jmaExact = createObservation("j1", "JMA_BULLETIN", "jma-dp1", time, 35.0, 139.0, 60.0, 5.0, "VALID", "J", "EARTHQUAKE");
        SilverSourceLink linkExact = SilverEntityResolver.evaluatePair(usgs, jmaExact, defaultConfig, EVAL_TIME);
        assertEquals(SilverEntityResolver.DECISION_ACCEPTED, linkExact.linkDecision());
        assertEquals(50.0, linkExact.depthDeltaKm(), 1e-6);
        assertFalse(linkExact.decisionReasonCodes().contains(SilverEntityResolver.REASON_DEPTH_DELTA_EXCEEDED));

        // Just beyond boundary: 50.001 km (rejected)
        SilverObservation jmaExceeded = createObservation("j2", "JMA_BULLETIN", "jma-dp2", time, 35.0, 139.0, 60.001, 5.0, "VALID", "J", "EARTHQUAKE");
        SilverSourceLink linkExceeded = SilverEntityResolver.evaluatePair(usgs, jmaExceeded, defaultConfig, EVAL_TIME);
        assertEquals(SilverEntityResolver.DECISION_REJECTED, linkExceeded.linkDecision());
        assertTrue(linkExceeded.depthDeltaKm() > 50.0);
        assertTrue(linkExceeded.decisionReasonCodes().contains(SilverEntityResolver.REASON_DEPTH_DELTA_EXCEEDED));
    }

    @Test
    void testBoundaryMagnitudeThreshold() {
        Instant time = Instant.parse("2023-09-01T12:00:00.000Z");
        SilverObservation usgs = createObservation("u1", "USGS", "usgs-m1", time, 35.0, 139.0, 10.0, 5.0, "VALID", null, "EARTHQUAKE");

        // Boundary: magnitude delta exactly 1.0 (allowed)
        SilverObservation jmaExact = createObservation("j1", "JMA_BULLETIN", "jma-m1", time, 35.0, 139.0, 10.0, 6.0, "VALID", "J", "EARTHQUAKE");
        SilverSourceLink linkExact = SilverEntityResolver.evaluatePair(usgs, jmaExact, defaultConfig, EVAL_TIME);
        assertEquals(SilverEntityResolver.DECISION_ACCEPTED, linkExact.linkDecision());
        assertEquals(1.0, linkExact.magnitudeDelta(), 1e-6);
        assertFalse(linkExact.decisionReasonCodes().contains(SilverEntityResolver.REASON_MAGNITUDE_DELTA_EXCEEDED));

        // Just beyond boundary: 1.001 (rejected)
        SilverObservation jmaExceeded = createObservation("j2", "JMA_BULLETIN", "jma-m2", time, 35.0, 139.0, 10.0, 6.001, "VALID", "J", "EARTHQUAKE");
        SilverSourceLink linkExceeded = SilverEntityResolver.evaluatePair(usgs, jmaExceeded, defaultConfig, EVAL_TIME);
        assertEquals(SilverEntityResolver.DECISION_REJECTED, linkExceeded.linkDecision());
        assertTrue(linkExceeded.magnitudeDelta() > 1.0);
        assertTrue(linkExceeded.decisionReasonCodes().contains(SilverEntityResolver.REASON_MAGNITUDE_DELTA_EXCEEDED));
    }

    @Test
    void testNullableFieldsDoNotCauseRejection() {
        Instant time = Instant.parse("2023-09-01T12:00:00.000Z");
        SilverObservation usgs = createObservation("u1", "USGS", "usgs-null", time, 35.0, 139.0, null, null, "VALID", null, "EARTHQUAKE");
        SilverObservation jma = createObservation("j1", "JMA_BULLETIN", "jma-null", time, 35.0, 139.0, null, 4.5, "VALID", "J", "EARTHQUAKE");

        SilverSourceLink link = SilverEntityResolver.evaluatePair(usgs, jma, defaultConfig, EVAL_TIME);
        assertEquals(SilverEntityResolver.DECISION_ACCEPTED, link.linkDecision());
        assertNull(link.depthDeltaKm());
        assertNull(link.magnitudeDelta());
        assertTrue(link.matchScore() >= defaultConfig.minMatchScore());
    }

    @Test
    void testAcceptanceCriteria1And2_NoDoubleCountAndPreserveBothObservations() {
        // 1 matched pair creates exactly 1 canonical event containing both observations
        Instant time = Instant.parse("2023-09-01T12:00:00.000Z");
        SilverObservation usgs = createObservation("u1", "USGS", "usgs-overlap", time, 35.0, 139.0, 10.0, 5.0, "VALID", null, "EARTHQUAKE");
        SilverObservation jma = createObservation("j1", "JMA_BULLETIN", "jma-overlap", time, 35.0, 139.0, 10.0, 5.0, "VALID", "J", "EARTHQUAKE");

        SilverResolutionResult result = SilverEntityResolver.resolve(List.of(usgs, jma), defaultConfig, EVAL_TIME);

        // AC 1: No double count overlap (1 canonical event, not 2)
        assertEquals(1, result.report().canonicalEventsCount());
        assertEquals(1, result.report().acceptedPairsCount());
        assertEquals(1, result.report().matchedEventsCount());

        // AC 2: Both original observations preserved in canonical_membership
        assertEquals(2, result.canonicalMemberships().size());
        String canonicalEventId = result.canonicalMemberships().get(0).canonicalEventId();
        assertEquals(canonicalEventId, result.canonicalMemberships().get(1).canonicalEventId());

        List<SilverCanonicalMembership> memberships = result.membershipsForEvent(canonicalEventId);
        assertEquals(2, memberships.size());

        Set<String> memberObsIds = Set.of(memberships.get(0).sourceObservationId(), memberships.get(1).sourceObservationId());
        assertTrue(memberObsIds.contains(usgs.sourceObservationId()));
        assertTrue(memberObsIds.contains(jma.sourceObservationId()));

        Set<String> statuses = Set.of(memberships.get(0).membershipStatus(), memberships.get(1).membershipStatus());
        assertTrue(statuses.contains(SilverEntityResolver.STATUS_PRIMARY));
        assertTrue(statuses.contains(SilverEntityResolver.STATUS_SUPPORTING));

        // JMA preferred primary rule: JMA is PRIMARY, USGS is SUPPORTING
        SilverCanonicalMembership jmaMember = memberships.stream()
                .filter(m -> m.sourceObservationId().equals(jma.sourceObservationId()))
                .findFirst().orElseThrow();
        assertEquals(SilverEntityResolver.STATUS_PRIMARY, jmaMember.membershipStatus());

        SilverCanonicalMembership usgsMember = memberships.stream()
                .filter(m -> m.sourceObservationId().equals(usgs.sourceObservationId()))
                .findFirst().orElseThrow();
        assertEquals(SilverEntityResolver.STATUS_SUPPORTING, usgsMember.membershipStatus());
    }

    @Test
    void testAcceptanceCriteria3_UnmatchedEventsRemainSingleSource() {
        // Unmatched events (e.g. distant or artificial) still exist as PRIMARY canonical events
        Instant time = Instant.parse("2023-09-01T12:00:00.000Z");
        SilverObservation usgs = createObservation("u1", "USGS", "usgs-solo", time, 35.0, 139.0, 10.0, 5.0, "VALID", null, "EARTHQUAKE");
        // JMA event 1 hour later (no match possible)
        SilverObservation jmaSolo = createObservation("j1", "JMA_BULLETIN", "jma-solo", time.plusSeconds(3600), 40.0, 142.0, 20.0, 4.0, "VALID", "J", "EARTHQUAKE");

        SilverResolutionResult result = SilverEntityResolver.resolve(List.of(usgs, jmaSolo), defaultConfig, EVAL_TIME);

        // AC 3: Unmatched events still exist
        assertEquals(2, result.report().canonicalEventsCount());
        assertEquals(0, result.report().acceptedPairsCount());
        assertEquals(1, result.report().usgsOnlyEventsCount());
        assertEquals(1, result.report().jmaOnlyEventsCount());

        // Both are PRIMARY with null source_link_id
        for (SilverCanonicalMembership membership : result.canonicalMemberships()) {
            assertEquals(SilverEntityResolver.STATUS_PRIMARY, membership.membershipStatus());
            assertNull(membership.sourceLinkId());
        }
    }

    @Test
    void testSourcePriorityRules() {
        Instant time = Instant.parse("2023-09-01T12:00:00.000Z");

        // Case 1: JMA valid with agency 'J' -> JMA is PRIMARY
        SilverObservation usgs1 = createObservation("u1", "USGS", "u-pri-1", time, 35.0, 139.0, 10.0, 5.0, "VALID", null, "EARTHQUAKE");
        SilverObservation jma1 = createObservation("j1", "JMA_BULLETIN", "j-pri-1", time, 35.0, 139.0, 10.0, 5.0, "VALID", "J", "EARTHQUAKE");
        assertTrue(SilverEntityResolver.isJmaPreferredPrimary(jma1, usgs1));

        // Case 2: JMA has WARNING quality -> USGS is PRIMARY
        SilverObservation jmaWarning = createObservation("j2", "JMA_BULLETIN", "j-pri-2", time, 35.0, 139.0, 10.0, 5.0, "WARNING", "J", "EARTHQUAKE");
        assertFalse(SilverEntityResolver.isJmaPreferredPrimary(jmaWarning, usgs1));

        // Case 3: JMA determining agency is 'U' (non-J/K) -> USGS is PRIMARY
        SilverObservation jmaAgencyU = createObservation("j3", "JMA_BULLETIN", "j-pri-3", time, 35.0, 139.0, 10.0, 5.0, "VALID", "U", "EARTHQUAKE");
        assertFalse(SilverEntityResolver.isJmaPreferredPrimary(jmaAgencyU, usgs1));

        // Resolution verification for Case 2 (USGS preferred)
        SilverResolutionResult res2 = SilverEntityResolver.resolve(List.of(usgs1, jmaWarning), defaultConfig, EVAL_TIME);
        assertEquals(1, res2.report().canonicalEventsCount());
        SilverCanonicalMembership usgsMember = res2.canonicalMemberships().stream()
                .filter(m -> m.sourceObservationId().equals(usgs1.sourceObservationId()))
                .findFirst().orElseThrow();
        assertEquals(SilverEntityResolver.STATUS_PRIMARY, usgsMember.membershipStatus());
        SilverCanonicalMembership jmaMember = res2.canonicalMemberships().stream()
                .filter(m -> m.sourceObservationId().equals(jmaWarning.sourceObservationId()))
                .findFirst().orElseThrow();
        assertEquals(SilverEntityResolver.STATUS_SUPPORTING, jmaMember.membershipStatus());
    }

    @Test
    void testFixtureFxLink01AmbiguousDoesNotAutoMerge() throws Exception {
        // FX-LINK-01: 1 USGS vs 2 synthetic JMA candidates -> link decision AMBIGUOUS, auto_merge=false
        byte[] usgsBytes = loadFixture("usgs/ambiguous.geojson");
        String dummySha = "0".repeat(64);
        UsgsParseContext usgsCtx = new UsgsParseContext("m-amb-u", "s3://bucket/usgs/amb.geojson", dummySha, "run-1", EVAL_TIME);
        UsgsParseResult usgsParsed = new UsgsGeoJsonParser().parse(usgsBytes, usgsCtx);
        assertEquals(1, usgsParsed.observations().size());

        byte[] jmaBytes = loadFixture("jma/fixed-width/ambiguous.hyp");
        JmaParseContext jmaCtx = new JmaParseContext("m-amb-j", "s3://bucket/jma/amb.hyp", dummySha, "run-1", "release-1", null, "https://example.invalid/jma", "ambiguous.hyp", EVAL_TIME);
        JmaParseResult jmaParsed = new JmaFixedWidthParser().parse(jmaBytes, jmaCtx);
        assertEquals(2, jmaParsed.observations().size());

        SilverObservation usgsObs = usgsParsed.observations().get(0);
        SilverObservation jmaObs1 = jmaParsed.observations().get(0);
        SilverObservation jmaObs2 = jmaParsed.observations().get(1);

        SilverResolutionResult result = SilverEntityResolver.resolve(List.of(usgsObs, jmaObs1, jmaObs2), defaultConfig, EVAL_TIME);

        // Assertions from cases.json FX-LINK-01
        assertEquals(2, result.report().candidatePairsEvaluated());
        assertEquals(2, result.report().ambiguousPairsCount());
        assertEquals(0, result.report().acceptedPairsCount());
        assertEquals(3, result.report().canonicalEventsCount()); // auto_merge=false: each gets its own canonical event!

        // All links have link_decision = AMBIGUOUS and reason AMBIGUOUS_SOURCE_MATCH
        for (SilverSourceLink link : result.sourceLinks()) {
            assertEquals(SilverEntityResolver.DECISION_AMBIGUOUS, link.linkDecision());
            assertTrue(link.decisionReasonCodes().contains(SilverEntityResolver.REASON_AMBIGUOUS_SOURCE_MATCH));
        }

        // Each observation remains separate canonical event with PRIMARY status and null link ID
        assertEquals(3, result.canonicalMemberships().size());
        for (SilverCanonicalMembership membership : result.canonicalMemberships()) {
            assertEquals(SilverEntityResolver.STATUS_PRIMARY, membership.membershipStatus());
            assertNull(membership.sourceLinkId());
        }
    }

    @Test
    void testFixtureFxLink02AcceptedMatch() throws Exception {
        // FX-LINK-02: 1 USGS vs 2 JMA (1 natural earthquake matching, 1 artificial quarry blast 25 min later)
        byte[] usgsBytes = loadFixture("usgs/success.geojson");
        String dummySha = "0".repeat(64);
        UsgsParseContext usgsCtx = new UsgsParseContext("m-suc-u", "s3://bucket/usgs/suc.geojson", dummySha, "run-1", EVAL_TIME);
        UsgsParseResult usgsParsed = new UsgsGeoJsonParser().parse(usgsBytes, usgsCtx);
        assertEquals(1, usgsParsed.observations().size());

        byte[] jmaBytes = loadFixture("jma/fixed-width/success.hyp");
        JmaParseContext jmaCtx = new JmaParseContext("m-suc-j", "s3://bucket/jma/suc.hyp", dummySha, "run-1", "release-1", null, "https://example.invalid/jma", "success.hyp", EVAL_TIME);
        JmaParseResult jmaParsed = new JmaFixedWidthParser().parse(jmaBytes, jmaCtx);
        assertEquals(2, jmaParsed.observations().size());

        SilverObservation usgsObs = usgsParsed.observations().get(0);
        SilverObservation jmaNatural = jmaParsed.observations().get(0);
        SilverObservation jmaArtificial = jmaParsed.observations().get(1);

        SilverResolutionResult result = SilverEntityResolver.resolve(
                List.of(usgsObs, jmaNatural, jmaArtificial),
                defaultConfig,
                EVAL_TIME);

        // Assertions from cases.json FX-LINK-02
        assertEquals(1, result.report().acceptedPairsCount());
        assertEquals(1, result.report().matchedEventsCount());
        assertEquals(2, result.report().canonicalEventsCount());
        assertEquals(1, result.report().jmaOnlyEventsCount());

        // Check matched event bridge count == 2
        SilverSourceLink acceptedLink = result.sourceLinks().stream()
                .filter(l -> SilverEntityResolver.DECISION_ACCEPTED.equals(l.linkDecision()))
                .findFirst().orElseThrow();
        assertEquals(0.0, acceptedLink.timeDeltaSeconds(), 1e-3);
        assertEquals(0.0, acceptedLink.distanceKm(), 1e-1);

        List<SilverCanonicalMembership> matchedMembers = result.canonicalMemberships().stream()
                .filter(m -> acceptedLink.sourceLinkId().equals(m.sourceLinkId()))
                .toList();
        assertEquals(2, matchedMembers.size()); // matched_event_bridge_count=2

        // Artificial event remains single source
        SilverCanonicalMembership artificialMember = result.canonicalMemberships().stream()
                .filter(m -> m.sourceObservationId().equals(jmaArtificial.sourceObservationId()))
                .findFirst().orElseThrow();
        assertEquals(SilverEntityResolver.STATUS_PRIMARY, artificialMember.membershipStatus());
        assertNull(artificialMember.sourceLinkId());
    }

    @Test
    void testRowConversionAndSchemaCompatibility() {
        Instant time = Instant.parse("2023-09-01T12:00:00.000Z");
        SilverObservation usgs = createObservation("u1", "USGS", "usgs-s1", time, 35.0, 139.0, 10.0, 5.0, "VALID", null, "EARTHQUAKE");
        SilverObservation jma = createObservation("j1", "JMA_BULLETIN", "jma-s1", time, 35.0, 139.0, 10.0, 5.0, "VALID", "J", "EARTHQUAKE");

        SilverSourceLink link = SilverEntityResolver.evaluatePair(usgs, jma, defaultConfig, EVAL_TIME);
        Row linkRow = link.toRow();
        assertEquals(12, linkRow.size());
        assertEquals(SilverSchemas.SOURCE_LINK_SCHEMA.fields().length, linkRow.size());
        assertEquals(link.sourceLinkId(), linkRow.getString(0));
        assertEquals(link.leftObservationId(), linkRow.getString(1));
        assertEquals(link.rightObservationId(), linkRow.getString(2));

        SilverCanonicalMembership membership = new SilverCanonicalMembership(
                "evt_test",
                usgs.sourceObservationId(),
                "PRIMARY",
                link.sourceLinkId(),
                defaultConfig.canonicalModelVersion(),
                EVAL_TIME);
        Row memberRow = membership.toRow();
        assertEquals(6, memberRow.size());
        assertEquals(SilverSchemas.CANONICAL_MEMBERSHIP_SCHEMA.fields().length, memberRow.size());
        assertEquals("evt_test", memberRow.getString(0));
        assertEquals(usgs.sourceObservationId(), memberRow.getString(1));
        assertEquals("PRIMARY", memberRow.getString(2));
    }

    @Test
    void testDeterministicAndSymmetricIds() {
        String id1 = "obs_11111111";
        String id2 = "obs_22222222";
        String linkId1 = SourceKeyGenerator.sourceLinkId("link_v1.0", id1, id2);
        String linkId2 = SourceKeyGenerator.sourceLinkId("link_v1.0", id2, id1);
        assertEquals(linkId1, linkId2, "sourceLinkId must be symmetric and deterministic");
        assertTrue(linkId1.startsWith("lnk_"));

        String canonId = SourceKeyGenerator.canonicalEventId("USGS", "usgs-test-id");
        assertTrue(canonId.startsWith("evt_"));
        assertEquals(36, canonId.length()); // "evt_" (4) + 32 hex = 36
    }
}
