package ie212.earthquake.spark.silver;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class JmaFixedWidthParserTest {
    static final Instant PROCESSED = Instant.parse("2026-10-07T03:00:00Z");
    static final String SOURCE_URL = "https://www.data.jma.go.jp/eqev/data/bulletin/data/hypo/h2023.zip";
    private final JmaFixedWidthParser parser = new JmaFixedWidthParser();
    @TempDir Path temporary;

    @Test
    void successMapsCompleteObservationAndNativeAuditFields() throws Exception {
        byte[] payload = fixture("fixed-width/success.hyp");
        JmaParseContext context = context(payload, "release-v1");
        JmaParseResult result = parser.parse(payload, context);
        assertEquals(2, result.parsedCount());
        assertEquals(2, result.validCount());
        assertEquals(0, result.rejectedCount());
        SilverObservation obs = result.observations().get(0);
        assertEquals("1.0", obs.schemaVersion());
        assertEquals("JMA_BULLETIN", obs.sourceSystem());
        assertEquals(Instant.parse("2023-09-01T03:34:56.780Z"), obs.eventTimeUtc());
        assertEquals(LocalDateTime.of(2023, 9, 1, 12, 34, 56, 780_000_000), obs.eventTimeJst());
        assertEquals(LocalDate.of(2023, 9, 1), obs.eventDateUtc());
        assertEquals(LocalDate.of(2023, 9, 1), obs.eventDateJst());
        assertEquals(2023, obs.eventYearUtc());
        assertEquals(9, obs.eventMonthUtc());
        assertEquals(35.67, obs.latitude(), 1e-10);
        assertEquals(139.76, obs.longitude(), 1e-10);
        assertEquals(10.0, obs.depthKm());
        assertEquals(5.2, obs.magnitude());
        assertEquals("J", obs.magnitudeType());
        assertEquals("EARTHQUAKE", obs.eventTypeCode());
        assertEquals("TOKYO BAY", obs.placeName());
        assertNull(obs.tsunamiFlag());
        assertNull(obs.alertLevel());
        assertNull(obs.significance());
        assertEquals("B", obs.maxIntensityCode());
        assertEquals("J", obs.determiningAgencyCode());
        assertEquals("UNIFIED", obs.catalogEra());
        assertEquals("K", obs.sourceStatus());
        assertEquals(SOURCE_URL, obs.sourceUrl());
        assertTrue(obs.isInStudyArea());
        assertEquals("VALID", obs.qualityStatus());
        assertTrue(obs.qualityFlags().isEmpty());
        assertEquals("release-v1", obs.catalogRelease());
        assertNull(obs.catalogReleaseAtUtc());
        assertNull(obs.sourceUpdatedAtUtc());
        assertFalse(obs.isCurrentSourceRevision());
        assertEquals(context.bronzeManifestId(), obs.bronzeManifestId());
        assertEquals(context.rawObjectUri(), obs.rawObjectUri());
        assertEquals(context.rawSha256(), obs.rawSha256());
        assertEquals(context.ingestRunId(), obs.ingestRunId());
        assertEquals("member=hypo.dat;line=1", obs.rawRecordLocator());
        assertEquals(JmaFixedWidthParser.PARSER_NAME, obs.parserName());
        assertEquals(JmaFixedWidthParser.PARSER_VERSION, obs.parserVersion());
        assertEquals(PROCESSED, obs.processedAtUtc());
        byte[] raw = Arrays.copyOf(payload, 96);
        assertEquals(SourceKeyGenerator.jmaRecordKeyFromLine(new String(raw, StandardCharsets.US_ASCII)), obs.sourceRecordKey());
        assertEquals(SourceKeyGenerator.jmaRevisionKey("release-v1", SourceKeyGenerator.sha256(raw)), obs.sourceRevisionKey());
        assertEquals(SourceKeyGenerator.observationId(obs.sourceSystem(), obs.sourceRecordKey(), obs.sourceRevisionKey()), obs.sourceObservationId());
        assertTrue(SilverLineage.of(obs).verifyRawRecord(raw));
        assertTrue(SilverLineage.of(obs).verifyRawObject(payload));
        JmaNativeFields nativeRow = result.nativeFields().get(0);
        assertEquals(obs.rawRecordLocator(), nativeRow.rawRecordLocator());
        assertEquals("52", nativeRow.magnitude1Raw());
        assertEquals("  ", nativeRow.magnitude2Raw());
        assertEquals("7", nativeRow.travelTimeTableRaw());
        assertEquals("1", nativeRow.locationPrecisionRaw());
        assertEquals("1", nativeRow.subsidiaryInformationRaw());
        assertEquals(" ", nativeRow.tsunamiClassRaw());
        assertEquals("3", nativeRow.districtNumberRaw());
        assertEquals("123", nativeRow.regionNumberRaw());
        assertEquals("042", nativeRow.stationCountRaw());
        assertEquals("K", nativeRow.determinationFlagRaw());
        assertEquals(24, nativeRow.regionNameRaw().length());
        assertEquals("ARTIFICIAL", result.observations().get(1).eventTypeCode());
    }

    @Test
    void sharedFixtureCountsMatchCon04WithoutDeduplication() throws Exception {
        JsonNode cases = new ObjectMapper().readTree(Files.readAllBytes(repositoryRoot().resolve("tests/fixtures/cases.json")));
        for (JsonNode item : cases.path("cases")) {
            if (!"JMA_BULLETIN".equals(item.path("source_system").asText())) continue;
            String category = item.path("category").asText();
            if (List.of("success", "empty", "duplicate", "timezone").contains(category)) {
                byte[] bytes = Files.readAllBytes(repositoryRoot().resolve("tests/fixtures/" + item.path("input_paths").get(0).asText()));
                JmaParseResult result = parser.parse(bytes, context(bytes, "release-v1"));
                assertEquals(item.path("expected").path("parsed_count").asInt(), result.parsedCount(), item.path("case_id").asText());
                assertEquals(item.path("expected").path("valid_count").asInt(), result.validCount());
                assertEquals(item.path("expected").path("rejected_count").asInt(), result.rejectedCount());
            }
        }
    }

    @Test
    void emptyMemberAndArchiveProduceZeroRowsNotRejects() throws Exception {
        byte[] empty = fixture("fixed-width/empty.hyp");
        assertEquals(0, parser.parse(empty, context(empty, "release-empty")).parsedCount());
        byte[] archive = fixture("archives/empty.zip");
        JmaParseResult result = parser.parseArchive(archive, context(archive, "release-empty"));
        assertEquals(0, result.parsedCount());
        assertEquals(0, result.nativeFields().size());
    }

    @Test
    void timezoneBoundaryAndCatalogEraAreBasedOnNativeOrigin() throws Exception {
        byte[] bytes = fixture("fixed-width/timezone-boundary.hyp");
        var result = parser.parse(bytes, context(bytes, "release-boundary"));
        assertEquals(Instant.parse("2023-12-31T14:59:00Z"), result.observations().get(0).eventTimeUtc());
        assertEquals(Instant.parse("1997-09-30T14:59:59Z"), result.observations().get(1).eventTimeUtc());
        assertEquals("LEGACY", result.observations().get(1).catalogEra());
        assertEquals(Instant.parse("1997-09-30T15:00:00Z"), result.observations().get(2).eventTimeUtc());
        assertEquals("UNIFIED", result.observations().get(2).catalogEra());
    }

    @Test
    void jstNewYearPartitionsInPreviousUtcYearAndMonth() throws Exception {
        byte[] row = put(firstRecord(), 2, 17, "2024010100000000");
        SilverObservation obs = one(row);
        assertEquals(Instant.parse("2023-12-31T15:00:00Z"), obs.eventTimeUtc());
        assertEquals(LocalDate.of(2024, 1, 1), obs.eventDateJst());
        assertEquals(2023, obs.eventYearUtc());
        assertEquals(12, obs.eventMonthUtc());
        assertEquals(new SilverPartitionKey(2023, 12, "JMA_BULLETIN"), SilverPartitionKey.from(obs));
    }

    @Test
    void conversionDoesNotUseHostTimezone() throws Exception {
        byte[] row = firstRecord();
        TimeZone original = TimeZone.getDefault();
        try {
            for (String zone : List.of("UTC", "Asia/Ho_Chi_Minh", "America/Los_Angeles")) {
                TimeZone.setDefault(TimeZone.getTimeZone(zone));
                assertEquals(Instant.parse("2023-09-01T03:34:56.780Z"), one(row).eventTimeUtc());
                assertEquals(LocalDateTime.of(2023, 9, 1, 12, 34, 56, 780_000_000), one(row).eventTimeJst());
            }
        } finally {
            TimeZone.setDefault(original);
        }
    }

    @Test
    void fixedHypocenterAndDepthSliceTrailingBlanksPreserveIntegerPlaces() throws Exception {
        byte[] row = put(firstRecord(), 14, 17, "56  ");
        row = put(row, 25, 28, "40  ");
        row = put(row, 37, 40, "45  ");
        row = put(row, 45, 49, "123  ");
        row = put(row, 60, 60, "2");
        SilverObservation obs = one(row);
        assertEquals(Instant.parse("2023-09-01T03:34:56Z"), obs.eventTimeUtc());
        assertEquals(35 + 40.0 / 60, obs.latitude(), 1e-10);
        assertEquals(139.75, obs.longitude(), 1e-10);
        assertEquals(123.0, obs.depthKm());
    }

    @Test
    void explicitDecimalOverridesImpliedScaleAndRightAlignmentIsNotWholeUnits() throws Exception {
        byte[] row = put(firstRecord(), 14, 17, "1.25");
        row = put(row, 25, 28, ".50 ");
        row = put(row, 37, 40, "2.50");
        row = put(row, 45, 49, "12.34");
        assertEquals(Instant.parse("2023-09-01T03:34:01.250Z"), one(row).eventTimeUtc());
        assertEquals(35 + 0.5 / 60, one(row).latitude(), 1e-10);
        assertEquals(139 + 2.5 / 60, one(row).longitude(), 1e-10);
        assertEquals(12.34, one(row).depthKm());
        assertEquals(0.5, one(put(firstRecord(), 53, 54, " 5")).magnitude());
        assertEquals(0.01, one(put(firstRecord(), 45, 49, "    1")).depthKm());
    }

    @Test
    void encodedNegativeMagnitudesAreNotDroppedOrZeroed() throws Exception {
        for (var entry : Map.of("-1", -0.1, "-9", -0.9, "A0", -1.0, "A9", -1.9, "B0", -2.0, "C0", -3.0).entrySet()) {
            assertEquals(entry.getValue(), one(put(firstRecord(), 53, 54, entry.getKey())).magnitude(), entry.getKey());
        }
    }

    @Test
    void magnitudeOneIsPreferredAndTwoOnlyFillsMissingOne() throws Exception {
        byte[] row = put(firstRecord(), 56, 58, "61W");
        assertEquals(5.2, one(row).magnitude());
        assertEquals("J", one(row).magnitudeType());
        row = put(row, 53, 55, "   ");
        assertEquals(6.1, one(row).magnitude());
        assertEquals("W", one(row).magnitudeType());
        row = put(row, 53, 54, "??");
        var result = parser.parse(row, context(row, "release-v1"));
        assertEquals(0, result.validCount());
        assertTrue(result.rejects().get(0).rejectReasonCodes().contains("INVALID_NUMBER"));
    }

    @Test
    void missingOptionalNumbersAndCodesStayNullNotZeroOrFalse() throws Exception {
        byte[] row = put(firstRecord(), 45, 58, " ".repeat(14));
        row = put(row, 61, 96, " ".repeat(36));
        SilverObservation obs = one(row);
        assertNull(obs.depthKm());
        assertNull(obs.magnitude());
        assertNull(obs.magnitudeType());
        assertNull(obs.maxIntensityCode());
        assertNull(obs.tsunamiFlag());
        assertNull(obs.sourceStatus());
        assertNull(obs.placeName());
        assertEquals("UNKNOWN", obs.eventTypeCode());
        assertEquals("VALID", obs.qualityStatus());
    }

    @Test
    void negativeDepthIsRetainedWithWarning() throws Exception {
        var obs = one(put(firstRecord(), 45, 49, "-0100"));
        assertEquals(-1.0, obs.depthKm());
        assertEquals("WARNING", obs.qualityStatus());
        assertEquals(List.of("NEGATIVE_DEPTH"), obs.qualityFlags());
    }

    @Test
    void allAgenciesKeepBulletinSourceAndAllDeterminationFlagsKeepCase() throws Exception {
        for (String agency : JmaCodeMapping.AGENCIES) {
            for (String flag : JmaCodeMapping.DETERMINATION_FLAGS.keySet()) {
                byte[] row = put(put(firstRecord(), 1, 1, agency), 96, 96, flag);
                SilverObservation obs = one(row);
                assertEquals("JMA_BULLETIN", obs.sourceSystem());
                assertEquals(agency, obs.determiningAgencyCode());
                assertEquals(flag, obs.sourceStatus());
                assertEquals("VALID", obs.qualityStatus());
            }
        }
        byte[] invalid = put(firstRecord(), 1, 1, "j");
        assertTrue(parser.parse(invalid, context(invalid, "release-v1")).rejects().get(0)
                .rejectReasonCodes().contains("UNSUPPORTED_RECORD_TYPE"));
    }

    @Test
    void categoryMappingRetainsArtificialEruptionLowFrequencyAndInsufficientStations() throws Exception {
        for (var entry : JmaCodeMapping.EVENT_TYPES.entrySet()) {
            assertEquals(entry.getValue(), one(put(firstRecord(), 61, 61, entry.getKey())).eventTypeCode());
        }
        SilverObservation unknown = one(put(firstRecord(), 61, 61, "9"));
        assertEquals("UNKNOWN", unknown.eventTypeCode());
        assertTrue(unknown.qualityFlags().contains("UNKNOWN_EVENT_CATEGORY"));
    }

    @Test
    void magnitudeTypesAndIntensityCodesPreserveNativeCaseWithoutMixingAdjacentColumns() throws Exception {
        for (String type : JmaCodeMapping.MAGNITUDE_TYPES.keySet()) {
            assertEquals(type, one(put(firstRecord(), 55, 55, type)).magnitudeType());
        }
        for (String intensity : JmaCodeMapping.INTENSITY_CODES) {
            assertEquals(intensity, one(put(firstRecord(), 62, 62, intensity)).maxIntensityCode());
        }
        byte[] row = put(firstRecord(), 53, 68, "52d31v7M5DYT4123");
        row = put(row, 69, 92, "ABCDEFGHIJKLMNOPQRSTUVWX");
        var result = parser.parse(row, context(row, "release-v1"));
        var obs = result.observations().get(0);
        var nativeRow = result.nativeFields().get(0);
        assertEquals("d", obs.magnitudeType());
        assertEquals("EARTHQUAKE", obs.eventTypeCode());
        assertEquals("D", obs.maxIntensityCode());
        assertEquals(Boolean.TRUE, obs.tsunamiFlag());
        assertEquals("ABCDEFGHIJKLMNOPQRSTUVWX", obs.placeName());
        assertEquals("31", nativeRow.magnitude2Raw());
        assertEquals("v", nativeRow.magnitudeType2Raw());
        assertEquals("M", nativeRow.locationPrecisionRaw());
        assertEquals("Y", nativeRow.damageClassRaw());
        assertEquals("4", nativeRow.districtNumberRaw());
        assertEquals("123", nativeRow.regionNumberRaw());
        assertEquals("042", nativeRow.stationCountRaw());
        assertEquals("K", nativeRow.determinationFlagRaw());
    }

    @Test
    void tsunamiOnlyMapsDocumentedPositiveCodesAndUnknownIsNotFalse() throws Exception {
        for (String code : JmaCodeMapping.TSUNAMI_CODES) {
            assertEquals(Boolean.TRUE, one(put(firstRecord(), 64, 64, code)).tsunamiFlag());
        }
        for (String unknown : List.of("0", "X")) {
            var obs = one(put(firstRecord(), 64, 64, unknown));
            assertNull(obs.tsunamiFlag());
            assertTrue(obs.qualityFlags().contains("UNKNOWN_TSUNAMI_CODE"));
        }
        assertNull(one(put(firstRecord(), 64, 64, " ")).tsunamiFlag());
    }

    @Test
    void coordinateSignsPolesAndMinutesRespectDegreesNotDecimalText() throws Exception {
        byte[] row = put(firstRecord(), 22, 28, "-353000");
        row = put(row, 33, 40, "-1391500");
        assertEquals(-35.5, one(row).latitude(), 1e-10);
        assertEquals(-139.25, one(row).longitude(), 1e-10);
        assertFalse(one(row).isInStudyArea());
        row = put(firstRecord(), 22, 28, "-003000");
        row = put(row, 33, 40, "-0003000");
        assertEquals(-0.5, one(row).latitude(), 1e-10);
        assertEquals(-0.5, one(row).longitude(), 1e-10);
        row = put(firstRecord(), 22, 28, "0900000");
        row = put(row, 33, 40, "01800000");
        assertEquals(90.0, one(row).latitude());
        assertEquals(180.0, one(row).longitude());
    }

    @Test
    void roiBoundariesClassifyWithoutDroppingOutOfAreaEvents() throws Exception {
        for (String coordinates : List.of("020000001200000", "050000001550000")) {
            byte[] row = put(firstRecord(), 22, 28, coordinates.substring(0, 7));
            row = put(row, 33, 40, coordinates.substring(7));
            assertTrue(one(row).isInStudyArea());
        }
        assertFalse(one(put(firstRecord(), 22, 28, "0195999")).isInStudyArea());
        assertFalse(one(put(firstRecord(), 33, 40, "01550001")).isInStudyArea());
    }

    @Test
    void invalidDatesMissingSecondsAndInvalidMinutesProduceAuditableRowRejects() throws Exception {
        for (String origin : List.of("2023130112345678", "2023023012345678", "2023022912345678",
                "0000090112345678", "2023090124345678",
                "2023090112605678", "2023090112346000", "202309011234    ", "    090112345678")) {
            byte[] row = put(firstRecord(), 2, 17, origin);
            var result = parser.parse(row, context(row, "release-v1"));
            assertEquals(1, result.parsedCount());
            assertEquals(0, result.validCount());
            var reject = result.rejects().get(0);
            assertTrue(reject.rejectReasonCodes().contains("INVALID_EVENT_TIME"), origin);
            assertEquals("PARSE", reject.rejectStage());
            assertEquals("member=hypo.dat;line=1", reject.rawRecordLocator());
            assertEquals(SourceKeyGenerator.sha256(row), reject.rawRecordHash());
            assertEquals(PROCESSED, reject.rejectedAtUtc());
            assertEquals("JMA_BULLETIN", reject.sourceSystem());
            assertNull(reject.sourceRecordKeyCandidate());
            assertEquals(0, result.nativeFields().size());
        }
    }

    @Test
    void invalidOrMissingCoordinatesNeverCreateFakeZeros() throws Exception {
        for (String latitude : List.of("0910000", "0900001", "0356000", "035-100", "   0000", "035    ", "abc4020")) {
            byte[] row = put(firstRecord(), 22, 28, latitude);
            var result = parser.parse(row, context(row, "release-v1"));
            assertEquals(0, result.validCount(), latitude);
            assertTrue(result.rejects().get(0).rejectReasonCodes().contains("INVALID_LATITUDE"));
        }
        for (String longitude : List.of("01810000", "01800001", "01396000", "    0000")) {
            byte[] row = put(firstRecord(), 33, 40, longitude);
            assertTrue(parser.parse(row, context(row, "release-v1")).rejects().get(0)
                    .rejectReasonCodes().contains("INVALID_LONGITUDE"));
        }
    }

    @Test
    void malformedOptionalNumbersRejectRatherThanMasqueradeAsMissing() throws Exception {
        for (int[] field : new int[][] {{18, 21}, {29, 32}, {41, 44}, {45, 49}, {50, 52},
                {53, 54}, {56, 57}, {65, 65}, {66, 68}, {93, 95}}) {
            byte[] row = put(firstRecord(), field[0], field[1], "?".repeat(field[1] - field[0] + 1));
            var result = parser.parse(row, context(row, "release-v1"));
            assertEquals(0, result.validCount());
            assertTrue(result.rejects().get(0).rejectReasonCodes().contains("INVALID_NUMBER"));
        }
    }

    @Test
    void lfCrLfAndUnterminatedRecordHaveIdenticalRawHashesAndIds() throws Exception {
        byte[] record = firstRecord();
        String line = new String(record, StandardCharsets.US_ASCII);
        var result = parser.parse((line + "\n" + line + "\r\n" + line).getBytes(StandardCharsets.US_ASCII), context(record, "release-v1"));
        assertEquals(3, result.validCount());
        assertEquals(1, result.observations().stream().map(SilverObservation::rawRecordHash).distinct().count());
        assertEquals(1, result.observations().stream().map(SilverObservation::sourceObservationId).distinct().count());
        assertEquals("member=hypo.dat;line=3", result.observations().get(2).rawRecordLocator());
    }

    @Test
    void structuralCorruptionFailsWholeInputInsteadOfReturningPartialSuccess() throws Exception {
        byte[] row = firstRecord();
        String line = new String(row, StandardCharsets.US_ASCII);
        for (byte[] malformed : List.of(fixture("fixed-width/invalid-record-length.hyp"), Arrays.copyOf(row, 97),
                (line + "\n\n").getBytes(StandardCharsets.US_ASCII), (line + "\r").getBytes(StandardCharsets.US_ASCII),
                (line + "\n" + line.substring(0, 95)).getBytes(StandardCharsets.US_ASCII))) {
            assertTrue(assertThrows(IOException.class, () -> parser.parse(malformed, context(malformed, "release-v1")))
                    .getMessage().contains("INVALID_RECORD_LENGTH"));
        }
    }

    @Test
    void nonAsciiBytesAreNotDecodedAsUtf8OrCountedAsCharacters() throws Exception {
        byte[] row = firstRecord();
        row[68] = (byte) 0xc3;
        row[69] = (byte) 0xa9;
        var result = parser.parse(row, context(row, "release-v1"));
        assertEquals(1, result.rejectedCount());
        assertTrue(result.rejects().get(0).rejectReasonCodes().contains("CONTRACT_MISMATCH"));
        assertEquals(SourceKeyGenerator.sha256(row), result.rejects().get(0).rawRecordHash());
        assertEquals(96, row.length);
    }

    @Test
    void duplicateAndRevisionKeysRemainStableWithoutChoosingCurrent() throws Exception {
        byte[] duplicate = fixture("fixed-width/duplicate.hyp");
        var result = parser.parse(duplicate, context(duplicate, "release-v1"));
        assertEquals(2, result.validCount());
        assertEquals(result.observations().get(0).sourceObservationId(), result.observations().get(1).sourceObservationId());
        assertEquals(result, parser.parse(duplicate, context(duplicate, "release-v1")));
        byte[] v1 = fixture("fixed-width/revision-v1.hyp");
        byte[] v2 = fixture("fixed-width/revision-v2.hyp");
        var first = parser.parse(v1, context(v1, "release-v1")).observations().get(0);
        var next = parser.parse(v2, context(v2, "release-v2")).observations().get(0);
        assertEquals(first.sourceRecordKey(), next.sourceRecordKey());
        assertNotEquals(first.sourceRevisionKey(), next.sourceRevisionKey());
        assertNotEquals(first.sourceObservationId(), next.sourceObservationId());
        assertEquals(4.0, first.magnitude());
        assertEquals(4.2, next.magnitude());
        assertFalse(first.isCurrentSourceRevision());
        assertFalse(next.isCurrentSourceRevision());
    }

    @Test
    void sparkRowsMatchExistingSchemasIncludingNullsAndRejectLineage() throws Exception {
        byte[] row = firstRecord();
        var obs = one(row);
        Row sparkRow = obs.toRow();
        assertEquals(SilverSchemas.OBSERVATION_SCHEMA.size(), sparkRow.size());
        assertEquals("JMA_BULLETIN", sparkRow.getString(SilverSchemas.OBSERVATION_SCHEMA.fieldIndex("source_system")));
        assertEquals("K", sparkRow.getString(SilverSchemas.OBSERVATION_SCHEMA.fieldIndex("source_status")));
        assertEquals(obs.eventTimeUtc(), sparkRow.getTimestamp(SilverSchemas.OBSERVATION_SCHEMA.fieldIndex("event_time_utc")).toInstant());
        assertTrue(sparkRow.isNullAt(SilverSchemas.OBSERVATION_SCHEMA.fieldIndex("source_updated_at_utc")));
        byte[] invalid = put(row, 1, 1, "?");
        var reject = parser.parse(invalid, context(invalid, "release-v1")).rejects().get(0);
        assertEquals(SilverSchemas.REJECT_SCHEMA.size(), reject.toRow().size());
        assertTrue(SilverLineage.of(reject).verifyRawRecord(invalid));
    }

    @Test
    void archiveApisVerifyChecksumMemberAndStructureBeforeParsing() throws Exception {
        byte[] archive = fixture("archives/success.zip");
        var context = context(archive, "release-v1");
        Path file = temporary.resolve("success.zip");
        Files.write(file, archive);
        assertEquals(parser.parseArchive(archive, context), parser.parseArchive(file, context));
        var wrongMember = new JmaParseContext(context.bronzeManifestId(), context.rawObjectUri(), context.rawSha256(),
                context.ingestRunId(), context.catalogRelease(), null, SOURCE_URL, "h2023", PROCESSED);
        assertTrue(assertThrows(IOException.class, () -> parser.parseArchive(archive, wrongMember))
                .getMessage().contains("UNEXPECTED_MEMBER_NAME"));
        byte[] invalid = fixture("archives/invalid-record-length.zip");
        assertTrue(assertThrows(IOException.class, () -> parser.parseArchive(invalid, context(invalid, "release-v1")))
                .getMessage().contains("INVALID_RECORD_LENGTH"));
        byte[] corrupt = archive.clone();
        corrupt[0] ^= 1;
        assertTrue(assertThrows(IOException.class, () -> parser.parseArchive(corrupt, context))
                .getMessage().contains("CHECKSUM_MISMATCH"));
        byte[] truncated = Arrays.copyOf(archive, archive.length - 22);
        assertThrows(IOException.class, () -> parser.parseArchive(truncated, context(truncated, "release-v1")));
    }

    @Test
    void resourceGuardsBoundArchiveAndMemberReads() throws Exception {
        byte[] archive = fixture("archives/success.zip");
        assertTrue(assertThrows(IOException.class, () -> new JmaFixedWidthParser(1, 512)
                .parseArchive(archive, context(archive, "release-v1"))).getMessage().contains("ARCHIVE_SIZE_LIMIT"));
        byte[] row = firstRecord();
        assertTrue(assertThrows(IOException.class, () -> new JmaFixedWidthParser(1024, 95)
                .parse(row, context(row, "release-v1"))).getMessage().contains("MEMBER_SIZE_LIMIT"));
        assertThrows(IllegalArgumentException.class, () -> new JmaFixedWidthParser(0, 96));
    }

    @Test
    void contextRequiresReleaseLineageSafeMemberAndPublicArchiveUrl() throws Exception {
        var good = context(firstRecord(), "release-v1");
        for (String release : Arrays.asList(null, "", " release ", "release\n")) {
            assertThrows(IllegalArgumentException.class, () -> new JmaParseContext(good.bronzeManifestId(), good.rawObjectUri(),
                    good.rawSha256(), good.ingestRunId(), release, null, SOURCE_URL, good.memberName(), PROCESSED));
        }
        for (String member : List.of("../h2023", "/h2023", "folder/h2023", "h2023\\file")) {
            assertThrows(IllegalArgumentException.class, () -> new JmaParseContext(good.bronzeManifestId(), good.rawObjectUri(),
                    good.rawSha256(), good.ingestRunId(), good.catalogRelease(), null, SOURCE_URL, member, PROCESSED));
        }
        for (String url : List.of("https://user:fixture@example.invalid/h.zip", "https://example.invalid/h.zip?token=fixture", "file:///tmp/h.zip")) {
            assertThrows(IllegalArgumentException.class, () -> new JmaParseContext(good.bronzeManifestId(), good.rawObjectUri(),
                    good.rawSha256(), good.ingestRunId(), good.catalogRelease(), null, url, good.memberName(), PROCESSED));
        }
        assertThrows(IllegalArgumentException.class, () -> new JmaParseContext(good.bronzeManifestId(), "s3://user:fixture@bucket/a.zip",
                good.rawSha256(), good.ingestRunId(), good.catalogRelease(), null, SOURCE_URL, good.memberName(), PROCESSED));
    }

    @Test
    void knownReleaseTimeIsPropagatedAndResultCollectionsAreDefensive() throws Exception {
        byte[] row = firstRecord();
        var good = context(row, "release-v1");
        Instant releaseTime = Instant.parse("2025-12-10T01:41:53Z");
        var context = new JmaParseContext(good.bronzeManifestId(), good.rawObjectUri(), good.rawSha256(), good.ingestRunId(),
                good.catalogRelease(), releaseTime, SOURCE_URL, good.memberName(), PROCESSED);
        var result = parser.parse(row, context);
        assertEquals(releaseTime, result.observations().get(0).catalogReleaseAtUtc());
        var observations = new ArrayList<>(result.observations());
        var copy = new JmaParseResult(observations, List.of(), result.nativeFields());
        observations.clear();
        assertEquals(1, copy.validCount());
        assertThrows(UnsupportedOperationException.class, () -> copy.observations().clear());
        assertThrows(UnsupportedOperationException.class, () -> copy.nativeFields().clear());
        assertThrows(IllegalArgumentException.class, () -> new JmaParseResult(result.observations(), List.of(), List.of()));
        Path member = temporary.resolve("member.hyp");
        Files.write(member, row);
        assertEquals(result, parser.parse(member, context));
    }

    private SilverObservation one(byte[] row) throws Exception {
        var result = parser.parse(row, context(row, "release-v1"));
        assertEquals(0, result.rejectedCount(), result.rejects().toString());
        assertEquals(1, result.validCount());
        return result.observations().get(0);
    }

    static JmaParseContext context(byte[] rawObject, String release) {
        return new JmaParseContext("manifest-jma-01", "s3://fixture-bucket/bronze/jma/archive.zip", SourceKeyGenerator.sha256(rawObject),
                "run-jma-01", release, null, SOURCE_URL, "hypo.dat", PROCESSED);
    }

    static byte[] firstRecord() throws Exception { return Arrays.copyOf(fixture("fixed-width/success.hyp"), 96); }

    static byte[] put(byte[] original, int start, int end, String value) {
        if (value.length() != end - start + 1) throw new IllegalArgumentException("test field width mismatch: " + value);
        byte[] result = original.clone();
        System.arraycopy(value.getBytes(StandardCharsets.US_ASCII), 0, result, start - 1, value.length());
        return result;
    }

    static byte[] fixture(String path) throws IOException { return Files.readAllBytes(repositoryRoot().resolve("tests/fixtures/jma/" + path)); }

    static Path repositoryRoot() {
        Path current = Path.of(System.getProperty("user.dir")).toAbsolutePath();
        while (current != null) {
            if (Files.isRegularFile(current.resolve("tests/fixtures/cases.json"))) return current;
            current = current.getParent();
        }
        throw new IllegalStateException("Repository fixture root not found");
    }
}
