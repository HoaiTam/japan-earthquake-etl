package ie212.earthquake.spark.silver;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import ie212.earthquake.spark.jma.JmaArchiveEntry;
import ie212.earthquake.spark.jma.JmaBronzeWriteRequest;
import ie212.earthquake.spark.jma.JmaBronzeWriter;
import ie212.earthquake.spark.jma.JmaHttpMetadata;
import ie212.earthquake.spark.usgs.FileBronzeObjectStore;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class JmaParserIntegrationTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Instant PROCESSED = JmaFixedWidthParserTest.PROCESSED;
    private final JmaFixedWidthParser parser = new JmaFixedWidthParser();
    private final SilverQualityValidator validator = new SilverQualityValidator(Clock.fixed(PROCESSED, ZoneOffset.UTC));
    @TempDir Path temporary;

    @Test
    void bronzeResolverParserQualityAndParquetKeepFieldsAndLineage() throws Exception {
        byte[] archive = JmaFixedWidthParserTest.fixture("archives/success.zip");
        ResolvedBronzeInput resolved = publishAndResolve(archive, "run-integration", "hypo.dat");
        var parsed = parser.parse(resolved, "hypo.dat", JmaFixedWidthParserTest.SOURCE_URL, PROCESSED);
        var quality = validator.validate(parsed, resolved.runId());
        assertEquals(2, quality.parsedCount());
        assertEquals(2, quality.validCount());
        assertFalse(quality.publishBlocked());
        var silver = new FileSilverObjectStore(temporary.resolve("silver-output"));
        var request = new SilverWriteRequest(resolved.runId(), quality.validObservations(), quality.rejectedRecords(), PROCESSED, true);
        var output = new SilverParquetWriter(silver).write(request, quality);
        assertEquals("SilverReady", output.silverStatus());
        assertEquals(2, output.totalObservations());
        assertEquals(1, output.publishedPartitions().size());
        var partition = output.publishedPartitions().get(0);
        assertEquals("JMA_BULLETIN", partition.sourceSystem());
        var file = partition.files().get(0);
        byte[] parquet = silver.read(file.relativePath());
        assertEquals(file.sha256(), SilverParquetWriter.sha256(parquet));
        SilverParquetSerializer.verifyParquet(parquet, 2, SilverParquetSerializer.OBSERVATION_PARQUET_SCHEMA);
        Path localParquet = temporary.resolve("readback.parquet");
        Files.write(localParquet, parquet);
        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(),
                new org.apache.hadoop.fs.Path(localParquet.toUri())).withConf(new Configuration()).build()) {
            Group first = reader.read();
            assertEquals("JMA_BULLETIN", first.getString("source_system", 0));
            assertEquals("release-integration", first.getString("catalog_release", 0));
            assertEquals("K", first.getString("source_status", 0));
            assertEquals("J", first.getString("determining_agency_code", 0));
            assertEquals("J", first.getString("magnitude_type", 0));
            assertEquals("B", first.getString("max_intensity_code", 0));
            assertEquals(5.2, first.getDouble("magnitude", 0));
            assertEquals(Instant.parse("2023-09-01T03:34:56.780Z").toEpochMilli(), first.getLong("event_time_utc", 0));
            assertEquals(resolved.manifestId(), first.getString("bronze_manifest_id", 0));
            assertEquals(resolved.sha256(), first.getString("raw_sha256", 0));
            assertEquals("member=hypo.dat;line=1", first.getString("raw_record_locator", 0));
            assertEquals(0, first.getFieldRepetitionCount("tsunami_flag"));
            assertEquals("ARTIFICIAL", reader.read().getString("event_type_code", 0));
            assertNull(reader.read());
        }
        byte[] marker = silver.read(SilverStorageLayout.successMarkerPath(new SilverPartitionKey(2023, 9, "JMA_BULLETIN")));
        var retry = new SilverParquetWriter(silver).write(request, quality);
        assertTrue(retry.idempotentReuse());
        assertArrayEquals(marker, silver.read(SilverStorageLayout.successMarkerPath(new SilverPartitionKey(2023, 9, "JMA_BULLETIN"))));
        assertArrayEquals(archive, Files.readAllBytes(resolved.stagedObject()));
    }

    @Test
    void parserAndQualityRejectsBothBlockCheckedPublicationWithoutStorageWrites() throws Exception {
        byte[] valid = JmaFixedWidthParserTest.firstRecord();
        byte[] invalidTime = JmaFixedWidthParserTest.put(valid, 6, 7, "13");
        byte[] invalidFlag = JmaFixedWidthParserTest.put(valid, 96, 96, "X");
        byte[] member = records(valid, invalidTime, invalidFlag);
        var parsed = parser.parse(member, JmaFixedWidthParserTest.context(member, "release-v1"));
        assertEquals(3, parsed.parsedCount());
        assertEquals(2, parsed.validCount());
        assertEquals(1, parsed.rejectedCount());
        var quality = validator.validate(parsed, "run-jma-01");
        assertEquals(3, quality.parsedCount());
        assertEquals(1, quality.validCount());
        assertEquals(2, quality.rejectedCount());
        assertEquals(1L, quality.reasonCounts().get("INVALID_EVENT_TIME"));
        assertEquals(1L, quality.reasonCounts().get("CONTRACT_MISMATCH"));
        assertEquals("PARSE", quality.rejectedRecords().get(0).rejectStage());
        assertEquals("VALIDATE", quality.rejectedRecords().get(1).rejectStage());
        assertTrue(quality.publishBlocked());
        Path storageRoot = temporary.resolve("blocked-output");
        var store = new FileSilverObjectStore(storageRoot);
        var request = new SilverWriteRequest("run-jma-01", quality.validObservations(), quality.rejectedRecords(), PROCESSED, true);
        assertThrows(IllegalStateException.class, () -> new SilverParquetWriter(store).write(request, quality));
        assertFalse(Files.exists(storageRoot)); // Gate ran before even creating the storage root.
        Path summary = new SilverQualitySummaryWriter().write(temporary.resolve("quality.json"), quality, "JMA_BULLETIN");
        JsonNode json = JSON.readTree(Files.readAllBytes(summary));
        assertTrue(json.path("publish_blocked").asBoolean());
        assertEquals(2, json.path("rejected_records").size());
        assertEquals("member=hypo.dat;line=2", json.path("rejected_records").get(0).path("raw_record_locator").asText());
        assertThrows(IllegalArgumentException.class, () -> validator.validate(parsed, "foreign-run"));
    }

    @Test
    void allParserRejectsCannotBeMistakenForEmptyValidInput() throws Exception {
        byte[] row = JmaFixedWidthParserTest.put(JmaFixedWidthParserTest.firstRecord(), 2, 17, " ".repeat(16));
        var result = parser.parse(row, JmaFixedWidthParserTest.context(row, "release-v1"));
        var quality = validator.validate(result, "run-jma-01");
        assertEquals(0, quality.validCount());
        assertEquals(1, quality.rejectedCount());
        assertTrue(quality.publishBlocked());
        var empty = parser.parse(new byte[0], JmaFixedWidthParserTest.context(row, "release-v1"));
        assertEquals(0, validator.validate(empty, "run-jma-01").parsedCount());
        assertFalse(validator.validate(empty, "run-jma-01").publishBlocked());
    }

    @Test
    void stagedArchiveChangesAndForeignContextFailBeforeAnyRowIsReturned() throws Exception {
        byte[] archive = JmaFixedWidthParserTest.fixture("archives/success.zip");
        var input = publishAndResolve(archive, "run-integrity", "hypo.dat");
        var context = JmaParseContext.of(input, "hypo.dat", JmaFixedWidthParserTest.SOURCE_URL, PROCESSED);
        var wrong = new JmaParseContext(context.bronzeManifestId(), context.rawObjectUri(), context.rawSha256(),
                context.ingestRunId(), "different-release", null, context.sourceUrl(), context.memberName(), PROCESSED);
        assertThrows(IllegalArgumentException.class, () -> parser.parse(input, wrong));
        var foreign = new ResolvedBronzeInput(input.manifestId(), "USGS", input.catalogRelease(), input.runId(), input.rawObjectUri(),
                input.rawObjectKey(), input.sha256(), input.contentLengthBytes(), input.stagedObject(), input.stagedManifest(), false);
        assertThrows(IllegalArgumentException.class, () -> JmaParseContext.of(foreign, "hypo.dat", null, PROCESSED));
        byte[] changed = archive.clone();
        changed[0] ^= 1;
        Files.write(input.stagedObject(), changed);
        assertTrue(assertThrows(IOException.class, () -> parser.parse(input, context)).getMessage().contains("CHECKSUM_MISMATCH"));
        Files.write(input.stagedObject(), new byte[0]);
        assertTrue(assertThrows(IOException.class, () -> parser.parse(input, context)).getMessage().contains("CONTRACT_MISMATCH"));
    }

    @Test
    void exactInventoryMemberNameIsUsedInsteadOfHardcodedFixtureName() throws Exception {
        byte[] record = JmaFixedWidthParserTest.firstRecord();
        var buffer = new ByteArrayOutputStream();
        try (var zip = new ZipOutputStream(buffer)) {
            zip.putNextEntry(new ZipEntry("h2023"));
            zip.write(record);
            zip.closeEntry();
        }
        var resolved = publishAndResolve(buffer.toByteArray(), "run-member", "h2023");
        var parsed = parser.parse(resolved, "h2023", JmaFixedWidthParserTest.SOURCE_URL, PROCESSED);
        assertEquals(1, parsed.validCount());
        assertEquals("member=h2023;line=1", parsed.observations().get(0).rawRecordLocator());
        assertThrows(IOException.class, () -> parser.parse(resolved, "hypo.dat", JmaFixedWidthParserTest.SOURCE_URL, PROCESSED));
    }

    @Test
    void lowPrecisionForeignAgencyWarningsAndArtificialRecordsRemainPublishable() throws Exception {
        byte[] row = JmaFixedWidthParserTest.firstRecord();
        row = JmaFixedWidthParserTest.put(row, 1, 1, "I");
        row = JmaFixedWidthParserTest.put(row, 45, 49, "-0100");
        row = JmaFixedWidthParserTest.put(row, 61, 61, "3");
        row = JmaFixedWidthParserTest.put(row, 96, 96, "N");
        var parsed = parser.parse(row, JmaFixedWidthParserTest.context(row, "release-v1"));
        var quality = validator.validate(parsed, "run-jma-01");
        assertFalse(quality.publishBlocked());
        assertEquals(1, quality.validCount());
        assertEquals("I", quality.validObservations().get(0).determiningAgencyCode());
        assertEquals("ARTIFICIAL", quality.validObservations().get(0).eventTypeCode());
        assertEquals("N", quality.validObservations().get(0).sourceStatus());
        assertEquals(-1.0, quality.validObservations().get(0).depthKm());
    }

    private ResolvedBronzeInput publishAndResolve(byte[] archive, String run, String member) throws Exception {
        URI url = URI.create(JmaFixedWidthParserTest.SOURCE_URL);
        var entry = new JmaArchiveEntry("1.0", 2023, "full-year", "2023-01-01T00:00:00+09:00", "2024-01-01T00:00:00+09:00",
                "h2023.zip", member, url, null, null, null, "application/zip", "jma-hypocenter-96-byte-v1", "UNIFIED");
        var request = new JmaBronzeWriteRequest(entry, archive, SourceKeyGenerator.sha256(archive), archive.length, "release-integration",
                new JmaHttpMetadata(200, "application/zip", (long) archive.length, null, null, url), run, 1,
                LocalDate.of(2026, 10, 7), PROCESSED, true, "JMA_BULLETIN|2023|full-year|release-integration", 30_000, null);
        var store = new FileBronzeObjectStore(temporary.resolve("bronze-store"));
        var written = new JmaBronzeWriter(store).write(request);
        assertTrue(written.ready());
        Path manifest = Path.of(URI.create(store.uriForKey(written.manifestKey())));
        return new SilverInputResolver(new ObjectStoreBronzeInputReader(store))
                .resolve(manifest, run, "JMA_BULLETIN", temporary.resolve("staging"));
    }

    private static byte[] records(byte[]... rows) throws IOException {
        var buffer = new ByteArrayOutputStream();
        for (byte[] row : rows) {
            buffer.write(row);
            buffer.write('\n');
        }
        return buffer.toByteArray();
    }
}
