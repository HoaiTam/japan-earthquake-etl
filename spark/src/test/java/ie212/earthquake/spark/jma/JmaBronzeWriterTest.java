package ie212.earthquake.spark.jma;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import ie212.earthquake.spark.silver.ObjectStoreBronzeInputReader;
import ie212.earthquake.spark.silver.SilverInputResolver;
import ie212.earthquake.spark.usgs.BronzeObjectStore;
import ie212.earthquake.spark.usgs.FileBronzeObjectStore;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.Clock;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.*;

class JmaBronzeWriterTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    @TempDir Path root;

    @Test
    void publishesExactZipAndCompleteManifestThenResolvesItWithSlv01() throws Exception {
        var store = new FileBronzeObjectStore(root.resolve("store"));
        byte[] archive = fixture("success");
        var request = input(archive, "release-v1", "run-success", "sha", 200);
        var result = new JmaBronzeWriter(store).write(request);
        assertTrue(result.ready());
        assertFalse(result.idempotentReuse());
        assertArrayEquals(archive, store.read(result.rawObjectKey()));
        JsonNode manifest = JSON.readTree(store.read(result.manifestKey()));
        assertEquals("1.0", manifest.path("manifest_version").asText());
        assertEquals("Asia/Tokyo", manifest.path("data_interval").path("native_timezone").asText());
        assertEquals("2023-01-01T00:00:00+09:00", manifest.path("data_interval").path("native_start").asText());
        assertEquals("hypo.dat", manifest.path("provenance").path("member_name").asText());
        assertEquals("Japanese Geodetic Datum 2000", manifest.path("provenance").path("geodetic_datum").asText());
        assertTrue(manifest.path("response").path("etag").isNull());
        for (String flag : new String[] {"object_write_completed", "raw_readback_verified", "checksum_verified",
                "source_structure_valid", "manifest_consistent"}) {
            assertTrue(manifest.path("validation").path(flag).asBoolean(), flag);
        }
        for (String field : new String[] {"manifest_id", "bronze_status", "source_system", "source_kind",
                "raw_object_uri", "raw_object_key", "media_type", "content_encoding", "content_length_bytes",
                "sha256", "run_id", "attempt", "ingest_date_utc", "retrieved_at_utc", "is_backfill",
                "logical_run_key", "data_interval", "request", "response", "catalog_release",
                "record_count_estimate", "validation", "writer", "lineage", "provenance"}) {
            assertTrue(manifest.has(field), field);
        }
        assertEquals(2L, manifest.path("record_count_estimate").asLong());
        String citation = manifest.path("provenance").path("citation_text").asText();
        assertTrue(citation.contains(request.entry().sourceUrl().toString()));
        assertTrue(citation.contains(request.retrievedAtUtc().toString()));
        assertTrue(citation.contains(request.catalogRelease()));
        var resolved = new SilverInputResolver(new ObjectStoreBronzeInputReader(store)).resolve(
                Path.of(URI.create(store.uriForKey(result.manifestKey()))), "run-success", "JMA_BULLETIN", root.resolve("silver"));
        assertArrayEquals(archive, Files.readAllBytes(resolved.stagedObject()));
        assertEquals("release-v1", resolved.catalogRelease());
    }

    @Test
    void preservesAllSharedFixtureRowsAndEmptyArchive() throws Exception {
        var store = new FileBronzeObjectStore(root);
        var writer = new JmaBronzeWriter(store);
        for (String name : new String[] {"empty", "success", "duplicate", "timezone-boundary", "ambiguous"}) {
            byte[] bytes = fixture(name);
            var result = writer.write(input(bytes, "release-" + name, "run-" + name, "sha", 200));
            assertTrue(result.ready(), name);
            assertArrayEquals(bytes, store.read(result.rawObjectKey()));
            long expected = switch (name) {
                case "empty" -> 0;
                case "timezone-boundary" -> 3;
                default -> 2;
            };
            assertEquals(expected, result.recordCountEstimate());
        }
    }

    @Test
    void rejectsBadSourceToQuarantineAndSilverCannotResolveIt() throws Exception {
        var store = new FileBronzeObjectStore(root);
        for (String name : new String[] {"invalid-record-length", "success"}) {
            byte[] bytes = fixture(name);
            var result = new JmaBronzeWriter(store).write(input(bytes, "release-invalid", "run-" + name, name.equals("success") ? "wrong" : "sha", 200));
            assertFalse(result.ready());
            assertTrue(result.manifestKey().endsWith("failure_manifest.json"));
            assertArrayEquals(bytes, store.read(result.rawObjectKey()));
            JsonNode manifest = JSON.readTree(store.read(result.manifestKey()));
            assertFalse(manifest.path("validation").path("manifest_consistent").asBoolean());
            assertThrows(IOException.class, () -> new SilverInputResolver(new ObjectStoreBronzeInputReader(store)).resolve(
                    Path.of(URI.create(store.uriForKey(result.manifestKey()))), "run-" + name, "JMA_BULLETIN", root.resolve("silver")));
            assertFalse(store.exists("bronze/jma/year=2023/catalog_release=release-invalid/ingest_date=2026-10-06/run_id=run-" + name + "/attempt=01/manifest.json"));
        }
        assertEquals("HTTP_STATUS_503", new JmaBronzeWriter(store).write(input(fixture("success"), "release-http", "run-http", "sha", 503)).rejectionReason());
    }

    @Test
    void rerunReusesIdenticalManifestAndRevisionKeepsBothReleases() throws Exception {
        var store = new FileBronzeObjectStore(root);
        var writer = new JmaBronzeWriter(store);
        var request = input(fixture("revision-v1"), "release-v1", "run-revision", "sha", 200);
        var first = writer.write(request);
        byte[] manifest = store.read(first.manifestKey());
        assertTrue(writer.write(request).idempotentReuse());
        assertArrayEquals(manifest, store.read(first.manifestKey()));
        var revision = writer.write(input(fixture("revision-v2"), "release-v2", "run-revision", "sha", 200));
        assertNotEquals(first.rawObjectKey(), revision.rawObjectKey());
        assertArrayEquals(request.archive(), store.read(first.rawObjectKey()));
        assertArrayEquals(fixture("revision-v2"), store.read(revision.rawObjectKey()));
    }

    @Test
    void refusesSameKeyDifferentBytesOrLogicalMetadataWithoutTouchingOldObjects() throws Exception {
        var store = new FileBronzeObjectStore(root);
        var writer = new JmaBronzeWriter(store);
        var first = writer.write(input(fixture("revision-v1"), "release-fixed", "run-fixed", "sha", 200));
        byte[] original = store.read(first.rawObjectKey());
        byte[] manifest = store.read(first.manifestKey());
        var conflict = assertThrows(IOException.class, () -> writer.write(input(fixture("revision-v2"), "release-fixed", "run-fixed", "sha", 200)));
        assertTrue(conflict.getMessage().contains("AMBIGUOUS_OVERWRITE"));
        var differentLogical = input(original, "release-fixed", "run-fixed", "sha", 200);
        var changed = new JmaBronzeWriteRequest(differentLogical.entry(), original, differentLogical.expectedSha256(), original.length,
                "release-fixed", differentLogical.http(), "run-fixed", 1, differentLogical.ingestDateUtc(), differentLogical.retrievedAtUtc(),
                false, "other-logical-run", 30_000, null);
        assertThrows(IOException.class, () -> writer.write(changed));
        assertArrayEquals(original, store.read(first.rawObjectKey()));
        assertArrayEquals(manifest, store.read(first.manifestKey()));
    }

    @Test
    void manifestLastAndRetryRecoversRawOnlyAfterManifestUploadFailure() throws Exception {
        var store = new MemoryStore();
        store.failManifest = true;
        var writer = new JmaBronzeWriter(store);
        var request = input(fixture("success"), "release-v1", "run-partial", "sha", 200);
        assertThrows(IOException.class, () -> writer.write(request));
        assertEquals(1, store.data.size());
        assertTrue(store.data.keySet().iterator().next().endsWith("archive.zip"));
        store.failManifest = false;
        var result = writer.write(request);
        assertTrue(result.ready());
        assertFalse(result.idempotentReuse()); // Raw alone is not a committed publication.
        assertEquals(List.of(result.rawObjectKey(), result.manifestKey()), store.writes);
    }

    @Test
    void readbackCorruptionAndPermissionFailureNeverPublishReadyManifest() throws Exception {
        var store = new MemoryStore();
        store.corruptRawRead = true;
        var request = input(fixture("success"), "release-v1", "run-corrupt", "sha", 200);
        assertTrue(assertThrows(IOException.class, () -> new JmaBronzeWriter(store).write(request)).getMessage().contains("RAW_READBACK_MISMATCH"));
        assertEquals(1, store.data.size());
        assertTrue(store.data.keySet().stream().noneMatch(key -> key.endsWith("manifest.json")));
        var denied = new MemoryStore();
        denied.denyWrites = true;
        assertThrows(IOException.class, () -> new JmaBronzeWriter(denied).write(request));
        assertTrue(denied.data.isEmpty());
    }

    @Test
    void validatesRunContextAndProtectsArchiveBytes() throws Exception {
        byte[] bytes = fixture("success");
        var request = input(bytes, "release-v1", "run-safe", "sha", 200);
        byte saved = request.archive()[0];
        bytes[0] ^= 1;
        assertEquals(saved, request.archive()[0]);
        request.archive()[0] ^= 1;
        assertEquals(saved, request.archive()[0]);
        for (String run : new String[] {"../run", "run/path", "run..x", "run\n"}) {
            assertThrows(IllegalArgumentException.class, () -> input(bytes, "release-v1", run, "sha", 200));
        }
        assertThrows(IllegalArgumentException.class, () -> input(bytes, "bad/release", "run-safe", "sha", 200));
    }

    @Test
    void matchesSharedFixtureBronzeStatusesAndReasonCodes() throws Exception {
        JsonNode cases = JSON.readTree(Files.readAllBytes(Path.of("../tests/fixtures/cases.json"))).path("cases");
        var writer = new JmaBronzeWriter(new FileBronzeObjectStore(root));
        for (JsonNode fixture : cases) {
            if (!"JMA_BULLETIN".equals(fixture.path("source_system").asText())) {
                continue;
            }
            String category = fixture.path("category").asText();
            if (!List.of("success", "empty", "invalid", "checksum_mismatch").contains(category)) {
                continue;
            }
            String zipPath = null;
            for (JsonNode path : fixture.path("input_paths")) {
                if (path.asText().endsWith(".zip")) {
                    zipPath = path.asText();
                }
            }
            assertNotNull(zipPath);
            byte[] bytes = Files.readAllBytes(Path.of("../tests/fixtures").resolve(zipPath));
            var result = writer.write(input(bytes, "release-" + category, "run-" + category,
                    category.equals("checksum_mismatch") ? "wrong" : "sha", 200));
            assertEquals(fixture.path("expected").path("bronze_status").asText(), result.bronzeStatus());
            if (!result.ready()) {
                assertEquals(fixture.path("expected").path("reason_codes").get(0).asText(), result.rejectionReason());
            }
        }
    }

    @Test
    void preservesOriginalRetrievalTimestampAndCitationOnRecheck() throws Exception {
        var store = new FileBronzeObjectStore(root);
        var writer = new JmaBronzeWriter(store);
        var request = input(fixture("success"), "release-v1", "run-recheck", "sha", 200);
        var first = writer.write(request);
        byte[] original = store.read(first.manifestKey());
        var recheck = copy(request, request.entry(), request.http(), request.expectedLengthBytes(), 1,
                request.retrievedAtUtc().plusSeconds(60), null);
        assertTrue(writer.write(recheck).idempotentReuse());
        assertArrayEquals(original, store.read(first.manifestKey()));
    }

    @Test
    void rejectsWrongTypeLengthAndCorruptZipWithoutReadyObjects() throws Exception {
        var store = new MemoryStore();
        var writer = new JmaBronzeWriter(store);
        var request = input(fixture("success"), "release-v1", "run-invalid", "sha", 200);
        var html = new JmaHttpMetadata(200, "text/html", request.http().contentLengthBytes(),
                null, request.http().lastModified(), request.http().finalUri());
        var badHeader = new JmaHttpMetadata(200, "application/zip", request.http().contentLengthBytes() + 1,
                null, request.http().lastModified(), request.http().finalUri());
        assertEquals("UNEXPECTED_CONTENT_TYPE", writer.write(copy(request, request.entry(), html,
                request.expectedLengthBytes(), 1, request.retrievedAtUtc(), null)).rejectionReason());
        assertEquals("SOURCE_LENGTH_MISMATCH", writer.write(copy(request, request.entry(), badHeader,
                request.expectedLengthBytes(), 2, request.retrievedAtUtc(), null)).rejectionReason());
        assertEquals("SOURCE_LENGTH_MISMATCH", writer.write(copy(request, request.entry(), request.http(),
                request.expectedLengthBytes() + 1, 3, request.retrievedAtUtc(), null)).rejectionReason());
        byte[] corrupt = "not-a-zip".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        assertEquals("INVALID_ZIP", writer.write(input(corrupt, "release-bad", "run-corrupt", "sha", 200)).rejectionReason());
        assertTrue(store.data.keySet().stream().allMatch(key -> key.startsWith("bronze/_quarantine/jma/")));
        assertTrue(store.data.keySet().stream().noneMatch(key -> key.endsWith("archive.zip")));
    }

    @Test
    void publishesTwo1997ArchivesWithSeparateNativeIntervalsAndLineage() throws Exception {
        var entries = JmaArchiveInventory.read(Path.of("../config/jma/hypocenter_archives_v1.csv"));
        var store = new FileBronzeObjectStore(root);
        var writer = new JmaBronzeWriter(store);
        List<String> rawKeys = new ArrayList<>();
        for (JmaArchiveEntry entry : entries.stream().filter(item -> item.year() == 1997).toList()) {
            // Business-invalid values and out-of-interval time are intentionally not inspected by Bronze.
            byte[] row = "?".repeat(96).getBytes(java.nio.charset.StandardCharsets.US_ASCII);
            byte[] bytes = JmaArchiveValidatorTest.zip(entry.memberName(), row);
            var base = input(bytes, "release-" + entry.segment(), "run-" + entry.segment(), "sha", 200);
            var http = new JmaHttpMetadata(200, "application/zip", (long) bytes.length, null,
                    base.http().lastModified(), entry.sourceUrl());
            var result = writer.write(copy(base, entry, http, bytes.length, 1, base.retrievedAtUtc(), null));
            assertTrue(result.ready());
            assertEquals(1L, result.recordCountEstimate());
            JsonNode manifest = JSON.readTree(store.read(result.manifestKey()));
            assertEquals(entry.nativeStartJst(), manifest.path("data_interval").path("native_start").asText());
            assertEquals(entry.nativeEndJst(), manifest.path("data_interval").path("native_end").asText());
            assertEquals(entry.catalogEra(), manifest.path("provenance").path("catalog_era").asText());
            assertEquals(entry.sourceUrl().toString(), manifest.path("request").path("url").asText());
            assertArrayEquals(bytes, store.read(result.rawObjectKey()));
            rawKeys.add(result.rawObjectKey());
        }
        assertEquals(2, rawKeys.size());
        assertNotEquals(rawKeys.get(0), rawKeys.get(1));
    }

    @Test
    void failsClosedWhenExistingManifestMetadataIsTampered() throws Exception {
        var store = new MemoryStore();
        var writer = new JmaBronzeWriter(store);
        var request = input(fixture("success"), "release-v1", "run-tamper", "sha", 200);
        var first = writer.write(request);
        byte[] originalRaw = store.read(first.rawObjectKey());
        JsonNode original = JSON.readTree(store.read(first.manifestKey()));
        for (String field : new String[] {"record_count_estimate", "validation", "retrieved_at_utc", "provenance"}) {
            var changed = ((com.fasterxml.jackson.databind.node.ObjectNode) original).deepCopy();
            changed.put(field, "tampered");
            byte[] tampered = JSON.writeValueAsBytes(changed);
            store.data.put(first.manifestKey(), tampered);
            assertThrows(IOException.class, () -> writer.write(request), field);
            assertArrayEquals(tampered, store.read(first.manifestKey()));
            assertArrayEquals(originalRaw, store.read(first.rawObjectKey()));
        }
    }

    @Test
    void handlesConcurrentImmutableCreateRacesWithoutOverwrite() throws Exception {
        var store = new MemoryStore();
        store.createRace = true;
        var request = input(fixture("success"), "release-v1", "run-race", "sha", 200);
        var result = new JmaBronzeWriter(store).write(request);
        assertTrue(result.ready());
        assertTrue(result.idempotentReuse());
        assertEquals(2, store.data.size());
        assertArrayEquals(request.archive(), store.read(result.rawObjectKey()));
        var corruptWinner = new MemoryStore();
        corruptWinner.createRace = true;
        corruptWinner.corruptRace = true;
        assertTrue(assertThrows(IOException.class, () -> new JmaBronzeWriter(corruptWinner).write(request))
                .getMessage().contains("AMBIGUOUS_OVERWRITE"));
        assertTrue(corruptWinner.data.keySet().stream().noneMatch(key -> key.endsWith("manifest.json")));
    }

    @Test
    void handsOffDownloaderStagingToBronzeAndSilverWithS3Uris() throws Exception {
        JmaArchiveEntry entry = JmaArchiveInventory.read(Path.of("../config/jma/hypocenter_archives_v1.csv"))
                .stream().filter(item -> item.year() == 2023).findFirst().orElseThrow();
        byte[] row = "J".repeat(96).getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        byte[] bytes = JmaArchiveValidatorTest.zip(entry.memberName(), row);
        var http = new JmaHttpMetadata(200, "application/zip", (long) bytes.length, null,
                "2025-12-10T01:41:53Z", entry.sourceUrl());
        var transport = new JmaArchiveTransport() {
            @Override public JmaHttpMetadata head(URI uri) { return http; }
            @Override public JmaHttpPayload get(URI uri, long start) {
                return new JmaHttpPayload(http.statusCode(), http.contentType(), http.contentLengthBytes(),
                        http.etag(), http.lastModified(), http.finalUri(), bytes);
            }
        };
        Instant retrieved = Instant.parse("2026-10-07T00:00:00Z");
        var download = new JmaArchiveDownloader(transport, root.resolve("download"),
                Clock.fixed(retrieved, ZoneOffset.UTC)).downloadOne(entry);
        assertTrue(download.succeeded());
        Path staged = Path.of(download.archivePath());
        var request = new JmaBronzeWriteRequest(entry, Files.readAllBytes(staged), download.sha256(),
                download.contentLengthBytes(), download.catalogRelease(), http, "run-handoff", 1,
                LocalDate.of(2026, 10, 7), retrieved, true,
                "JMA_BULLETIN|2023|full-year|" + download.catalogRelease(), 30_000, staged.toUri().toString());
        var store = new MemoryStore();
        var writer = new JmaBronzeWriter(store);
        var result = writer.write(request);
        assertTrue(result.ready());
        JsonNode manifest = JSON.readTree(store.read(result.manifestKey()));
        assertEquals(staged.toUri().toString(), manifest.path("lineage").path("staged_object_uri").asText());
        assertTrue(manifest.path("raw_object_uri").asText().startsWith("s3://test-bucket/bronze/jma/"));
        // SLV-01 currently accepts a local manifest path, while its adapter reads the raw key from the store.
        Path localManifest = root.resolve("bronze-manifest.json");
        Files.write(localManifest, store.read(result.manifestKey()));
        var resolved = new SilverInputResolver(new ObjectStoreBronzeInputReader(store))
                .resolve(localManifest, "run-handoff", "JMA_BULLETIN", root.resolve("silver"));
        assertArrayEquals(bytes, Files.readAllBytes(resolved.stagedObject()));
        assertEquals(1L, result.recordCountEstimate());
        assertTrue(writer.write(request).idempotentReuse());
        assertEquals("REUSED", new JmaArchiveDownloader(transport, root.resolve("download"),
                Clock.fixed(retrieved, ZoneOffset.UTC)).downloadOne(entry).status());
    }

    @Test
    void rejectsInvalidIntervalEraAndCredentialBearingProvenance() throws Exception {
        var request = input(fixture("success"), "release-v1", "run-context", "sha", 200);
        var entry = request.entry();
        for (String start : new String[] {"2023-01-01T00:00:00Z", "2023-02-01T00:00:00+09:00",
                "2023-01-01T01:00:00+09:00"}) {
            var invalid = new JmaArchiveEntry(entry.inventoryVersion(), entry.year(), entry.segment(), start,
                    entry.nativeEndJst(), entry.archiveName(), entry.memberName(), entry.sourceUrl(), null, null,
                    null, entry.mediaType(), entry.recordFormat(), entry.catalogEra());
            assertThrows(IllegalArgumentException.class, () -> copy(request, invalid, request.http(),
                    request.expectedLengthBytes(), 1, request.retrievedAtUtc(), null));
        }
        var badEra = new JmaArchiveEntry(entry.inventoryVersion(), entry.year(), entry.segment(), entry.nativeStartJst(),
                entry.nativeEndJst(), entry.archiveName(), entry.memberName(), entry.sourceUrl(), null, null,
                null, entry.mediaType(), entry.recordFormat(), "LEGACY");
        assertThrows(IllegalArgumentException.class, () -> copy(request, badEra, request.http(),
                request.expectedLengthBytes(), 1, request.retrievedAtUtc(), null));
        for (String url : new String[] {"https://user:password@example.invalid/h2023.zip",
                "https://example.invalid/h2023.zip?token=fixture", "file:///tmp/h2023.zip"}) {
            var badHttp = new JmaHttpMetadata(200, "application/zip", request.http().contentLengthBytes(), null,
                    request.http().lastModified(), URI.create(url));
            assertThrows(IllegalArgumentException.class, () -> copy(request, entry, badHttp,
                    request.expectedLengthBytes(), 1, request.retrievedAtUtc(), null));
        }
        for (String uri : new String[] {"s3://user:password@bucket/archive.zip", "file:///tmp/archive.zip?token=fixture",
                "https://example.invalid/archive.zip", "relative/path"}) {
            assertThrows(IllegalArgumentException.class, () -> copy(request, entry, request.http(),
                    request.expectedLengthBytes(), 1, request.retrievedAtUtc(), uri));
        }
    }

    private static JmaBronzeWriteRequest copy(JmaBronzeWriteRequest request, JmaArchiveEntry entry,
            JmaHttpMetadata http, long expectedLength, int attempt, Instant retrievedAt, String stagedUri) {
        return new JmaBronzeWriteRequest(entry, request.archive(), request.expectedSha256(), expectedLength,
                request.catalogRelease(), http, request.runId(), attempt, request.ingestDateUtc(), retrievedAt,
                request.backfill(), request.logicalRunKey(), request.requestTimeoutMs(), stagedUri);
    }

    static byte[] fixture(String name) throws Exception {
        return Files.readAllBytes(JmaArchiveValidatorTest.FIXTURES.resolve(name + ".zip"));
    }

    static JmaBronzeWriteRequest input(byte[] bytes, String release, String run, String checksum, int httpStatus) {
        URI url = URI.create("https://www.data.jma.go.jp/eqev/data/bulletin/data/hypo/h2023.zip");
        var entry = new JmaArchiveEntry("1.0", 2023, "full-year", "2023-01-01T00:00:00+09:00", "2024-01-01T00:00:00+09:00",
                "h2023.zip", "hypo.dat", url, null, null, null, "application/zip", "jma-hypocenter-96-byte-v1", "UNIFIED");
        return new JmaBronzeWriteRequest(entry, bytes, checksum.equals("wrong") ? "0".repeat(64) : JmaBronzeWriter.sha256(bytes), bytes.length,
                release, new JmaHttpMetadata(httpStatus, "application/zip", (long) bytes.length, null, "2025-12-10T01:41:53Z", url),
                run, 1, LocalDate.of(2026, 10, 6), Instant.parse("2026-10-06T01:00:00Z"), true, "JMA_BULLETIN|2023|full-year|" + release, 30_000, null);
    }

    private static final class MemoryStore implements BronzeObjectStore {
        final Map<String, byte[]> data = new LinkedHashMap<>();
        final List<String> writes = new ArrayList<>();
        boolean failManifest;
        boolean corruptRawRead;
        boolean denyWrites;
        boolean createRace;
        boolean corruptRace;
        @Override public void putIfAbsent(String key, byte[] payload, String type) throws IOException {
            if (denyWrites || (failManifest && key.endsWith("manifest.json"))) {
                throw new IOException("simulated storage failure");
            }
            if (data.containsKey(key)) {
                throw new FileAlreadyExistsException(key);
            }
            writes.add(key);
            data.put(key, payload.clone());
            if (createRace) {
                if (corruptRace) {
                    data.get(key)[0] ^= 1;
                }
                throw new FileAlreadyExistsException(key);
            }
        }
        @Override public byte[] read(String key) {
            byte[] bytes = data.get(key).clone();
            if (corruptRawRead && key.endsWith("archive.zip")) {
                bytes[0] ^= 1;
            }
            return bytes;
        }
        @Override public boolean exists(String key) { return data.containsKey(key); }
        @Override public String uriForKey(String key) { return "s3://test-bucket/" + key; }
    }
}
