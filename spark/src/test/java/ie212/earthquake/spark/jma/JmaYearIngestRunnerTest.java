package ie212.earthquake.spark.jma;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import ie212.earthquake.spark.usgs.BronzeObjectStore;
import ie212.earthquake.spark.usgs.FileBronzeObjectStore;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class JmaYearIngestRunnerTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-07T00:00:00Z"), ZoneOffset.UTC);
    private static final Path INVENTORY = Path.of("../config/jma/hypocenter_archives_v1.csv");
    @TempDir Path temporary;

    @Test void rerunAnotherRunAndHeaderOnlyChangeReuseExactPublishedManifest() throws Exception {
        FakeTransport transport = new FakeTransport();
        CountingStore store = new CountingStore(temporary.resolve("store"));
        JmaYearIngestRunner runner = runner(transport, store);
        Map<String, Object> first = runner.run(context(2023, "full-year", "run-a", 1, false));
        assertEquals("BronzeReady", first.get("status"));
        assertEquals(2L, first.get("record_count_estimate"));
        byte[] manifest = store.read((String) first.get("manifest_key"));
        assertEquals(200, JSON.readTree(manifest).path("response").path("http_status").asInt());
        assertEquals("2026-10-07", JSON.readTree(manifest).path("ingest_date_utc").asText());
        Map<String, Object> second = runner.run(context(2023, "full-year", "run-b", 1, false));
        assertEquals(first.get("manifest_uri"), second.get("manifest_uri"));
        assertEquals(true, second.get("publication_reused"));
        assertEquals(1, transport.gets.get());
        transport.modified = "Thu, 01 Jan 2026 00:00:00 GMT";
        Map<String, Object> sameBytes = runner.run(context(2023, "full-year", "run-c", 1, false));
        assertEquals(first.get("manifest_uri"), sameBytes.get("manifest_uri"));
        assertArrayEquals(manifest, store.read((String) first.get("manifest_key")));
        assertEquals(2, transport.gets.get());
        assertEquals(2, store.writes.get()); // one raw + one manifest; no rerun copies
    }

    @Test void forcedHeaderStableRevisionKeepsOldReleaseAndChangesExactManifest() throws Exception {
        FakeTransport transport = new FakeTransport();
        CountingStore store = new CountingStore(temporary.resolve("store"));
        JmaYearIngestRunner runner = runner(transport, store);
        Map<String, Object> first = runner.run(context(2023, "full-year", "run-a", 1, false));
        byte[] original = store.read((String) first.get("raw_object_key"));
        transport.recordValue = 'X';
        Map<String, Object> revised = runner.run(context(2023, "full-year", "run-b", 1, true));
        assertEquals("BronzeReady", revised.get("status"));
        assertNotEquals(first.get("catalog_release"), revised.get("catalog_release"));
        assertNotEquals(first.get("manifest_key"), revised.get("manifest_key"));
        assertArrayEquals(original, store.read((String) first.get("raw_object_key")));
        assertEquals(4, store.writes.get());
    }

    @Test void manifestFailureIsNotReadyAndSameAttemptRecoversWithoutRawOverwrite() throws Exception {
        FakeTransport transport = new FakeTransport();
        CountingStore store = new CountingStore(temporary.resolve("store"));
        store.failManifest = true;
        JmaYearIngestRunner runner = runner(transport, store);
        Path input = context(2023, "full-year", "run-a", 1, false);
        Map<String, Object> failed = runner.run(input);
        assertEquals("FAILED", failed.get("status"));
        assertEquals(false, failed.get("verified"));
        assertNull(failed.get("bronze_status"));
        Map<String, Object> recovered = runner.run(input);
        assertEquals("BronzeReady", recovered.get("status"));
        assertEquals(1, transport.gets.get());
        assertEquals(2, store.writes.get());
    }

    @Test void corruptedArchiveIsQuarantinedAndNeverCachedAsReady() throws Exception {
        FakeTransport transport = new FakeTransport();
        transport.corrupt = true;
        CountingStore store = new CountingStore(temporary.resolve("store"));
        Map<String, Object> failed = runner(transport, store).run(context(2023, "full-year", "bad", 1, false));
        assertEquals("FAILED", failed.get("status"));
        assertEquals("Rejected", failed.get("bronze_status"));
        assertEquals(false, failed.get("verified"));
        assertTrue(((String) failed.get("manifest_key")).startsWith("bronze/_quarantine/"));
        assertEquals("Rejected", JSON.readTree(store.read((String) failed.get("manifest_key"))).path("bronze_status").asText());
        assertFalse(Files.exists(temporary.resolve("staging/downloads/year=2023/segment=full-year/publication-"
                + failed.get("sha256") + ".json")));
    }

    @Test void wrongRecordLengthIsRejectedByCompleteRunnerAndCannotBecomeReadyOnRerun() throws Exception {
        FakeTransport transport = new FakeTransport(); transport.recordLength = 95;
        CountingStore store = new CountingStore(temporary.resolve("store"));
        var runner = runner(transport, store);
        var first = runner.run(context(2000, "full-year", "bad-length", 1, false));
        assertEquals("INVALID_RECORD_LENGTH", first.get("reason"));
        assertEquals("Rejected", first.get("bronze_status"));
        var second = runner.run(context(2000, "full-year", "bad-length-rerun", 1, false));
        assertEquals(false, second.get("verified"));
        assertEquals("FAILED", second.get("status"));
        assertEquals(1, transport.gets.get());
    }

    @Test void trackedRangeResumePublishesWholeZipAndFullCountNotOnlyTail() throws Exception {
        byte[] zip = archive("h2000", 'J'); int offset = 31;
        URI uri = JmaArchiveInventory.read(INVENTORY).stream().filter(e -> e.year() == 2000).findFirst().orElseThrow().sourceUrl();
        var transport = new JmaArchiveTransport() {
            public JmaHttpMetadata head(URI source) { return new JmaHttpMetadata(200, "application/zip", (long) zip.length, null,
                    "Wed, 10 Dec 2025 01:41:53 GMT", source); }
            public JmaHttpPayload get(URI source, long start) {
                assertEquals(offset, start);
                return new JmaHttpPayload(206, "application/zip", (long) zip.length - start, null,
                        "Wed, 10 Dec 2025 01:41:53 GMT", source, Arrays.copyOfRange(zip, (int) start, zip.length),
                        "bytes " + start + "-" + (zip.length - 1) + "/" + zip.length);
            }
        };
        Path root = temporary.resolve("staging/downloads/year=2000/segment=full-year"); Files.createDirectories(root);
        Files.write(root.resolve("archive.zip.part"), Arrays.copyOf(zip, offset));
        Files.write(root.resolve("partial-state.json"), JSON.writeValueAsBytes(JSON.createObjectNode()
                .put("source_url", uri.toString()).put("content_length_bytes", zip.length).putNull("etag")
                .put("last_modified", "Wed, 10 Dec 2025 01:41:53 GMT")));
        CountingStore store = new CountingStore(temporary.resolve("store"));
        var runner = new JmaYearIngestRunner(Map.of("JMA_INVENTORY_PATH", INVENTORY.toString(),
                "JMA_STAGING_ROOT", temporary.resolve("staging").toString()), transport, () -> store, CLOCK);
        var result = runner.run(context(2000, "full-year", "resume", 1, false));
        assertEquals("BronzeReady", result.get("status"));
        assertEquals(2L, result.get("record_count_estimate"));
        assertArrayEquals(zip, store.read((String) result.get("raw_object_key")));
        assertEquals(zip.length, JSON.readTree(store.read((String) result.get("manifest_key"))).path("content_length_bytes").asInt());
    }

    @Test void tamperedRawOrManifestCannotPassRerunVerification() throws Exception {
        FakeTransport transport = new FakeTransport();
        CountingStore store = new CountingStore(temporary.resolve("store"));
        JmaYearIngestRunner runner = runner(transport, store);
        Map<String, Object> first = runner.run(context(2023, "full-year", "one", 1, false));
        Path manifest = store.root.resolve((String) first.get("manifest_key"));
        byte[] manifestBytes = Files.readAllBytes(manifest);
        Files.writeString(manifest, "{}");
        assertEquals("FAILED", runner.run(context(2023, "full-year", "two", 1, false)).get("status"));
        Files.write(manifest, manifestBytes);
        Files.writeString(store.root.resolve((String) first.get("raw_object_key")), "damaged");
        assertEquals("FAILED", runner.run(context(2023, "full-year", "three", 1, false)).get("status"));
        assertEquals(2, store.writes.get()); // no silent repair/overwrite of published objects
    }

    @Test void year1997SegmentsHaveSeparateLineageAndQuarantineNamespaces() throws Exception {
        FakeTransport transport = new FakeTransport();
        CountingStore store = new CountingStore(temporary.resolve("store"));
        JmaYearIngestRunner runner = runner(transport, store);
        var jan = runner.run(context(1997, "jan-sep", "year", 1, false));
        var oct = runner.run(context(1997, "oct-dec", "year", 1, false));
        assertEquals("BronzeReady", jan.get("status"));
        assertEquals("BronzeReady", oct.get("status"));
        assertNotEquals(jan.get("manifest_key"), oct.get("manifest_key"));
        var manifest = JSON.readTree(store.read((String) oct.get("manifest_key")));
        assertEquals("1997-10-01T00:00:00+09:00", manifest.path("data_interval").path("native_start").asText());
        transport.corrupt = true;
        transport.modified = "Thu, 01 Jan 2026 00:00:00 GMT";
        var rejectedJan = runner.run(context(1997, "jan-sep", "failed-year", 1, false));
        var rejectedOct = runner.run(context(1997, "oct-dec", "failed-year", 1, false));
        assertNotEquals(rejectedJan.get("manifest_key"), rejectedOct.get("manifest_key"));
        assertEquals("FAILED", rejectedOct.get("status"));
        assertTrue(store.exists((String) jan.get("manifest_key")));
    }

    @Test void changedInventoryOrOutOfWindowScopeFailsBeforeNetworkOrStorage() throws Exception {
        FakeTransport transport = new FakeTransport();
        CountingStore store = new CountingStore(temporary.resolve("store"));
        JmaYearIngestRunner runner = runner(transport, store);
        Path input = context(2023, "full-year", "one", 1, false);
        var node = (com.fasterxml.jackson.databind.node.ObjectNode) JSON.readTree(Files.readAllBytes(input));
        ((com.fasterxml.jackson.databind.node.ObjectNode) node.path("run_context")).put("inventory_sha256", "0".repeat(64));
        Files.write(input, JSON.writeValueAsBytes(node));
        assertThrows(IOException.class, () -> runner.run(input));
        assertEquals(0, transport.gets.get());
        assertEquals(0, store.writes.get());
        Path wrongSegment = context(1997, "full-year", "two", 1, false);
        assertThrows(IOException.class, () -> runner.run(wrongSegment));
    }

    @Test void downloadFailureDoesNotWriteBronzeAndUnrelatedYearRemainsRunnable() throws Exception {
        FakeTransport transport = new FakeTransport();
        CountingStore store = new CountingStore(temporary.resolve("store"));
        JmaYearIngestRunner runner = runner(transport, store);
        transport.fail = true;
        var failed = runner.run(context(2023, "full-year", "one", 1, false));
        assertEquals("DOWNLOAD_FAILED", failed.get("reason"));
        assertEquals(0, store.writes.get());
        transport.fail = false;
        assertEquals("BronzeReady", runner.run(context(2000, "full-year", "one", 1, false)).get("status"));
    }

    @Test void unchangedProbeReadsVerifiedPublicationWithoutGetOrWrite() throws Exception {
        FakeTransport transport = new FakeTransport(); CountingStore store = new CountingStore(temporary.resolve("store"));
        var runner = runner(transport, store);
        var first = runner.run(context(2023, "full-year", "bootstrap", 1, false));
        var probe = runner.run(probeContext(2023, "full-year", "probe", false));
        assertEquals("BronzeReady", probe.get("status"));
        assertEquals("UNCHANGED", probe.get("readiness_decision"));
        assertEquals(first.get("manifest_uri"), probe.get("manifest_uri"));
        assertEquals(first.get("manifest_sha256"), probe.get("manifest_sha256"));
        assertEquals(JmaBronzeWriter.sha256(store.read((String) first.get("manifest_key"))), probe.get("manifest_sha256"));
        assertEquals(1, transport.gets.get()); assertEquals(2, store.writes.get());
    }

    @Test void changedAndWeeklyAuditProbesRequestScopedIngestButNeverGet() throws Exception {
        FakeTransport transport = new FakeTransport(); CountingStore store = new CountingStore(temporary.resolve("store"));
        var runner = runner(transport, store);
        runner.run(context(2000, "full-year", "bootstrap", 1, false));
        var audit = runner.run(probeContext(2000, "full-year", "audit", true));
        assertEquals("NeedsIngest", audit.get("status")); assertEquals("CHECKSUM_AUDIT", audit.get("readiness_decision"));
        assertEquals(false, audit.get("verified"));
        transport.modified = "Thu, 01 Jan 2026 00:00:00 GMT";
        var changed = runner.run(probeContext(2000, "full-year", "changed", false));
        assertEquals("NeedsIngest", changed.get("status"));
        assertEquals("CHANGED_OR_UNINITIALIZED", changed.get("readiness_decision"));
        assertEquals(1, transport.gets.get()); assertEquals(2, store.writes.get());
    }

    @Test void uninitializedAndMissingValidatorProbesAreNotReady() throws Exception {
        FakeTransport transport = new FakeTransport(); CountingStore store = new CountingStore(temporary.resolve("store"));
        var runner = runner(transport, store);
        assertEquals("NeedsIngest", runner.run(probeContext(2023, "full-year", "missing", false)).get("status"));
        assertEquals(0, transport.gets.get()); assertEquals(0, store.writes.get());
        runner.run(context(2023, "full-year", "bootstrap", 1, false));
        transport.modified = null;
        assertEquals("NeedsIngest", runner.run(probeContext(2023, "full-year", "no-validator", false)).get("status"));
        assertEquals(1, transport.gets.get());
    }

    @Test void sourceFailureOrCorruptPublicationProbeFailsClosedWithoutRepair() throws Exception {
        FakeTransport transport = new FakeTransport(); CountingStore store = new CountingStore(temporary.resolve("store"));
        var runner = runner(transport, store);
        var first = runner.run(context(2023, "full-year", "bootstrap", 1, false));
        transport.fail = true;
        assertEquals("FAILED", runner.run(probeContext(2023, "full-year", "offline", false)).get("status"));
        transport.fail = false;
        Files.writeString(store.root.resolve((String) first.get("manifest_key")), "{}");
        assertEquals("FAILED", runner.run(probeContext(2023, "full-year", "corrupt", false)).get("status"));
        assertEquals(1, transport.gets.get()); assertEquals(2, store.writes.get());
    }

    @Test void probeRequiresBoth1997SegmentsIndependently() throws Exception {
        FakeTransport transport = new FakeTransport(); CountingStore store = new CountingStore(temporary.resolve("store"));
        var runner = runner(transport, store);
        runner.run(context(1997, "jan-sep", "bootstrap", 1, false));
        assertEquals("BronzeReady", runner.run(probeContext(1997, "jan-sep", "check", false)).get("status"));
        assertEquals("NeedsIngest", runner.run(probeContext(1997, "oct-dec", "check", false)).get("status"));
        assertEquals(1, transport.gets.get());
    }

    private Path probeContext(int year, String segment, String run, boolean force) throws IOException {
        Path path = context(year, segment, run, 1, force);
        var input = (com.fasterxml.jackson.databind.node.ObjectNode) JSON.readTree(Files.readAllBytes(path));
        input.put("phase", "probe"); Files.write(path, JSON.writeValueAsBytes(input)); return path;
    }

    private JmaYearIngestRunner runner(FakeTransport transport, BronzeObjectStore store) {
        return new JmaYearIngestRunner(Map.of("JMA_INVENTORY_PATH", INVENTORY.toString(),
                "JMA_STAGING_ROOT", temporary.resolve("staging").toString()), transport, () -> store, CLOCK);
    }

    private Path context(int year, String segment, String run, int attempt, boolean force) throws IOException {
        var root = JSON.createObjectNode();
        root.put("phase", "ingest"); root.put("attempt", attempt); root.put("force_download", force);
        var context = root.putObject("run_context");
        context.put("run_id", run); context.put("run_id_path", run); context.put("config_version", "1");
        context.put("processing_date", "2023-01-01"); context.put("is_backfill", true);
        context.put("window_start_utc", "1983-12-31T15:00:00Z");
        context.put("window_end_utc", "2023-12-31T15:00:00Z");
        context.put("inventory_sha256", JmaBronzeWriter.sha256(Files.readAllBytes(INVENTORY)));
        root.putObject("archive").put("year", year).put("segment", segment);
        Path path = temporary.resolve("context-" + run + "-" + year + "-" + segment + ".json");
        Files.write(path, JSON.writeValueAsBytes(root));
        return path;
    }

    private static byte[] archive(String member, char value) throws IOException {
        return archive(member, value, 96);
    }

    private static byte[] archive(String member, char value, int length) throws IOException {
        var bytes = new ByteArrayOutputStream();
        try (var zip = new ZipOutputStream(bytes)) {
            ZipEntry entry = new ZipEntry(member); entry.setTime(0); zip.putNextEntry(entry);
            byte[] record = new byte[length]; Arrays.fill(record, (byte) value);
            zip.write(record); zip.write('\n'); zip.write(record); // duplicates preserved at Bronze
            zip.closeEntry();
        }
        return bytes.toByteArray();
    }

    private static final class FakeTransport implements JmaArchiveTransport {
        String modified = "Wed, 10 Dec 2025 01:41:53 GMT";
        char recordValue = 'J'; boolean corrupt; boolean fail;
        int recordLength = 96;
        AtomicInteger gets = new AtomicInteger();
        private byte[] body(URI uri) throws IOException {
            String name = Path.of(uri.getPath()).getFileName().toString().replace(".zip", "");
            return corrupt ? "invalid zip".getBytes() : archive(name, recordValue, recordLength);
        }
        @Override public JmaHttpMetadata head(URI uri) throws IOException {
            if (fail) { throw new IOException("simulated source failure"); }
            return new JmaHttpMetadata(200, "application/zip", (long) body(uri).length, null, modified, uri);
        }
        @Override public JmaHttpPayload get(URI uri, long offset) throws IOException {
            gets.incrementAndGet();
            byte[] bytes = body(uri);
            return new JmaHttpPayload(200, "application/zip", (long) bytes.length, null, modified, uri, bytes);
        }
    }

    private static final class CountingStore implements BronzeObjectStore {
        final Path root; final FileBronzeObjectStore delegate; final AtomicInteger writes = new AtomicInteger();
        boolean failManifest;
        CountingStore(Path root) { this.root = root; delegate = new FileBronzeObjectStore(root); }
        @Override public void putIfAbsent(String key, byte[] bytes, String type) throws IOException {
            if (failManifest && key.endsWith("manifest.json")) {
                failManifest = false; throw new IOException("simulated manifest failure");
            }
            delegate.putIfAbsent(key, bytes, type); writes.incrementAndGet();
        }
        @Override public byte[] read(String key) throws IOException { return delegate.read(key); }
        @Override public boolean exists(String key) throws IOException { return delegate.exists(key); }
        @Override public String uriForKey(String key) { return delegate.uriForKey(key); }
    }
}
