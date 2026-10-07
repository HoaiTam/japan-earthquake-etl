package ie212.earthquake.spark.jma;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.usgs.BronzeObjectStore;
import ie212.earthquake.spark.usgs.FileBronzeObjectStore;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class JmaBronzeQaVerifierTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Path INVENTORY = Path.of("../config/jma/hypocenter_archives_v1.csv");
    @TempDir Path temporary;

    @Test void verifiesFourArchivesPinnedSamplesAndNewRunRerunAgainstPersistedBaseline() throws Exception {
        Fixture f = fixture();
        byte[] catalogBefore = JSON.writeValueAsBytes(f.catalog);
        ObjectNode first = verify(f, null);
        assertEquals(4, first.path("archives").size());
        assertEquals(2, first.path("dat01_readback").size());
        assertFalse(first.path("rerun_verified").asBoolean());
        // Re-read JSON from disk: small counts become IntNodes, not LongNodes.
        JsonNode persisted = JSON.readTree(JSON.writeValueAsBytes(first));
        rerun(f);
        var second = verify(f, persisted);
        assertTrue(second.path("rerun_verified").asBoolean());
        assertEquals(first.path("archives").get(2).path("manifest_sha256"), second.path("archives").get(2).path("manifest_sha256"));
        assertArrayEquals(catalogBefore, JSON.writeValueAsBytes(f.catalog));
    }

    @Test void missingSegmentDuplicateScopePreviewOrPartialYearNeverPass() throws Exception {
        Fixture f = fixture();
        var original = f.summary.deepCopy();
        f.summary.put("preview", true);
        assertThrows(IOException.class, () -> verify(f, null));
        f.summary = original.deepCopy();
        ((com.fasterxml.jackson.databind.node.ArrayNode) f.summary.path("years").get(0).path("segments")).remove(1);
        assertThrows(IOException.class, () -> verify(f, null));
        f.summary = original.deepCopy();
        ((ObjectNode) f.summary.path("years").get(0)).put("status", "PARTIAL");
        assertThrows(IOException.class, () -> verify(f, null));
        f.summary = original.deepCopy();
        ((ObjectNode) f.summary.path("years").get(0).path("segments").get(1)).put("segment", "jan-sep");
        assertThrows(IOException.class, () -> verify(f, null));
    }

    @Test void incorrectSummaryCountOrChecksumCannotBeTrusted() throws Exception {
        Fixture f = fixture(); ObjectNode entry = firstSegment(f);
        entry.put("record_count_estimate", 999);
        assertThrows(IOException.class, () -> verify(f, null));
        entry.put("record_count_estimate", 2).put("sha256", "0".repeat(64));
        assertThrows(IOException.class, () -> verify(f, null));
    }

    @Test void actualRawCorruptionOrManifestFlagTamperFailsClosed() throws Exception {
        Fixture f = fixture(); ObjectNode entry = firstSegment(f);
        Path raw = f.root.resolve(entry.path("raw_object_key").asText());
        byte[] original = Files.readAllBytes(raw); Files.writeString(raw, "corrupted");
        assertThrows(IOException.class, () -> verify(f, null));
        Files.write(raw, original);
        Path path = f.root.resolve(entry.path("manifest_key").asText());
        ObjectNode manifest = (ObjectNode) JSON.readTree(Files.readAllBytes(path));
        ((ObjectNode) manifest.path("validation")).put("checksum_verified", "true");
        Files.write(path, JSON.writeValueAsBytes(manifest));
        assertThrows(IOException.class, () -> verify(f, null));
    }

    @Test void changedManifestBetweenRunsEvenWithSameRawIsDetected() throws Exception {
        Fixture f = fixture(); ObjectNode baseline = verify(f, null); rerun(f);
        Path path = f.root.resolve(firstSegment(f).path("manifest_key").asText());
        ObjectNode manifest = (ObjectNode) JSON.readTree(Files.readAllBytes(path));
        manifest.put("retrieved_at_utc", "2026-10-08T00:00:00Z");
        Files.write(path, JSON.writeValueAsBytes(manifest));
        assertThrows(IOException.class, () -> verify(f, baseline));
    }

    @Test void wrongCatalogStateSignedUriOrChangedInventoryIsRejected() throws Exception {
        Fixture f = fixture(); ObjectNode jma = (ObjectNode) f.catalog.path("samples").get(1);
        jma.put("sample_state", "BRONZE_READY");
        assertThrows(IOException.class, () -> verify(f, null));
        jma.put("sample_state", "STAGED_SOURCE");
        jma.put("staged_object_uri", jma.path("staged_object_uri").asText() + "?token=forbidden");
        assertThrows(IOException.class, () -> verify(f, null));
        assertThrows(IOException.class, () -> JmaBronzeQaVerifier.verify(f.summary, f.catalog, "0".repeat(64), f.entries, f.store, null));
    }

    @Test void rerunRequiresDifferentRunAndBothCacheAndPublicationReuse() throws Exception {
        Fixture f = fixture(); ObjectNode baseline = verify(f, null);
        assertThrows(IOException.class, () -> verify(f, baseline));
        rerun(f); firstSegment(f).put("download_reused", false);
        assertThrows(IOException.class, () -> verify(f, baseline));
    }

    private ObjectNode verify(Fixture f, JsonNode baseline) throws IOException {
        return JmaBronzeQaVerifier.verify(f.summary, f.catalog, f.inventorySha, f.entries, f.store, baseline);
    }
    private static ObjectNode firstSegment(Fixture f) { return (ObjectNode) f.summary.path("years").get(0).path("segments").get(0); }
    private static void rerun(Fixture f) {
        ((ObjectNode) f.summary.path("run_context")).put("run_id", "qa-second");
        for (JsonNode year : f.summary.path("years")) {
            for (JsonNode segment : year.path("segments")) {
                ((ObjectNode) segment).put("run_id", "qa-second").put("download_reused", true).put("publication_reused", true);
            }
        }
    }

    private Fixture fixture() throws Exception {
        Fixture f = new Fixture(); f.root = temporary.resolve("store");
        FileBronzeObjectStore files = new FileBronzeObjectStore(f.root);
        f.store = new BronzeObjectStore() {
            public void putIfAbsent(String key, byte[] bytes, String type) throws IOException { files.putIfAbsent(key, bytes, type); }
            public byte[] read(String key) throws IOException { return files.read(key); }
            public boolean exists(String key) throws IOException { return files.exists(key); }
            public String uriForKey(String key) { return "s3://test-bucket/" + key; }
        };
        f.entries = JmaArchiveInventory.read(INVENTORY); f.inventorySha = JmaBronzeWriter.sha256(Files.readAllBytes(INVENTORY));
        f.summary = JSON.createObjectNode().put("preview", false).put("status", "BronzeReady").put("verified", true)
                .put("planned_archives", 4).put("ready_archives", 4);
        f.summary.putObject("run_context").put("run_id", "qa-first").put("inventory_sha256", f.inventorySha)
                .put("is_backfill", true).put("config_version", "1");
        f.catalog = JSON.createObjectNode(); var samples = f.catalog.putArray("samples");
        byte[] usgs = "{\"type\":\"FeatureCollection\",\"features\":[{}]}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        ObjectNode usgsSample = samples.addObject().put("sample_id", "usgs-test").put("source_system", "USGS")
                .put("sample_state", "BRONZE_READY").put("raw_object_uri", "s3://test-bucket/bronze/usgs/response.geojson")
                .put("manifest_uri", "s3://test-bucket/bronze/usgs/manifest.json")
                .put("sha256", JmaBronzeWriter.sha256(usgs)).put("content_length_bytes", usgs.length).put("expected_record_count", 1);
        ObjectNode usgsManifest = usgsSample.deepCopy().put("bronze_status", "BronzeReady").put("record_count_estimate", 1);
        var flags = usgsManifest.putObject("validation");
        for (String flag : List.of("object_write_completed", "raw_readback_verified", "checksum_verified", "source_structure_valid", "manifest_consistent")) {
            flags.put(flag, true);
        }
        f.store.putIfAbsent("bronze/usgs/response.geojson", usgs, "application/json");
        f.store.putIfAbsent("bronze/usgs/manifest.json", JSON.writeValueAsBytes(usgsManifest), "application/json");
        var years = f.summary.putArray("years");
        for (int year : new int[] {1997, 2000, 2023}) {
            var segments = years.addObject().put("year", year).put("status", "BronzeReady").putArray("segments");
            for (var entry : f.entries.stream().filter(e -> e.year() == year).toList()) {
                byte[] raw = archive(entry.memberName()); String sha = JmaBronzeWriter.sha256(raw);
                String release = "jma-lm-20251210T014153Z-sha256-" + sha.substring(0, 12);
                var written = new JmaBronzeWriter(f.store).write(new JmaBronzeWriteRequest(entry, raw, sha, raw.length, release,
                        new JmaHttpMetadata(200, "application/zip", (long) raw.length, null, "Wed, 10 Dec 2025 01:41:53 GMT", entry.sourceUrl()),
                        "qa-first-" + year + "-" + entry.segment(), 1, LocalDate.of(2026, 10, 7), Instant.parse("2026-10-07T00:00:00Z"),
                        true, "test", 60000, null));
                segments.addObject().put("year", year).put("segment", entry.segment()).put("run_id", "qa-first").put("attempt", 1)
                        .put("status", "BronzeReady").put("bronze_status", "BronzeReady").put("verified", true)
                        .put("sha256", sha).put("catalog_release", release).put("raw_object_key", written.rawObjectKey())
                        .put("manifest_key", written.manifestKey()).put("raw_object_uri", f.store.uriForKey(written.rawObjectKey()))
                        .put("manifest_uri", f.store.uriForKey(written.manifestKey())).put("record_count_estimate", 2);
                if (year == 2023) {
                    f.store.putIfAbsent("bronze/_staging/jma/archive.zip", raw, "application/zip");
                    samples.addObject().put("sample_id", "jma-test").put("source_system", "JMA_BULLETIN").put("sample_state", "STAGED_SOURCE")
                            .put("staged_object_uri", "s3://test-bucket/bronze/_staging/jma/archive.zip").put("member_name", entry.memberName())
                            .put("sha256", sha).put("catalog_release", release).put("content_length_bytes", raw.length).put("expected_record_count", 2);
                }
            }
        }
        return f;
    }

    private static byte[] archive(String member) throws IOException {
        var bytes = new ByteArrayOutputStream();
        try (var zip = new ZipOutputStream(bytes)) {
            var entry = new ZipEntry(member); entry.setTime(0); zip.putNextEntry(entry);
            zip.write("J".repeat(96).getBytes(java.nio.charset.StandardCharsets.US_ASCII)); zip.write('\n');
            zip.write("J".repeat(96).getBytes(java.nio.charset.StandardCharsets.US_ASCII)); zip.closeEntry();
        }
        return bytes.toByteArray();
    }
    private static final class Fixture {
        ObjectNode summary, catalog; Path root; BronzeObjectStore store; String inventorySha; List<JmaArchiveEntry> entries;
    }
}
