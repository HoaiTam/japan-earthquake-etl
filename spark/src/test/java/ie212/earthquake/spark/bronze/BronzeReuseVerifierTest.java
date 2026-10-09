package ie212.earthquake.spark.bronze;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.jma.JmaBronzeWriter;
import ie212.earthquake.spark.usgs.BronzeObjectStore;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class BronzeReuseVerifierTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private final Store store = new Store();

    @Test void exactReadbackRerunPreservesEveryObjectAndDoesNotListOrWrite() throws Exception {
        ObjectNode request = request("success", "success");
        var before = new HashMap<>(store.objects);
        assertEquals(BronzeReuseVerifier.verify(request, store), BronzeReuseVerifier.verify(request, store));
        assertEquals(before.keySet(), store.objects.keySet());
        for (var key : before.keySet()) { assertArrayEquals(before.get(key), store.objects.get(key)); }
        assertEquals(0, store.writes);
    }

    @Test void validEmptyResponsesRemainReady() throws Exception {
        assertTrue(BronzeReuseVerifier.verify(request("empty", "empty"), store).path("verified").asBoolean());
    }

    @Test void optionalTelemetryProjectsOnlyFreshVerifiedPinsAndCounts() throws Exception {
        var request = request("success", "success");
        assertEquals(5, BronzeReuseVerifier.verify(request, store).size());
        request.put("observability_version", "orc-04-v1");
        var result = BronzeReuseVerifier.verify(request, store);
        var rows = result.path("observability").path("bronze_inputs");
        assertEquals("orc-04-v1", result.path("observability").path("version").asText());
        assertEquals(2, rows.size());
        for (int i = 0; i < rows.size(); i++) {
            var row = (ObjectNode) rows.get(i);
            assertTrue(row.path("record_count_estimate").isIntegralNumber());
            assertTrue(row.path("raw_object_uri").asText().startsWith("s3://lake/bronze/"));
            row.remove("record_count_estimate"); row.remove("raw_object_uri");
            assertEquals(request.path("bronze_inputs").get(i), row);
        }
        assertEquals(0, store.writes);
    }

    @Test void optionalTelemetryRetainsVerifiedEmptyCounts() throws Exception {
        var request = request("empty", "empty"); request.put("observability_version", "orc-04-v1");
        for (var row : BronzeReuseVerifier.verify(request, store).path("observability").path("bronze_inputs")) {
            assertEquals(0, row.path("record_count_estimate").asLong());
        }
    }

    @Test void unsupportedTelemetryVersionFailsClosed() throws Exception {
        var request = request("success", "success"); request.put("observability_version", "v99");
        assertThrows(IOException.class, () -> BronzeReuseVerifier.verify(request, store));
    }

    @Test void duplicateRowsArePreservedForSilverNotDroppedByReadback() throws Exception {
        assertEquals(2, BronzeReuseVerifier.verify(request("duplicate", "duplicate"), store)
                .path("bronze_inputs").size());
        assertEquals(0, store.writes);
    }

    @Test void rawTamperAndMissingManifestFailClosed() throws Exception {
        var request = request("success", "success");
        String manifest = request.path("bronze_inputs").get(0).path("manifest_uri").asText().substring(10);
        byte[] saved = store.objects.remove(manifest);
        assertThrows(IOException.class, () -> BronzeReuseVerifier.verify(request, store));
        store.objects.put(manifest, saved);
        String raw = JSON.readTree(saved).path("raw_object_key").asText();
        store.objects.put(raw, "tampered".getBytes());
        assertThrows(IOException.class, () -> BronzeReuseVerifier.verify(request, store));
    }

    @Test void changedManifestBytesFailEvenWhenRawChecksumStillMatches() throws Exception {
        var request = request("success", "success");
        var pin = request.path("bronze_inputs").get(0);
        String key = pin.path("manifest_uri").asText().substring(10);
        var node = (ObjectNode) JSON.readTree(store.read(key)); node.put("run_id", "other");
        store.objects.put(key, JSON.writeValueAsBytes(node));
        assertThrows(IOException.class, () -> BronzeReuseVerifier.verify(request, store));
    }

    @Test void wrongReleaseSegmentIntervalBucketAndDuplicatePinAreRejected() throws Exception {
        var request = request("success", "success");
        for (String field : new String[] {"catalog_release", "segment", "year"}) {
            var copy = request.deepCopy(); var pin = (ObjectNode) copy.path("bronze_inputs").get(1);
            if (field.equals("year")) { pin.put(field, 2000); } else { pin.put(field, "wrong"); }
            assertThrows(IOException.class, () -> BronzeReuseVerifier.verify(copy, store));
        }
        var copy = request.deepCopy();
        ((ObjectNode) copy.path("bronze_inputs").get(0)).put("window_end_utc", "2023-01-05T00:00:00Z");
        final var wrongInterval = copy;
        assertThrows(IOException.class, () -> BronzeReuseVerifier.verify(wrongInterval, store));
        copy = request.deepCopy();
        ((ObjectNode) copy.path("bronze_inputs").get(0)).put("manifest_uri", "s3://other/bronze/usgs/manifest.json");
        final var wrongBucket = copy;
        assertThrows(IOException.class, () -> BronzeReuseVerifier.verify(wrongBucket, store));
        var duplicate = request.deepCopy(); duplicate.withArray("bronze_inputs").add(request.path("bronze_inputs").get(0));
        assertThrows(IOException.class, () -> BronzeReuseVerifier.verify(duplicate, store));
    }

    @Test void rejectedFlagAndWrongCountFailAfterRepinningManifest() throws Exception {
        var request = request("success", "success");
        var pin = (ObjectNode) request.path("bronze_inputs").get(0);
        String key = pin.path("manifest_uri").asText().substring(10);
        var original = (ObjectNode) JSON.readTree(store.read(key));
        for (String mutation : new String[] {"flag", "count", "status"}) {
            var changed = original.deepCopy();
            if (mutation.equals("flag")) { ((ObjectNode) changed.path("validation")).put("checksum_verified", false); }
            else if (mutation.equals("count")) { changed.put("record_count_estimate", 999); }
            else { changed.put("bronze_status", "Rejected"); }
            byte[] bytes = JSON.writeValueAsBytes(changed); store.objects.put(key, bytes);
            pin.put("manifest_sha256", JmaBronzeWriter.sha256(bytes));
            assertThrows(IOException.class, () -> BronzeReuseVerifier.verify(request, store));
        }
    }

    @Test void invalidStructureCannotPassWithForgedReadyFlags() throws Exception {
        assertThrows(IOException.class, () -> BronzeReuseVerifier.verify(request("invalid-json", "success"), store));
        assertThrows(IOException.class, () -> BronzeReuseVerifier.verify(request("success", "invalid-record-length"), store));
    }

    @Test void revisionsCanBeReadIndependentlyWithoutReplacingOldRelease() throws Exception {
        var old = request("revision-v1", "revision-v1");
        var newPin = request("revision-v2", "revision-v2");
        assertTrue(BronzeReuseVerifier.verify(old, store).path("verified").asBoolean());
        assertTrue(BronzeReuseVerifier.verify(newPin, store).path("verified").asBoolean());
        assertNotEquals(old.path("bronze_inputs").get(1).path("catalog_release"),
                newPin.path("bronze_inputs").get(1).path("catalog_release"));
        assertEquals(0, store.writes);
    }

    private ObjectNode request(String usgsCase, String jmaCase) throws Exception {
        ObjectNode request = JSON.createObjectNode();
        request.put("contract_version", "orc-03-v1"); request.put("scope_sha256", "a".repeat(64));
        var pins = request.putArray("bronze_inputs");
        for (String source : new String[] {"USGS", "JMA_BULLETIN"}) {
            boolean usgs = source.equals("USGS"); String name = usgs ? usgsCase : jmaCase;
            Path fixtures = Path.of("../tests/fixtures");
            if (!Files.isDirectory(fixtures)) { fixtures = Path.of("tests/fixtures"); }
            byte[] raw = Files.readAllBytes(fixtures.resolve(usgs ? "usgs/" + name + ".geojson" : "jma/archives/" + name + ".zip"));
            String prefix = "bronze/" + (usgs ? "usgs/" + name : "jma/year=2023/catalog_release=release-" + name)
                    + "/ingest_date=2026-10-08/run_id=fixture/attempt=01/";
            String rawKey = prefix + (usgs ? "response.geojson" : "archive.zip"), manifestKey = prefix + "manifest.json";
            ObjectNode manifest = JSON.createObjectNode();
            manifest.put("manifest_version", "1.0"); manifest.put("bronze_status", "BronzeReady");
            manifest.put("source_system", source); manifest.put("sha256", JmaBronzeWriter.sha256(raw));
            manifest.put("raw_object_uri", store.uriForKey(rawKey)); manifest.put("raw_object_key", rawKey);
            manifest.put("content_length_bytes", raw.length);
            var zipValidation = usgs ? null : new ie212.earthquake.spark.jma.JmaArchiveValidator().validate(raw, "hypo.dat");
            manifest.put("record_count_estimate", usgs
                    ? new ie212.earthquake.spark.usgs.UsgsGeoJsonValidator().validate(raw).featureCount()
                    : zipValidation.valid() ? zipValidation.recordCount() : 0);
            manifest.put("catalog_release", "release-" + name);
            var flags = manifest.putObject("validation");
            for (String flag : new String[] {"object_write_completed", "raw_readback_verified", "checksum_verified",
                    "source_structure_valid", "manifest_consistent"}) { flags.put(flag, true); }
            var interval = manifest.putObject("data_interval"); interval.put("interval_semantics", "[start,end)");
            var pin = pins.addObject(); pin.put("source_system", source); pin.put("manifest_uri", store.uriForKey(manifestKey));
            pin.put("sha256", JmaBronzeWriter.sha256(raw));
            if (usgs) {
                for (var node : new ObjectNode[] {interval, pin}) {
                    node.put("window_start_utc", "2023-01-01T00:00:00Z"); node.put("window_end_utc", "2023-01-04T00:00:00Z");
                }
            } else {
                interval.put("year", 2023); interval.put("segment", "full-year"); interval.put("native_timezone", "Asia/Tokyo");
                manifest.putObject("provenance").put("member_name", "hypo.dat");
                pin.put("year", 2023); pin.put("segment", "full-year"); pin.put("catalog_release", "release-" + name);
            }
            byte[] bytes = JSON.writeValueAsBytes(manifest); pin.put("manifest_sha256", JmaBronzeWriter.sha256(bytes));
            store.objects.put(manifestKey, bytes); store.objects.put(rawKey, raw);
        }
        return request;
    }

    private static final class Store implements BronzeObjectStore {
        final Map<String, byte[]> objects = new HashMap<>(); int writes;
        public void putIfAbsent(String key, byte[] bytes, String type) { writes++; fail("read-only verifier wrote data"); }
        public byte[] read(String key) throws IOException {
            if (!objects.containsKey(key)) { throw new IOException("missing"); } return objects.get(key);
        }
        public boolean exists(String key) { fail("readback must use exact keys without existence scan"); return false; }
        public String uriForKey(String key) { return "s3://lake/" + key; }
    }
}
