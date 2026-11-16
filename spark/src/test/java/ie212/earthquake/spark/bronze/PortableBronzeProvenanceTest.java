package ie212.earthquake.spark.bronze;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.jma.JmaBronzeWriter;
import ie212.earthquake.spark.usgs.BronzeObjectStore;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class PortableBronzeProvenanceTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String SHA = "a".repeat(64);
    private final Store store = new Store();
    private ObjectNode plan() throws Exception {
        Path fixtures = Files.isDirectory(Path.of("../tests/fixtures")) ? Path.of("../tests/fixtures") : Path.of("tests/fixtures");
        byte[] raw = Files.readAllBytes(fixtures.resolve("usgs/empty.geojson"));
        var manifest = JSON.createObjectNode().put("manifest_version", "1.0").put("manifest_id", "owner-id").put("bronze_status", "BronzeReady")
                .put("source_system", "USGS").put("source_kind", "event_api").put("catalog_release", "frozen")
                .put("sha256", JmaBronzeWriter.sha256(raw)).put("content_length_bytes", raw.length)
                .put("record_count_estimate", 0).put("media_type", "application/geo+json").put("content_encoding", "identity");
        var interval = manifest.putObject("data_interval").put("window_start_utc", "2023-01-01T00:00:00Z")
                .put("window_end_utc", "2023-01-04T00:00:00Z").put("interval_semantics", "[start,end)");
        manifest.putObject("request").put("method", "GET").put("url", "https://earthquake.usgs.gov/fdsnws/event/1/query")
                .putObject("query").put("starttime", "2023-01-01T00:00:00Z");
        var validation = manifest.putObject("validation");
        for (String f : List.of("object_write_completed", "raw_readback_verified", "checksum_verified", "source_structure_valid", "manifest_consistent")) validation.put(f, true);
        String root = "bronze/usgs/owner/";
        manifest.put("raw_object_key", root + "response.geojson").put("raw_object_uri", store.uriForKey(root + "response.geojson"));
        byte[] original = JSON.writeValueAsBytes(manifest); store.objects.put(root + "manifest.json", original); store.objects.put(root + "response.geojson", raw);
        var plan = JSON.createObjectNode().put("version", "dat03-bronze-relocation-plan-v1");
        var pin = plan.putArray("physical_bronze_inputs").addObject().put("source_system", "USGS")
                .put("manifest_uri", store.uriForKey(root + "manifest.json")).put("manifest_sha256", JmaBronzeWriter.sha256(original))
                .put("sha256", JmaBronzeWriter.sha256(raw));
        pin.put("window_start_utc", interval.path("window_start_utc").asText()).put("window_end_utc", interval.path("window_end_utc").asText());
        var sources = plan.putArray("original_sources");
        for (String owner : List.of("ThanhTris", "rosy179")) {
            var copy = manifest.deepCopy(); String prefix = "bronze/usgs/" + owner + "/";
            copy.put("manifest_id", owner + "-id").put("raw_object_key", prefix + "response.geojson").put("raw_object_uri", store.uriForKey(prefix + "response.geojson"));
            byte[] bytes = JSON.writeValueAsBytes(copy);
            sources.addObject().put("owner", owner).put("manifest_uri", store.uriForKey(prefix + "manifest.json"))
                    .put("manifest_sha256", JmaBronzeWriter.sha256(bytes)).put("manifest_base64", Base64.getEncoder().encodeToString(bytes));
        }
        return plan;
    }
    private void mutate(ObjectNode plan, java.util.function.Consumer<ObjectNode> mutation) throws Exception {
        var source = (ObjectNode) plan.path("original_sources").get(0);
        var manifest = (ObjectNode) JSON.readTree(Base64.getDecoder().decode(source.path("manifest_base64").asText()));
        mutation.accept(manifest); byte[] bytes = JSON.writeValueAsBytes(manifest);
        source.put("manifest_base64", Base64.getEncoder().encodeToString(bytes)).put("manifest_sha256", JmaBronzeWriter.sha256(bytes));
    }
    @Test void freshPhysicalReadbackMapsBothWorkersWithoutRestoringKeysOrWriting() throws Exception {
        var receipt = PortableBronzeProvenance.verify(plan(), SHA, store);
        assertEquals(2, receipt.path("original_source_count").asInt()); assertEquals(2, receipt.path("worker_count").asInt());
        for (var row : receipt.path("mappings")) {
            assertFalse(row.path("original_s3_key_verified").asBoolean()); assertTrue(row.path("physical_raw_readback_verified").asBoolean());
            assertNotEquals(row.path("original_raw_object_uri"), row.path("physical_raw_object_uri"));
            assertNotEquals(row.path("original_manifest_id"), row.path("physical_manifest_id"));
        }
        assertFalse(receipt.path("portable_import_ready").asBoolean()); assertEquals(0, store.writes);
        assertEquals(3, store.reads); // Raw+manifest shared verifier, then exact manifest once; no N^2 lookups.
    }
    @Test void sameEmptyBytesCannotMapDifferentSourceWindows() throws Exception {
        var p = plan(); mutate(p, m -> ((ObjectNode) m.path("data_interval")).put("window_end_utc", "2023-02-01T00:00:00Z"));
        assertThrows(IOException.class, () -> PortableBronzeProvenance.verify(p, SHA, store));
    }
    @Test void changedQueryReleaseOrProvenanceCannotUseHashAlone() throws Exception {
        for (String field : List.of("request", "catalog_release", "provenance")) {
            var p = plan(); mutate(p, m -> { if (field.equals("catalog_release")) m.put(field, "changed"); else m.putObject(field).put("different", true); });
            assertThrows(Exception.class, () -> PortableBronzeProvenance.verify(p, SHA, store));
        }
    }
    @Test void physicalRawTamperingFailsEvenWithReadyMetadata() throws Exception {
        var p = plan(); store.objects.put("bronze/usgs/owner/response.geojson", "{}".getBytes());
        assertThrows(IOException.class, () -> PortableBronzeProvenance.verify(p, SHA, store));
    }
    @Test void originalMetadataShaIsMandatory() throws Exception {
        var p = plan(); ((ObjectNode) p.path("original_sources").get(0)).put("manifest_sha256", "b".repeat(64));
        assertThrows(IOException.class, () -> PortableBronzeProvenance.verify(p, SHA, store));
    }
    @Test void duplicateWorkerLogicalSourceIsRejected() throws Exception {
        var p = plan(); var row = p.path("original_sources").get(0).deepCopy();
        ((ObjectNode) row).put("manifest_uri", "s3://lake/bronze/usgs/duplicate/manifest.json"); p.withArray("original_sources").add(row);
        assertThrows(IOException.class, () -> PortableBronzeProvenance.verify(p, SHA, store));
    }
    @Test void rejectedOriginalCannotBeRelocated() throws Exception {
        var p = plan(); mutate(p, m -> m.put("bronze_status", "Rejected"));
        assertThrows(IOException.class, () -> PortableBronzeProvenance.verify(p, SHA, store));
    }
    @Test void jmaCitationTimestampIsRetainedButDoesNotDefineLogicalArchiveIdentity() throws Exception {
        var p = plan(); var original = p.path("original_sources").get(0);
        var m = (ObjectNode) JSON.readTree(Base64.getDecoder().decode(original.path("manifest_base64").asText()));
        m.put("source_system", "JMA_BULLETIN").put("source_kind", "annual_archive");
        var provenance = m.putObject("provenance");
        for (String field : List.of("dataset_title", "inventory_version", "archive_name", "member_name", "record_format",
                "catalog_era", "geodetic_datum", "index_url", "final_url", "citation_version", "terms_url", "update_url", "errata_url")) provenance.put(field, field);
        provenance.put("record_length_bytes", 96).put("citation_text", "retrieved yesterday");
        var before = PortableBronzeProvenance.identity(m);
        provenance.put("citation_text", "retrieved today"); assertEquals(before, PortableBronzeProvenance.identity(m));
        provenance.put("member_name", "different"); assertNotEquals(before, PortableBronzeProvenance.identity(m));
        provenance.remove("record_format"); assertThrows(IOException.class, () -> PortableBronzeProvenance.identity(m));
    }
    private static final class Store implements BronzeObjectStore {
        final Map<String, byte[]> objects = new HashMap<>(); int writes, reads;
        public void putIfAbsent(String k, byte[] b, String t) { writes++; fail("Read-only mapper wrote"); }
        public byte[] read(String k) throws IOException { reads++; if (!objects.containsKey(k)) throw new IOException("MISSING"); return objects.get(k); }
        public boolean exists(String k) { return objects.containsKey(k); }
        public String uriForKey(String k) { return "s3://lake/" + k; }
    }
}
