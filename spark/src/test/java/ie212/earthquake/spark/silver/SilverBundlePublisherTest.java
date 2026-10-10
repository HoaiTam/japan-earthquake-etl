package ie212.earthquake.spark.silver;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class SilverBundlePublisherTest {
    private final MinioSilverObjectStore store = MinioSilverObjectStore.inMemory("earthquake-lake", "silver");
    private static final Instant TIME = Instant.parse("2026-10-10T00:00:00Z");
    private static ObjectNode context() {
        var node = new ObjectMapper().createObjectNode(); node.put("quality_passed", true);
        node.put("reconciliation_balanced", true); node.put("config_version", "test-v1"); return node;
    }
    private static SilverWriteRequest empty(String run) {
        return new SilverWriteRequest(run, List.of(), List.of(), List.of(), List.of(), TIME, false);
    }
    @Test void allFourEmptyDatasetsAndRerunWithZeroWrites() throws Exception {
        var publisher = new SilverBundlePublisher(store);
        assertFalse(publisher.publish(empty("empty"), context()).idempotentReuse());
        var receipt = publisher.verify("empty"); assertEquals(4, receipt.path("datasets").size());
        for (var row : receipt.path("datasets")) { assertEquals(0, row.path("record_count").asInt()); }
        var noWrites = new ForwardingStore(store) {
            @Override public void put(String k, byte[] b, String t) { fail("committed rerun must not write"); }
        };
        assertTrue(new SilverBundlePublisher(noWrites).publish(empty("empty"), context()).idempotentReuse());
    }
    @Test void changedContextCannotMutateCommittedBundle() throws Exception {
        var publisher = new SilverBundlePublisher(store); publisher.publish(empty("fixed"), context());
        byte[] old = store.read("bundles/fixed/manifest.json"); var changed = context(); changed.put("config_version", "v2");
        assertThrows(IOException.class, () -> publisher.publish(empty("fixed"), changed));
        assertArrayEquals(old, store.read("bundles/fixed/manifest.json")); publisher.verify("fixed");
    }
    @Test void changedBytesWithSameRunAndCountsCannotReuseOldBundle() throws Exception {
        var publisher = new SilverBundlePublisher(store);
        var original = new SilverSourceLink("link", "left", "right", 1, 1, 1.0, 0.1, 0.9,
                "ACCEPTED", List.of(), "v1", TIME);
        var changed = new SilverSourceLink("link", "left", "right", 1, 1, 1.0, 0.1, 0.8,
                "ACCEPTED", List.of(), "v1", TIME);
        publisher.publish(new SilverWriteRequest("bytes", List.of(), List.of(), List.of(original), List.of(), TIME, false), context());
        assertThrows(IOException.class, () -> publisher.publish(new SilverWriteRequest("bytes", List.of(), List.of(),
                List.of(changed), List.of(), TIME, false), context()));
        publisher.verify("bytes");
    }
    @Test void failedPutHasNoMarkerAndSameIdentityCanResumeWithoutTouchingOtherRun() throws Exception {
        new SilverBundlePublisher(store).publish(empty("other"), context());
        byte[] old = store.read("bundles/other/manifest.json");
        var failure = new ForwardingStore(store) {
            @Override public void put(String k, byte[] b, String t) throws IOException {
                super.put(k, b, t); if (k.endsWith("source_link/part-00000.parquet")) { throw new IOException("after put"); }
            }
        };
        assertThrows(IOException.class, () -> new SilverBundlePublisher(failure).publish(empty("failed"), context()));
        assertFalse(store.exists("bundles/failed/_SUCCESS"));
        assertThrows(IOException.class, () -> new SilverBundlePublisher(store).verify("failed"));
        var changed = context(); changed.put("config_version", "v2");
        assertThrows(IOException.class, () -> new SilverBundlePublisher(store).publish(empty("failed"), changed));
        assertFalse(new SilverBundlePublisher(store).publish(empty("failed"), context()).idempotentReuse());
        assertArrayEquals(old, store.read("bundles/other/manifest.json"));
    }
    @Test void finalReadbackCorruptionNeverPublishes() throws Exception {
        var failure = new ForwardingStore(store) {
            @Override public void put(String k, byte[] b, String t) throws IOException {
                super.put(k, k.endsWith(".parquet") ? new byte[]{1} : b, t);
            }
        };
        assertThrows(IOException.class, () -> new SilverBundlePublisher(failure).publish(empty("bad"), context()));
        assertFalse(store.exists("bundles/bad/_SUCCESS"));
    }
    @Test void consumersAndRerunsRejectCorruptFilesDespiteMarker() throws Exception {
        var publisher = new SilverBundlePublisher(store); publisher.publish(empty("bad"), context());
        store.put("bundles/bad/canonical_membership/part-00000.parquet", new byte[]{1}, "application/octet-stream");
        assertThrows(IOException.class, () -> publisher.verify("bad"));
        assertThrows(IOException.class, () -> publisher.publish(empty("bad"), context()));
    }
    @Test void manifestAndMarkerCannotBeTrustedWithoutHashVerification() throws Exception {
        var publisher = new SilverBundlePublisher(store); publisher.publish(empty("marker"), context());
        store.put("bundles/marker/_SUCCESS", new byte[]{1}, "text/plain");
        assertThrows(IOException.class, () -> publisher.verify("marker"));
        publisher.publish(empty("schema"), context());
        store.put("bundles/schema/source_link/manifest.json", "{}".getBytes(), "application/json");
        assertThrows(IOException.class, () -> publisher.verify("schema"));
    }
    @Test void gatesBlockBeforeAnyWrite() throws Exception {
        var publisher = new SilverBundlePublisher(store); var blocked = context(); blocked.put("quality_passed", false);
        assertThrows(IOException.class, () -> publisher.publish(empty("blocked"), blocked));
        blocked.put("quality_passed", true); blocked.put("reconciliation_balanced", false);
        assertThrows(IOException.class, () -> publisher.publish(empty("blocked"), blocked));
        assertTrue(store.list("bundles/blocked/").isEmpty());
    }
    @Test void legacyFailedRerunInvalidatesOldSuccessWithS3PrefixSemantics() throws Exception {
        var link = new SilverSourceLink("link", "left", "right", 1, 1, 1.0, 0.1, 0.9,
                "ACCEPTED", List.of(), "v1", TIME);
        var request = new SilverWriteRequest("legacy", List.of(), List.of(), List.of(link), List.of(), TIME, true);
        new SilverParquetWriter(store).write(request);
        assertFalse(store.exists(SilverStorageLayout.linkPartitionPath("legacy")));
        var failure = new ForwardingStore(store) {
            @Override public void move(String a, String b) throws IOException { super.move(a, b); throw new IOException("after move"); }
        };
        assertThrows(IOException.class, () -> new SilverParquetWriter(failure).write(request));
        assertFalse(store.exists(SilverStorageLayout.linkSuccessMarkerPath("legacy")));
    }
    @Test void escapingRunIdRefused() {
        for (var run : List.of("../bad", "a/b", "", "a?b")) {
            assertThrows(IllegalArgumentException.class, () -> SilverBundlePublisher.root(run));
        }
    }
    @Test void sameColumnNamesWithWrongParquetTypesAreRejected() throws Exception {
        var schema = SilverParquetSerializer.OBSERVATION_PARQUET_SCHEMA;
        var fields = new java.util.ArrayList<>(schema.getFields());
        fields.set(0, org.apache.parquet.schema.Types.required(
                org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32).named(fields.get(0).getName()));
        var wrong = new org.apache.parquet.schema.MessageType(schema.getName(), fields);
        byte[] bytes = new SilverParquetSerializer().serializeObservations(List.of());
        assertThrows(IOException.class, () -> SilverParquetSerializer.verifyParquet(bytes, 0, wrong));
    }
    static class ForwardingStore implements SilverObjectStore {
        final SilverObjectStore delegate;
        ForwardingStore(SilverObjectStore delegate) { this.delegate = delegate; }
        public void put(String k, byte[] b, String t) throws IOException { delegate.put(k, b, t); }
        public byte[] read(String k) throws IOException { return delegate.read(k); }
        public boolean exists(String k) throws IOException { return delegate.exists(k); }
        public List<String> list(String k) throws IOException { return delegate.list(k); }
        public void delete(String k) throws IOException { delegate.delete(k); }
        public void deletePrefix(String k) throws IOException { delegate.deletePrefix(k); }
        public void move(String a, String b) throws IOException { delegate.move(a, b); }
        public String uriForKey(String k) { return delegate.uriForKey(k); }
    }
}
