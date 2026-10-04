package ie212.earthquake.spark.usgs;

import java.nio.file.FileAlreadyExistsException;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

class MinioBronzeObjectStoreTest {
    @Test
    void createsReadsAndResolvesOnlyConfiguredBronzeKeys() throws Exception {
        FakeOperations operations = new FakeOperations();
        MinioBronzeObjectStore store = MinioBronzeObjectStore.forTests(
                operations, "japan-earthquake", "bronze");
        String key = "bronze/usgs/ingest_date=2023-01-01/run_id=test/attempt=01/response.geojson";
        byte[] payload = "{}".getBytes(java.nio.charset.StandardCharsets.UTF_8);

        assertFalse(store.exists(key));
        store.putIfAbsent(key, payload, "application/geo+json");

        assertTrue(store.exists(key));
        assertArrayEquals(payload, store.read(key));
        assertEquals("s3://japan-earthquake/" + key, store.uriForKey(key));
        assertEquals("application/geo+json", operations.contentTypes.get(key));
    }

    @Test
    void translatesConditionalWriteConflictWithoutOverwriting() throws Exception {
        FakeOperations operations = new FakeOperations();
        MinioBronzeObjectStore store = MinioBronzeObjectStore.forTests(
                operations, "japan-earthquake", "bronze");
        String key = "bronze/usgs/test.json";
        store.putIfAbsent(key, new byte[] {1}, "application/json");

        assertThrows(
                FileAlreadyExistsException.class,
                () -> store.putIfAbsent(key, new byte[] {2}, "application/json"));
        assertArrayEquals(new byte[] {1}, store.read(key));
    }

    @Test
    void rejectsKeysOutsideBronzePrefixAndInvalidRuntimeConfiguration() {
        MinioBronzeObjectStore store = MinioBronzeObjectStore.forTests(
                new FakeOperations(), "japan-earthquake", "bronze");

        assertThrows(IllegalArgumentException.class, () -> store.exists("silver/output.parquet"));
        assertThrows(IllegalArgumentException.class, () -> store.exists("bronze/../secret"));

        Map<String, String> environment = new HashMap<>();
        environment.put("MINIO_ENDPOINT", "http://user:password@minio:9000");
        environment.put("MINIO_ACCESS_KEY", "pipeline");
        environment.put("MINIO_SECRET_KEY", "secret");
        environment.put("DATA_BUCKET", "japan-earthquake");
        environment.put("BRONZE_PREFIX", "bronze");
        assertThrows(
                IllegalArgumentException.class,
                () -> MinioBronzeObjectStore.fromEnvironment(environment));
    }

    @Test
    void closesUnderlyingSdkOperations() throws Exception {
        FakeOperations operations = new FakeOperations();
        MinioBronzeObjectStore store = MinioBronzeObjectStore.forTests(
                operations, "japan-earthquake", "bronze");

        store.close();

        assertTrue(operations.closed);
    }

    private static final class FakeOperations
            implements MinioBronzeObjectStore.Operations, AutoCloseable {
        private final Map<String, byte[]> objects = new HashMap<>();
        private final Map<String, String> contentTypes = new HashMap<>();
        private boolean closed;

        @Override
        public void stat(String bucket, String key) throws Exception {
            if (!objects.containsKey(key)) {
                throw new MinioBronzeObjectStore.ObjectMissingException(key);
            }
        }

        @Override
        public void putIfAbsent(
                String bucket,
                String key,
                byte[] payload,
                String contentType)
                throws Exception {
            if (objects.containsKey(key)) {
                throw new FileAlreadyExistsException(key);
            }
            objects.put(key, payload.clone());
            contentTypes.put(key, contentType);
        }

        @Override
        public byte[] read(String bucket, String key) throws Exception {
            byte[] payload = objects.get(key);
            if (payload == null) {
                throw new MinioBronzeObjectStore.ObjectMissingException(key);
            }
            return payload.clone();
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
