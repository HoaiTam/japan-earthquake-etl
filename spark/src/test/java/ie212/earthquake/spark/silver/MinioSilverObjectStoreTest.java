package ie212.earthquake.spark.silver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MinioSilverObjectStoreTest {

    @Test
    void testRejectsInvalidEnvironmentParameters() {
        Map<String, String> env = new HashMap<>();
        assertThrows(IllegalArgumentException.class, () -> MinioSilverObjectStore.fromEnvironment(env));

        env.put("MINIO_ENDPOINT", "http://localhost:9000");
        assertThrows(IllegalArgumentException.class, () -> MinioSilverObjectStore.fromEnvironment(env));

        env.put("MINIO_ACCESS_KEY", "access");
        assertThrows(IllegalArgumentException.class, () -> MinioSilverObjectStore.fromEnvironment(env));

        env.put("MINIO_SECRET_KEY", "secret");
        assertThrows(IllegalArgumentException.class, () -> MinioSilverObjectStore.fromEnvironment(env));

        env.put("DATA_BUCKET", "INVALID_UPPERCASE_BUCKET");
        assertThrows(IllegalArgumentException.class, () -> MinioSilverObjectStore.fromEnvironment(env));

        env.put("DATA_BUCKET", "japan-earthquake");
        env.put("SILVER_PREFIX", "silver/../escape");
        assertThrows(IllegalArgumentException.class, () -> MinioSilverObjectStore.fromEnvironment(env));
    }

    @Test
    void testOperationsDelegationWithMock() throws Exception {
        MockMinioOperations mock = new MockMinioOperations();
        MinioSilverObjectStore store = MinioSilverObjectStore.forTests(mock, "japan-earthquake", "silver");

        byte[] payload = "test-content".getBytes();
        store.put("partition/data.parquet", payload, "application/octet-stream");

        assertTrue(store.exists("partition/data.parquet"));
        assertArrayEquals(payload, store.read("partition/data.parquet"));
        assertEquals("s3://japan-earthquake/silver/partition/data.parquet", store.uriForKey("partition/data.parquet"));

        List<String> list = store.list("partition");
        assertEquals(1, list.size());
        assertEquals("partition/data.parquet", list.get(0));

        store.move("partition/data.parquet", "partition/moved.parquet");
        assertFalse(store.exists("partition/data.parquet"));
        assertTrue(store.exists("partition/moved.parquet"));

        store.deletePrefix("partition");
        assertFalse(store.exists("partition/moved.parquet"));
    }

    @Test
    void statFailureMustNotBecomeMissingObject() {
        var operations = new MockMinioOperations() {
            @Override public void stat(String bucket, String key) throws Exception { throw new IOException("connection unavailable"); }
        };
        assertThrows(IOException.class, () -> MinioSilverObjectStore.forTests(operations,
                "japan-earthquake", "silver").exists("key"));
    }

    private static class MockMinioOperations implements MinioSilverObjectStore.Operations, AutoCloseable {
        final Map<String, byte[]> objects = new HashMap<>();

        @Override
        public void stat(String bucket, String key) throws Exception {
            if (!objects.containsKey(key)) {
                throw new java.io.FileNotFoundException("Object not found: " + key);
            }
        }

        @Override
        public void put(String bucket, String key, byte[] payload, String contentType) {
            objects.put(key, payload);
        }

        @Override
        public byte[] read(String bucket, String key) throws Exception {
            if (!objects.containsKey(key)) {
                throw new IOException("Object not found: " + key);
            }
            return objects.get(key);
        }

        @Override
        public List<String> list(String bucket, String prefix) {
            List<String> matched = new ArrayList<>();
            for (String key : objects.keySet()) {
                if (key.startsWith(prefix)) {
                    matched.add(key);
                }
            }
            return matched;
        }

        @Override
        public void delete(String bucket, String key) {
            objects.remove(key);
        }

        @Override
        public void close() {
        }
    }
}
