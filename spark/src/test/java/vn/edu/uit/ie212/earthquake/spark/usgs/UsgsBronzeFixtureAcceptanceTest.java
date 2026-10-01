package vn.edu.uit.ie212.earthquake.spark.usgs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpHeaders;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** USG-05 fixture-to-Bronze acceptance checks without calling the network. */
class UsgsBronzeFixtureAcceptanceTest {
    private static final Instant WINDOW_START = Instant.parse("2023-09-13T00:00:00Z");
    private static final Instant WINDOW_END = Instant.parse("2023-09-16T00:00:00Z");

    @Test
    void successFixtureCreatesReadyObjectManifestAndRecordCount() throws Exception {
        byte[] payload = fixture("usgs/success.geojson");
        Path root = Files.createTempDirectory("usg-05-success-");
        UsgsBronzeWriter writer = new UsgsBronzeWriter(new FileBronzeObjectStore(root));

        UsgsBronzeWriteResult result = writer.write(input(payload, "usg05-success", 1));

        assertTrue(result.ready());
        assertEquals(1, result.recordCountEstimate());
        assertTrue(result.rawObjectKey().contains("run_id=usg05-success/"));
        FileBronzeObjectStore store = new FileBronzeObjectStore(root);
        assertEquals(UsgsBronzeWriter.sha256(payload), result.sha256());
        assertArrayEquals(payload, store.read(result.rawObjectKey()));

        JsonNode manifest = new ObjectMapper().readTree(store.read(result.manifestKey()));
        assertEquals("BronzeReady", manifest.path("bronze_status").asText());
        assertEquals("usg05-success", manifest.path("run_id").asText());
        assertEquals(1, manifest.path("record_count_estimate").asInt());
        assertEquals(payload.length, manifest.path("content_length_bytes").asInt());
        assertTrue(manifest.path("validation").path("checksum_verified").asBoolean());
    }

    @Test
    void emptyFixtureIsAReadyRunWithZeroRecords() throws Exception {
        byte[] payload = fixture("usgs/empty.geojson");
        Path root = Files.createTempDirectory("usg-05-empty-");

        UsgsBronzeWriteResult result = new UsgsBronzeWriter(new FileBronzeObjectStore(root))
                .write(input(payload, "usg05-empty", 1));

        assertTrue(result.ready());
        assertEquals(0, result.recordCountEstimate());
    }

    @Test
    void malformedFixtureIsRejectedAndNeverPublishesReadyManifest() throws Exception {
        byte[] payload = fixture("usgs/invalid-json.geojson");
        Path root = Files.createTempDirectory("usg-05-invalid-");
        FileBronzeObjectStore store = new FileBronzeObjectStore(root);

        UsgsBronzeWriteResult result = new UsgsBronzeWriter(store)
                .write(input(payload, "usg05-invalid", 1));

        assertFalse(result.ready());
        assertEquals("Rejected", result.bronzeStatus());
        assertTrue(result.rawObjectKey().contains("bronze/_quarantine/usgs/"));
        assertFalse(store.exists("bronze/usgs/ingest_date=2023-09-16/run_id=usg05-invalid/attempt=01/manifest.json"));
        assertEquals(payload.length, store.read(result.rawObjectKey()).length);
    }

    @Test
    void checksumMismatchFixtureAndCorruptReadbackCannotBecomeReady() throws Exception {
        byte[] payload = fixture("usgs/success.geojson");
        String expectedDigest = Files.readString(
                        repositoryRoot().resolve("tests/fixtures/usgs/checksum-mismatch.sha256"),
                        StandardCharsets.UTF_8)
                .trim()
                .split("\\s+")[0];
        assertNotEquals(expectedDigest, UsgsBronzeWriter.sha256(payload));

        Path root = Files.createTempDirectory("usg-05-checksum-");
        BronzeObjectStore corruptingStore = new CorruptingBronzeObjectStore(
                new FileBronzeObjectStore(root));
        IOException exception = assertThrows(
                IOException.class,
                () -> new UsgsBronzeWriter(corruptingStore)
                        .write(input(payload, "usg05-checksum", 1)));
        assertTrue(exception.getMessage().contains("checksum"));
        assertFalse(corruptingStore.exists(
                "bronze/usgs/ingest_date=2023-09-16/run_id=usg05-checksum/attempt=01/manifest.json"));
    }

    private static byte[] fixture(String relativePath) throws IOException {
        return Files.readAllBytes(repositoryRoot().resolve("tests/fixtures").resolve(relativePath));
    }

    private static Path repositoryRoot() {
        Path candidate = Path.of(System.getProperty("user.dir")).toAbsolutePath().normalize();
        while (candidate != null) {
            if (Files.isRegularFile(candidate.resolve("tests/fixtures/cases.json"))) {
                return candidate;
            }
            candidate = candidate.getParent();
        }
        throw new IllegalStateException("Could not locate repository tests/fixtures");
    }

    private static UsgsBronzeWriteRequest input(byte[] payload, String runId, int attempt) {
        UsgsRequest request = new UsgsRequest(
                URI.create("https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson"),
                WINDOW_START,
                WINDOW_END,
                20_000,
                0);
        UsgsHttpResponse response = new UsgsHttpResponse(
                "usg05-" + runId,
                request,
                200,
                HttpHeaders.of(
                        Map.of("content-type", List.of("application/geo+json")),
                        (name, value) -> true),
                payload,
                1,
                Duration.ofMillis(10));
        return new UsgsBronzeWriteRequest(
                response,
                runId,
                attempt,
                LocalDate.of(2023, 9, 16),
                Instant.parse("2023-09-16T00:15:08Z"),
                false,
                "USGS|2023-09-13T00:00:00Z|2023-09-16T00:00:00Z|daily",
                30_000);
    }

    private static final class CorruptingBronzeObjectStore implements BronzeObjectStore {
        private final BronzeObjectStore delegate;

        private CorruptingBronzeObjectStore(BronzeObjectStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public void putIfAbsent(String key, byte[] payload, String contentType) throws IOException {
            byte[] corrupted = payload.clone();
            corrupted[0] = (byte) (corrupted[0] ^ 0x01);
            delegate.putIfAbsent(key, corrupted, contentType);
        }

        @Override
        public byte[] read(String key) throws IOException {
            return delegate.read(key);
        }

        @Override
        public boolean exists(String key) throws IOException {
            return delegate.exists(key);
        }

        @Override
        public String uriForKey(String key) {
            return delegate.uriForKey(key);
        }
    }
}
