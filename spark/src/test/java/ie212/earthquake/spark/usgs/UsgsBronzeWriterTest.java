package ie212.earthquake.spark.usgs;

import java.net.URI;
import java.net.http.HttpHeaders;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UsgsBronzeWriterTest {
    private static final Instant WINDOW_START = Instant.parse("2023-09-13T00:00:00Z");
    private static final Instant WINDOW_END = Instant.parse("2023-09-16T00:00:00Z");

    @Test
    void validatesFeatureCollectionAndAcceptsEmptyFeatures() {
        UsgsGeoJsonValidator validator = new UsgsGeoJsonValidator();

        UsgsGeoJsonValidationResult valid = validator.validate(
                "{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\"}]}"
                        .getBytes(StandardCharsets.UTF_8));
        UsgsGeoJsonValidationResult empty = validator.validate(
                "{\"type\":\"FeatureCollection\",\"features\":[]}"
                        .getBytes(StandardCharsets.UTF_8));
        UsgsGeoJsonValidationResult invalid = validator.validate(
                "{\"type\":\"FeatureCollection\",\"features\":["
                        .getBytes(StandardCharsets.UTF_8));

        assertTrue(valid.valid());
        assertEquals(1, valid.featureCount());
        assertTrue(empty.valid());
        assertEquals(0, empty.featureCount());
        assertFalse(invalid.valid());
        assertEquals("INVALID_JSON", invalid.reason());
    }

    @Test
    void writesRawAndReadyManifestWithReadbackChecksum(@TempDir Path root) throws Exception {
        FileBronzeObjectStore store = new FileBronzeObjectStore(root);
        UsgsBronzeWriter writer = new UsgsBronzeWriter(store);
        UsgsBronzeWriteRequest input = input(response(successPayload()), "run-20230916", 1);

        UsgsBronzeWriteResult result = writer.write(input);

        assertTrue(result.ready());
        assertFalse(result.idempotentReuse());
        assertEquals(1, result.recordCountEstimate());
        assertTrue(store.exists(result.rawObjectKey()));
        assertTrue(store.exists(result.manifestKey()));
        assertEquals(successPayload(), new String(store.read(result.rawObjectKey()), StandardCharsets.UTF_8));

        JsonNode manifest = new ObjectMapper().readTree(store.read(result.manifestKey()));
        assertEquals("BronzeReady", manifest.path("bronze_status").asText());
        assertEquals(result.sha256(), manifest.path("sha256").asText());
        assertEquals(result.rawObjectKey(), manifest.path("raw_object_key").asText());
        assertEquals(1, manifest.path("record_count_estimate").asInt());
        assertEquals("[start,end)", manifest.path("data_interval").path("interval_semantics").asText());
        assertTrue(manifest.path("validation").path("raw_readback_verified").asBoolean());
    }

    @Test
    void rerunReusesSameImmutableRawAndManifest() throws Exception {
        FileBronzeObjectStore store = new FileBronzeObjectStore(root());
        UsgsBronzeWriter writer = new UsgsBronzeWriter(store);
        UsgsBronzeWriteRequest input = input(response(successPayload()), "run-idempotent", 1);

        UsgsBronzeWriteResult first = writer.write(input);
        UsgsBronzeWriteResult second = writer.write(input);

        assertFalse(first.idempotentReuse());
        assertTrue(second.idempotentReuse());
        assertEquals(first.rawObjectKey(), second.rawObjectKey());
        assertEquals(first.manifestKey(), second.manifestKey());
    }

    @Test
    void rejectsInvalidPayloadIntoQuarantineWithoutBronzeReady() throws Exception {
        FileBronzeObjectStore store = new FileBronzeObjectStore(root());
        UsgsBronzeWriter writer = new UsgsBronzeWriter(store);
        UsgsBronzeWriteResult result = writer.write(
                input(response("{\"type\":\"FeatureCollection\"}"), "run-invalid", 1));

        assertFalse(result.ready());
        assertEquals("Rejected", result.bronzeStatus());
        assertEquals("FEATURES_NOT_ARRAY", result.rejectionReason());
        assertTrue(result.rawObjectKey().startsWith("bronze/_quarantine/usgs/"));
        assertTrue(result.manifestKey().endsWith("failure_manifest.json"));
        assertFalse(store.exists("bronze/usgs/ingest_date=2023-09-16/run_id=run-invalid/attempt=01/response.geojson"));
        JsonNode manifest = new ObjectMapper().readTree(store.read(result.manifestKey()));
        assertEquals("Rejected", manifest.path("bronze_status").asText());
        assertEquals("FEATURES_NOT_ARRAY", manifest.path("failure_reason").asText());
    }

    @Test
    void refusesAmbiguousOverwriteWithDifferentRawBytes() throws Exception {
        FileBronzeObjectStore store = new FileBronzeObjectStore(root());
        UsgsBronzeWriter writer = new UsgsBronzeWriter(store);
        writer.write(input(response(successPayload()), "run-overwrite", 1));

        java.io.IOException exception = assertThrows(
                java.io.IOException.class,
                () -> writer.write(input(response(successPayload().replace("4.2", "4.3")), "run-overwrite", 1)));
        assertTrue(exception.getMessage().contains("AMBIGUOUS_OVERWRITE"));
    }

    private static Path root() throws Exception {
        return Files.createTempDirectory("usg-03-bronze-test-");
    }

    private static UsgsBronzeWriteRequest input(
            UsgsHttpResponse response,
            String runId,
            int attempt) {
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

    private static UsgsHttpResponse response(String payload) {
        UsgsRequest request = new UsgsRequest(
                URI.create("https://earthquake.usgs.gov/fdsnws/event/1/query"
                        + "?format=geojson&eventtype=earthquake&limit=20000&offset=1"),
                WINDOW_START,
                WINDOW_END,
                20_000,
                1);
        return new UsgsHttpResponse(
                "request-123",
                request,
                200,
                HttpHeaders.of(
                        Map.of(
                                "content-type", List.of("application/geo+json"),
                                "etag", List.of("\"fixture-v1\"")),
                        (name, value) -> true),
                payload.getBytes(StandardCharsets.UTF_8),
                1,
                java.time.Duration.ofMillis(12));
    }

    private static String successPayload() {
        return "{\"type\":\"FeatureCollection\",\"features\":["
                + "{\"type\":\"Feature\",\"properties\":{\"mag\":4.2},"
                + "\"geometry\":null,\"id\":\"fx-001\"}]}";
    }

}
