package ie212.earthquake.spark.usgs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.http.HttpHeaders;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UsgsIngestRunnerTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Instant RETRIEVED_AT = Instant.parse("2023-01-05T00:00:00Z");
    private static final byte[] VALID_BODY = (
            "{\"type\":\"FeatureCollection\",\"features\":["
                    + "{\"type\":\"Feature\",\"id\":\"us-test\",\"properties\":{},"
                    + "\"geometry\":null}]}")
            .getBytes(StandardCharsets.UTF_8);

    @TempDir
    Path temporaryDirectory;

    @Test
    void executesAllPhasesAndReusesSameLogicalRunWithoutRefetching() throws Exception {
        AtomicInteger fetchCalls = new AtomicInteger();
        Map<String, String> environment = environment();
        FileBronzeObjectStore objectStore = new FileBronzeObjectStore(
                temporaryDirectory.resolve("object-store"));
        UsgsIngestRunner runner = runner(environment, VALID_BODY, fetchCalls, objectStore);

        Map<String, Object> firstFetch = run(runner, "fetch");
        Map<String, Object> secondFetch = run(runner, "fetch");
        Map<String, Object> validation = run(runner, "validate");
        Map<String, Object> firstUpload = run(runner, "upload");
        Map<String, Object> secondUpload = run(runner, "upload");
        Map<String, Object> verification = run(runner, "verify");

        assertEquals(1, fetchCalls.get());
        assertEquals(false, firstFetch.get("idempotent_reuse"));
        assertEquals(true, secondFetch.get("idempotent_reuse"));
        assertEquals(true, validation.get("valid"));
        assertEquals(1, validation.get("record_count_estimate"));
        assertEquals("BronzeReady", firstUpload.get("bronze_status"));
        assertEquals(false, firstUpload.get("idempotent_reuse"));
        assertEquals(true, secondUpload.get("idempotent_reuse"));
        assertEquals(firstUpload.get("manifest_uri"), secondUpload.get("manifest_uri"));
        assertEquals(true, verification.get("verified"));
        assertEquals(firstUpload.get("sha256"), verification.get("sha256"));
        assertEquals(1, verification.get("record_count_estimate"));
        assertTrue(((String) verification.get("manifest_sha256")).matches("[0-9a-f]{64}"));
        assertNotEquals(verification.get("sha256"), verification.get("manifest_sha256"));
    }

    @Test
    void checksumMismatchStopsBeforeBronzeWrite() throws Exception {
        AtomicInteger fetchCalls = new AtomicInteger();
        FileBronzeObjectStore objectStore = new FileBronzeObjectStore(
                temporaryDirectory.resolve("object-store"));
        UsgsIngestRunner runner = runner(environment(), VALID_BODY, fetchCalls, objectStore);
        run(runner, "fetch");

        Files.writeString(
                temporaryDirectory.resolve("staging/usg06-test/fetch-response.geojson"),
                "tampered",
                StandardCharsets.UTF_8);

        IOException exception = assertThrows(IOException.class, () -> run(runner, "upload"));
        assertTrue(exception.getMessage().contains("checksum or length mismatch"));
        assertFalse(Files.exists(temporaryDirectory.resolve("object-store/bronze/usgs")));
    }

    @Test
    void invalidPayloadIsQuarantinedAndCannotVerify() throws Exception {
        FileBronzeObjectStore objectStore = new FileBronzeObjectStore(
                temporaryDirectory.resolve("object-store"));
        UsgsIngestRunner runner = runner(
                environment(),
                "not-json".getBytes(StandardCharsets.UTF_8),
                new AtomicInteger(),
                objectStore);
        run(runner, "fetch");

        Map<String, Object> validation = run(runner, "validate");

        assertEquals(false, validation.get("valid"));
        assertEquals("Rejected", validation.get("bronze_status"));
        assertTrue(String.valueOf(validation.get("quarantine_uri"))
                .contains("failure_manifest.json"));
        assertThrows(IOException.class, () -> run(runner, "verify"));
    }

    @Test
    void cliRejectsMissingArgumentsWithoutPrintingEnvironmentSecrets() {
        Map<String, String> environment = new HashMap<>();
        environment.put("MINIO_SECRET_KEY", "must-not-appear");
        ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        ByteArrayOutputStream stderr = new ByteArrayOutputStream();

        int status = UsgsIngestRunner.executeCli(
                new String[0],
                environment,
                new PrintStream(stdout),
                new PrintStream(stderr));

        assertEquals(2, status);
        assertTrue(stdout.toString(StandardCharsets.UTF_8).isEmpty());
        assertFalse(stderr.toString(StandardCharsets.UTF_8).contains("must-not-appear"));
    }

    private UsgsIngestRunner runner(
            Map<String, String> environment,
            byte[] body,
            AtomicInteger fetchCalls,
            BronzeObjectStore objectStore) {
        return new UsgsIngestRunner(
                environment,
                config -> request -> {
                    fetchCalls.incrementAndGet();
                    return new UsgsHttpResponse(
                            "test-request",
                            request,
                            200,
                            HttpHeaders.of(
                                    Map.of("content-type", java.util.List.of("application/geo+json")),
                                    (name, value) -> true),
                            body,
                            1,
                            Duration.ofMillis(10));
                },
                ignored -> objectStore,
                Clock.fixed(RETRIEVED_AT, ZoneOffset.UTC));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> run(UsgsIngestRunner runner, String phase) throws Exception {
        Path contextFile = contextFile(phase);
        return runner.runPhase(phase, contextFile);
    }

    private Path contextFile(String phase) throws Exception {
        Path contextFile = temporaryDirectory.resolve(phase + "-input.json");
        ObjectNode root = JSON.createObjectNode();
        root.put("phase", phase);
        ObjectNode context = root.putObject("run_context");
        context.put("dag_id", "usg_04_usgs_ingest");
        context.put("run_id", "usg06-test");
        context.put("run_id_path", "usg06-test");
        context.put("window_start_utc", "2023-01-01T00:00:00Z");
        context.put("window_end_utc", "2023-01-04T00:00:00Z");
        context.put("target_window_start_utc", "2023-01-01T00:00:00Z");
        context.put("target_window_end_utc", "2023-01-04T00:00:00Z");
        context.put("processing_date", "2023-01-01");
        context.put("is_backfill", true);
        context.put("logical_run_key", "USGS|2023-01-01T00:00:00Z|2023-01-04T00:00:00Z|backfill");
        root.putObject("upstream");
        Files.write(contextFile, JSON.writeValueAsBytes(root));
        return contextFile;
    }

    private Map<String, String> environment() {
        Map<String, String> values = new HashMap<>();
        values.put("USGS_STAGING_ROOT", temporaryDirectory.resolve("staging").toString());
        values.put("USGS_API_BASE_URL", "https://earthquake.usgs.gov/fdsnws/event/1/query");
        values.put("USGS_MIN_LATITUDE", "20.0");
        values.put("USGS_MAX_LATITUDE", "50.0");
        values.put("USGS_MIN_LONGITUDE", "120.0");
        values.put("USGS_MAX_LONGITUDE", "155.0");
        values.put("USGS_SEED_START_UTC", "2023-01-01T00:00:00Z");
        values.put("PIPELINE_OVERLAP_DAYS", "3");
        values.put("USGS_MAX_WINDOW_DAYS", "3");
        values.put("USGS_REQUEST_LIMIT", "20000");
        values.put("USGS_HTTP_TIMEOUT_MS", "30000");
        values.put("USGS_HTTP_MAX_ATTEMPTS", "4");
        values.put("USGS_HTTP_INITIAL_BACKOFF_MS", "250");
        values.put("USGS_HTTP_MAX_BACKOFF_MS", "4000");
        values.put("USGS_MAX_RESPONSE_BYTES", "10485760");
        values.put("USGS_EVENT_TYPE", "earthquake");
        return values;
    }
}
