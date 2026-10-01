package vn.edu.uit.ie212.earthquake.spark.usgs;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.net.http.HttpClient;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UsgsHttpClientTest {
    private static final Instant WINDOW_START = Instant.parse("2023-09-13T00:00:00Z");
    private static final Instant WINDOW_END = Instant.parse("2023-09-16T00:00:00Z");

    private HttpServer server;

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    }

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void retriesServerErrorsWithBoundedExponentialBackoffAndLogsMetadata() throws Exception {
        AtomicInteger hits = new AtomicInteger();
        RecordingHandler logs = new RecordingHandler();
        Logger logger = testLogger(logs);
        server.createContext("/query", exchange -> {
            if (hits.incrementAndGet() < 3) {
                respond(exchange, 503, "temporarily unavailable");
            } else {
                respond(exchange, 200, "{\"type\":\"FeatureCollection\",\"features\":[]}");
            }
        });
        server.start();

        List<Duration> delays = new ArrayList<>();
        UsgsHttpClient client = newClient(testConfig(), delays, logger);
        UsgsHttpResponse response = client.fetch(request(20000));

        assertEquals(3, hits.get());
        assertEquals(200, response.statusCode());
        assertEquals(3, response.attempts());
        assertEquals(List.of(Duration.ofMillis(250), Duration.ofMillis(500)), delays);
        assertNotNull(response.requestId());
        assertTrue(logs.text().contains("request_id=" + response.requestId()));
        assertTrue(logs.text().contains("window_start_utc=" + WINDOW_START));
        assertTrue(logs.text().contains("window_end_utc=" + WINDOW_END));
        assertFalse(logs.text().contains("temporarily unavailable"));
    }

    @Test
    void honorsRetryAfterForRateLimit() throws Exception {
        AtomicInteger hits = new AtomicInteger();
        server.createContext("/query", exchange -> {
            if (hits.incrementAndGet() == 1) {
                exchange.getResponseHeaders().add("Retry-After", "1");
                respond(exchange, 429, "slow down");
            } else {
                respond(exchange, 200, "ok");
            }
        });
        server.start();

        List<Duration> delays = new ArrayList<>();
        UsgsHttpClient client = newClient(testConfig(), delays, testLogger(new RecordingHandler()));
        UsgsHttpResponse response = client.fetch(request(20000));

        assertEquals(2, hits.get());
        assertEquals(200, response.statusCode());
        assertEquals(List.of(Duration.ofSeconds(1)), delays);
    }

    @Test
    void doesNotRetryClientErrors() throws Exception {
        AtomicInteger hits = new AtomicInteger();
        server.createContext("/query", exchange -> {
            hits.incrementAndGet();
            respond(exchange, 400, "invalid query");
        });
        server.start();

        List<Duration> delays = new ArrayList<>();
        UsgsHttpException exception = assertThrows(
                UsgsHttpException.class,
                () -> newClient(testConfig(), delays, testLogger(new RecordingHandler()))
                        .fetch(request(20000)));

        assertEquals(1, hits.get());
        assertEquals(400, exception.statusCode());
        assertEquals(1, exception.attempts());
        assertFalse(exception.retryable());
        assertTrue(delays.isEmpty());
    }

    @Test
    void rejectsBodyOverConfiguredSizeGuardBeforeReturningPayload() throws Exception {
        server.createContext("/query", exchange -> respond(exchange, 200, "x".repeat(1025)));
        server.start();

        Map<String, String> values = testConfig();
        values.put("USGS_MAX_RESPONSE_BYTES", "1024");
        UsgsResponseTooLargeException exception = assertThrows(
                UsgsResponseTooLargeException.class,
                () -> newClient(values, new ArrayList<>(), testLogger(new RecordingHandler()))
                        .fetch(request(20000)));

        assertEquals(1024, exception.maximumBytes());
    }

    @Test
    void stopsTimedOutRequestWithoutRetryingWhenAttemptsAreExhausted() throws Exception {
        server.createContext("/query", exchange -> {
            try {
                Thread.sleep(2_000L);
                respond(exchange, 200, "late response");
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
        });
        server.start();

        Map<String, String> values = testConfig();
        values.put("USGS_HTTP_TIMEOUT_MS", "1000");
        values.put("USGS_HTTP_MAX_ATTEMPTS", "1");
        UsgsHttpException exception = assertThrows(
                UsgsHttpException.class,
                () -> newClient(values, new ArrayList<>(), testLogger(new RecordingHandler()))
                        .fetch(request(20000)));

        assertEquals(1, exception.attempts());
        assertTrue(exception.getCause() instanceof HttpTimeoutException);
    }

    @Test
    void countPrecheckPaginatesUsingStableOffsets() throws Exception {
        List<Integer> offsets = new ArrayList<>();
        AtomicInteger countCalls = new AtomicInteger();
        server.createContext("/query", exchange -> {
            Map<String, String> query = queryParameters(exchange.getRequestURI());
            if ("count".equals(query.get("format"))) {
                countCalls.incrementAndGet();
                respond(exchange, 200, "5\n");
                return;
            }
            int offset = Integer.parseInt(query.get("offset"));
            offsets.add(offset);
            respond(exchange, 200, "page-" + offset);
        });
        server.start();

        List<UsgsHttpResponse> pages = newClient(
                testConfig(), new ArrayList<>(), testLogger(new RecordingHandler()))
                .fetchAll(request(2));

        assertEquals(1, countCalls.get());
        assertEquals(List.of(0, 2, 4), offsets);
        assertEquals(List.of("page-0", "page-2", "page-4"),
                pages.stream().map(UsgsHttpResponse::bodyAsUtf8).toList());
        assertEquals(List.of(0, 2, 4), pages.stream()
                .map(page -> page.request().offset()).toList());
    }

    private UsgsHttpClient newClient(
            Map<String, String> values,
            List<Duration> delays,
            Logger logger) {
        return new UsgsHttpClient(
                UsgsRequestConfig.fromEnvironment(values),
                HttpClient.newHttpClient(),
                delay -> delays.add(delay),
                logger);
    }

    private UsgsRequest request(int limit) {
        URI uri = URI.create(
                "http://127.0.0.1:" + server.getAddress().getPort()
                        + "/query?format=geojson&eventtype=earthquake"
                        + "&starttime=2023-09-13T00%3A00%3A00Z"
                        + "&endtime=2023-09-15T23%3A59%3A59.999Z"
                        + "&limit=" + limit + "&offset=0");
        return new UsgsRequest(uri, WINDOW_START, WINDOW_END, limit, 0);
    }

    private static Map<String, String> testConfig() {
        Map<String, String> values = new HashMap<>();
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

    private static Logger testLogger(RecordingHandler handler) {
        Logger logger = Logger.getLogger("usgs-http-test-" + UUID.randomUUID());
        logger.setUseParentHandlers(false);
        logger.setLevel(Level.ALL);
        logger.addHandler(handler);
        return logger;
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        try (var output = exchange.getResponseBody()) {
            output.write(bytes);
        }
    }

    private static Map<String, String> queryParameters(URI uri) {
        Map<String, String> parameters = new HashMap<>();
        for (String parameter : uri.getRawQuery().split("&")) {
            String[] pair = parameter.split("=", 2);
            parameters.put(
                    URLDecoder.decode(pair[0], StandardCharsets.UTF_8),
                    URLDecoder.decode(pair.length == 1 ? "" : pair[1], StandardCharsets.UTF_8));
        }
        return parameters;
    }

    private static final class RecordingHandler extends Handler {
        private final List<String> messages = new ArrayList<>();

        @Override
        public void publish(LogRecord record) {
            messages.add(record.getMessage());
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }

        String text() {
            return String.join("\n", messages);
        }
    }
}
