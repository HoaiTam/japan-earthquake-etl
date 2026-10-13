package ie212.earthquake.spark.jma;

import static org.junit.jupiter.api.Assertions.*;
import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLHandshakeException;
import org.junit.jupiter.api.Test;

class JmaHeadRetryTransportTest {
    private static final URI URI = java.net.URI.create("https://example.invalid/h2000.zip");
    @FunctionalInterface interface Head { JmaHttpMetadata call() throws IOException, InterruptedException; }
    private static JmaArchiveTransport delegate(Head head, AtomicInteger gets) {
        return new JmaArchiveTransport() {
            public JmaHttpMetadata head(URI uri) throws IOException, InterruptedException { return head.call(); }
            public JmaHttpPayload get(URI uri, long range) throws IOException {
                gets.incrementAndGet(); throw new IOException("GET_MUST_NOT_RETRY");
            }
        };
    }
    private static JmaHttpMetadata status(int status) { return new JmaHttpMetadata(status, "application/zip", 42L, null, null, URI); }

    @Test void timeoutThenSuccessRetriesOnlyHeadWithBackoff() throws Exception {
        var calls = new AtomicInteger(); var gets = new AtomicInteger(); var sleeps = new ArrayList<Long>();
        var transport = new JmaHeadRetryTransport(delegate(() -> {
            if (calls.incrementAndGet() < 3) { throw new HttpConnectTimeoutException("DO_NOT_LEAK_TOKEN"); }
            return status(200);
        }, gets), 3, 500, sleeps::add);
        assertEquals(200, transport.head(URI).statusCode());
        assertEquals(3, transport.attempts()); assertEquals(List.of(500L, 1000L), sleeps);
        assertEquals(0, gets.get());
    }

    @Test void exhaustedTimeoutReturnsControlledReasonNotExceptionText() {
        var calls = new AtomicInteger(); var sleeps = new ArrayList<Long>();
        var transport = new JmaHeadRetryTransport(delegate(() -> {
            calls.incrementAndGet(); throw new HttpConnectTimeoutException("credential=PRIVATE");
        }, new AtomicInteger()), 3, 0, sleeps::add);
        var error = assertThrows(JmaHeadRetryTransport.Failure.class, () -> transport.head(URI));
        assertEquals("JMA_HEAD_CONNECT_TIMEOUT", error.getMessage()); assertEquals(3, error.attempts);
        assertEquals(3, calls.get()); assertEquals(2, sleeps.size());
        assertNull(error.getCause()); assertFalse(error.toString().contains("PRIVATE"));
    }

    @Test void retryableStatusSetIsBoundedAndPermanentHttpFailuresDoNotRetry() throws Exception {
        for (int code : List.of(429, 500, 502, 503, 504, 401, 403, 404, 400)) {
            var calls = new AtomicInteger(); var sleeps = new ArrayList<Long>();
            var transport = new JmaHeadRetryTransport(delegate(() -> { calls.incrementAndGet(); return status(code); }, new AtomicInteger()), 3, 500, sleeps::add);
            var error = assertThrows(JmaHeadRetryTransport.Failure.class, () -> transport.head(URI));
            int expected = List.of(429, 500, 502, 503, 504).contains(code) ? 3 : 1;
            assertEquals(expected, calls.get()); assertEquals(expected - 1, sleeps.size());
            assertEquals("JMA_HEAD_HTTP_" + code, error.getMessage());
        }
    }

    @Test void tlsAndUnknownIoErrorsAreNotRetriedOrLeaked() {
        for (IOException cause : List.of(new SSLHandshakeException("private cert detail"), new IOException("private local detail"))) {
            var calls = new AtomicInteger();
            var transport = new JmaHeadRetryTransport(delegate(() -> { calls.incrementAndGet(); throw cause; }, new AtomicInteger()), 3, 0, ms -> fail("no sleep"));
            var error = assertThrows(JmaHeadRetryTransport.Failure.class, () -> transport.head(URI));
            assertEquals(cause instanceof SSLHandshakeException ? "JMA_HEAD_TLS_FAILED" : "JMA_HEAD_IO_FAILED", error.getMessage());
            assertEquals(1, calls.get()); assertNull(error.getCause());
        }
    }

    @Test void wrappedTlsFailureTakesPriorityOverNestedConnectFailure() {
        var tls = new SSLHandshakeException("SECRET"); tls.initCause(new ConnectException("SECRET"));
        var transport = new JmaHeadRetryTransport(delegate(() -> { throw new IOException("SECRET", tls); }, new AtomicInteger()), 3, 0, ms -> fail("no sleep"));
        assertEquals("JMA_HEAD_TLS_FAILED", assertThrows(JmaHeadRetryTransport.Failure.class, () -> transport.head(URI)).getMessage());
    }

    @Test void timeoutDnsConnectionAndEofAreClassifiedWithoutRawMessages() {
        for (IOException cause : List.of(new HttpTimeoutException("private"), new java.net.UnknownHostException("private"),
                new ConnectException("private"), new java.io.EOFException("private"), new java.net.SocketTimeoutException("private"))) {
            var transport = new JmaHeadRetryTransport(delegate(() -> { throw cause; }, new AtomicInteger()), 2, 0, ms -> {});
            var error = assertThrows(JmaHeadRetryTransport.Failure.class, () -> transport.head(URI));
            assertEquals(2, error.attempts); assertTrue(error.getMessage().startsWith("JMA_HEAD_"));
            assertFalse(error.toString().contains("private"));
        }
    }

    @Test void interruptedSleepStopsImmediatelyAndPreservesInterrupt() {
        var calls = new AtomicInteger();
        var transport = new JmaHeadRetryTransport(delegate(() -> { calls.incrementAndGet(); throw new HttpTimeoutException("private"); }, new AtomicInteger()),
                3, 500, ms -> { throw new InterruptedException(); });
        try {
            assertThrows(InterruptedException.class, () -> transport.head(URI));
            assertTrue(Thread.currentThread().isInterrupted()); assertEquals(1, calls.get());
        } finally { Thread.interrupted(); }
    }

    @Test void getIsNeverRetriedAndHeadAttemptCounterResetsPerCall() throws Exception {
        var gets = new AtomicInteger(); var transport = new JmaHeadRetryTransport(delegate(() -> status(200), gets), 3, 0, ms -> {});
        assertThrows(IOException.class, () -> transport.get(URI, 31)); assertEquals(1, gets.get());
        transport.head(URI); transport.head(URI); assertEquals(1, transport.attempts());
    }

    @Test void backoffIsCappedAndPolicyCanDisableRetry() {
        var sleeps = new ArrayList<Long>(); var calls = new AtomicInteger();
        var transport = new JmaHeadRetryTransport(delegate(() -> { calls.incrementAndGet(); return status(503); }, new AtomicInteger()), 5, 1500, sleeps::add);
        assertThrows(IOException.class, () -> transport.head(URI)); assertEquals(List.of(1500L, 2000L, 2000L, 2000L), sleeps);
        var single = new JmaHeadRetryTransport(delegate(() -> status(503), new AtomicInteger()), 1, 0, ms -> fail("no sleep"));
        assertThrows(IOException.class, () -> single.head(URI)); assertEquals(1, single.attempts());
        assertThrows(IllegalArgumentException.class, () -> new JmaHeadRetryTransport(delegate(() -> status(200), new AtomicInteger()), 6, 0, ms -> {}));
        assertThrows(IllegalArgumentException.class, () -> new JmaHeadRetryTransport(delegate(() -> status(200), new AtomicInteger()), 1, 2001, ms -> {}));
    }

    @Test void productionHeadRetryWorksAgainstLocalHttpServer() throws Exception {
        var calls = new AtomicInteger();
        var server = com.sun.net.httpserver.HttpServer.create(new java.net.InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/head", exchange -> { int code = calls.incrementAndGet() == 1 ? 503 : 200;
            exchange.getResponseHeaders().set("Content-Type", "application/zip"); exchange.sendResponseHeaders(code, -1); exchange.close(); });
        server.start();
        try {
            var raw = JmaArchiveDownloader.httpTransport(java.net.http.HttpClient.newHttpClient(), java.time.Duration.ofSeconds(2), 100);
            var transport = new JmaHeadRetryTransport(raw, 3, 0, ms -> {});
            assertEquals(200, transport.head(java.net.URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/head")).statusCode());
            assertEquals(2, calls.get());
        } finally { server.stop(0); }
    }
}
