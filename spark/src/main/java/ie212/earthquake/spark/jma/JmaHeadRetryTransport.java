package ie212.earthquake.spark.jma;

import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.UnknownHostException;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpTimeoutException;
import java.util.Objects;
import javax.net.ssl.SSLException;

/** Retry HEAD only: no cache mutation, GET replay, credential or exception-text logging. */
final class JmaHeadRetryTransport implements JmaArchiveTransport {
    @FunctionalInterface interface Sleeper { void sleep(long milliseconds) throws InterruptedException; }
    static final class Failure extends IOException {
        final int attempts;
        Failure(String reason, int attempts) { super(reason); this.attempts = attempts; }
    }
    private final JmaArchiveTransport delegate;
    private final int maxAttempts;
    private final long initialBackoff;
    private final Sleeper sleeper;
    private int attempts;

    JmaHeadRetryTransport(JmaArchiveTransport delegate, int maxAttempts, long initialBackoff, Sleeper sleeper) {
        this.delegate = Objects.requireNonNull(delegate);
        if (maxAttempts < 1 || maxAttempts > 5 || initialBackoff < 0 || initialBackoff > 2000) {
            throw new IllegalArgumentException("INVALID_JMA_HEAD_RETRY_POLICY");
        }
        this.maxAttempts = maxAttempts;
        this.initialBackoff = initialBackoff;
        this.sleeper = Objects.requireNonNull(sleeper);
    }

    int attempts() { return attempts; }

    @Override public JmaHttpMetadata head(URI uri) throws IOException, InterruptedException {
        attempts = 0;
        while (true) {
            attempts++;
            String reason;
            boolean retry;
            try {
                JmaHttpMetadata response = delegate.head(uri);
                int status = response.statusCode();
                if (status >= 200 && status < 300) { return response; }
                reason = "JMA_HEAD_HTTP_" + (status >= 100 && status <= 599 ? status : "INVALID");
                retry = status == 429 || status == 500 || status == 502 || status == 503 || status == 504;
            } catch (IOException error) {
                reason = reason(error);
                retry = !reason.equals("JMA_HEAD_TLS_FAILED") && !reason.equals("JMA_HEAD_IO_FAILED");
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw error;
            }
            if (!retry || attempts == maxAttempts) { throw new Failure(reason, attempts); }
            try {
                sleeper.sleep(Math.min(2000, initialBackoff * (1L << (attempts - 1))));
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw error;
            }
        }
    }

    private static String reason(IOException error) {
        // Inspect classes, never arbitrary messages/URIs. Bound malformed cause chains.
        Throwable tlsCause = error;
        for (int depth = 0; depth < 8 && tlsCause != null; depth++, tlsCause = tlsCause.getCause()) {
            if (tlsCause instanceof SSLException) { return "JMA_HEAD_TLS_FAILED"; }
        }
        Throwable cause = error;
        for (int depth = 0; depth < 8 && cause != null; depth++, cause = cause.getCause()) {
            if (cause instanceof HttpConnectTimeoutException) { return "JMA_HEAD_CONNECT_TIMEOUT"; }
            if (cause instanceof HttpTimeoutException || cause instanceof SocketTimeoutException) { return "JMA_HEAD_TIMEOUT"; }
            if (cause instanceof UnknownHostException) { return "JMA_HEAD_DNS_FAILED"; }
            if (cause instanceof ConnectException || cause instanceof SocketException || cause instanceof EOFException) { return "JMA_HEAD_CONNECTION_FAILED"; }
        }
        return "JMA_HEAD_IO_FAILED";
    }

    @Override public JmaHttpPayload get(URI uri, long rangeStart) throws IOException, InterruptedException {
        return delegate.get(uri, rangeStart); // Existing downloader owns resume/validation/publication.
    }
}
