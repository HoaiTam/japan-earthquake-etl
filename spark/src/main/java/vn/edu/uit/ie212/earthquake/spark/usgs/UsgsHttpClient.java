package vn.edu.uit.ie212.earthquake.spark.usgs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Synchronous USGS client with bounded retries, response-size protection and
 * optional count-based pagination. It never logs a response body.
 */
public final class UsgsHttpClient {
    private static final String USER_AGENT = "japan-earthquake-etl/usg-02";
    private static final String ACCEPT = "application/geo+json, application/json;q=0.9, text/plain;q=0.5";

    @FunctionalInterface
    interface Sleeper {
        void sleep(Duration delay) throws InterruptedException;
    }

    private final UsgsRequestConfig config;
    private final HttpClient httpClient;
    private final Sleeper sleeper;
    private final Logger logger;

    public UsgsHttpClient(UsgsRequestConfig config) {
        this(
                config,
                HttpClient.newBuilder()
                        .connectTimeout(config.requestTimeout())
                        .followRedirects(HttpClient.Redirect.NEVER)
                        .build(),
                duration -> Thread.sleep(duration.toMillis()),
                Logger.getLogger(UsgsHttpClient.class.getName()));
    }

    UsgsHttpClient(
            UsgsRequestConfig config,
            HttpClient httpClient,
            Sleeper sleeper,
            Logger logger) {
        this.config = Objects.requireNonNull(config, "config");
        this.httpClient = Objects.requireNonNull(httpClient, "httpClient");
        this.sleeper = Objects.requireNonNull(sleeper, "sleeper");
        this.logger = Objects.requireNonNull(logger, "logger");
    }

    /** Fetches one GeoJSON page represented by the request URI. */
    public UsgsHttpResponse fetch(UsgsRequest request) throws IOException {
        Objects.requireNonNull(request, "request");
        return execute(request, request.uri());
    }

    /**
     * Performs a count pre-check and fetches every page using the request limit.
     * The returned responses keep each raw page separate for the Bronze writer.
     */
    public List<UsgsHttpResponse> fetchAll(UsgsRequest request) throws IOException {
        Objects.requireNonNull(request, "request");
        long total = count(request);
        if (total == 0) {
            return List.of();
        }
        if (total > Integer.MAX_VALUE) {
            throw new IOException("USGS result count exceeds supported offset range: " + total);
        }

        List<UsgsHttpResponse> pages = new ArrayList<>();
        long offset = 0;
        while (offset < total) {
            UsgsRequest pageRequest = withOffset(request, (int) offset);
            pages.add(fetch(pageRequest));
            offset += request.limit();
        }
        return List.copyOf(pages);
    }

    /** Performs the USGS FDSN count request for the same window and filters. */
    public long count(UsgsRequest request) throws IOException {
        Objects.requireNonNull(request, "request");
        UsgsRequest countRequest = new UsgsRequest(
                replaceQueryParameter(request.uri(), "format", "count"),
                request.windowStartUtc(),
                request.windowEndExclusiveUtc(),
                request.limit(),
                0);
        UsgsHttpResponse response = execute(countRequest, countRequest.uri());
        String value = response.bodyAsUtf8().trim();
        try {
            long count = Long.parseLong(value);
            if (count < 0) {
                throw new NumberFormatException("negative count");
            }
            return count;
        } catch (NumberFormatException exception) {
            throw new IOException(
                    "USGS count response was not a non-negative integer for request "
                            + response.requestId(),
                    exception);
        }
    }

    private UsgsHttpResponse execute(UsgsRequest request, URI uri) throws IOException {
        String requestId = UUID.randomUUID().toString();
        long startedNanos = System.nanoTime();

        for (int attempt = 1; attempt <= config.httpMaxAttempts(); attempt++) {
            logRequest(requestId, request, uri, attempt);
            HttpRequest httpRequest = HttpRequest.newBuilder(uri)
                    .timeout(config.requestTimeout())
                    .header("Accept", ACCEPT)
                    .header("User-Agent", USER_AGENT)
                    .GET()
                    .build();

            try {
                HttpResponse<InputStream> response = httpClient.send(
                        httpRequest,
                        HttpResponse.BodyHandlers.ofInputStream());
                byte[] body = readBody(response, requestId);
                int statusCode = response.statusCode();
                if (statusCode >= 200 && statusCode < 300) {
                    Duration elapsed = elapsedSince(startedNanos);
                    logResponse(requestId, request, statusCode, attempt, body.length);
                    return new UsgsHttpResponse(
                            requestId,
                            request,
                            statusCode,
                            response.headers(),
                            body,
                            attempt,
                            elapsed);
                }

                boolean retryable = isRetryableStatus(statusCode);
                if (!retryable || attempt == config.httpMaxAttempts()) {
                    logFailure(requestId, request, statusCode, attempt, retryable);
                    throw new UsgsHttpException(
                            "USGS request failed with HTTP status " + statusCode
                                    + " (request_id=" + requestId + ")",
                            requestId,
                            request,
                            statusCode,
                            attempt,
                            retryable);
                }
                retry(requestId, request, statusCode, attempt, response.headers().map());
            } catch (UsgsResponseTooLargeException exception) {
                logger.log(Level.WARNING, "usgs_http_rejected request_id=" + requestId
                        + " window_start_utc=" + request.windowStartUtc()
                        + " window_end_utc=" + request.windowEndExclusiveUtc()
                        + " reason=response_too_large", exception);
                throw exception;
            } catch (UsgsHttpException exception) {
                // A classified HTTP failure must not re-enter the network retry path.
                throw exception;
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new UsgsHttpException(
                        "USGS request interrupted (request_id=" + requestId + ")",
                        exception,
                        requestId,
                        request,
                        attempt);
            } catch (IOException exception) {
                if (attempt == config.httpMaxAttempts()) {
                    logger.log(Level.WARNING, "usgs_http_failed request_id=" + requestId
                            + " window_start_utc=" + request.windowStartUtc()
                            + " window_end_utc=" + request.windowEndExclusiveUtc()
                            + " attempts=" + attempt, exception);
                    throw new UsgsHttpException(
                            "USGS network request failed after " + attempt
                                    + " attempt(s) (request_id=" + requestId + ")",
                            exception,
                            requestId,
                            request,
                            attempt);
                }
                retry(requestId, request, -1, attempt, Map.of());
            }
        }

        throw new AssertionError("retry loop exited unexpectedly");
    }

    private byte[] readBody(HttpResponse<InputStream> response, String requestId)
            throws IOException {
        long declaredLength = response.headers()
                .firstValueAsLong("Content-Length")
                .orElse(-1L);
        if (declaredLength > config.maxResponseBytes()) {
            closeQuietly(response.body());
            throw new UsgsResponseTooLargeException(requestId, config.maxResponseBytes());
        }

        try (InputStream input = response.body()) {
            ByteArrayOutputStream output = new ByteArrayOutputStream(
                    (int) Math.min(config.maxResponseBytes(), 8192));
            byte[] buffer = new byte[8192];
            int read;
            while ((read = input.read(buffer)) != -1) {
                if ((long) output.size() + read > config.maxResponseBytes()) {
                    throw new UsgsResponseTooLargeException(requestId, config.maxResponseBytes());
                }
                output.write(buffer, 0, read);
            }
            return output.toByteArray();
        }
    }

    private void retry(
            String requestId,
            UsgsRequest request,
            int statusCode,
            int attempt,
            Map<String, List<String>> headers)
            throws IOException {
        Duration delay = retryDelay(attempt, headers);
        logger.info("usgs_http_retry request_id=" + requestId
                + " window_start_utc=" + request.windowStartUtc()
                + " window_end_utc=" + request.windowEndExclusiveUtc()
                + " status=" + statusCode
                + " attempt=" + attempt
                + " backoff_ms=" + delay.toMillis());
        try {
            sleeper.sleep(delay);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new UsgsHttpException(
                    "USGS retry interrupted (request_id=" + requestId + ")",
                    exception,
                    requestId,
                    request,
                    attempt);
        }
    }

    private Duration retryDelay(int attempt, Map<String, List<String>> headers) {
        String retryAfter = firstHeader(headers, "retry-after");
        if (retryAfter != null) {
            try {
                long seconds = Long.parseLong(retryAfter.trim());
                if (seconds >= 0) {
                    long milliseconds = Math.min(
                            config.retryMaxBackoff().toMillis(),
                            Math.multiplyExact(seconds, 1_000L));
                    return Duration.ofMillis(milliseconds);
                }
            } catch (ArithmeticException | NumberFormatException ignored) {
                // Fall back to bounded exponential backoff for malformed headers.
            }
        }

        long initial = config.retryInitialBackoff().toMillis();
        long multiplier = 1L << Math.min(Math.max(attempt - 1, 0), 30);
        long delay;
        try {
            delay = Math.multiplyExact(initial, multiplier);
        } catch (ArithmeticException exception) {
            delay = Long.MAX_VALUE;
        }
        return Duration.ofMillis(Math.min(config.retryMaxBackoff().toMillis(), delay));
    }

    private static String firstHeader(Map<String, List<String>> headers, String name) {
        return headers.entrySet().stream()
                .filter(entry -> entry.getKey() != null && entry.getKey().equalsIgnoreCase(name))
                .flatMap(entry -> entry.getValue().stream())
                .findFirst()
                .orElse(null);
    }

    private void logRequest(String requestId, UsgsRequest request, URI uri, int attempt) {
        logger.info("usgs_http_request request_id=" + requestId
                + " window_start_utc=" + request.windowStartUtc()
                + " window_end_utc=" + request.windowEndExclusiveUtc()
                + " offset=" + request.offset()
                + " limit=" + request.limit()
                + " attempt=" + attempt
                + " uri=" + uri);
    }

    private void logResponse(
            String requestId,
            UsgsRequest request,
            int statusCode,
            int attempts,
            int bodyLength) {
        logger.info("usgs_http_response request_id=" + requestId
                + " window_start_utc=" + request.windowStartUtc()
                + " window_end_utc=" + request.windowEndExclusiveUtc()
                + " status=" + statusCode
                + " attempts=" + attempts
                + " bytes=" + bodyLength);
    }

    private void logFailure(
            String requestId,
            UsgsRequest request,
            int statusCode,
            int attempts,
            boolean retryable) {
        logger.warning("usgs_http_failed request_id=" + requestId
                + " window_start_utc=" + request.windowStartUtc()
                + " window_end_utc=" + request.windowEndExclusiveUtc()
                + " status=" + statusCode
                + " attempts=" + attempts
                + " retryable=" + retryable);
    }

    private static boolean isRetryableStatus(int statusCode) {
        return statusCode == 429 || statusCode >= 500 && statusCode <= 599;
    }

    private static UsgsRequest withOffset(UsgsRequest request, int offset) {
        return new UsgsRequest(
                replaceQueryParameter(request.uri(), "offset", Integer.toString(offset)),
                request.windowStartUtc(),
                request.windowEndExclusiveUtc(),
                request.limit(),
                offset);
    }

    private static URI replaceQueryParameter(URI uri, String key, String value) {
        String rawQuery = uri.getRawQuery();
        List<String> parameters = new ArrayList<>();
        boolean replaced = false;
        if (rawQuery != null && !rawQuery.isEmpty()) {
            for (String parameter : rawQuery.split("&", -1)) {
                int separator = parameter.indexOf('=');
                String parameterKey = separator < 0 ? parameter : parameter.substring(0, separator);
                if (parameterKey.equals(key)) {
                    parameters.add(key + "=" + java.net.URLEncoder.encode(
                            value, java.nio.charset.StandardCharsets.UTF_8));
                    replaced = true;
                } else {
                    parameters.add(parameter);
                }
            }
        }
        if (!replaced) {
            parameters.add(key + "=" + java.net.URLEncoder.encode(
                    value, java.nio.charset.StandardCharsets.UTF_8));
        }

        String source = uri.toString();
        int queryStart = source.indexOf('?');
        String prefix = queryStart < 0 ? source : source.substring(0, queryStart);
        String fragment = uri.getRawFragment() == null ? "" : "#" + uri.getRawFragment();
        return URI.create(prefix + "?" + String.join("&", parameters) + fragment);
    }

    private static Duration elapsedSince(long startedNanos) {
        return Duration.ofNanos(Math.max(0L, System.nanoTime() - startedNanos));
    }

    private static void closeQuietly(InputStream input) {
        try {
            input.close();
        } catch (IOException ignored) {
            // The response is already being rejected; no payload is retained.
        }
    }
}
