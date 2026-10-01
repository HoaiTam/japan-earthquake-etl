package vn.edu.uit.ie212.earthquake.spark.usgs;

import java.net.URI;
import java.time.Instant;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/** Immutable, validated runtime configuration for the USGS event request. */
public final class UsgsRequestConfig {
    static final int MAX_USGS_LIMIT = 20_000;
    static final int MAX_WINDOW_DAYS = 31;
    static final int MAX_TIMEOUT_MILLIS = 300_000;
    static final int MAX_HTTP_ATTEMPTS = 8;
    static final int MAX_BACKOFF_MILLIS = 300_000;
    static final int MAX_RESPONSE_BYTES = 50_000_000;

    private final URI endpoint;
    private final double minLatitude;
    private final double maxLatitude;
    private final double minLongitude;
    private final double maxLongitude;
    private final Instant seedStartUtc;
    private final int revisionWindowDays;
    private final int maxWindowDays;
    private final int requestLimit;
    private final Duration requestTimeout;
    private final int httpMaxAttempts;
    private final Duration retryInitialBackoff;
    private final Duration retryMaxBackoff;
    private final int maxResponseBytes;
    private final String eventType;

    private UsgsRequestConfig(
            URI endpoint,
            double minLatitude,
            double maxLatitude,
            double minLongitude,
            double maxLongitude,
            Instant seedStartUtc,
            int revisionWindowDays,
            int maxWindowDays,
            int requestLimit,
            Duration requestTimeout,
            int httpMaxAttempts,
            Duration retryInitialBackoff,
            Duration retryMaxBackoff,
            int maxResponseBytes,
            String eventType) {
        this.endpoint = endpoint;
        this.minLatitude = minLatitude;
        this.maxLatitude = maxLatitude;
        this.minLongitude = minLongitude;
        this.maxLongitude = maxLongitude;
        this.seedStartUtc = seedStartUtc;
        this.revisionWindowDays = revisionWindowDays;
        this.maxWindowDays = maxWindowDays;
        this.requestLimit = requestLimit;
        this.requestTimeout = requestTimeout;
        this.httpMaxAttempts = httpMaxAttempts;
        this.retryInitialBackoff = retryInitialBackoff;
        this.retryMaxBackoff = retryMaxBackoff;
        this.maxResponseBytes = maxResponseBytes;
        this.eventType = eventType;
    }

    /** Reads and validates the process environment. */
    public static UsgsRequestConfig fromEnvironment() {
        return fromEnvironment(System.getenv());
    }

    /** Reads and validates a supplied environment map for deterministic tests. */
    public static UsgsRequestConfig fromEnvironment(Map<String, String> environment) {
        Objects.requireNonNull(environment, "environment");

        URI endpoint = parseEndpoint(required(environment, "USGS_API_BASE_URL"));
        double minLatitude = parseDouble(environment, "USGS_MIN_LATITUDE");
        double maxLatitude = parseDouble(environment, "USGS_MAX_LATITUDE");
        double minLongitude = parseDouble(environment, "USGS_MIN_LONGITUDE");
        double maxLongitude = parseDouble(environment, "USGS_MAX_LONGITUDE");
        validateBounds(minLatitude, maxLatitude, minLongitude, maxLongitude);

        Instant seedStartUtc = parseUtcInstant(environment, "USGS_SEED_START_UTC");
        int revisionWindowDays = parseInteger(environment, "PIPELINE_OVERLAP_DAYS", 0, MAX_WINDOW_DAYS);
        int maxWindowDays = parseInteger(environment, "USGS_MAX_WINDOW_DAYS", 1, MAX_WINDOW_DAYS);
        int requestLimit = parseInteger(environment, "USGS_REQUEST_LIMIT", 1, MAX_USGS_LIMIT);
        int timeoutMillis = parseInteger(environment, "USGS_HTTP_TIMEOUT_MS", 1_000, MAX_TIMEOUT_MILLIS);
        int httpMaxAttempts = parseInteger(environment, "USGS_HTTP_MAX_ATTEMPTS", 1, MAX_HTTP_ATTEMPTS);
        int retryInitialBackoffMillis = parseInteger(
                environment, "USGS_HTTP_INITIAL_BACKOFF_MS", 0, MAX_BACKOFF_MILLIS);
        int retryMaxBackoffMillis = parseInteger(
                environment, "USGS_HTTP_MAX_BACKOFF_MS", 0, MAX_BACKOFF_MILLIS);
        if (retryMaxBackoffMillis < retryInitialBackoffMillis) {
            throw invalid("USGS_HTTP_MAX_BACKOFF_MS", "must be greater than or equal to initial backoff");
        }
        int maxResponseBytes = parseInteger(
                environment, "USGS_MAX_RESPONSE_BYTES", 1_024, MAX_RESPONSE_BYTES);
        String eventType = required(environment, "USGS_EVENT_TYPE")
                .toLowerCase(Locale.ROOT);
        if (!eventType.equals("earthquake")) {
            throw invalid("USGS_EVENT_TYPE", "must be earthquake for the baseline query");
        }

        return new UsgsRequestConfig(
                endpoint,
                minLatitude,
                maxLatitude,
                minLongitude,
                maxLongitude,
                seedStartUtc,
                revisionWindowDays,
                maxWindowDays,
                requestLimit,
                Duration.ofMillis(timeoutMillis),
                httpMaxAttempts,
                Duration.ofMillis(retryInitialBackoffMillis),
                Duration.ofMillis(retryMaxBackoffMillis),
                maxResponseBytes,
                eventType);
    }

    private static URI parseEndpoint(String value) {
        try {
            URI endpoint = URI.create(value);
            if (!"https".equalsIgnoreCase(endpoint.getScheme())
                    || endpoint.getHost() == null
                    || endpoint.getUserInfo() != null
                    || endpoint.getQuery() != null
                    || endpoint.getFragment() != null) {
                throw invalid("USGS_API_BASE_URL", "must be an HTTPS URI without query, fragment or credentials");
            }
            return endpoint;
        } catch (IllegalArgumentException exception) {
            throw invalid("USGS_API_BASE_URL", "must be a valid HTTPS URI");
        }
    }

    private static double parseDouble(Map<String, String> environment, String key) {
        String value = required(environment, key);
        try {
            double parsed = Double.parseDouble(value);
            if (!Double.isFinite(parsed)) {
                throw invalid(key, "must be finite");
            }
            return parsed;
        } catch (NumberFormatException exception) {
            throw invalid(key, "must be a finite decimal number");
        }
    }

    private static int parseInteger(Map<String, String> environment, String key, int minimum, int maximum) {
        String value = required(environment, key);
        try {
            int parsed = Integer.parseInt(value);
            if (parsed < minimum || parsed > maximum) {
                throw invalid(key, "must be between " + minimum + " and " + maximum);
            }
            return parsed;
        } catch (NumberFormatException exception) {
            throw invalid(key, "must be an integer");
        }
    }

    private static Instant parseUtcInstant(Map<String, String> environment, String key) {
        String value = required(environment, key);
        if (!value.endsWith("Z")) {
            throw invalid(key, "must use an explicit UTC Z suffix");
        }
        try {
            return Instant.parse(value);
        } catch (java.time.format.DateTimeParseException exception) {
            throw invalid(key, "must be an ISO-8601 UTC timestamp");
        }
    }

    private static void validateBounds(
            double minLatitude,
            double maxLatitude,
            double minLongitude,
            double maxLongitude) {
        if (minLatitude < -90.0 || maxLatitude > 90.0 || minLatitude > maxLatitude) {
            throw invalid("USGS_*_LATITUDE", "must satisfy -90 <= min <= max <= 90");
        }
        if (minLongitude < -180.0 || maxLongitude > 180.0 || minLongitude > maxLongitude) {
            throw invalid("USGS_*_LONGITUDE", "must satisfy -180 <= min <= max <= 180");
        }
    }

    private static String required(Map<String, String> environment, String key) {
        String value = environment.get(key);
        if (value == null || value.isBlank()) {
            throw invalid(key, "is required and must not be blank");
        }
        return value.trim();
    }

    private static IllegalArgumentException invalid(String key, String message) {
        return new IllegalArgumentException(key + " " + message);
    }

    public URI endpoint() {
        return endpoint;
    }

    public double minLatitude() {
        return minLatitude;
    }

    public double maxLatitude() {
        return maxLatitude;
    }

    public double minLongitude() {
        return minLongitude;
    }

    public double maxLongitude() {
        return maxLongitude;
    }

    public Instant seedStartUtc() {
        return seedStartUtc;
    }

    public int revisionWindowDays() {
        return revisionWindowDays;
    }

    public int maxWindowDays() {
        return maxWindowDays;
    }

    public int requestLimit() {
        return requestLimit;
    }

    public Duration requestTimeout() {
        return requestTimeout;
    }

    public int httpMaxAttempts() {
        return httpMaxAttempts;
    }

    public Duration retryInitialBackoff() {
        return retryInitialBackoff;
    }

    public Duration retryMaxBackoff() {
        return retryMaxBackoff;
    }

    public int maxResponseBytes() {
        return maxResponseBytes;
    }

    public String eventType() {
        return eventType;
    }
}
