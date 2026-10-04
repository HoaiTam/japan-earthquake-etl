package ie212.earthquake.spark.usgs;

import java.net.http.HttpHeaders;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Bounded raw HTTP response metadata and bytes returned by the USGS client. */
public final class UsgsHttpResponse {
    private final String requestId;
    private final UsgsRequest request;
    private final int statusCode;
    private final Map<String, List<String>> headers;
    private final byte[] body;
    private final int attempts;
    private final Duration elapsed;

    UsgsHttpResponse(
            String requestId,
            UsgsRequest request,
            int statusCode,
            HttpHeaders headers,
            byte[] body,
            int attempts,
            Duration elapsed) {
        this.requestId = Objects.requireNonNull(requestId, "requestId");
        this.request = Objects.requireNonNull(request, "request");
        this.statusCode = statusCode;
        this.headers = copyHeaders(Objects.requireNonNull(headers, "headers").map());
        this.body = Objects.requireNonNull(body, "body").clone();
        this.attempts = attempts;
        this.elapsed = Objects.requireNonNull(elapsed, "elapsed");
    }

    private static Map<String, List<String>> copyHeaders(Map<String, List<String>> source) {
        return source.entrySet().stream()
                .collect(java.util.stream.Collectors.toUnmodifiableMap(
                        Map.Entry::getKey,
                        entry -> List.copyOf(entry.getValue())));
    }

    public String requestId() {
        return requestId;
    }

    public UsgsRequest request() {
        return request;
    }

    public int statusCode() {
        return statusCode;
    }

    public Map<String, List<String>> headers() {
        return headers;
    }

    public byte[] body() {
        return body.clone();
    }

    public String bodyAsUtf8() {
        return new String(body, StandardCharsets.UTF_8);
    }

    public int bodyLengthBytes() {
        return body.length;
    }

    public int attempts() {
        return attempts;
    }

    public Duration elapsed() {
        return elapsed;
    }
}
