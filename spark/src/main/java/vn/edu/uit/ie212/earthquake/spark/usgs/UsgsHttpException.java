package vn.edu.uit.ie212.earthquake.spark.usgs;

import java.io.IOException;
import java.util.Objects;

/** A bounded HTTP/network failure with request metadata but no response payload. */
public class UsgsHttpException extends IOException {
    private final String requestId;
    private final UsgsRequest request;
    private final int statusCode;
    private final int attempts;
    private final boolean retryable;

    UsgsHttpException(
            String message,
            String requestId,
            UsgsRequest request,
            int statusCode,
            int attempts,
            boolean retryable) {
        super(message);
        this.requestId = Objects.requireNonNull(requestId, "requestId");
        this.request = Objects.requireNonNull(request, "request");
        this.statusCode = statusCode;
        this.attempts = attempts;
        this.retryable = retryable;
    }

    UsgsHttpException(
            String message,
            Throwable cause,
            String requestId,
            UsgsRequest request,
            int attempts) {
        super(message, cause);
        this.requestId = Objects.requireNonNull(requestId, "requestId");
        this.request = Objects.requireNonNull(request, "request");
        this.statusCode = -1;
        this.attempts = attempts;
        this.retryable = true;
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

    public int attempts() {
        return attempts;
    }

    public boolean retryable() {
        return retryable;
    }
}
