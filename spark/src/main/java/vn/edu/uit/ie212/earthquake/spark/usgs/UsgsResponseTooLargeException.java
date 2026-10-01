package vn.edu.uit.ie212.earthquake.spark.usgs;

import java.io.IOException;

/** Raised before a response larger than the configured guard can be retained. */
public final class UsgsResponseTooLargeException extends IOException {
    private final String requestId;
    private final int maximumBytes;

    UsgsResponseTooLargeException(String requestId, int maximumBytes) {
        super("USGS response exceeded the configured size guard: " + maximumBytes + " bytes");
        this.requestId = requestId;
        this.maximumBytes = maximumBytes;
    }

    public String requestId() {
        return requestId;
    }

    public int maximumBytes() {
        return maximumBytes;
    }
}
