package vn.edu.uit.ie212.earthquake.spark.usgs;

import java.net.URI;
import java.time.Instant;
import java.util.Objects;

/** One deterministic USGS request for a half-open UTC window. */
public record UsgsRequest(
        URI uri,
        Instant windowStartUtc,
        Instant windowEndExclusiveUtc,
        int limit,
        int offset) {

    public UsgsRequest {
        Objects.requireNonNull(uri, "uri");
        Objects.requireNonNull(windowStartUtc, "windowStartUtc");
        Objects.requireNonNull(windowEndExclusiveUtc, "windowEndExclusiveUtc");
        if (!windowStartUtc.isBefore(windowEndExclusiveUtc)) {
            throw new IllegalArgumentException("window start must be before exclusive end");
        }
        if (limit < 1 || offset < 0) {
            throw new IllegalArgumentException("limit must be positive and offset must not be negative");
        }
    }
}
