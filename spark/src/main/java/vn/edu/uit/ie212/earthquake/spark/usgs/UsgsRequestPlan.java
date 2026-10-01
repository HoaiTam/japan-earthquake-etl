package vn.edu.uit.ie212.earthquake.spark.usgs;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/** Target/query windows and the request chunks that cover the query interval. */
public record UsgsRequestPlan(
        Instant targetStartUtc,
        Instant targetEndExclusiveUtc,
        Instant queryStartUtc,
        Instant queryEndExclusiveUtc,
        List<UsgsRequest> requests) {

    public UsgsRequestPlan {
        Objects.requireNonNull(targetStartUtc, "targetStartUtc");
        Objects.requireNonNull(targetEndExclusiveUtc, "targetEndExclusiveUtc");
        Objects.requireNonNull(queryStartUtc, "queryStartUtc");
        Objects.requireNonNull(queryEndExclusiveUtc, "queryEndExclusiveUtc");
        requests = List.copyOf(Objects.requireNonNull(requests, "requests"));
        if (!targetStartUtc.isBefore(targetEndExclusiveUtc)) {
            throw new IllegalArgumentException("target window must be non-empty");
        }
        if (!queryStartUtc.isBefore(queryEndExclusiveUtc)) {
            throw new IllegalArgumentException("query window must be non-empty");
        }
        if (requests.isEmpty()) {
            throw new IllegalArgumentException("request plan must contain at least one request");
        }
    }
}
