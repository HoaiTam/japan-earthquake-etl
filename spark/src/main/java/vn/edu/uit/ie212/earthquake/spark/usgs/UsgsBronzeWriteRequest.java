package vn.edu.uit.ie212.earthquake.spark.usgs;

import java.time.Instant;
import java.time.LocalDate;
import java.util.Objects;
import java.util.regex.Pattern;

/** Run metadata required to publish one USGS response into Bronze. */
public record UsgsBronzeWriteRequest(
        UsgsHttpResponse response,
        String runId,
        int attempt,
        LocalDate ingestDateUtc,
        Instant retrievedAtUtc,
        boolean backfill,
        String logicalRunKey,
        int requestTimeoutMs) {

    private static final Pattern SAFE_RUN_ID = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._~-]*");

    public UsgsBronzeWriteRequest {
        Objects.requireNonNull(response, "response");
        Objects.requireNonNull(ingestDateUtc, "ingestDateUtc");
        Objects.requireNonNull(retrievedAtUtc, "retrievedAtUtc");
        if (runId == null || !SAFE_RUN_ID.matcher(runId).matches()) {
            throw new IllegalArgumentException("runId must be URL-safe and must not contain separators");
        }
        if (attempt < 1) {
            throw new IllegalArgumentException("attempt must be positive");
        }
        if (logicalRunKey == null || logicalRunKey.isBlank()) {
            throw new IllegalArgumentException("logicalRunKey must not be blank");
        }
        if (requestTimeoutMs < 1) {
            throw new IllegalArgumentException("requestTimeoutMs must be positive");
        }
    }
}
