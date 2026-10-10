package ie212.earthquake.spark.silver;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Metric summary for source-local deduplication and revision accounting (CON-03, SLV-06).
 * Enforces the invariant: inputCount == currentCount + duplicateCount + supersededCount.
 */
public record SourceDedupMetrics(
        int inputCount,
        int currentCount,
        int duplicateCount,
        int supersededCount,
        Map<String, SourceDedupMetrics> bySourceSystem) implements Serializable {

    public SourceDedupMetrics {
        bySourceSystem = bySourceSystem != null ? Collections.unmodifiableMap(bySourceSystem) : Map.of();
        if (inputCount < 0 || currentCount < 0 || duplicateCount < 0 || supersededCount < 0) {
            throw new IllegalArgumentException("Counts must be non-negative");
        }
        if (inputCount != currentCount + duplicateCount + supersededCount) {
            throw new IllegalArgumentException(String.format(
                    "inputCount (%d) must equal currentCount (%d) + duplicateCount (%d) + supersededCount (%d)",
                    inputCount, currentCount, duplicateCount, supersededCount));
        }
    }

    public static SourceDedupMetrics of(int inputCount, int currentCount, int duplicateCount, int supersededCount) {
        return new SourceDedupMetrics(inputCount, currentCount, duplicateCount, supersededCount, Map.of());
    }

    public static SourceDedupMetrics of(
            int inputCount,
            int currentCount,
            int duplicateCount,
            int supersededCount,
            Map<String, SourceDedupMetrics> bySourceSystem) {
        return new SourceDedupMetrics(inputCount, currentCount, duplicateCount, supersededCount, bySourceSystem);
    }
}
