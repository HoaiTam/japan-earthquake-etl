package ie212.earthquake.spark.silver;

import java.util.List;
import java.util.Map;

public record SilverQualityResult(
        String ingestRunId,
        int parsedCount,
        int validCount,
        int rejectedCount,
        boolean publishBlocked,
        List<SilverObservation> validObservations,
        List<SilverRejectRecord> rejectedRecords,
        Map<String, Long> reasonCounts) {

    public SilverQualityResult {
        validObservations = List.copyOf(validObservations);
        rejectedRecords = List.copyOf(rejectedRecords);
        reasonCounts = Map.copyOf(reasonCounts);
        if (validCount + rejectedCount != parsedCount) {
            throw new IllegalArgumentException("valid + rejected must equal parsed");
        }
        if (validCount != validObservations.size() || rejectedCount != rejectedRecords.size()) {
            throw new IllegalArgumentException("counts must match observation and reject datasets");
        }
    }

    /** Fail before a caller touches storage; a boolean summary alone is not a publish gate. */
    public void requirePublishable() {
        if (publishBlocked || rejectedCount > 0) {
            throw new IllegalStateException("Silver publish blocked by quality validation for run " + ingestRunId);
        }
    }
}
