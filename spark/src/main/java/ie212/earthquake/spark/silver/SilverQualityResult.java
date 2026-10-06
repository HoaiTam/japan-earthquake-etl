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
    }
}
