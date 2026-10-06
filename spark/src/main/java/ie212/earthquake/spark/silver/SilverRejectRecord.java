package ie212.earthquake.spark.silver;

import java.util.List;

public record SilverRejectRecord(
        SilverObservation observation,
        String rejectStage,
        List<String> reasonCodes) {

    public SilverRejectRecord {
        reasonCodes = List.copyOf(reasonCodes);
    }
}
