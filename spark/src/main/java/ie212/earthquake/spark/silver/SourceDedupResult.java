package ie212.earthquake.spark.silver;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Output of source-local deduplication and revision resolution (CON-03, SLV-06).
 * Delivers current observations to downstream SLV-07 while preserving full observation history for SLV-09.
 */
public record SourceDedupResult(
        List<SilverObservation> currentObservations,
        List<SilverObservation> allObservations,
        List<SilverObservation> historyObservations,
        SourceDedupMetrics metrics) implements Serializable {

    public SourceDedupResult {
        Objects.requireNonNull(currentObservations, "currentObservations");
        Objects.requireNonNull(allObservations, "allObservations");
        Objects.requireNonNull(historyObservations, "historyObservations");
        Objects.requireNonNull(metrics, "metrics");

        currentObservations = Collections.unmodifiableList(currentObservations);
        allObservations = Collections.unmodifiableList(allObservations);
        historyObservations = Collections.unmodifiableList(historyObservations);

        if (currentObservations.size() != metrics.currentCount()) {
            throw new IllegalArgumentException(String.format(
                    "currentObservations size (%d) does not match metrics currentCount (%d)",
                    currentObservations.size(), metrics.currentCount()));
        }
        if (allObservations.size() != metrics.inputCount()) {
            throw new IllegalArgumentException(String.format(
                    "allObservations size (%d) does not match metrics inputCount (%d)",
                    allObservations.size(), metrics.inputCount()));
        }
        if (historyObservations.size() != metrics.duplicateCount() + metrics.supersededCount()) {
            throw new IllegalArgumentException(String.format(
                    "historyObservations size (%d) does not match metrics duplicate + superseded (%d)",
                    historyObservations.size(), metrics.duplicateCount() + metrics.supersededCount()));
        }
    }

    public int currentCount() {
        return metrics.currentCount();
    }

    public int duplicateCount() {
        return metrics.duplicateCount();
    }

    public int supersededCount() {
        return metrics.supersededCount();
    }

    public int inputCount() {
        return metrics.inputCount();
    }
}
