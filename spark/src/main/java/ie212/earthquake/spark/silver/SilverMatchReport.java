package ie212.earthquake.spark.silver;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/**
 * Summary reconciliation report for Silver entity resolution and canonical selection (SLV-07).
 * Tracks evaluation metrics, accepted/rejected/ambiguous counts, and source coverage breakdown.
 */
public record SilverMatchReport(
        int totalObservationsEvaluated,
        int usgsObservationCount,
        int jmaObservationCount,
        int candidatePairsEvaluated,
        int acceptedPairsCount,
        int rejectedPairsCount,
        int ambiguousPairsCount,
        int canonicalEventsCount,
        int matchedEventsCount,
        int usgsOnlyEventsCount,
        int jmaOnlyEventsCount,
        String matchModelVersion,
        String canonicalModelVersion,
        Instant generatedAtUtc) implements Serializable {

    public SilverMatchReport {
        Objects.requireNonNull(matchModelVersion, "matchModelVersion");
        Objects.requireNonNull(canonicalModelVersion, "canonicalModelVersion");
        Objects.requireNonNull(generatedAtUtc, "generatedAtUtc");
    }

    /**
     * Formats this report as a concise key-value summary string.
     */
    public String toSummaryString() {
        return String.format(
                "SilverMatchReport[totalObs=%d, usgsObs=%d, jmaObs=%d, candidatePairs=%d, acceptedPairs=%d, " +
                "rejectedPairs=%d, ambiguousPairs=%d, canonicalEvents=%d, matchedEvents=%d, usgsOnly=%d, jmaOnly=%d, " +
                "modelVersion=%s, canonicalVersion=%s, generatedAt=%s]",
                totalObservationsEvaluated, usgsObservationCount, jmaObservationCount,
                candidatePairsEvaluated, acceptedPairsCount, rejectedPairsCount, ambiguousPairsCount,
                canonicalEventsCount, matchedEventsCount, usgsOnlyEventsCount, jmaOnlyEventsCount,
                matchModelVersion, canonicalModelVersion, generatedAtUtc);
    }
}
