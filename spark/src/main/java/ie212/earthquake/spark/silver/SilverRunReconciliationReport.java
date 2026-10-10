package ie212.earthquake.spark.silver;

import java.io.Serializable;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Run-level reconciliation report for multi-source Silver processing (SLV-09, CON-03, ORC-04).
 * Reconciles counts across parse, validation, source-local dedup, cross-source linking,
 * and canonical event creation without conflating history with current observations.
 */
public record SilverRunReconciliationReport(
        String runId,
        Instant executedAtUtc,
        String matchModelVersion,
        String canonicalModelVersion,
        // Raw & Parsed
        int usgsParsedCount,
        int jmaParsedCount,
        int totalParsedCount,
        // Validation / Quality
        int usgsValidCount,
        int jmaValidCount,
        int totalValidCount,
        int usgsRejectCount,
        int jmaRejectCount,
        int totalRejectCount,
        Map<String, Long> rejectReasonCounts,
        // Deduplication & Revision
        int usgsCurrentCount,
        int jmaCurrentCount,
        int totalCurrentCount,
        int usgsDuplicateCount,
        int jmaDuplicateCount,
        int totalDuplicateCount,
        int usgsSupersededCount,
        int jmaSupersededCount,
        int totalSupersededCount,
        // Entity Resolution & Canonical Events
        int candidatePairsEvaluated,
        int acceptedLinksCount,
        int rejectedLinksCount,
        int ambiguousLinksCount,
        int canonicalEventsCount,
        int matchedEventsCount,
        int usgsOnlyEventsCount,
        int jmaOnlyEventsCount,
        // Persistence
        int totalObservationsWritten,
        int totalRejectsWritten,
        int partitionsWrittenCount) implements Serializable {

    public SilverRunReconciliationReport {
        Objects.requireNonNull(runId, "runId");
        Objects.requireNonNull(executedAtUtc, "executedAtUtc");
        Objects.requireNonNull(matchModelVersion, "matchModelVersion");
        Objects.requireNonNull(canonicalModelVersion, "canonicalModelVersion");
        rejectReasonCounts = rejectReasonCounts != null
                ? Collections.unmodifiableMap(rejectReasonCounts)
                : Map.of();
    }

    /**
     * Verifies that parsed records strictly equal valid records plus rejected records.
     */
    public boolean isParsedBalanced() {
        return totalParsedCount == (totalValidCount + totalRejectCount)
                && usgsParsedCount == (usgsValidCount + usgsRejectCount)
                && jmaParsedCount == (jmaValidCount + jmaRejectCount);
    }

    /**
     * Verifies that valid records strictly equal current plus duplicate plus superseded revisions.
     */
    public boolean isDedupBalanced() {
        return totalValidCount == (totalCurrentCount + totalDuplicateCount + totalSupersededCount)
                && usgsValidCount == (usgsCurrentCount + usgsDuplicateCount + usgsSupersededCount)
                && jmaValidCount == (jmaCurrentCount + jmaDuplicateCount + jmaSupersededCount);
    }

    /**
     * Verifies that total canonical events match the sum of matched and single-source coverage.
     */
    public boolean isCanonicalBalanced() {
        return canonicalEventsCount == (matchedEventsCount + usgsOnlyEventsCount + jmaOnlyEventsCount);
    }

    /**
     * Verifies that total memberships equal the total current observation count:
     * each matched canonical event has 2 members (bridge count = 2),
     * while each single-source event has 1 member.
     */
    public boolean isBridgeCountBalanced() {
        return (matchedEventsCount * 2) + usgsOnlyEventsCount + jmaOnlyEventsCount == totalCurrentCount;
    }

    /**
     * Overall reconciliation check: all accounting dimensions must balance perfectly.
     */
    public boolean isReconciliationBalanced() {
        return isParsedBalanced() && isDedupBalanced() && isCanonicalBalanced() && isBridgeCountBalanced();
    }

    public String toSummaryString() {
        return String.format(
                "SilverRunReconciliationReport[runId=%s, balanced=%b, parsed=%d, valid=%d, rejected=%d, " +
                "current=%d, duplicate=%d, superseded=%d, candidatePairs=%d, acceptedLinks=%d, " +
                "ambiguousLinks=%d, canonicalEvents=%d (matched=%d, usgsOnly=%d, jmaOnly=%d), partitionsWritten=%d]",
                runId, isReconciliationBalanced(), totalParsedCount, totalValidCount, totalRejectCount,
                totalCurrentCount, totalDuplicateCount, totalSupersededCount, candidatePairsEvaluated,
                acceptedLinksCount, ambiguousLinksCount, canonicalEventsCount, matchedEventsCount,
                usgsOnlyEventsCount, jmaOnlyEventsCount, partitionsWrittenCount);
    }
}
