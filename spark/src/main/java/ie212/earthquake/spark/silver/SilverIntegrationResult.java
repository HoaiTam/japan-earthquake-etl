package ie212.earthquake.spark.silver;

import java.io.Serializable;
import java.util.Objects;

/**
 * Result of end-to-end multi-source Silver integration processing (SLV-09).
 * Contains outputs from dedup, cross-source resolution, quality validation, persistence, and run reconciliation.
 */
public record SilverIntegrationResult(
        String runId,
        SourceDedupResult dedupResult,
        SilverResolutionResult resolutionResult,
        SilverQualityResult qualityResult,
        SilverWriteResult writeResult,
        SilverRunReconciliationReport reconciliationReport) implements Serializable {

    public SilverIntegrationResult {
        Objects.requireNonNull(runId, "runId");
        Objects.requireNonNull(dedupResult, "dedupResult");
        Objects.requireNonNull(resolutionResult, "resolutionResult");
        Objects.requireNonNull(qualityResult, "qualityResult");
        Objects.requireNonNull(reconciliationReport, "reconciliationReport");
    }

    public boolean isPublishable() {
        return !qualityResult.publishBlocked() && reconciliationReport.isReconciliationBalanced();
    }
}
