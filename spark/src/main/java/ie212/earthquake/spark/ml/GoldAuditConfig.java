package ie212.earthquake.spark.ml;

import java.time.Instant;
import java.util.Objects;

/**
 * Immutable configuration for Gold input audit and completeness evaluation.
 * Enforces strict CON-03 reproduction vs extension boundaries and catalog rules.
 */
public record GoldAuditConfig(
        String datasetSplit,
        Instant periodStartUtc,
        Instant periodEndUtcExclusive,
        Instant observationCutoffUtc,
        String requiredCanonicalSource,
        String requiredCatalogEra,
        boolean requireNaturalEarthquake,
        boolean requireInStudyArea,
        String auditRuleVersion,
        String mcMethodVersion,
        String mcRegionVersion,
        Double fixedMcValue,
        double mcSensitivityStep,
        double mcBinWidth,
        int minEventsForMc,
        String datasetConfigVersion,
        long goldSnapshotId,
        String goldPublicationId,
        String buildRunId,
        String auditReportUri
) {
    public static final String REPRODUCTION_START = "2000-01-01T00:00:00Z";
    public static final String REPRODUCTION_END = "2018-10-01T00:00:00Z";
    public static final String EXTENSION_START = "2018-10-01T00:00:00Z";
    public static final String EXTENSION_END = "2024-01-01T00:00:00Z";

    public GoldAuditConfig {
        Objects.requireNonNull(datasetSplit, "datasetSplit");
        Objects.requireNonNull(periodStartUtc, "periodStartUtc");
        Objects.requireNonNull(periodEndUtcExclusive, "periodEndUtcExclusive");
        Objects.requireNonNull(requiredCanonicalSource, "requiredCanonicalSource");
        Objects.requireNonNull(requiredCatalogEra, "requiredCatalogEra");
        Objects.requireNonNull(auditRuleVersion, "auditRuleVersion");
        Objects.requireNonNull(mcMethodVersion, "mcMethodVersion");
        Objects.requireNonNull(mcRegionVersion, "mcRegionVersion");
        Objects.requireNonNull(datasetConfigVersion, "datasetConfigVersion");
        Objects.requireNonNull(buildRunId, "buildRunId");

        if ("REPRODUCTION".equals(datasetSplit)) {
            if (!Instant.parse(REPRODUCTION_START).equals(periodStartUtc)
                    || !Instant.parse(REPRODUCTION_END).equals(periodEndUtcExclusive)) {
                throw new IllegalArgumentException("DS_INVALID_PERIOD: Reproduction period must be ["
                        + REPRODUCTION_START + ", " + REPRODUCTION_END + ")");
            }
        } else if ("EXTENSION".equals(datasetSplit)) {
            if (!Instant.parse(EXTENSION_START).equals(periodStartUtc)
                    || !Instant.parse(EXTENSION_END).equals(periodEndUtcExclusive)) {
                throw new IllegalArgumentException("DS_INVALID_PERIOD: Extension period must be ["
                        + EXTENSION_START + ", " + EXTENSION_END + ")");
            }
        } else {
            throw new IllegalArgumentException("DS_INVALID_PERIOD: Unknown dataset split " + datasetSplit);
        }

        if (fixedMcValue != null && (!Double.isFinite(fixedMcValue) || fixedMcValue < 0)) {
            throw new IllegalArgumentException("fixedMcValue must be non-negative and finite: " + fixedMcValue);
        }
        if (mcSensitivityStep <= 0 || !Double.isFinite(mcSensitivityStep)) {
            throw new IllegalArgumentException("mcSensitivityStep must be positive and finite: " + mcSensitivityStep);
        }
        if (mcBinWidth <= 0 || !Double.isFinite(mcBinWidth)) {
            throw new IllegalArgumentException("mcBinWidth must be positive and finite: " + mcBinWidth);
        }
    }

    public static GoldAuditConfig forReproduction(String buildRunId) {
        return forReproduction(buildRunId, null);
    }

    public static GoldAuditConfig forReproduction(String buildRunId, Double fixedMc) {
        Instant start = Instant.parse(REPRODUCTION_START);
        Instant end = Instant.parse(REPRODUCTION_END);
        return new GoldAuditConfig(
                "REPRODUCTION",
                start,
                end,
                end,
                "JMA_BULLETIN",
                "UNIFIED",
                true,
                true,
                "1.0",
                "1.0",
                "overall-v1",
                fixedMc,
                0.2,
                0.1,
                50,
                "1.0",
                1L,
                "pub_gold_snapshot_1",
                buildRunId,
                "staging/audit/reproduction_audit_report.json"
        );
    }

    public static GoldAuditConfig forExtension(String buildRunId) {
        return forExtension(buildRunId, null);
    }

    public static GoldAuditConfig forExtension(String buildRunId, Double fixedMc) {
        Instant start = Instant.parse(EXTENSION_START);
        Instant end = Instant.parse(EXTENSION_END);
        return new GoldAuditConfig(
                "EXTENSION",
                start,
                end,
                end,
                "JMA_BULLETIN",
                "UNIFIED",
                true,
                true,
                "1.0",
                "1.0",
                "overall-v1",
                fixedMc,
                0.2,
                0.1,
                50,
                "1.0",
                1L,
                "pub_gold_snapshot_1",
                buildRunId,
                "staging/audit/extension_audit_report.json"
        );
    }
}
