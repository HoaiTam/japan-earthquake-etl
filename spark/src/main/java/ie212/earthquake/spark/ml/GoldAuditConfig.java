package ie212.earthquake.spark.ml;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
        Objects.requireNonNull(observationCutoffUtc, "observationCutoffUtc");
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
        if (minEventsForMc < 1 || goldSnapshotId <= 0 || goldPublicationId == null || goldPublicationId.isBlank())
            throw new IllegalArgumentException("ML_INVALID_AUDIT_PIN_OR_SAMPLE_LIMIT");
    }

    /** Runtime uses actual MLD-01 fields. forReproduction/forExtension are offline fixture helpers. */
    public static GoldAuditConfig fromManifest(JsonNode manifest, String run, String reportUri) throws Exception {
        var filters = new ObjectMapper().readTree(manifest.path("filter_config_json").asText());
        var mc = filters.path("completeness_config");
        return new GoldAuditConfig(manifest.path("dataset_split").asText(),
            Instant.parse(manifest.path("period_start_utc").asText()), Instant.parse(manifest.path("period_end_utc_exclusive").asText()),
            Instant.parse(manifest.path("observation_cutoff_utc").asText()), filters.path("canonical_source_system").asText(),
            filters.path("catalog_era").asText(), filters.path("is_natural_earthquake").asBoolean(), filters.path("is_in_study_area").asBoolean(),
            manifest.path("audit_rule_version").asText(), manifest.path("mc_method_version").asText(),
            mc.path("region_version").asText("overall-v1"), null, mc.path("sensitivity_step").asDouble(0.2),
            mc.path("bin_width").asDouble(0.1), mc.path("min_events").asInt(50), manifest.path("dataset_config_version").asText(),
            manifest.path("gold_snapshot_id").asLong(), manifest.path("gold_publication_id").asText(), run, reportUri);
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
