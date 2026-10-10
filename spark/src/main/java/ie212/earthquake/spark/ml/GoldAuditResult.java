package ie212.earthquake.spark.ml;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.util.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Result of Gold input audit and magnitude of completeness estimation.
 * Provides machine-readable and markdown report exports and reconciles counts.
 */
public record GoldAuditResult(
        Dataset<Row> auditedEvents,
        Dataset<Row> eligibleEvents,
        Dataset<Row> excludedEvents,
        long inputEventCount,
        long eligibleEventCount,
        long excludedEventCount,
        Map<String, Long> exclusionCountsByReason,
        Map<String, Long> allExclusionCounts,
        CompletenessResult completenessResult,
        Map<String, CompletenessResult> depthCompletenessResults,
        SortedMap<Integer, Long> inputEventsByYear,
        SortedMap<Integer, Long> eligibleEventsByYear,
        SortedMap<Integer, Long> excludedEventsByYear,
        SortedMap<String, Long> depthBandCounts,
        SortedMap<String, Long> magnitudeBandCounts,
        List<NetworkShiftMetric> networkShiftMetrics,
        GoldAuditConfig config,
        Instant auditedAtUtc,
        String mcConfigJson,
        String mcConfigSha256,
        double mcApplied
) {
    private static final ObjectMapper JSON = new ObjectMapper();

    public record NetworkShiftMetric(
            String milestoneName,
            String milestoneDate,
            long countBefore,
            long countAfter,
            double rateBeforePerYear,
            double rateAfterPerYear,
            double shiftRatio,
            String assessment
    ) {
        public ObjectNode toJsonNode() {
            ObjectNode node = JSON.createObjectNode();
            node.put("milestone_name", milestoneName);
            node.put("milestone_date", milestoneDate);
            node.put("count_before", countBefore);
            node.put("count_after", countAfter);
            node.put("rate_before_per_year", Math.round(rateBeforePerYear * 10.0) / 10.0);
            node.put("rate_after_per_year", Math.round(rateAfterPerYear * 10.0) / 10.0);
            node.put("shift_ratio", Math.round(shiftRatio * 100.0) / 100.0);
            node.put("assessment", assessment);
            return node;
        }
    }

    public void assertReconciled() {
        if (inputEventCount != eligibleEventCount + excludedEventCount) {
            throw new IllegalStateException("Audit count mismatch: input (" + inputEventCount
                    + ") != eligible (" + eligibleEventCount + ") + excluded (" + excludedEventCount + ")");
        }
        long sumPrimary = exclusionCountsByReason.values().stream().mapToLong(Long::longValue).sum();
        if (sumPrimary != excludedEventCount) {
            throw new IllegalStateException("Exclusion reason sum mismatch: sum(" + sumPrimary
                    + ") != excluded (" + excludedEventCount + ")");
        }
    }

    public ObjectNode toJsonReport() {
        assertReconciled();
        ObjectNode root = JSON.createObjectNode();
        root.put("schema_version", "1.0");
        root.put("report_type", "GOLD_INPUT_AUDIT_AND_COMPLETENESS");
        root.put("dataset_split", config.datasetSplit());
        root.put("period_start_utc", config.periodStartUtc().toString());
        root.put("period_end_utc_exclusive", config.periodEndUtcExclusive().toString());
        root.put("audit_rule_version", config.auditRuleVersion());
        root.put("mc_method", completenessResult.method());
        root.put("mc_method_version", config.mcMethodVersion());
        root.put("mc_region_version", config.mcRegionVersion());
        root.put("build_run_id", config.buildRunId());
        root.put("audited_at_utc", auditedAtUtc.toString());

        // Counts
        ObjectNode counts = JSON.createObjectNode();
        counts.put("input_event_count", inputEventCount);
        counts.put("eligible_event_count", eligibleEventCount);
        counts.put("excluded_event_count", excludedEventCount);
        root.set("counts", counts);

        // Exclusions
        ObjectNode exclusions = JSON.createObjectNode();
        for (Map.Entry<String, Long> entry : exclusionCountsByReason.entrySet()) {
            exclusions.put(entry.getKey(), entry.getValue());
        }
        root.set("exclusion_counts_by_primary_reason", exclusions);

        ObjectNode allReasons = JSON.createObjectNode();
        for (Map.Entry<String, Long> entry : allExclusionCounts.entrySet()) {
            allReasons.put(entry.getKey(), entry.getValue());
        }
        root.set("total_occurrences_by_reason", allReasons);

        // Completeness
        root.set("completeness_overall", completenessResult.toJsonNode());
        if (depthCompletenessResults != null && !depthCompletenessResults.isEmpty()) {
            ObjectNode depthNode = JSON.createObjectNode();
            for (Map.Entry<String, CompletenessResult> entry : depthCompletenessResults.entrySet()) {
                depthNode.set(entry.getKey(), entry.getValue().toJsonNode());
            }
            root.set("completeness_by_depth", depthNode);
        }
        root.put("mc_config_json", mcConfigJson);
        root.put("mc_config_sha256", mcConfigSha256);

        // Distributions
        ObjectNode years = JSON.createObjectNode();
        for (Map.Entry<Integer, Long> entry : inputEventsByYear.entrySet()) {
            ObjectNode yNode = JSON.createObjectNode();
            yNode.put("input", entry.getValue());
            yNode.put("eligible", eligibleEventsByYear.getOrDefault(entry.getKey(), 0L));
            yNode.put("excluded", excludedEventsByYear.getOrDefault(entry.getKey(), 0L));
            years.set(entry.getKey().toString(), yNode);
        }
        root.set("annual_distribution", years);

        ObjectNode depthBands = JSON.createObjectNode();
        for (Map.Entry<String, Long> entry : depthBandCounts.entrySet()) {
            depthBands.put(entry.getKey(), entry.getValue());
        }
        root.set("depth_band_distribution", depthBands);

        ObjectNode magBands = JSON.createObjectNode();
        for (Map.Entry<String, Long> entry : magnitudeBandCounts.entrySet()) {
            magBands.put(entry.getKey(), entry.getValue());
        }
        root.set("magnitude_band_distribution", magBands);

        // Network shift metrics
        if (networkShiftMetrics != null && !networkShiftMetrics.isEmpty()) {
            var shiftsArr = JSON.createArrayNode();
            for (NetworkShiftMetric shift : networkShiftMetrics) {
                shiftsArr.add(shift.toJsonNode());
            }
            root.set("network_change_shifts", shiftsArr);
        }

        return root;
    }

    public String toMarkdownReport() {
        assertReconciled();
        StringBuilder sb = new StringBuilder();
        sb.append("# Gold Input Audit & Completeness Report\n\n");
        sb.append("- **Dataset Split:** `").append(config.datasetSplit()).append("`\n");
        sb.append("- **Period:** `[").append(config.periodStartUtc()).append(" .. ").append(config.periodEndUtcExclusive()).append(")`\n");
        sb.append("- **Audit Rule Version:** `").append(config.auditRuleVersion()).append("`\n");
        sb.append("- **Mc Method:** `").append(completenessResult.method()).append("` v").append(config.mcMethodVersion()).append("\n");
        sb.append("- **Mc Applied:** `").append(completenessResult.isReliable() ? completenessResult.centralMc() : "UNRELIABLE / FIXED")
                .append("` (Sensitivity: [").append(completenessResult.sensitivityLower()).append(" .. ")
                .append(completenessResult.sensitivityUpper()).append("])\n");
        sb.append("- **Build Run ID:** `").append(config.buildRunId()).append("`\n");
        sb.append("- **Audited At (UTC):** `").append(auditedAtUtc).append("`\n\n");

        sb.append("## 1. Executive Summary & Counts Reconciliation\n\n");
        sb.append("| Metric | Count | Percentage |\n");
        sb.append("|---|---:|---:|\n");
        sb.append(String.format(Locale.ROOT, "| Total Input Events | %,d | 100.0%% |\n", inputEventCount));
        double eligiblePct = inputEventCount > 0 ? (100.0 * eligibleEventCount / inputEventCount) : 0.0;
        double excludedPct = inputEventCount > 0 ? (100.0 * excludedEventCount / inputEventCount) : 0.0;
        sb.append(String.format(Locale.ROOT, "| Eligible Events | %,d | %.1f%% |\n", eligibleEventCount, eligiblePct));
        sb.append(String.format(Locale.ROOT, "| Excluded Events | %,d | %.1f%% |\n\n", excludedEventCount, excludedPct));

        sb.append("## 2. Exclusion Breakdown by Primary Reason\n\n");
        sb.append("| Reason Code | Description | Count | Share of Excluded |\n");
        sb.append("|---|---|---:|---:|\n");
        for (Map.Entry<String, Long> entry : exclusionCountsByReason.entrySet()) {
            double share = excludedEventCount > 0 ? (100.0 * entry.getValue() / excludedEventCount) : 0.0;
            sb.append(String.format(Locale.ROOT, "| `%s` | %s | %,d | %.1f%% |\n",
                    entry.getKey(), describeReason(entry.getKey()), entry.getValue(), share));
        }
        sb.append("\n");

        sb.append("## 3. Completeness Estimation (MAXC)\n\n");
        sb.append("- **Reliable:** `").append(completenessResult.isReliable()).append("`\n");
        if (completenessResult.unreliableReason() != null) {
            sb.append("- **Reason / Limitation:** ").append(completenessResult.unreliableReason()).append("\n");
        }
        sb.append("- **Central Mc:** `").append(completenessResult.centralMc()).append("`\n");
        sb.append("- **Sensitivity Lower Bound:** `").append(completenessResult.sensitivityLower()).append("`\n");
        sb.append("- **Sensitivity Upper Bound:** `").append(completenessResult.sensitivityUpper()).append("`\n");
        sb.append("- **Sample Size:** `").append(completenessResult.sampleCount()).append("`\n");
        sb.append("- **Events >= Mc:** `").append(completenessResult.eventsAboveMc()).append("` (")
                .append(String.format(Locale.ROOT, "%.1f%%", completenessResult.completenessFraction() * 100.0)).append(")\n");
        if (completenessResult.estimatedBValue() != null) {
            sb.append("- **Gutenberg-Richter b-value (Aki-Utsu):** `")
                    .append(completenessResult.estimatedBValue()).append(" ± ")
                    .append(completenessResult.estimatedBValueStdErr()).append("`\n");
        }
        sb.append("- **Config SHA-256:** `").append(mcConfigSha256).append("`\n\n");

        if (depthCompletenessResults != null && !depthCompletenessResults.isEmpty()) {
            sb.append("### Depth-Stratified Mc Estimates\n\n");
            sb.append("| Depth Stratum | Events | Central Mc | Sensitivity [Lower, Upper] | b-value | Reliable |\n");
            sb.append("|---|---:|---:|---|---|---|\n");
            for (Map.Entry<String, CompletenessResult> entry : depthCompletenessResults.entrySet()) {
                CompletenessResult cr = entry.getValue();
                String bStr = cr.estimatedBValue() != null ? String.format(Locale.ROOT, "%.2f", cr.estimatedBValue()) : "N/A";
                sb.append(String.format(Locale.ROOT, "| %s | %,d | %s | [%s, %s] | %s | %s |\n",
                        entry.getKey(), cr.sampleCount(),
                        cr.isReliable() ? String.format(Locale.ROOT, "%.1f", cr.centralMc()) : "N/A",
                        cr.isReliable() ? String.format(Locale.ROOT, "%.1f", cr.sensitivityLower()) : "N/A",
                        cr.isReliable() ? String.format(Locale.ROOT, "%.1f", cr.sensitivityUpper()) : "N/A",
                        bStr, cr.isReliable() ? "Yes" : "No"));
            }
            sb.append("\n");
        }

        sb.append("## 4. Annual Event Distribution\n\n");
        sb.append("| Year | Total Input | Eligible | Excluded |\n");
        sb.append("|---:|---:|---:|---:|\n");
        for (Map.Entry<Integer, Long> entry : inputEventsByYear.entrySet()) {
            int y = entry.getKey();
            sb.append(String.format(Locale.ROOT, "| %d | %,d | %,d | %,d |\n",
                    y, entry.getValue(), eligibleEventsByYear.getOrDefault(y, 0L), excludedEventsByYear.getOrDefault(y, 0L)));
        }
        sb.append("\n");

        if (networkShiftMetrics != null && !networkShiftMetrics.isEmpty()) {
            sb.append("## 5. Network & Catalog Change Shift Analysis\n\n");
            sb.append("| Milestone | Date | Rate Before (/yr) | Rate After (/yr) | Shift Ratio | Assessment |\n");
            sb.append("|---|---|---:|---:|---:|---|\n");
            for (NetworkShiftMetric shift : networkShiftMetrics) {
                sb.append(String.format(Locale.ROOT, "| %s | %s | %,.1f | %,.1f | %.2f | %s |\n",
                        shift.milestoneName(), shift.milestoneDate(), shift.rateBeforePerYear(),
                        shift.rateAfterPerYear(), shift.shiftRatio(), shift.assessment()));
            }
            sb.append("\n");
        }

        return sb.toString();
    }

    public ObjectNode enrichManifest(ObjectNode manifest) {
        assertReconciled();
        manifest.put("input_event_count", inputEventCount);
        manifest.put("eligible_event_count", eligibleEventCount);
        manifest.put("excluded_event_count", excludedEventCount);

        ObjectNode exclusionsNode = JSON.createObjectNode();
        for (Map.Entry<String, Long> entry : exclusionCountsByReason.entrySet()) {
            exclusionsNode.put(entry.getKey(), entry.getValue());
        }
        manifest.put("exclusion_counts_json", exclusionsNode.toString());

        manifest.put("mc_method", completenessResult.method());
        manifest.put("mc_method_version", config.mcMethodVersion());
        if (config.fixedMcValue() != null) {
            manifest.put("mc_value", config.fixedMcValue());
        } else if (completenessResult.isReliable()) {
            manifest.put("mc_value", completenessResult.centralMc());
        } else if (Double.isFinite(mcApplied)) {
            manifest.put("mc_value", mcApplied);
        } else {
            manifest.putNull("mc_value");
        }
        manifest.put("mc_region_version", config.mcRegionVersion());
        manifest.put("mc_config_json", mcConfigJson);
        manifest.put("mc_config_sha256", mcConfigSha256);
        if (config.auditReportUri() != null) {
            manifest.put("audit_report_uri", config.auditReportUri());
        }
        return manifest;
    }

    private static String describeReason(String code) {
        return switch (code) {
            case "ML_MISSING_COORDINATE" -> "Missing, NaN or non-finite latitude/longitude";
            case "ML_MISSING_DEPTH" -> "Missing, NaN or non-finite hypocenter depth";
            case "ML_MISSING_MAGNITUDE" -> "Missing, NaN or non-finite event magnitude";
            case "ML_NON_NATURAL_EVENT" -> "Non-earthquake event type (e.g. artificial/explosion)";
            case "ML_OUTSIDE_STUDY_AREA" -> "Outside designated geographic region of interest (ROI)";
            case "ML_SOURCE_NOT_COMPARABLE" -> "Not from primary comparable catalog source (e.g. non-JMA)";
            case "ML_LEGACY_CATALOG_ERA" -> "Legacy hypocenter catalog era (prior to UNIFIED era)";
            case "ML_OUTSIDE_TIME_RANGE" -> "Event time outside dataset split period interval";
            case "ML_BELOW_COMPLETENESS" -> "Magnitude below catalog completeness threshold (Mc)";
            case "ML_NON_FINITE_FEATURE" -> "Non-finite spatial or temporal feature";
            case "ML_RESOURCE_LIMIT_EXCEEDED" -> "Sequence candidate window exceeded resource guard limit";
            default -> "Catalog exclusion rule";
        };
    }
}
