package ie212.earthquake.spark.ml;

import static org.apache.spark.sql.functions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.util.*;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Result container for mainshock selection and candidate window generation (MLD-03).
 */
public record MainshockWindowResult(
        Dataset<Row> mainshockSnapshot,
        Dataset<Row> candidateSnapshot,
        long mainshockCount,
        long totalCandidateRowCount,
        long distinctCandidateEventCount,
        Map<String, Long> candidatesPerWindow,
        Map<String, Integer> nestedMainshocksPerWindow,
        Map<String, String> resourceGuardStatusPerWindow,
        long flaggedWindowCount,
        long rejectedWindowCount,
        String datasetId,
        MainshockSelectionConfig selectionConfig,
        WindowModelConfig windowConfig,
        double mcApplied,
        Instant createdAtUtc
) {
    private static final ObjectMapper CANONICAL_MAPPER = new ObjectMapper()
            .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);

    /**
     * Validates all contract invariants required by CON-03 and MLD-03:
     * 1. Grain (dataset_id, mainshock_event_id, candidate_event_id) is unique.
     * 2. Every window contains exactly its mainshock once (is_mainshock = true, role = MAINSHOCK).
     * 3. Mainshock vector has dx=0, dy=0, dz=0, dist3d=0, delta_time=0.
     * 4. Delta time preserves microseconds; simultaneous non-self events are POST with zero delta.
     * 5. No silent truncation when exceeding hard limit.
     */
    public void assertInvariants() {
        // All row/grain/vector checks stay distributed. Only a boolean/count reaches the driver.
        long mainRows = mainshockSnapshot.count();
        long candidateRows = candidateSnapshot.count();
        if (mainRows != mainshockCount || candidateRows != totalCandidateRowCount) {
            throw new AssertionError("Snapshot row counts do not match result metadata");
        }
        reject(mainshockSnapshot.groupBy("dataset_id", "mainshock_event_id").count()
                .filter(col("count").notEqual(1)), "Mainshock grain is not unique");
        reject(candidateSnapshot.groupBy("dataset_id", "mainshock_event_id", "candidate_event_id").count()
                .filter(col("count").notEqual(1)), "Candidate grain is not unique");

        Column self = col("mainshock_event_id").equalTo(col("candidate_event_id"));
        Dataset<Row> counts = candidateSnapshot.groupBy("dataset_id", "mainshock_event_id")
                .agg(count(lit(1)).alias("_observed_count"),
                        sum(when(self, lit(1L)).otherwise(lit(0L))).alias("_self_count"));
        Dataset<Row> reconciled = mainshockSnapshot
                .select("dataset_id", "mainshock_event_id", "candidate_count")
                .join(counts, new String[]{"dataset_id", "mainshock_event_id"}, "full_outer");
        reject(reconciled.filter(col("candidate_count").isNull()
                .or(col("_observed_count").isNull())
                .or(col("candidate_count").notEqual(col("_observed_count")))
                .or(col("_self_count").notEqual(1))),
                "Missing/orphan window, self-mainshock or candidate count mismatch");

        Dataset<Row> checked = candidateSnapshot.join(mainshockSnapshot.select(
                col("dataset_id"), col("mainshock_event_id"), col("event_time_utc").alias("_mainshock_time")),
                new String[]{"dataset_id", "mainshock_event_id"}, "left");
        Column expectedRole = when(self, lit("MAINSHOCK"))
                .when(col("candidate_time_utc").lt(col("_mainshock_time")), lit("PRE"))
                .otherwise(lit("POST"));
        Column expectedDelta = when(self, lit(0.0)).otherwise(
                unix_micros(col("candidate_time_utc")).minus(unix_micros(col("_mainshock_time")))
                        .cast("double").divide(3_600_000_000.0));
        Column invalid = col("dataset_id").isNull().or(col("dataset_id").notEqual(lit(datasetId)))
                .or(col("mainshock_event_id").isNull()).or(col("candidate_event_id").isNull())
                .or(col("_mainshock_time").isNull()).or(col("candidate_time_utc").isNull())
                .or(col("is_mainshock").isNull()).or(col("is_mainshock").notEqual(self))
                .or(col("relative_time_role").isNull()).or(col("relative_time_role").notEqual(expectedRole))
                .or(abs(col("delta_time_hours").minus(expectedDelta)).gt(1e-12))
                .or(self.and(col("candidate_time_utc").notEqual(col("_mainshock_time"))));
        for (String field : List.of("dx_km", "dy_km", "dz_km", "distance_3d_km", "delta_time_hours")) {
            Column value = col(field);
            invalid = invalid.or(value.isNull()).or(isnan(value))
                    .or(abs(value).equalTo(Double.POSITIVE_INFINITY))
                    .or(self.and(value.notEqual(0.0)));
        }
        invalid = invalid.or(col("distance_3d_km").lt(0));
        reject(checked.filter(invalid), "Candidate role, timestamp, finite or self-zero-vector invariant failed");
    }

    private static void reject(Dataset<Row> invalidRows, String reason) {
        if (invalidRows.limit(1).count() != 0) {
            throw new AssertionError(reason);
        }
    }

    public ObjectNode toJsonNode() {
        ObjectNode root = CANONICAL_MAPPER.createObjectNode();
        root.put("schema_version", "1.0");
        root.put("dataset_id", datasetId);
        root.put("created_at_utc", createdAtUtc.toString());
        root.put("mainshock_count", mainshockCount);
        root.put("total_candidate_row_count", totalCandidateRowCount);
        root.put("distinct_candidate_event_count", distinctCandidateEventCount);
        root.put("flagged_window_count", flaggedWindowCount);
        root.put("rejected_window_count", rejectedWindowCount);
        root.put("mc_applied", mcApplied);

        root.set("selection_config", selectionConfig.toJsonNode());
        root.set("window_config", windowConfig.toJsonNode());

        ObjectNode windowsNode = root.putObject("windows");
        for (Map.Entry<String, Long> entry : candidatesPerWindow.entrySet()) {
            ObjectNode w = windowsNode.putObject(entry.getKey());
            w.put("candidate_count", entry.getValue());
            w.put("nested_mainshock_count", nestedMainshocksPerWindow.getOrDefault(entry.getKey(), 0));
            w.put("resource_guard_status", resourceGuardStatusPerWindow.getOrDefault(entry.getKey(), "PASS"));
        }
        return root;
    }

    public String toJsonReport() {
        try {
            return CANONICAL_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(toJsonNode());
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize MainshockWindowResult JSON report", e);
        }
    }

    public String toMarkdownSummary() {
        StringBuilder sb = new StringBuilder();
        sb.append("# Mainshock Candidate Window Summary (MLD-03)\n\n");
        sb.append("- **Dataset ID:** `").append(datasetId).append("`\n");
        sb.append("- **Created At (UTC):** `").append(createdAtUtc).append("`\n");
        sb.append("- **Selection Rule:** `").append(selectionConfig.selectionRuleVersion()).append("` (M > ")
                .append(selectionConfig.minMagnitudeExclusive()).append(", depth [")
                .append(selectionConfig.minDepthKm()).append(", ").append(selectionConfig.maxDepthKm()).append("] km)\n");
        sb.append("- **Window Model:** `").append(windowConfig.windowModelVersion()).append("` (").append(windowConfig.modelType()).append(")\n");
        sb.append("- **Completeness Threshold (Mc):** `").append(mcApplied).append("`\n");
        sb.append("- **Mainshock Count:** `").append(mainshockCount).append("`\n");
        sb.append("- **Total Candidate Rows:** `").append(totalCandidateRowCount).append("`\n");
        sb.append("- **Distinct Candidate Events:** `").append(distinctCandidateEventCount).append("`\n");
        sb.append("- **Flagged/Rejected Windows:** `").append(flaggedWindowCount).append(" FLAGGED, ")
                .append(rejectedWindowCount).append(" REJECTED`\n\n");

        sb.append("## Window Details\n\n");
        sb.append("| Mainshock Event ID | Candidate Count | Nested Mainshocks | Guard Status |\n");
        sb.append("|---|---:|---:|---|\n");
        for (Map.Entry<String, Long> entry : candidatesPerWindow.entrySet()) {
            String mid = entry.getKey();
            sb.append("| `").append(mid).append("` | ")
                    .append(entry.getValue()).append(" | ")
                    .append(nestedMainshocksPerWindow.getOrDefault(mid, 0)).append(" | `")
                    .append(resourceGuardStatusPerWindow.getOrDefault(mid, "PASS")).append("` |\n");
        }
        return sb.toString();
    }

    public void enrichManifest(ObjectNode manifestNode) {
        manifestNode.put("mainshock_count", mainshockCount);
        manifestNode.put("candidate_row_count", totalCandidateRowCount);
        manifestNode.put("window_model_version", windowConfig.windowModelVersion());
    }
}
