package ie212.earthquake.spark.ml;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.util.*;
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
     * 4. Delta time preserves correct sign for PRE (< 0) and POST (>= 0).
     * 5. No silent truncation when exceeding hard limit.
     */
    public void assertInvariants() {
        if (mainshockCount == 0) {
            return;
        }

        // 1. Verify uniqueness of candidate grain
        long actualRows = candidateSnapshot.count();
        long distinctGrain = candidateSnapshot.select("dataset_id", "mainshock_event_id", "candidate_event_id").distinct().count();
        if (actualRows != distinctGrain) {
            throw new AssertionError("Candidate grain (dataset_id, mainshock_event_id, candidate_event_id) is not unique: total="
                    + actualRows + ", distinct=" + distinctGrain);
        }

        // 2. Verify each window contains exactly its own mainshock once
        List<Row> mainshockRows = mainshockSnapshot.select("mainshock_event_id", "candidate_count", "resource_guard_status").collectAsList();
        Set<String> mainshockIds = new HashSet<>();
        for (Row m : mainshockRows) {
            String mid = m.getString(0);
            mainshockIds.add(mid);

            long cCount = m.getLong(1);
            if (cCount < 1) {
                throw new AssertionError("Window for mainshock " + mid + " has candidate_count < 1: " + cCount);
            }
        }

        List<Row> windowMembers = candidateSnapshot.select(
                "mainshock_event_id", "candidate_event_id", "is_mainshock", "relative_time_role",
                "dx_km", "dy_km", "dz_km", "distance_3d_km", "delta_time_hours"
        ).collectAsList();

        Map<String, Integer> selfMainshockCountPerWindow = new HashMap<>();
        for (Row r : windowMembers) {
            String mid = r.getString(0);
            String cid = r.getString(1);
            boolean isMainshock = r.getBoolean(2);
            String role = r.getString(3);
            double dx = r.getDouble(4);
            double dy = r.getDouble(5);
            double dz = r.getDouble(6);
            double dist3d = r.getDouble(7);
            double dt = r.getDouble(8);

            if (mid.equals(cid)) {
                if (!isMainshock) {
                    throw new AssertionError("Self mainshock row has is_mainshock=false for window " + mid);
                }
                if (!"MAINSHOCK".equals(role)) {
                    throw new AssertionError("Self mainshock row has role=" + role + " instead of MAINSHOCK for window " + mid);
                }
                if (Math.abs(dx) > 1e-4 || Math.abs(dy) > 1e-4 || Math.abs(dz) > 1e-4 || Math.abs(dist3d) > 1e-4 || Math.abs(dt) > 1e-4) {
                    throw new AssertionError("Self mainshock vector is non-zero in window " + mid + ": dx=" + dx + ", dy=" + dy + ", dt=" + dt);
                }
                selfMainshockCountPerWindow.merge(mid, 1, Integer::sum);
            } else {
                if (isMainshock) {
                    throw new AssertionError("Non-self candidate marked as is_mainshock=true in window " + mid + ": cid=" + cid);
                }
                if ("MAINSHOCK".equals(role)) {
                    throw new AssertionError("Non-self candidate marked with role=MAINSHOCK in window " + mid + ": cid=" + cid);
                }
                if ("PRE".equals(role) && dt > 0.0) {
                    throw new AssertionError("PRE role candidate has positive delta_time_hours: " + dt + " in window " + mid);
                }
                if ("POST".equals(role) && dt < 0.0) {
                    throw new AssertionError("POST role candidate has negative delta_time_hours: " + dt + " in window " + mid);
                }
            }
        }

        for (String mid : mainshockIds) {
            int count = selfMainshockCountPerWindow.getOrDefault(mid, 0);
            if (count != 1) {
                throw new AssertionError("Window for mainshock " + mid + " contains " + count + " self-mainshock records (expected exactly 1)");
            }
        }

        // 3. Verify no silent truncation: count in candidateSnapshot matches candidate_count in mainshockSnapshot
        for (Row m : mainshockRows) {
            String mid = m.getString(0);
            long expectedCount = m.getLong(1);
            long actualInCandidates = windowMembers.stream().filter(r -> r.getString(0).equals(mid)).count();
            if (expectedCount != actualInCandidates) {
                throw new AssertionError("Candidate count mismatch for window " + mid + ": snapshot=" + expectedCount + ", actual=" + actualInCandidates);
            }
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
