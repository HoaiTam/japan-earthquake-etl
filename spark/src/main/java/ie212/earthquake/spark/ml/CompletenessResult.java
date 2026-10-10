package ie212.earthquake.spark.ml;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Result of magnitude completeness estimation (MAXC) with central and sensitivity bounds.
 */
public record CompletenessResult(
        String method,
        String methodVersion,
        double binWidth,
        double centralMc,
        double sensitivityLower,
        double sensitivityUpper,
        double sensitivityStep,
        long sampleCount,
        long eventsAboveMc,
        double completenessFraction,
        double maxCurvatureBin,
        long maxCurvatureBinCount,
        Double estimatedBValue,
        Double estimatedBValueStdErr,
        boolean isReliable,
        String unreliableReason,
        String groupName
) {
    private static final ObjectMapper JSON = new ObjectMapper();

    public ObjectNode toJsonNode() {
        ObjectNode node = JSON.createObjectNode();
        node.put("method", method);
        node.put("method_version", methodVersion);
        node.put("bin_width", binWidth);
        node.put("central_mc", centralMc);
        node.put("sensitivity_lower", sensitivityLower);
        node.put("sensitivity_upper", sensitivityUpper);
        node.put("sensitivity_step", sensitivityStep);
        node.put("sample_count", sampleCount);
        node.put("events_above_mc", eventsAboveMc);
        node.put("completeness_fraction", Math.round(completenessFraction * 1000.0) / 1000.0);
        node.put("max_curvature_bin", maxCurvatureBin);
        node.put("max_curvature_bin_count", maxCurvatureBinCount);
        if (estimatedBValue != null) {
            node.put("estimated_b_value", estimatedBValue);
        } else {
            node.putNull("estimated_b_value");
        }
        if (estimatedBValueStdErr != null) {
            node.put("estimated_b_value_stderr", estimatedBValueStdErr);
        } else {
            node.putNull("estimated_b_value_stderr");
        }
        node.put("is_reliable", isReliable);
        if (unreliableReason != null) {
            node.put("unreliable_reason", unreliableReason);
        } else {
            node.putNull("unreliable_reason");
        }
        if (groupName != null) {
            node.put("group_name", groupName);
        }
        return node;
    }
}
