package ie212.earthquake.spark.ml;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.HashSet;
import java.util.Set;

/** Explicit, versioned scales. Adaptive values use window metadata, never fitted candidates. */
public record FeatureScalingConfig(String featureVersion, String scalingConfigVersion, String mode,
        double spaceScaleKm, double depthScaleKm, double timeScaleHours, long maxCandidatesPerWindow,
        double coordinateToleranceKm, double timeToleranceHours) {
    public FeatureScalingConfig {
        if (!"relative-4d-v1".equals(featureVersion)
                || scalingConfigVersion == null || !scalingConfigVersion.matches("[a-z][a-z0-9_-]{0,95}"))
            throw new IllegalArgumentException("ML_FEATURE_VERSION_REQUIRED");
        if (!Set.of("GLOBAL", "ADAPTIVE_WINDOW").contains(mode)
                || !positive(spaceScaleKm) || !positive(depthScaleKm) || !positive(timeScaleHours)
                || maxCandidatesPerWindow < 1 || maxCandidatesPerWindow > 1_000_000
                || !positive(coordinateToleranceKm) || coordinateToleranceKm > 0.00001
                || !positive(timeToleranceHours) || timeToleranceHours > 1e-12)
            throw new IllegalArgumentException("ML_INVALID_SCALING_CONFIG");
    }
    private static boolean positive(double value) { return Double.isFinite(value) && value > 0; }
    public static FeatureScalingConfig fromJson(JsonNode node) {
        Set<String> keys = Set.of("feature_version", "scaling_config_version", "mode", "space_scale_km",
            "depth_scale_km", "time_scale_hours", "max_candidates_per_window", "coordinate_tolerance_km", "time_tolerance_hours");
        var actual = new HashSet<String>(); node.fieldNames().forEachRemaining(actual::add);
        if (!node.isObject() || !actual.equals(keys) || !node.path("max_candidates_per_window").isIntegralNumber()
                || !node.path("max_candidates_per_window").canConvertToLong())
            throw new IllegalArgumentException("ML_TYPED_SCALING_CONFIG_REQUIRED");
        for (String key : keys) {
            if (Set.of("feature_version", "scaling_config_version", "mode").contains(key)) {
                if (!node.path(key).isTextual()) throw new IllegalArgumentException("ML_TYPED_SCALING_CONFIG_REQUIRED");
            } else if (!node.path(key).isNumber()) throw new IllegalArgumentException("ML_TYPED_SCALING_CONFIG_REQUIRED");
        }
        return new FeatureScalingConfig(node.path("feature_version").asText(), node.path("scaling_config_version").asText(),
            node.path("mode").asText(), node.path("space_scale_km").doubleValue(), node.path("depth_scale_km").doubleValue(),
            node.path("time_scale_hours").doubleValue(), node.path("max_candidates_per_window").longValue(),
            node.path("coordinate_tolerance_km").doubleValue(), node.path("time_tolerance_hours").doubleValue());
    }
}
