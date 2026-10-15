package ie212.earthquake.spark.ml;

import static org.junit.jupiter.api.Assertions.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class FeatureScalingConfigTest {
    static FeatureScalingConfig config(String mode, double space, long limit) {
        return new FeatureScalingConfig("relative-4d-v1", "test-scaling-v1", mode, space, 30, 720, limit, 1e-8, 1e-12);
    }
    @Test void acceptsBothExplicitModes() {
        assertEquals(50, config("GLOBAL", 50, 10000).spaceScaleKm());
        assertEquals("ADAPTIVE_WINDOW", config("ADAPTIVE_WINDOW", 50, 10000).mode());
    }
    @Test void rejectsZeroNegativeNonFiniteAndUnknownMode() {
        for (double scale : new double[]{0, -1, Double.NaN, Double.POSITIVE_INFINITY})
            assertThrows(IllegalArgumentException.class, () -> config("GLOBAL", scale, 10000));
        assertThrows(IllegalArgumentException.class, () -> config("AUTO_FIT", 50, 10000));
        assertThrows(IllegalArgumentException.class, () -> config("GLOBAL", 50, 0));
        assertThrows(IllegalArgumentException.class, () -> config("GLOBAL", 50, 1000001));
    }
    @Test void rejectsUnversionedOrLooseConfig() throws Exception {
        var json = new ObjectMapper();
        var raw = json.readTree("""
            {"feature_version":"relative-4d-v1","scaling_config_version":"test-global-v1","mode":"GLOBAL",
             "space_scale_km":50.0,"depth_scale_km":30.0,"time_scale_hours":720.0,"max_candidates_per_window":10000,
             "coordinate_tolerance_km":0.00000001,"time_tolerance_hours":0.000000000001}
            """);
        assertEquals("GLOBAL", FeatureScalingConfig.fromJson(raw).mode());
        var extra = raw.deepCopy(); ((com.fasterxml.jackson.databind.node.ObjectNode) extra).put("secret", "not-allowed");
        assertThrows(IllegalArgumentException.class, () -> FeatureScalingConfig.fromJson(extra));
        var text = raw.deepCopy(); ((com.fasterxml.jackson.databind.node.ObjectNode) text).put("space_scale_km", "50");
        assertThrows(IllegalArgumentException.class, () -> FeatureScalingConfig.fromJson(text));
        var overflow = raw.deepCopy(); ((com.fasterxml.jackson.databind.node.ObjectNode) overflow)
            .put("max_candidates_per_window", new java.math.BigInteger("18446744073709551617"));
        assertThrows(IllegalArgumentException.class, () -> FeatureScalingConfig.fromJson(overflow));
        var adaptive = raw.deepCopy(); ((com.fasterxml.jackson.databind.node.ObjectNode) adaptive).put("mode", "ADAPTIVE_WINDOW");
        assertEquals("ADAPTIVE_WINDOW", FeatureScalingConfig.fromJson(adaptive).mode());
    }
}
