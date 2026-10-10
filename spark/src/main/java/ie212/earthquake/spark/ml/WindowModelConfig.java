package ie212.earthquake.spark.ml;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

/**
 * Versioned configuration for magnitude-dependent seismological window models (MLD-03).
 */
public record WindowModelConfig(
        WindowModelType modelType,
        String windowModelVersion,
        double preWindowRatio,
        double minPreWindowHours,
        Double maxPreWindowHours,
        Double maxPostWindowHours,
        Double maxSearchRadiusKm,
        Double customRadiusA,
        Double customRadiusB,
        Double customTimeA,
        Double customTimeB
) {
    private static final ObjectMapper CANONICAL_MAPPER = new ObjectMapper()
            .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);

    public WindowModelConfig {
        Objects.requireNonNull(modelType, "modelType");
        Objects.requireNonNull(windowModelVersion, "windowModelVersion");
        if (preWindowRatio < 0.0) {
            throw new IllegalArgumentException("preWindowRatio must be non-negative: " + preWindowRatio);
        }
        if (minPreWindowHours < 0.0) {
            throw new IllegalArgumentException("minPreWindowHours must be non-negative: " + minPreWindowHours);
        }
    }

    /**
     * Standard Uhrhammer (1986) model version 1.0.
     * Radius: d(M) = exp(-1.024 + 0.804 * M) km
     * Post-window: t(M) = exp(-2.870 + 1.235 * M) days -> hours = t(M) * 24.0
     * Pre-window: max(168.0h, 0.1 * post_window)
     */
    public static WindowModelConfig uhrhammerV1() {
        return new WindowModelConfig(
                WindowModelType.UHRHAMMER,
                "wm_uhrhammer_v1.0",
                0.1,
                168.0, // 7 days baseline
                null,
                null,
                null,
                null,
                null,
                null,
                null
        );
    }

    /**
     * Standard Gardner & Knopoff (1974) model version 1.0.
     * Radius: L(M) = 10^(0.1238 * M + 0.983) km
     * Post-window: T(M) = 10^(0.5409 * M - 0.547) days -> hours = T(M) * 24.0
     * Pre-window: max(168.0h, 0.1 * post_window)
     */
    public static WindowModelConfig gardnerKnopoffV1() {
        return new WindowModelConfig(
                WindowModelType.GARDNER_KNOPOFF,
                "wm_gardner_knopoff_v1.0",
                0.1,
                168.0,
                null,
                null,
                null,
                null,
                null,
                null,
                null
        );
    }

    /**
     * Expanded baseline model version 1.0 with conservative radius and duration caps.
     */
    public static WindowModelConfig expandedV1() {
        return new WindowModelConfig(
                WindowModelType.EXPANDED,
                "wm_expanded_v1.0",
                0.15,
                168.0,
                720.0,    // 30 days max pre-window
                17520.0,  // 2 years max post-window
                250.0,    // 250 km max radius
                0.359,
                0.804,
                0.0567,
                1.235
        );
    }

    public ObjectNode toJsonNode() {
        ObjectNode node = CANONICAL_MAPPER.createObjectNode();
        node.put("model_type", modelType.name());
        node.put("window_model_version", windowModelVersion);
        node.put("pre_window_ratio", preWindowRatio);
        node.put("min_pre_window_hours", minPreWindowHours);
        if (maxPreWindowHours != null) {
            node.put("max_pre_window_hours", maxPreWindowHours);
        } else {
            node.putNull("max_pre_window_hours");
        }
        if (maxPostWindowHours != null) {
            node.put("max_post_window_hours", maxPostWindowHours);
        } else {
            node.putNull("max_post_window_hours");
        }
        if (maxSearchRadiusKm != null) {
            node.put("max_search_radius_km", maxSearchRadiusKm);
        } else {
            node.putNull("max_search_radius_km");
        }
        if (customRadiusA != null) node.put("custom_radius_a", customRadiusA);
        if (customRadiusB != null) node.put("custom_radius_b", customRadiusB);
        if (customTimeA != null) node.put("custom_time_a", customTimeA);
        if (customTimeB != null) node.put("custom_time_b", customTimeB);
        return node;
    }

    public String canonicalJson() {
        try {
            return CANONICAL_MAPPER.writeValueAsString(toJsonNode());
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize canonical WindowModelConfig JSON", e);
        }
    }

    public String sha256() {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(canonicalJson().getBytes(StandardCharsets.UTF_8));
            StringBuilder hexString = new StringBuilder();
            for (byte b : hash) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) hexString.append('0');
                hexString.append(hex);
            }
            return hexString.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }
}
