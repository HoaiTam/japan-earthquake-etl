package ie212.earthquake.spark.silver;

import java.io.Serializable;
import java.util.Objects;

/**
 * Configuration and threshold parameters for Silver entity resolution and cross-source linking (SLV-07).
 */
public record SilverLinkConfig(
        double maxTimeDeltaSeconds,
        double maxDistanceKm,
        Double maxDepthDeltaKm,
        Double maxMagnitudeDelta,
        double ambiguityMarginScore,
        double minMatchScore,
        String matchModelVersion,
        String canonicalModelVersion) implements Serializable {

    public static final double DEFAULT_MAX_TIME_DELTA_SECONDS = 16.0;
    public static final double DEFAULT_MAX_DISTANCE_KM = 100.0;
    public static final double DEFAULT_MAX_DEPTH_DELTA_KM = 50.0;
    public static final double DEFAULT_MAX_MAGNITUDE_DELTA = 1.0;
    public static final double DEFAULT_AMBIGUITY_MARGIN_SCORE = 0.15;
    public static final double DEFAULT_MIN_MATCH_SCORE = 0.60;
    public static final String DEFAULT_MATCH_MODEL_VERSION = "link_v1.0";
    public static final String DEFAULT_CANONICAL_MODEL_VERSION = "canon_v1.0";

    public SilverLinkConfig {
        if (maxTimeDeltaSeconds < 0) {
            throw new IllegalArgumentException("maxTimeDeltaSeconds must be non-negative");
        }
        if (maxDistanceKm < 0) {
            throw new IllegalArgumentException("maxDistanceKm must be non-negative");
        }
        if (maxDepthDeltaKm != null && maxDepthDeltaKm < 0) {
            throw new IllegalArgumentException("maxDepthDeltaKm must be non-negative if specified");
        }
        if (maxMagnitudeDelta != null && maxMagnitudeDelta < 0) {
            throw new IllegalArgumentException("maxMagnitudeDelta must be non-negative if specified");
        }
        if (ambiguityMarginScore < 0 || ambiguityMarginScore > 1.0) {
            throw new IllegalArgumentException("ambiguityMarginScore must be between 0.0 and 1.0");
        }
        if (minMatchScore < 0 || minMatchScore > 1.0) {
            throw new IllegalArgumentException("minMatchScore must be between 0.0 and 1.0");
        }
        Objects.requireNonNull(matchModelVersion, "matchModelVersion");
        Objects.requireNonNull(canonicalModelVersion, "canonicalModelVersion");
    }

    /**
     * Default standard production configuration for Silver entity resolution.
     */
    public static SilverLinkConfig defaultConfig() {
        return new SilverLinkConfig(
                DEFAULT_MAX_TIME_DELTA_SECONDS,
                DEFAULT_MAX_DISTANCE_KM,
                DEFAULT_MAX_DEPTH_DELTA_KM,
                DEFAULT_MAX_MAGNITUDE_DELTA,
                DEFAULT_AMBIGUITY_MARGIN_SCORE,
                DEFAULT_MIN_MATCH_SCORE,
                DEFAULT_MATCH_MODEL_VERSION,
                DEFAULT_CANONICAL_MODEL_VERSION);
    }
}
