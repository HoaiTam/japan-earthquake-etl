package ie212.earthquake.spark.ml;

import java.io.Serializable;
import java.util.Objects;

/**
 * Resolver for magnitude-dependent seismological spatio-temporal windows (MLD-03).
 * Computes search radius, pre/post durations, and conservative spatial bounding box.
 */
public final class WindowModelResolver implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final double KM_PER_DEG_LAT = 110.57;
    public static final double KM_PER_DEG_LON_EQUATOR = 111.32;

    private final WindowModelConfig config;

    public WindowModelResolver(WindowModelConfig config) {
        this.config = Objects.requireNonNull(config, "config");
    }

    public WindowModelConfig config() {
        return config;
    }

    /**
     * Resolves window dimensions for a mainshock event.
     *
     * @param magnitude mainshock magnitude
     * @param latitudeDeg mainshock latitude in degrees
     * @return resolved WindowDimensions
     */
    public WindowDimensions resolve(double magnitude, double latitudeDeg) {
        if (!Double.isFinite(magnitude)) {
            throw new IllegalArgumentException("Magnitude must be finite: " + magnitude);
        }
        if (!Double.isFinite(latitudeDeg) || Math.abs(latitudeDeg) > 90.0) {
            throw new IllegalArgumentException("Latitude must be in [-90, 90]: " + latitudeDeg);
        }

        double radiusKm;
        double postHours;

        switch (config.modelType()) {
            case UHRHAMMER -> {
                // Uhrhammer (1986): d = exp(-1.024 + 0.804 * M) km, t = exp(-2.870 + 1.235 * M) days
                radiusKm = Math.exp(-1.024 + 0.804 * magnitude);
                double postDays = Math.exp(-2.870 + 1.235 * magnitude);
                postHours = postDays * 24.0;
            }
            case GARDNER_KNOPOFF -> {
                // Gardner & Knopoff (1974): L = 10^(0.1238 * M + 0.983) km, T = 10^(0.5409 * M - 0.547) days
                radiusKm = Math.pow(10.0, 0.1238 * magnitude + 0.983);
                double postDays = Math.pow(10.0, 0.5409 * magnitude - 0.547);
                postHours = postDays * 24.0;
            }
            case EXPANDED -> {
                double a = config.customRadiusA() != null ? config.customRadiusA() : 0.359;
                double b = config.customRadiusB() != null ? config.customRadiusB() : 0.804;
                double c = config.customTimeA() != null ? config.customTimeA() : 0.0567;
                double d = config.customTimeB() != null ? config.customTimeB() : 1.235;

                radiusKm = a * Math.exp(b * magnitude);
                double postDays = c * Math.exp(d * magnitude);
                postHours = postDays * 24.0;
            }
            default -> throw new UnsupportedOperationException("Unknown window model: " + config.modelType());
        }

        // Apply caps if configured
        if (config.maxSearchRadiusKm() != null) {
            radiusKm = Math.min(radiusKm, config.maxSearchRadiusKm());
        }
        if (config.maxPostWindowHours() != null) {
            postHours = Math.min(postHours, config.maxPostWindowHours());
        }

        // Compute pre-window duration
        double preHours = Math.max(config.minPreWindowHours(), postHours * config.preWindowRatio());
        if (config.maxPreWindowHours() != null) {
            preHours = Math.min(preHours, config.maxPreWindowHours());
        }

        // Compute conservative outer spatial bounding box deltas
        double deltaLatDeg = radiusKm / KM_PER_DEG_LAT;
        double cosLat = Math.cos(Math.toRadians(latitudeDeg));
        double deltaLonDeg;
        if (Math.abs(latitudeDeg) >= 89.0 || Math.abs(cosLat) < 1e-4) {
            deltaLonDeg = 180.0;
        } else {
            deltaLonDeg = Math.min(180.0, radiusKm / (KM_PER_DEG_LON_EQUATOR * cosLat));
        }

        return new WindowDimensions(preHours, postHours, radiusKm, deltaLatDeg, deltaLonDeg);
    }
}
