package ie212.earthquake.spark.ml;

/**
 * Window model formula types for magnitude-dependent seismological windowing.
 */
public enum WindowModelType {
    /**
     * Gardner & Knopoff (1974) formulation:
     * L(M) = 10^(0.1238 * M + 0.983) km
     * T(M) = 10^(0.5409 * M - 0.547) days -> hours = T(M) * 24.0
     */
    GARDNER_KNOPOFF,

    /**
     * Uhrhammer (1986) formulation:
     * d(M) = exp(-1.024 + 0.804 * M) km
     * t(M) = exp(-2.870 + 1.235 * M) days -> hours = t(M) * 24.0
     */
    UHRHAMMER,

    /**
     * Expanded baseline model with configurable coefficients and min/max bounds.
     */
    EXPANDED
}
