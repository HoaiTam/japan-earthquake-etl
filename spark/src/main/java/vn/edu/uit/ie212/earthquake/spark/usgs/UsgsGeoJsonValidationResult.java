package vn.edu.uit.ie212.earthquake.spark.usgs;

/** Structural validation result for one raw USGS GeoJSON response. */
public record UsgsGeoJsonValidationResult(
        boolean valid,
        int featureCount,
        String reason) {

    public UsgsGeoJsonValidationResult {
        if (featureCount < 0) {
            throw new IllegalArgumentException("featureCount must not be negative");
        }
        if (valid && reason != null) {
            throw new IllegalArgumentException("valid response must not have a failure reason");
        }
        if (!valid && (reason == null || reason.isBlank())) {
            throw new IllegalArgumentException("invalid response must have a failure reason");
        }
    }

    public static UsgsGeoJsonValidationResult valid(int featureCount) {
        return new UsgsGeoJsonValidationResult(true, featureCount, null);
    }

    public static UsgsGeoJsonValidationResult invalid(String reason) {
        return new UsgsGeoJsonValidationResult(false, 0, reason);
    }
}
