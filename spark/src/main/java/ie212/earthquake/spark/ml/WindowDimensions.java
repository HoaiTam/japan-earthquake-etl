package ie212.earthquake.spark.ml;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Resolved spatial and temporal window dimensions for a mainshock event.
 */
public record WindowDimensions(
        double preWindowHours,
        double postWindowHours,
        double searchRadiusKm,
        double deltaLatDeg,
        double deltaLonDeg
) {
    private static final ObjectMapper JSON = new ObjectMapper();

    public ObjectNode toJsonNode() {
        ObjectNode node = JSON.createObjectNode();
        node.put("pre_window_hours", Math.round(preWindowHours * 100.0) / 100.0);
        node.put("post_window_hours", Math.round(postWindowHours * 100.0) / 100.0);
        node.put("search_radius_km", Math.round(searchRadiusKm * 100.0) / 100.0);
        node.put("delta_lat_deg", Math.round(deltaLatDeg * 10000.0) / 10000.0);
        node.put("delta_lon_deg", Math.round(deltaLonDeg * 10000.0) / 10000.0);
        return node;
    }
}
