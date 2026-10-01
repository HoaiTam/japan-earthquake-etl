package vn.edu.uit.ie212.earthquake.spark.usgs;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Objects;

/** Validates only the USGS response envelope; business fields stay untouched. */
public final class UsgsGeoJsonValidator {
    private final ObjectMapper objectMapper;

    public UsgsGeoJsonValidator() {
        this(new ObjectMapper());
    }

    UsgsGeoJsonValidator(ObjectMapper objectMapper) {
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
    }

    public UsgsGeoJsonValidationResult validate(byte[] payload) {
        if (payload == null || payload.length == 0) {
            return UsgsGeoJsonValidationResult.invalid("EMPTY_BODY");
        }

        try (JsonParser parser = objectMapper.getFactory().createParser(payload)) {
            JsonNode root = objectMapper.readTree(parser);
            if (root == null || !root.isObject()) {
                return UsgsGeoJsonValidationResult.invalid("ROOT_NOT_OBJECT");
            }
            if (!"FeatureCollection".equals(root.path("type").asText(null))) {
                return UsgsGeoJsonValidationResult.invalid("ROOT_TYPE_NOT_FEATURE_COLLECTION");
            }
            JsonNode features = root.get("features");
            if (features == null || !features.isArray()) {
                return UsgsGeoJsonValidationResult.invalid("FEATURES_NOT_ARRAY");
            }
            int featureCount = 0;
            for (JsonNode feature : features) {
                if (!feature.isObject()
                        || !"Feature".equals(feature.path("type").asText(null))) {
                    return UsgsGeoJsonValidationResult.invalid("FEATURE_NOT_GEOJSON_FEATURE");
                }
                featureCount++;
            }
            if (parser.nextToken() != null) {
                return UsgsGeoJsonValidationResult.invalid("TRAILING_JSON_CONTENT");
            }
            return UsgsGeoJsonValidationResult.valid(featureCount);
        } catch (IOException exception) {
            return UsgsGeoJsonValidationResult.invalid("INVALID_JSON");
        }
    }
}
