package ie212.earthquake.spark.silver;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * Parses raw USGS GeoJSON FeatureCollections into standardized Silver observations
 * and reject records in strict compliance with CON-03 (v1.0).
 */
public final class UsgsGeoJsonParser {
    public static final String PARSER_NAME = "usgs-geojson";
    public static final String PARSER_VERSION = "slv-02-v1";
    public static final String SCHEMA_VERSION = "1.0";
    public static final String SOURCE_SYSTEM = "USGS";

    public static final ZoneId ASIA_TOKYO = ZoneId.of("Asia/Tokyo");

    // Technical study envelope from CON-01
    public static final double ROI_MIN_LAT = 20.0;
    public static final double ROI_MAX_LAT = 50.0;
    public static final double ROI_MIN_LON = 120.0;
    public static final double ROI_MAX_LON = 155.0;

    private static final ObjectMapper JSON = new ObjectMapper();

    public UsgsGeoJsonParser() {
    }

    /**
     * Parses a staged Bronze input resolved by SLV-01.
     */
    public UsgsParseResult parse(ResolvedBronzeInput input) throws IOException {
        Objects.requireNonNull(input, "input");
        byte[] payload = Files.readAllBytes(input.stagedObject());
        return parse(payload, UsgsParseContext.of(input));
    }

    /**
     * Parses a local GeoJSON file with the provided context.
     */
    public UsgsParseResult parse(Path filePath, UsgsParseContext context) throws IOException {
        Objects.requireNonNull(filePath, "filePath");
        byte[] payload = Files.readAllBytes(filePath);
        return parse(payload, context);
    }

    /**
     * Parses a GeoJSON byte payload with the provided context.
     */
    public UsgsParseResult parse(byte[] payload, UsgsParseContext context) throws IOException {
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(context, "context");

        JsonNode root = JSON.readTree(payload);
        if (root == null || !root.isObject()) {
            throw new IOException("USGS GeoJSON root must be an object");
        }
        if (!"FeatureCollection".equals(root.path("type").asText(null))) {
            throw new IOException("USGS GeoJSON root type must be FeatureCollection");
        }

        JsonNode features = root.get("features");
        if (features == null || !features.isArray()) {
            throw new IOException("USGS GeoJSON missing features array");
        }

        List<SilverObservation> observations = new ArrayList<>();
        List<SilverRejectRecord> rejects = new ArrayList<>();

        for (int i = 0; i < features.size(); i++) {
            JsonNode feature = features.get(i);
            String locator = "features[" + i + "]";
            byte[] featureBytes;
            try {
                featureBytes = JSON.writeValueAsBytes(feature);
            } catch (JsonProcessingException e) {
                featureBytes = feature.toString().getBytes(StandardCharsets.UTF_8);
            }
            String rawRecordHash = sha256(featureBytes);

            JsonNode idNode = feature.get("id");
            String candidateKey = (idNode != null && !idNode.isNull() && !idNode.asText().isBlank())
                    ? idNode.asText().trim()
                    : null;

            Set<String> reasonCodes = new LinkedHashSet<>();

            // 1. Source record key validation
            if (candidateKey == null) {
                reasonCodes.add("MISSING_SOURCE_KEY");
            }

            // 2. Event time validation
            JsonNode properties = feature.get("properties");
            Instant eventTimeUtc = null;
            if (properties == null || !properties.has("time") || properties.get("time").isNull()) {
                reasonCodes.add("INVALID_EVENT_TIME");
            } else {
                JsonNode timeNode = properties.get("time");
                if (timeNode.isNumber()) {
                    long timeMillis = timeNode.asLong();
                    try {
                        eventTimeUtc = Instant.ofEpochMilli(timeMillis);
                    } catch (Exception exception) {
                        reasonCodes.add("INVALID_EVENT_TIME");
                    }
                } else if (timeNode.isTextual()) {
                    try {
                        long timeMillis = Long.parseLong(timeNode.asText().trim());
                        eventTimeUtc = Instant.ofEpochMilli(timeMillis);
                    } catch (Exception exception) {
                        reasonCodes.add("INVALID_EVENT_TIME");
                    }
                } else {
                    reasonCodes.add("INVALID_EVENT_TIME");
                }
            }

            // 3. Geometry and coordinates validation
            JsonNode geometry = feature.get("geometry");
            Double lon = null;
            Double lat = null;
            Double depth = null;

            if (geometry == null || geometry.isNull() || !geometry.isObject()) {
                reasonCodes.add("CONTRACT_MISMATCH");
            } else {
                String geomType = geometry.path("type").asText(null);
                if (!"Point".equalsIgnoreCase(geomType)) {
                    reasonCodes.add("UNSUPPORTED_RECORD_TYPE");
                }

                JsonNode coords = geometry.get("coordinates");
                if (coords == null || !coords.isArray() || coords.size() < 2) {
                    reasonCodes.add("CONTRACT_MISMATCH");
                } else {
                    // Coordinate 0: Longitude
                    JsonNode lonNode = coords.get(0);
                    if (lonNode == null || lonNode.isNull()) {
                        reasonCodes.add("INVALID_LONGITUDE");
                    } else if (!isNumeric(lonNode)) {
                        reasonCodes.add("INVALID_NUMBER");
                    } else {
                        double lonVal = parseNumber(lonNode);
                        if (Double.isNaN(lonVal) || Double.isInfinite(lonVal)) {
                            reasonCodes.add("NON_FINITE_NUMBER");
                        } else if (lonVal < -180.0 || lonVal > 180.0) {
                            reasonCodes.add("INVALID_LONGITUDE");
                        } else {
                            lon = lonVal;
                        }
                    }

                    // Coordinate 1: Latitude
                    JsonNode latNode = coords.get(1);
                    if (latNode == null || latNode.isNull()) {
                        reasonCodes.add("INVALID_LATITUDE");
                    } else if (!isNumeric(latNode)) {
                        reasonCodes.add("INVALID_NUMBER");
                    } else {
                        double latVal = parseNumber(latNode);
                        if (Double.isNaN(latVal) || Double.isInfinite(latVal)) {
                            reasonCodes.add("NON_FINITE_NUMBER");
                        } else if (latVal < -90.0 || latVal > 90.0) {
                            reasonCodes.add("INVALID_LATITUDE");
                        } else {
                            lat = latVal;
                        }
                    }

                    // Coordinate 2: Depth (optional, nullable)
                    if (coords.size() >= 3) {
                        JsonNode depthNode = coords.get(2);
                        if (depthNode != null && !depthNode.isNull()) {
                            if (!isNumeric(depthNode)) {
                                reasonCodes.add("INVALID_NUMBER");
                            } else {
                                double depthVal = parseNumber(depthNode);
                                if (Double.isNaN(depthVal) || Double.isInfinite(depthVal)) {
                                    reasonCodes.add("NON_FINITE_NUMBER");
                                } else {
                                    depth = depthVal;
                                }
                            }
                        }
                    }
                }
            }

            // 4. Magnitude validation (optional, nullable)
            Double mag = null;
            if (properties != null && properties.has("mag") && !properties.get("mag").isNull()) {
                JsonNode magNode = properties.get("mag");
                if (!isNumeric(magNode)) {
                    reasonCodes.add("INVALID_NUMBER");
                } else {
                    double magVal = parseNumber(magNode);
                    if (Double.isNaN(magVal) || Double.isInfinite(magVal)) {
                        reasonCodes.add("NON_FINITE_NUMBER");
                    } else {
                        mag = magVal;
                    }
                }
            }

            // 5. Updated timestamp validation
            Instant sourceUpdatedAtUtc = null;
            Long updatedMillis = null;
            if (properties != null && properties.has("updated") && !properties.get("updated").isNull()) {
                JsonNode updatedNode = properties.get("updated");
                if (updatedNode.isNumber()) {
                    updatedMillis = updatedNode.asLong();
                    sourceUpdatedAtUtc = Instant.ofEpochMilli(updatedMillis);
                } else if (updatedNode.isTextual()) {
                    try {
                        updatedMillis = Long.parseLong(updatedNode.asText().trim());
                        sourceUpdatedAtUtc = Instant.ofEpochMilli(updatedMillis);
                    } catch (NumberFormatException ignored) {
                        // Keep null
                    }
                }
            }

            // Decide reject vs valid
            if (!reasonCodes.isEmpty()) {
                rejects.add(new SilverRejectRecord(
                        SCHEMA_VERSION,
                        SOURCE_SYSTEM,
                        candidateKey,
                        context.bronzeManifestId(),
                        context.rawObjectUri(),
                        context.rawSha256(),
                        locator,
                        rawRecordHash,
                        "VALIDATE",
                        new ArrayList<>(reasonCodes),
                        context.ingestRunId(),
                        PARSER_VERSION,
                        context.processedAtUtc()));
            } else {
                // Construct revision key and source observation ID
                String sourceRevisionKey = (updatedMillis != null ? updatedMillis : "null") + ":" + rawRecordHash;
                String sourceObservationId = "obs_" + sha256(SOURCE_SYSTEM + "|" + candidateKey + "|" + sourceRevisionKey);

                // Derived timestamps and dates
                LocalDateTime eventTimeJst = LocalDateTime.ofInstant(eventTimeUtc, ASIA_TOKYO);
                LocalDate eventDateUtc = eventTimeUtc.atZone(ZoneOffset.UTC).toLocalDate();
                LocalDate eventDateJst = eventTimeUtc.atZone(ASIA_TOKYO).toLocalDate();
                int eventYearUtc = eventDateUtc.getYear();
                int eventMonthUtc = eventDateUtc.getMonthValue();

                // Spatial study area envelope check
                boolean isInStudyArea = (lat >= ROI_MIN_LAT && lat <= ROI_MAX_LAT
                        && lon >= ROI_MIN_LON && lon <= ROI_MAX_LON);

                // Event type mapping
                String rawType = properties != null ? nullableString(properties.get("type")) : null;
                String eventTypeCode = mapEventType(rawType);

                // Quality flags & status
                List<String> qualityFlags = new ArrayList<>();
                if (depth != null && depth < 0.0) {
                    qualityFlags.add("NEGATIVE_DEPTH");
                }
                String qualityStatus = qualityFlags.isEmpty() ? "VALID" : "WARNING";

                // Additional optional attributes
                String placeName = properties != null ? nullableString(properties.get("place")) : null;
                String magType = properties != null ? nullableString(properties.get("magType")) : null;
                String sourceStatus = properties != null ? nullableString(properties.get("status")) : null;

                String sourceUrl = null;
                if (properties != null) {
                    sourceUrl = nullableString(properties.get("url"));
                    if (sourceUrl == null) {
                        sourceUrl = nullableString(properties.get("detail"));
                    }
                }

                Boolean tsunamiFlag = null;
                if (properties != null && properties.has("tsunami") && !properties.get("tsunami").isNull()) {
                    int tVal = properties.get("tsunami").asInt();
                    if (tVal == 1) {
                        tsunamiFlag = Boolean.TRUE;
                    } else if (tVal == 0) {
                        tsunamiFlag = Boolean.FALSE;
                    }
                }

                String alertLevel = null;
                if (properties != null && properties.has("alert") && !properties.get("alert").isNull()) {
                    String a = properties.get("alert").asText().trim();
                    if (!a.isEmpty()) {
                        alertLevel = a.toLowerCase(Locale.ROOT);
                    }
                }

                Integer significance = (properties != null && properties.has("sig") && !properties.get("sig").isNull())
                        ? properties.get("sig").asInt()
                        : null;

                observations.add(new SilverObservation(
                        SCHEMA_VERSION,
                        sourceObservationId,
                        SOURCE_SYSTEM,
                        candidateKey,
                        sourceRevisionKey,
                        sourceUpdatedAtUtc,
                        null,
                        null,
                        false,
                        eventTimeUtc,
                        eventTimeJst,
                        eventDateUtc,
                        eventDateJst,
                        eventYearUtc,
                        eventMonthUtc,
                        lat,
                        lon,
                        depth,
                        mag,
                        magType,
                        eventTypeCode,
                        placeName,
                        tsunamiFlag,
                        alertLevel,
                        significance,
                        null,
                        null,
                        null,
                        sourceStatus,
                        sourceUrl,
                        isInStudyArea,
                        qualityStatus,
                        qualityFlags,
                        context.bronzeManifestId(),
                        context.rawObjectUri(),
                        context.rawSha256(),
                        locator,
                        rawRecordHash,
                        context.ingestRunId(),
                        PARSER_NAME,
                        PARSER_VERSION,
                        context.processedAtUtc()));
            }
        }

        return new UsgsParseResult(observations, rejects);
    }

    public static String mapEventType(String rawType) {
        if (rawType == null || rawType.isBlank()) {
            return "UNKNOWN";
        }
        String normalized = rawType.trim().toLowerCase(Locale.ROOT);
        return switch (normalized) {
            case "earthquake" -> "EARTHQUAKE";
            case "quarry blast", "quarry", "explosion", "mining explosion", "mining hazard",
                 "rock burst", "rockslide", "sonic boom", "nuclear explosion", "chemical explosion",
                 "accidental explosion", "collapse", "induced or triggered event" -> "ARTIFICIAL";
            case "volcanic eruption", "volcanic explosion", "volcano" -> "ERUPTION";
            default -> "OTHER";
        };
    }

    private static boolean isNumeric(JsonNode node) {
        if (node.isNumber()) {
            return true;
        }
        if (node.isTextual()) {
            try {
                Double.parseDouble(node.asText().trim());
                return true;
            } catch (NumberFormatException ignored) {
                return false;
            }
        }
        return false;
    }

    private static double parseNumber(JsonNode node) {
        if (node.isNumber()) {
            return node.asDouble();
        }
        return Double.parseDouble(node.asText().trim());
    }

    private static String nullableString(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        String text = node.asText().trim();
        return text.isEmpty() ? null : text;
    }

    public static String sha256(byte[] data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(data));
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("JVM does not support SHA-256", exception);
        }
    }

    public static String sha256(String text) {
        return sha256(text.getBytes(StandardCharsets.UTF_8));
    }
}
