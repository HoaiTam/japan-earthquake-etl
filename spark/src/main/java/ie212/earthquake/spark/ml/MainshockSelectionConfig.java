package ie212.earthquake.spark.ml;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Objects;

/**
 * Configuration for mainshock selection and candidate window boundary filters (MLD-03).
 */
public record MainshockSelectionConfig(
        String selectionRuleVersion,
        double minDepthKm,
        double maxDepthKm,
        double minMagnitudeExclusive,
        String datasetSplit,
        Instant periodStartUtc,
        Instant periodEndUtcExclusive,
        long maxCandidatesPerWindow,
        boolean failOnResourceExceeded
) {
    private static final ObjectMapper CANONICAL_MAPPER = new ObjectMapper()
            .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);

    public static final Instant REPRODUCTION_START = Instant.parse("2000-01-01T00:00:00Z");
    public static final Instant REPRODUCTION_END = Instant.parse("2018-10-01T00:00:00Z");
    public static final Instant EXTENSION_START = Instant.parse("2018-10-01T00:00:00Z");
    public static final Instant EXTENSION_END = Instant.parse("2024-01-01T00:00:00Z");

    public MainshockSelectionConfig {
        Objects.requireNonNull(selectionRuleVersion, "selectionRuleVersion");
        Objects.requireNonNull(datasetSplit, "datasetSplit");
        Objects.requireNonNull(periodStartUtc, "periodStartUtc");
        Objects.requireNonNull(periodEndUtcExclusive, "periodEndUtcExclusive");

        if (minDepthKm > maxDepthKm) {
            throw new IllegalArgumentException("minDepthKm must be <= maxDepthKm: " + minDepthKm + " > " + maxDepthKm);
        }
        if (!periodStartUtc.isBefore(periodEndUtcExclusive)) {
            throw new IllegalArgumentException("periodStartUtc must be before periodEndUtcExclusive: "
                    + periodStartUtc + " >= " + periodEndUtcExclusive);
        }
        if (maxCandidatesPerWindow <= 0) {
            throw new IllegalArgumentException("maxCandidatesPerWindow must be > 0: " + maxCandidatesPerWindow);
        }

        // Validate dataset split boundaries (DS_INVALID_PERIOD guard)
        if ("REPRODUCTION".equalsIgnoreCase(datasetSplit)) {
            if (periodStartUtc.isBefore(REPRODUCTION_START) || periodEndUtcExclusive.isAfter(REPRODUCTION_END)) {
                throw new IllegalArgumentException("DS_INVALID_PERIOD: REPRODUCTION period must fall within ["
                        + REPRODUCTION_START + ", " + REPRODUCTION_END + "), but got ["
                        + periodStartUtc + ", " + periodEndUtcExclusive + ")");
            }
        } else if ("EXTENSION".equalsIgnoreCase(datasetSplit)) {
            if (periodStartUtc.isBefore(EXTENSION_START) || periodEndUtcExclusive.isAfter(EXTENSION_END)) {
                throw new IllegalArgumentException("DS_INVALID_PERIOD: EXTENSION period must fall within ["
                        + EXTENSION_START + ", " + EXTENSION_END + "), but got ["
                        + periodStartUtc + ", " + periodEndUtcExclusive + ")");
            }
        } else {
            throw new IllegalArgumentException("Unknown datasetSplit: " + datasetSplit);
        }
    }

    public static MainshockSelectionConfig reproductionBaseline() {
        return new MainshockSelectionConfig(
                "sel_baseline_m55_d50_200_v1.0",
                50.0,
                200.0,
                5.5,
                "REPRODUCTION",
                REPRODUCTION_START,
                REPRODUCTION_END,
                10000L,
                false
        );
    }

    public static MainshockSelectionConfig extensionBaseline() {
        return new MainshockSelectionConfig(
                "sel_baseline_m55_d50_200_v1.0",
                50.0,
                200.0,
                5.5,
                "EXTENSION",
                EXTENSION_START,
                EXTENSION_END,
                10000L,
                false
        );
    }

    public ObjectNode toJsonNode() {
        ObjectNode node = CANONICAL_MAPPER.createObjectNode();
        node.put("selection_rule_version", selectionRuleVersion);
        node.put("min_depth_km", minDepthKm);
        node.put("max_depth_km", maxDepthKm);
        node.put("min_magnitude_exclusive", minMagnitudeExclusive);
        node.put("dataset_split", datasetSplit);
        node.put("period_start_utc", periodStartUtc.toString());
        node.put("period_end_utc_exclusive", periodEndUtcExclusive.toString());
        node.put("max_candidates_per_window", maxCandidatesPerWindow);
        node.put("fail_on_resource_exceeded", failOnResourceExceeded);
        return node;
    }

    public String canonicalJson() {
        try {
            return CANONICAL_MAPPER.writeValueAsString(toJsonNode());
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize canonical MainshockSelectionConfig JSON", e);
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
