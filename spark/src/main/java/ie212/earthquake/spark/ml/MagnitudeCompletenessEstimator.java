package ie212.earthquake.spark.ml;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Versioned Magnitude of Completeness (Mc) Estimator.
 * Implements the Maximum Curvature (MAXC) method with Gutenberg-Richter b-value validation
 * and deterministic central, lower, and upper sensitivity thresholds.
 */
public final class MagnitudeCompletenessEstimator {
    public static final String METHOD_NAME = "MAXIMUM_CURVATURE";
    public static final String DEFAULT_METHOD_VERSION = "1.0";
    public static final double DEFAULT_BIN_WIDTH = 0.1;
    public static final double DEFAULT_SENSITIVITY_STEP = 0.2;
    public static final int DEFAULT_MIN_EVENTS = 50;

    private static final ObjectMapper JSON = new ObjectMapper();

    private final double binWidth;
    private final double sensitivityStep;
    private final int minEvents;
    private final String methodVersion;

    public MagnitudeCompletenessEstimator() {
        this(DEFAULT_BIN_WIDTH, DEFAULT_SENSITIVITY_STEP, DEFAULT_MIN_EVENTS, DEFAULT_METHOD_VERSION);
    }

    public MagnitudeCompletenessEstimator(double binWidth, double sensitivityStep, int minEvents, String methodVersion) {
        if (binWidth <= 0 || !Double.isFinite(binWidth)) {
            throw new IllegalArgumentException("binWidth must be positive and finite: " + binWidth);
        }
        if (sensitivityStep <= 0 || !Double.isFinite(sensitivityStep)) {
            throw new IllegalArgumentException("sensitivityStep must be positive and finite: " + sensitivityStep);
        }
        if (minEvents < 1) {
            throw new IllegalArgumentException("minEvents must be >= 1: " + minEvents);
        }
        this.binWidth = Math.round(binWidth * 1000.0) / 1000.0;
        this.sensitivityStep = Math.round(sensitivityStep * 1000.0) / 1000.0;
        this.minEvents = minEvents;
        this.methodVersion = Objects.requireNonNull(methodVersion, "methodVersion");
    }

    public CompletenessResult estimate(List<Double> magnitudes) {
        return estimate(magnitudes, "ALL");
    }

    public CompletenessResult estimate(List<Double> magnitudes, String groupName) {
        FrequencyMagnitudeDistribution fmd = new FrequencyMagnitudeDistribution(magnitudes, binWidth);
        long n = fmd.totalEvents();

        if (n < minEvents) {
            return new CompletenessResult(
                    METHOD_NAME,
                    methodVersion,
                    binWidth,
                    Double.NaN,
                    Double.NaN,
                    Double.NaN,
                    sensitivityStep,
                    n,
                    0,
                    0.0,
                    fmd.modeBin(),
                    fmd.maxBinCount(),
                    null,
                    null,
                    false,
                    "INSUFFICIENT_DATA: " + n + " < " + minEvents,
                    groupName
            );
        }

        double modeBin = fmd.modeBin();
        double centralMc = Math.round(modeBin * 100.0) / 100.0;
        double sensLower = Math.round((centralMc - sensitivityStep) * 100.0) / 100.0;
        double sensUpper = Math.round((centralMc + sensitivityStep) * 100.0) / 100.0;
        long eventsAbove = fmd.eventCountAbove(centralMc);
        double fraction = (double) eventsAbove / n;
        Double bValue = fmd.estimateBValue(centralMc);
        Double bStdErr = fmd.estimateBValueStdErr(centralMc);

        return new CompletenessResult(
                METHOD_NAME,
                methodVersion,
                binWidth,
                centralMc,
                sensLower,
                sensUpper,
                sensitivityStep,
                n,
                eventsAbove,
                fraction,
                modeBin,
                fmd.maxBinCount(),
                bValue,
                bStdErr,
                true,
                null,
                groupName
        );
    }

    public Map<String, CompletenessResult> estimateStratifiedByDepth(Dataset<Row> events) {
        Map<String, List<Double>> depthMagnitudes = new HashMap<>();
        depthMagnitudes.put("SHALLOW", new ArrayList<>());
        depthMagnitudes.put("INTERMEDIATE", new ArrayList<>());
        depthMagnitudes.put("DEEP", new ArrayList<>());

        List<Row> rows = events.select("depth_km", "magnitude").collectAsList();
        for (Row row : rows) {
            if (row.isNullAt(0) || row.isNullAt(1)) continue;
            double depth = row.getDouble(0);
            double mag = row.getDouble(1);
            if (!Double.isFinite(depth) || !Double.isFinite(mag)) continue;

            if (depth >= 0 && depth < 70) {
                depthMagnitudes.get("SHALLOW").add(mag);
            } else if (depth >= 70 && depth < 300) {
                depthMagnitudes.get("INTERMEDIATE").add(mag);
            } else if (depth >= 300) {
                depthMagnitudes.get("DEEP").add(mag);
            }
        }

        Map<String, CompletenessResult> results = new TreeMap<>();
        for (Map.Entry<String, List<Double>> entry : depthMagnitudes.entrySet()) {
            results.put(entry.getKey(), estimate(entry.getValue(), entry.getKey()));
        }
        return Collections.unmodifiableMap(results);
    }

    public static String canonicalJsonString(ObjectNode node) {
        TreeMap<String, JsonNode> sorted = new TreeMap<>();
        node.fields().forEachRemaining(e -> sorted.put(e.getKey(), canonicalNode(e.getValue())));
        ObjectNode canonical = JSON.createObjectNode();
        sorted.forEach(canonical::set);
        return canonical.toString();
    }

    private static JsonNode canonicalNode(JsonNode node) {
        if (node.isObject()) {
            TreeMap<String, JsonNode> sorted = new TreeMap<>();
            node.fields().forEachRemaining(e -> sorted.put(e.getKey(), canonicalNode(e.getValue())));
            ObjectNode res = JSON.createObjectNode();
            sorted.forEach(res::set);
            return res;
        } else if (node.isArray()) {
            var arr = JSON.createArrayNode();
            node.forEach(child -> arr.add(canonicalNode(child)));
            return arr;
        }
        return node.deepCopy();
    }

    public static String sha256(String text) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(text.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to calculate SHA-256", e);
        }
    }

    public static ObjectNode buildCanonicalMcConfigNode(
            CompletenessResult overall,
            Map<String, CompletenessResult> depthGroups
    ) {
        ObjectNode root = JSON.createObjectNode();
        root.put("mc_method", overall.method());
        root.put("mc_method_version", overall.methodVersion());
        root.put("bin_width", overall.binWidth());
        root.put("sensitivity_step", overall.sensitivityStep());
        if (overall.isReliable()) {
            root.put("central_mc", overall.centralMc());
            root.put("sensitivity_lower", overall.sensitivityLower());
            root.put("sensitivity_upper", overall.sensitivityUpper());
        } else {
            root.putNull("central_mc");
            root.putNull("sensitivity_lower");
            root.putNull("sensitivity_upper");
        }
        root.put("sample_event_count", overall.sampleCount());
        root.put("events_above_mc", overall.eventsAboveMc());
        root.put("completeness_fraction", Math.round(overall.completenessFraction() * 1000.0) / 1000.0);
        root.put("max_curvature_bin", overall.maxCurvatureBin());
        root.put("max_curvature_bin_count", overall.maxCurvatureBinCount());
        if (overall.estimatedBValue() != null) {
            root.put("estimated_b_value", overall.estimatedBValue());
        } else {
            root.putNull("estimated_b_value");
        }
        if (overall.estimatedBValueStdErr() != null) {
            root.put("estimated_b_value_stderr", overall.estimatedBValueStdErr());
        } else {
            root.putNull("estimated_b_value_stderr");
        }
        root.put("is_reliable", overall.isReliable());
        if (overall.unreliableReason() != null) {
            root.put("unreliable_reason", overall.unreliableReason());
        } else {
            root.putNull("unreliable_reason");
        }

        if (depthGroups != null && !depthGroups.isEmpty()) {
            ObjectNode groups = JSON.createObjectNode();
            for (Map.Entry<String, CompletenessResult> entry : depthGroups.entrySet()) {
                groups.set(entry.getKey(), entry.getValue().toJsonNode());
            }
            root.set("depth_stratified", groups);
        }

        return root;
    }
}
