package ie212.earthquake.spark.ml;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.*;
import org.junit.jupiter.api.Test;

class MagnitudeCompletenessEstimatorTest {

    @Test
    void frequencyMagnitudeDistributionCalculatesHistogramAndAkiUtsuBValue() {
        // Create synthetic catalog with Gutenberg-Richter distribution:
        // Mc = 2.0, b = 1.0. Log10 N(>=M) = 4 - 1.0 * M
        // M=2.0: 100, M=2.1: 79, M=2.2: 63, M=2.3: 50, M=2.4: 40, M=2.5: 32...
        // Below Mc: incomplete detection: M=1.5: 5, M=1.6: 15, M=1.7: 35, M=1.8: 60, M=1.9: 85
        List<Double> mags = new ArrayList<>();
        // Incomplete part
        addRepeated(mags, 1.5, 5);
        addRepeated(mags, 1.6, 15);
        addRepeated(mags, 1.7, 35);
        addRepeated(mags, 1.8, 60);
        addRepeated(mags, 1.9, 85);
        // Peak at Mc = 2.0
        addRepeated(mags, 2.0, 120);
        // Power-law tail above Mc
        addRepeated(mags, 2.1, 95);
        addRepeated(mags, 2.2, 75);
        addRepeated(mags, 2.3, 60);
        addRepeated(mags, 2.4, 48);
        addRepeated(mags, 2.5, 38);
        addRepeated(mags, 2.6, 30);
        addRepeated(mags, 2.7, 24);
        addRepeated(mags, 2.8, 19);
        addRepeated(mags, 2.9, 15);
        addRepeated(mags, 3.0, 12);
        addRepeated(mags, 3.5, 4);
        addRepeated(mags, 4.0, 1);

        FrequencyMagnitudeDistribution fmd = new FrequencyMagnitudeDistribution(mags, 0.1);
        assertEquals(mags.size(), fmd.totalEvents());
        assertEquals(2.0, fmd.modeBin(), 1e-4);
        assertEquals(120, fmd.maxBinCount());
        assertEquals(120, fmd.countAt(2.0));
        assertTrue(fmd.cumulativeCountAt(2.0) > 400);

        Double bValue = fmd.estimateBValue(2.0);
        assertNotNull(bValue);
        // b-value should be close to 1.0 (typical Gutenberg-Richter)
        assertTrue(bValue >= 0.8 && bValue <= 1.3, "b-value should be ~1.0, was: " + bValue);
        Double bErr = fmd.estimateBValueStdErr(2.0);
        assertNotNull(bErr);
        assertTrue(bErr > 0 && bErr < 0.2);
    }

    @Test
    void maxcEstimatorDeterminesCentralMcAndSensitivityBounds() {
        List<Double> mags = new ArrayList<>();
        addRepeated(mags, 1.8, 20);
        addRepeated(mags, 1.9, 45);
        addRepeated(mags, 2.2, 110); // Peak / Mode
        addRepeated(mags, 2.3, 80);
        addRepeated(mags, 2.4, 60);
        addRepeated(mags, 2.5, 40);

        MagnitudeCompletenessEstimator estimator = new MagnitudeCompletenessEstimator(0.1, 0.2, 50, "1.0");
        CompletenessResult result = estimator.estimate(mags);

        assertTrue(result.isReliable());
        assertNull(result.unreliableReason());
        assertEquals("MAXIMUM_CURVATURE", result.method());
        assertEquals("1.0", result.methodVersion());
        assertEquals(2.2, result.centralMc(), 1e-4);
        assertEquals(2.0, result.sensitivityLower(), 1e-4); // 2.2 - 0.2
        assertEquals(2.4, result.sensitivityUpper(), 1e-4); // 2.2 + 0.2
        assertEquals(0.2, result.sensitivityStep(), 1e-4);
        assertEquals(mags.size(), result.sampleCount());
        assertTrue(result.eventsAboveMc() > 0);
        assertTrue(result.completenessFraction() > 0.5);
    }

    @Test
    void tiedModeBinPicksLowestMagnitudeConservatively() {
        List<Double> mags = new ArrayList<>();
        addRepeated(mags, 2.0, 50); // Tie at count 50
        addRepeated(mags, 2.1, 50); // Tie at count 50
        addRepeated(mags, 2.5, 10);

        MagnitudeCompletenessEstimator estimator = new MagnitudeCompletenessEstimator(0.1, 0.2, 50, "1.0");
        CompletenessResult result = estimator.estimate(mags);

        assertTrue(result.isReliable());
        // Lowest magnitude bin with max count
        assertEquals(2.0, result.centralMc(), 1e-4);
    }

    @Test
    void insufficientDataFlagsUnreliableWithoutFabricatingMc() {
        List<Double> smallSample = List.of(2.0, 2.1, 2.2, 2.5); // Only 4 events (< 50)

        MagnitudeCompletenessEstimator estimator = new MagnitudeCompletenessEstimator(0.1, 0.2, 50, "1.0");
        CompletenessResult result = estimator.estimate(smallSample);

        assertFalse(result.isReliable());
        assertNotNull(result.unreliableReason());
        assertTrue(result.unreliableReason().contains("INSUFFICIENT_DATA"));
        assertTrue(Double.isNaN(result.centralMc()));
        assertTrue(Double.isNaN(result.sensitivityLower()));
        assertTrue(Double.isNaN(result.sensitivityUpper()));
    }

    @Test
    void handlesNullAndNonFiniteMagnitudesSafely() {
        List<Double> mags = new ArrayList<>();
        mags.add(null);
        mags.add(Double.NaN);
        mags.add(Double.POSITIVE_INFINITY);
        mags.add(Double.NEGATIVE_INFINITY);
        addRepeated(mags, 2.0, 60);
        addRepeated(mags, 2.5, 30);

        MagnitudeCompletenessEstimator estimator = new MagnitudeCompletenessEstimator(0.1, 0.2, 50, "1.0");
        CompletenessResult result = estimator.estimate(mags);

        assertTrue(result.isReliable());
        assertEquals(90, result.sampleCount());
        assertEquals(2.0, result.centralMc(), 1e-4);
    }

    @Test
    void canonicalMcConfigBuildsDeterministicSha256() {
        List<Double> mags = new ArrayList<>();
        addRepeated(mags, 2.0, 80);
        addRepeated(mags, 2.5, 40);

        MagnitudeCompletenessEstimator estimator = new MagnitudeCompletenessEstimator();
        CompletenessResult overall = estimator.estimate(mags, "ALL");

        CompletenessResult shallow = estimator.estimate(mags.subList(0, 60), "SHALLOW");
        CompletenessResult inter = estimator.estimate(mags.subList(0, 30), "INTERMEDIATE");
        Map<String, CompletenessResult> depthGroups = new TreeMap<>();
        depthGroups.put("SHALLOW", shallow);
        depthGroups.put("INTERMEDIATE", inter);

        ObjectNode node1 = MagnitudeCompletenessEstimator.buildCanonicalMcConfigNode(overall, depthGroups);
        String json1 = MagnitudeCompletenessEstimator.canonicalJsonString(node1);
        String sha1 = MagnitudeCompletenessEstimator.sha256(json1);

        ObjectNode node2 = MagnitudeCompletenessEstimator.buildCanonicalMcConfigNode(overall, depthGroups);
        String json2 = MagnitudeCompletenessEstimator.canonicalJsonString(node2);
        String sha2 = MagnitudeCompletenessEstimator.sha256(json2);

        assertEquals(json1, json2);
        assertEquals(sha1, sha2);
        assertEquals(64, sha1.length());
        assertTrue(json1.contains("\"mc_method\":\"MAXIMUM_CURVATURE\""));
        assertTrue(json1.contains("\"central_mc\":2.0"));
        assertTrue(json1.contains("\"sensitivity_lower\":1.8"));
        assertTrue(json1.contains("\"sensitivity_upper\":2.2"));
    }

    private static void addRepeated(List<Double> list, double value, int times) {
        for (int i = 0; i < times; i++) {
            list.add(value);
        }
    }
}
