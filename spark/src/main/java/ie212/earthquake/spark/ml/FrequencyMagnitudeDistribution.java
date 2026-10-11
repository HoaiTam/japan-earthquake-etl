package ie212.earthquake.spark.ml;

import java.util.*;

/**
 * Frequency-Magnitude Distribution (FMD) for earthquake catalog completeness analysis.
 * Bins magnitudes into discrete intervals (default width 0.1) and calculates both
 * non-cumulative and cumulative event counts for Gutenberg-Richter analysis.
 */
public final class FrequencyMagnitudeDistribution {
    private static final double LOG10_E = Math.log10(Math.E); // ~0.4342944819
    private final double binWidth;
    private final SortedMap<Double, Long> nonCumulativeCounts;
    private final NavigableMap<Double, Long> cumulativeCounts;
    private final long totalEvents;
    private final double minMagnitude;
    private final double maxMagnitude;
    private final double modeBin;
    private final long maxBinCount;

    public FrequencyMagnitudeDistribution(List<Double> magnitudes, double binWidth) {
        if (binWidth <= 0 || !Double.isFinite(binWidth)) {
            throw new IllegalArgumentException("binWidth must be positive and finite: " + binWidth);
        }
        this.binWidth = Math.round(binWidth * 1000.0) / 1000.0;
        SortedMap<Double, Long> nonCum = new TreeMap<>();
        long count = 0;
        double minM = Double.POSITIVE_INFINITY;
        double maxM = Double.NEGATIVE_INFINITY;

        if (magnitudes != null) {
            for (Double mag : magnitudes) {
                if (mag == null || !Double.isFinite(mag)) {
                    continue;
                }
                double bin = roundBin(mag, this.binWidth);
                nonCum.put(bin, nonCum.getOrDefault(bin, 0L) + 1L);
                count++;
                if (mag < minM) minM = mag;
                if (mag > maxM) maxM = mag;
            }
        }

        this.nonCumulativeCounts = Collections.unmodifiableSortedMap(nonCum);
        this.totalEvents = count;
        this.minMagnitude = count > 0 ? minM : Double.NaN;
        this.maxMagnitude = count > 0 ? maxM : Double.NaN;

        // Cumulative counts: N(>= M) = sum of counts for m >= M
        NavigableMap<Double, Long> cum = new TreeMap<>();
        long runningSum = 0;
        List<Double> sortedBins = new ArrayList<>(nonCum.keySet());
        for (int i = sortedBins.size() - 1; i >= 0; i--) {
            double bin = sortedBins.get(i);
            runningSum += nonCum.get(bin);
            cum.put(bin, runningSum);
        }
        this.cumulativeCounts = Collections.unmodifiableNavigableMap(cum);

        // Find mode bin (maximum non-cumulative frequency; tie-breaker: lowest magnitude)
        double bestBin = Double.NaN;
        long bestCount = 0;
        for (Map.Entry<Double, Long> entry : nonCum.entrySet()) {
            if (entry.getValue() > bestCount) {
                bestCount = entry.getValue();
                bestBin = entry.getKey();
            }
        }
        this.modeBin = bestBin;
        this.maxBinCount = bestCount;
    }

    public static double roundBin(double magnitude, double binWidth) {
        double factor = 1.0 / binWidth;
        double rounded = Math.round(magnitude * factor) / factor;
        return Math.round(rounded * 1000.0) / 1000.0;
    }

    public double binWidth() { return binWidth; }
    public long totalEvents() { return totalEvents; }
    public double minMagnitude() { return minMagnitude; }
    public double maxMagnitude() { return maxMagnitude; }
    public double modeBin() { return modeBin; }
    public long maxBinCount() { return maxBinCount; }
    public SortedMap<Double, Long> nonCumulativeCounts() { return nonCumulativeCounts; }
    public NavigableMap<Double, Long> cumulativeCounts() { return cumulativeCounts; }

    public long countAt(double bin) {
        return nonCumulativeCounts.getOrDefault(roundBin(bin, binWidth), 0L);
    }

    public long cumulativeCountAt(double magnitude) {
        double bin = roundBin(magnitude, binWidth);
        Map.Entry<Double, Long> entry = cumulativeCounts.ceilingEntry(bin);
        return entry != null ? entry.getValue() : 0L;
    }

    public double meanMagnitudeAbove(double mc) {
        double roundedMc = roundBin(mc, binWidth);
        long n = 0;
        double sum = 0.0;
        for (Map.Entry<Double, Long> entry : nonCumulativeCounts.tailMap(roundedMc).entrySet()) {
            double bin = entry.getKey();
            long c = entry.getValue();
            sum += bin * c;
            n += c;
        }
        return n > 0 ? (sum / n) : Double.NaN;
    }

    public long eventCountAbove(double mc) {
        double roundedMc = roundBin(mc, binWidth);
        return cumulativeCounts.getOrDefault(roundedMc, 0L);
    }

    /**
     * Estimates Gutenberg-Richter b-value above Mc using the Aki-Utsu maximum likelihood estimator:
     * b = log10(e) / (mean(M) - (Mc - binWidth / 2)).
     * Returns null if sample size above Mc < 10 or mean(M) <= (Mc - binWidth / 2).
     */
    public Double estimateBValue(double mc) {
        long n = eventCountAbove(mc);
        if (n < 10) return null;
        double mean = meanMagnitudeAbove(mc);
        double denom = mean - (mc - binWidth / 2.0);
        if (denom <= 0.0 || !Double.isFinite(denom)) return null;
        double b = LOG10_E / denom;
        return Double.isFinite(b) && b > 0 ? Math.round(b * 1000.0) / 1000.0 : null;
    }

    /**
     * Standard error of Aki-Utsu b-value: sigma_b = b / sqrt(N).
     */
    public Double estimateBValueStdErr(double mc) {
        Double b = estimateBValue(mc);
        if (b == null) return null;
        long n = eventCountAbove(mc);
        if (n < 10) return null;
        double err = b / Math.sqrt(n);
        return Math.round(err * 1000.0) / 1000.0;
    }
}
