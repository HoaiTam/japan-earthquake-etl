package ie212.earthquake.spark.ml;

import static org.apache.spark.sql.functions.*;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

/**
 * Gold Input Audit and Completeness Engine (MLD-02).
 * Audits Gold events against CON-03 constraints (uniqueness, finite values, natural earthquake,
 * study area ROI, primary JMA comparability, UNIFIED era, and period interval).
 * Estimates versioned catalog completeness (Mc) using Maximum Curvature (MAXC)
 * without deleting records from Gold (exclusion with reason codes only).
 */
public final class GoldInputAuditEngine {

    public GoldAuditResult audit(Dataset<Row> goldEvents, GoldAuditConfig config, Instant auditedAtUtc) {
        Objects.requireNonNull(goldEvents, "goldEvents");
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(auditedAtUtc, "auditedAtUtc");

        SparkSession spark = goldEvents.sparkSession();
        if (!"UTC".equals(spark.conf().get("spark.sql.session.timeZone"))) {
            throw new IllegalArgumentException("Spark session timezone must be UTC");
        }

        // Validate required columns
        requireColumns(goldEvents, "canonical_event_id", "canonical_source_system", "event_time_utc",
                "latitude", "longitude", "depth_km", "magnitude", "event_type_code",
                "is_natural_earthquake", "is_in_study_area", "catalog_era");

        // Validate uniqueness of canonical_event_id
        long totalInputCount = goldEvents.count();
        long uniqueCanonicalCount = goldEvents.select("canonical_event_id").distinct().count();
        if (totalInputCount != uniqueCanonicalCount) {
            throw new IllegalArgumentException("Duplicate canonical_event_id in Gold input: total="
                    + totalInputCount + ", unique=" + uniqueCanonicalCount);
        }

        // Define filtering conditions
        Column isMissingCoord = col("latitude").isNull().or(isnan(col("latitude"))).or(abs(col("latitude")).gt(90.0))
                .or(col("longitude").isNull()).or(isnan(col("longitude"))).or(abs(col("longitude")).gt(180.0));
        Column isMissingDepth = col("depth_km").isNull().or(isnan(col("depth_km"))).or(abs(col("depth_km")).gt(Double.MAX_VALUE));
        Column isMissingMag = col("magnitude").isNull().or(isnan(col("magnitude"))).or(abs(col("magnitude")).gt(Double.MAX_VALUE));
        Column isNonNatural = col("is_natural_earthquake").isNull().or(not(col("is_natural_earthquake")))
                .or(col("event_type_code").isNull()).or(not(col("event_type_code").equalTo("EARTHQUAKE")));
        Column isOutsideArea = col("is_in_study_area").isNull().or(not(col("is_in_study_area")));
        Column isNotComparable = col("canonical_source_system").isNull()
                .or(not(col("canonical_source_system").equalTo(config.requiredCanonicalSource())));
        Column isLegacyEra = col("catalog_era").isNull()
                .or(not(col("catalog_era").equalTo(config.requiredCatalogEra())));
        Column isOutsidePeriod = col("event_time_utc").isNull()
                .or(col("event_time_utc").lt(lit(Timestamp.from(config.periodStartUtc()))))
                .or(col("event_time_utc").geq(lit(Timestamp.from(config.periodEndUtcExclusive()))));

        // Candidate filter for Mc estimation (passed rules 1-8)
        Column passedPreCompleteness = not(isMissingCoord).and(not(isMissingDepth)).and(not(isMissingMag))
                .and(not(isNonNatural)).and(not(isOutsideArea)).and(not(isNotComparable))
                .and(not(isLegacyEra)).and(not(isOutsidePeriod));

        Dataset<Row> preCompletenessEvents = goldEvents.filter(passedPreCompleteness);
        List<Row> magRows = preCompletenessEvents.select("magnitude").collectAsList();
        List<Double> magnitudes = new ArrayList<>(magRows.size());
        for (Row r : magRows) {
            if (!r.isNullAt(0)) {
                double m = r.getDouble(0);
                if (Double.isFinite(m)) magnitudes.add(m);
            }
        }

        // Estimate Mc
        MagnitudeCompletenessEstimator estimator = new MagnitudeCompletenessEstimator(
                config.mcBinWidth(), config.mcSensitivityStep(), config.minEventsForMc(), config.mcMethodVersion());
        CompletenessResult completenessResult = estimator.estimate(magnitudes);
        Map<String, CompletenessResult> depthGroups = estimator.estimateStratifiedByDepth(preCompletenessEvents);

        double mcToApply;
        if (config.fixedMcValue() != null) {
            mcToApply = config.fixedMcValue();
        } else if (completenessResult.isReliable()) {
            mcToApply = completenessResult.centralMc();
        } else if (!Double.isNaN(completenessResult.maxCurvatureBin())) {
            mcToApply = completenessResult.maxCurvatureBin();
        } else {
            mcToApply = 0.0;
        }

        Column isBelowMc = not(isMissingMag).and(col("magnitude").lt(lit(mcToApply)));

        // Deterministic hierarchy for primary exclusion reason
        Column primaryReason = when(isMissingCoord, "ML_MISSING_COORDINATE")
                .when(isMissingDepth, "ML_MISSING_DEPTH")
                .when(isMissingMag, "ML_MISSING_MAGNITUDE")
                .when(isNonNatural, "ML_NON_NATURAL_EVENT")
                .when(isOutsideArea, "ML_OUTSIDE_STUDY_AREA")
                .when(isNotComparable, "ML_SOURCE_NOT_COMPARABLE")
                .when(isLegacyEra, "ML_LEGACY_CATALOG_ERA")
                .when(isOutsidePeriod, "ML_OUTSIDE_TIME_RANGE")
                .when(isBelowMc, "ML_BELOW_COMPLETENESS")
                .otherwise(lit(null).cast(DataTypes.StringType));

        Column isEligible = primaryReason.isNull();

        Column allReasonsArray = filter(
                array(
                        when(isMissingCoord, "ML_MISSING_COORDINATE").otherwise(lit(null).cast(DataTypes.StringType)),
                        when(isMissingDepth, "ML_MISSING_DEPTH").otherwise(lit(null).cast(DataTypes.StringType)),
                        when(isMissingMag, "ML_MISSING_MAGNITUDE").otherwise(lit(null).cast(DataTypes.StringType)),
                        when(isNonNatural, "ML_NON_NATURAL_EVENT").otherwise(lit(null).cast(DataTypes.StringType)),
                        when(isOutsideArea, "ML_OUTSIDE_STUDY_AREA").otherwise(lit(null).cast(DataTypes.StringType)),
                        when(isNotComparable, "ML_SOURCE_NOT_COMPARABLE").otherwise(lit(null).cast(DataTypes.StringType)),
                        when(isLegacyEra, "ML_LEGACY_CATALOG_ERA").otherwise(lit(null).cast(DataTypes.StringType)),
                        when(isOutsidePeriod, "ML_OUTSIDE_TIME_RANGE").otherwise(lit(null).cast(DataTypes.StringType)),
                        when(isBelowMc, "ML_BELOW_COMPLETENESS").otherwise(lit(null).cast(DataTypes.StringType))
                ),
                c -> c.isNotNull()
        );

        Dataset<Row> auditedEvents = goldEvents
                .withColumn("is_eligible", isEligible)
                .withColumn("primary_exclusion_reason", primaryReason)
                .withColumn("all_exclusion_reasons", allReasonsArray)
                .withColumn("mc_applied", lit(mcToApply));

        Dataset<Row> eligibleEvents = auditedEvents.filter(col("is_eligible"));
        Dataset<Row> excludedEvents = auditedEvents.filter(not(col("is_eligible")));

        long eligibleCount = eligibleEvents.count();
        long excludedCount = excludedEvents.count();

        // Primary reason counts
        Map<String, Long> exclusionCountsByReason = new TreeMap<>();
        List<Row> reasonRows = excludedEvents.groupBy("primary_exclusion_reason").count().collectAsList();
        for (Row r : reasonRows) {
            exclusionCountsByReason.put(r.getString(0), r.getLong(1));
        }

        // Total occurrences of each reason
        Map<String, Long> allExclusionCounts = new TreeMap<>();
        if (excludedCount > 0) {
            Dataset<Row> exploded = excludedEvents.select(explode(col("all_exclusion_reasons")).alias("reason"));
            for (Row r : exploded.groupBy("reason").count().collectAsList()) {
                allExclusionCounts.put(r.getString(0), r.getLong(1));
            }
        }

        // Yearly distribution
        SortedMap<Integer, Long> inputEventsByYear = new TreeMap<>();
        SortedMap<Integer, Long> eligibleEventsByYear = new TreeMap<>();
        SortedMap<Integer, Long> excludedEventsByYear = new TreeMap<>();
        for (Row r : auditedEvents.groupBy(year(col("event_time_utc")).alias("y")).count().collectAsList()) {
            if (!r.isNullAt(0)) inputEventsByYear.put(r.getInt(0), r.getLong(1));
        }
        for (Row r : eligibleEvents.groupBy(year(col("event_time_utc")).alias("y")).count().collectAsList()) {
            if (!r.isNullAt(0)) eligibleEventsByYear.put(r.getInt(0), r.getLong(1));
        }
        for (Row r : excludedEvents.groupBy(year(col("event_time_utc")).alias("y")).count().collectAsList()) {
            if (!r.isNullAt(0)) excludedEventsByYear.put(r.getInt(0), r.getLong(1));
        }

        // Depth and magnitude band distributions
        SortedMap<String, Long> depthBandCounts = new TreeMap<>();
        if (hasColumn(goldEvents, "depth_band_code")) {
            for (Row r : auditedEvents.groupBy("depth_band_code").count().collectAsList()) {
                depthBandCounts.put(r.isNullAt(0) ? "UNKNOWN" : r.getString(0), r.getLong(1));
            }
        }
        SortedMap<String, Long> magnitudeBandCounts = new TreeMap<>();
        if (hasColumn(goldEvents, "magnitude_band_code")) {
            for (Row r : auditedEvents.groupBy("magnitude_band_code").count().collectAsList()) {
                magnitudeBandCounts.put(r.isNullAt(0) ? "UNKNOWN" : r.getString(0), r.getLong(1));
            }
        }

        // Canonical mc_config JSON and hash
        ObjectNode mcConfigNode = MagnitudeCompletenessEstimator.buildCanonicalMcConfigNode(completenessResult, depthGroups);
        String mcConfigJson = MagnitudeCompletenessEstimator.canonicalJsonString(mcConfigNode);
        String mcConfigSha256 = MagnitudeCompletenessEstimator.sha256(mcConfigJson);

        // Network change shifts
        List<GoldAuditResult.NetworkShiftMetric> shifts = computeNetworkShifts(auditedEvents);

        GoldAuditResult result = new GoldAuditResult(
                auditedEvents,
                eligibleEvents,
                excludedEvents,
                totalInputCount,
                eligibleCount,
                excludedCount,
                Collections.unmodifiableMap(exclusionCountsByReason),
                Collections.unmodifiableMap(allExclusionCounts),
                completenessResult,
                depthGroups,
                inputEventsByYear,
                eligibleEventsByYear,
                excludedEventsByYear,
                depthBandCounts,
                magnitudeBandCounts,
                shifts,
                config,
                auditedAtUtc,
                mcConfigJson,
                mcConfigSha256,
                mcToApply
        );

        result.assertReconciled();
        return result;
    }

    private static List<GoldAuditResult.NetworkShiftMetric> computeNetworkShifts(Dataset<Row> auditedEvents) {
        List<GoldAuditResult.NetworkShiftMetric> metrics = new ArrayList<>();
        // Milestones:
        // 1. 1997-10-01: JMA Unified Catalog transition
        // 2. 2000-10-01: Hi-net full integration
        // 3. 2011-03-11: 2011 Tohoku M9.0 earthquake
        // 4. 2018-10-01: Reproduction vs Extension split
        record Milestone(String name, String date, Instant instant) {}
        List<Milestone> milestones = List.of(
                new Milestone("JMA Unified Catalog Era", "1997-10-01", Instant.parse("1997-10-01T00:00:00Z")),
                new Milestone("Hi-net Seismic Network Expansion", "2000-10-01", Instant.parse("2000-10-01T00:00:00Z")),
                new Milestone("Tohoku Mw 9.0 Earthquake Crisis", "2011-03-11", Instant.parse("2011-03-11T05:46:00Z")),
                new Milestone("Reproduction to Extension Split", "2018-10-01", Instant.parse("2018-10-01T00:00:00Z"))
        );

        for (Milestone m : milestones) {
            Instant winStart = m.instant.minusSeconds(365L * 86400L);
            Instant winEnd = m.instant.plusSeconds(365L * 86400L);
            long before = auditedEvents.filter(col("event_time_utc").geq(lit(Timestamp.from(winStart)))
                    .and(col("event_time_utc").lt(lit(Timestamp.from(m.instant))))).count();
            long after = auditedEvents.filter(col("event_time_utc").geq(lit(Timestamp.from(m.instant)))
                    .and(col("event_time_utc").lt(lit(Timestamp.from(winEnd))))).count();

            if (before > 0 || after > 0) {
                double ratio = before > 0 ? ((double) after / before) : (after > 0 ? Double.POSITIVE_INFINITY : 1.0);
                String assessment = assessShift(ratio, m.name);
                metrics.add(new GoldAuditResult.NetworkShiftMetric(
                        m.name, m.date, before, after, before, after, ratio, assessment));
            }
        }
        return Collections.unmodifiableList(metrics);
    }

    private static String assessShift(double ratio, String milestone) {
        if (Double.isInfinite(ratio)) return "New catalog phase: previous annual rate was zero.";
        if (ratio > 2.0) return "High detection rate surge (>200%): significant network density or aftershock saturation.";
        if (ratio < 0.5) return "Significant rate decrease (<50%): network sensitivity shift or post-crisis decay.";
        return "Stable catalog detection rate across milestone.";
    }

    private static void requireColumns(Dataset<Row> dataset, String... columnNames) {
        Set<String> existing = new HashSet<>(Arrays.asList(dataset.columns()));
        for (String col : columnNames) {
            if (!existing.contains(col)) {
                throw new IllegalArgumentException("Missing required column in Gold input: " + col);
            }
        }
    }

    private static boolean hasColumn(Dataset<Row> dataset, String columnName) {
        return Arrays.asList(dataset.columns()).contains(columnName);
    }
}
