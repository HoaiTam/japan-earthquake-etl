package ie212.earthquake.spark.gold;

import static org.apache.spark.sql.functions.*;

import java.time.Instant;
import java.util.Objects;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * GLD-02 transformation: aggregates canonical earthquake events into daily and monthly rollups
 * for dashboard and Power BI serving (CON-03 section 11.2).
 *
 * <p>Key Guarantees:
 * <ul>
 *   <li>Only natural earthquakes in study area (gold.earthquake_event_current) are aggregated.</li>
 *   <li>No double counting: fact rows are grouped strictly by canonical event grain (0 bridge explosion).</li>
 *   <li>Preserves honest NULL policy: missing magnitude and depth remain NULL, never coerced to 0.</li>
 *   <li>Additive counts and sums allow exact monthly/regional rollup parity with fact tables.</li>
 * </ul>
 */
public final class GoldAggregateTransformer {

    public static final StructType EVENT_DAILY_SCHEMA = new StructType()
            .add("event_date_key_jst", DataTypes.IntegerType, false)
            .add("event_date_jst", DataTypes.DateType, false)
            .add("year_month_jst", DataTypes.IntegerType, false)
            .add("region_key", DataTypes.StringType, false)
            .add("region_name", DataTypes.StringType, false)
            .add("region_category", DataTypes.StringType, false)
            .add("magnitude_band_code", DataTypes.StringType, false)
            .add("depth_band_code", DataTypes.StringType, false)
            .add("source_coverage_code", DataTypes.StringType, false)
            .add("event_count", DataTypes.LongType, false)
            .add("magnitude_non_null_count", DataTypes.LongType, false)
            .add("magnitude_sum", DataTypes.DoubleType, true)
            .add("magnitude_avg", DataTypes.DoubleType, true)
            .add("magnitude_max", DataTypes.DoubleType, true)
            .add("depth_non_null_count", DataTypes.LongType, false)
            .add("depth_sum", DataTypes.DoubleType, true)
            .add("depth_avg", DataTypes.DoubleType, true)
            .add("strong_earthquake_count", DataTypes.LongType, false)
            .add("tsunami_flagged_count", DataTypes.LongType, false)
            .add("alerted_event_count", DataTypes.LongType, false)
            .add("intensity_reported_count", DataTypes.LongType, false)
            .add("offshore_or_unknown_count", DataTypes.LongType, false)
            .add("gold_run_id", DataTypes.StringType, false)
            .add("aggregated_at_utc", DataTypes.TimestampType, false);

    public static final StructType EVENT_MONTHLY_SCHEMA = new StructType()
            .add("year_month_jst", DataTypes.IntegerType, false)
            .add("year_jst", DataTypes.IntegerType, false)
            .add("month_jst", DataTypes.IntegerType, false)
            .add("region_key", DataTypes.StringType, false)
            .add("region_name", DataTypes.StringType, false)
            .add("region_category", DataTypes.StringType, false)
            .add("magnitude_band_code", DataTypes.StringType, false)
            .add("depth_band_code", DataTypes.StringType, false)
            .add("source_coverage_code", DataTypes.StringType, false)
            .add("event_count", DataTypes.LongType, false)
            .add("magnitude_non_null_count", DataTypes.LongType, false)
            .add("magnitude_sum", DataTypes.DoubleType, true)
            .add("magnitude_avg", DataTypes.DoubleType, true)
            .add("magnitude_max", DataTypes.DoubleType, true)
            .add("depth_non_null_count", DataTypes.LongType, false)
            .add("depth_sum", DataTypes.DoubleType, true)
            .add("depth_avg", DataTypes.DoubleType, true)
            .add("strong_earthquake_count", DataTypes.LongType, false)
            .add("tsunami_flagged_count", DataTypes.LongType, false)
            .add("alerted_event_count", DataTypes.LongType, false)
            .add("intensity_reported_count", DataTypes.LongType, false)
            .add("offshore_or_unknown_count", DataTypes.LongType, false)
            .add("gold_run_id", DataTypes.StringType, false)
            .add("aggregated_at_utc", DataTypes.TimestampType, false);

    /**
     * Transforms fact earthquake events into daily aggregates.
     * Accepts either gold.earthquake_event_current or gold.event_current.
     */
    public Dataset<Row> transformDaily(Dataset<Row> events, GoldRunContext context, Instant updatedAtUtc) {
        Objects.requireNonNull(events, "events");
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(updatedAtUtc, "updatedAtUtc");

        SparkSession spark = events.sparkSession();
        if (!"UTC".equals(spark.conf().get("spark.sql.session.timeZone"))) {
            throw new IllegalArgumentException("Spark session timezone must be UTC");
        }

        // Filter strictly to natural earthquakes in study area
        Dataset<Row> naturalEvents = events.filter(col("is_natural_earthquake").and(col("is_in_study_area")));

        Dataset<Row> aggregated = naturalEvents.groupBy(
                col("event_date_key_jst"),
                col("event_date_jst"),
                (col("event_date_key_jst").divide(100)).cast(DataTypes.IntegerType).alias("year_month_jst"),
                col("region_key"),
                col("region_name"),
                col("region_category"),
                col("magnitude_band_code"),
                col("depth_band_code"),
                col("source_coverage_code")
        ).agg(
                count(lit(1)).cast(DataTypes.LongType).alias("event_count"),
                count(col("magnitude")).cast(DataTypes.LongType).alias("magnitude_non_null_count"),
                sum(col("magnitude")).cast(DataTypes.DoubleType).alias("magnitude_sum"),
                avg(col("magnitude")).cast(DataTypes.DoubleType).alias("magnitude_avg"),
                max(col("magnitude")).cast(DataTypes.DoubleType).alias("magnitude_max"),
                count(col("depth_km")).cast(DataTypes.LongType).alias("depth_non_null_count"),
                sum(col("depth_km")).cast(DataTypes.DoubleType).alias("depth_sum"),
                avg(col("depth_km")).cast(DataTypes.DoubleType).alias("depth_avg"),
                sum(when(col("magnitude").geq(5.0), 1L).otherwise(0L)).cast(DataTypes.LongType).alias("strong_earthquake_count"),
                sum(when(col("tsunami_flag").equalTo(true), 1L).otherwise(0L)).cast(DataTypes.LongType).alias("tsunami_flagged_count"),
                sum(when(col("alert_level").isNotNull().and(not(lower(trim(col("alert_level"))).isin("none", ""))), 1L).otherwise(0L)).cast(DataTypes.LongType).alias("alerted_event_count"),
                sum(when(col("max_intensity_code").isNotNull().and(not(lower(trim(col("max_intensity_code"))).isin("none", ""))), 1L).otherwise(0L)).cast(DataTypes.LongType).alias("intensity_reported_count"),
                sum(when(col("region_category").isin("OFFSHORE", "UNKNOWN"), 1L).otherwise(0L)).cast(DataTypes.LongType).alias("offshore_or_unknown_count")
        ).select(
                col("event_date_key_jst"),
                col("event_date_jst"),
                col("year_month_jst"),
                col("region_key"),
                col("region_name"),
                col("region_category"),
                col("magnitude_band_code"),
                col("depth_band_code"),
                col("source_coverage_code"),
                col("event_count"),
                col("magnitude_non_null_count"),
                col("magnitude_sum"),
                col("magnitude_avg"),
                col("magnitude_max"),
                col("depth_non_null_count"),
                col("depth_sum"),
                col("depth_avg"),
                col("strong_earthquake_count"),
                col("tsunami_flagged_count"),
                col("alerted_event_count"),
                col("intensity_reported_count"),
                col("offshore_or_unknown_count"),
                lit(context.runId()).alias("gold_run_id"),
                lit(java.sql.Timestamp.from(updatedAtUtc)).alias("aggregated_at_utc")
        );

        // Verification & reconciliations
        long expectedCount = naturalEvents.count();
        if (expectedCount > 0) {
            Row countRow = aggregated.agg(sum("event_count")).head();
            long actualCount = countRow.isNullAt(0) ? 0L : countRow.getLong(0);
            if (actualCount != expectedCount) {
                throw new IllegalArgumentException("Daily aggregate count mismatch: expected " + expectedCount + ", got " + actualCount);
            }
            long duplicates = aggregated.groupBy("event_date_key_jst", "region_key", "magnitude_band_code", "depth_band_code", "source_coverage_code")
                    .count().filter(col("count").gt(1)).count();
            if (duplicates > 0) {
                throw new IllegalArgumentException("Duplicate grain found in daily aggregate: " + duplicates);
            }
        }

        return aggregated;
    }

    /**
     * Rolls up daily aggregates into monthly aggregates with exact additive consistency.
     */
    public Dataset<Row> transformMonthly(Dataset<Row> dailyAggregates, Instant updatedAtUtc) {
        Objects.requireNonNull(dailyAggregates, "dailyAggregates");
        Objects.requireNonNull(updatedAtUtc, "updatedAtUtc");

        Dataset<Row> monthly = dailyAggregates.groupBy(
                col("year_month_jst"),
                (col("year_month_jst").divide(100)).cast(DataTypes.IntegerType).alias("year_jst"),
                (col("year_month_jst").mod(100)).cast(DataTypes.IntegerType).alias("month_jst"),
                col("region_key"),
                col("region_name"),
                col("region_category"),
                col("magnitude_band_code"),
                col("depth_band_code"),
                col("source_coverage_code")
        ).agg(
                sum("event_count").cast(DataTypes.LongType).alias("event_count"),
                sum("magnitude_non_null_count").cast(DataTypes.LongType).alias("magnitude_non_null_count"),
                sum("magnitude_sum").cast(DataTypes.DoubleType).alias("magnitude_sum"),
                max("magnitude_max").cast(DataTypes.DoubleType).alias("magnitude_max"),
                sum("depth_non_null_count").cast(DataTypes.LongType).alias("depth_non_null_count"),
                sum("depth_sum").cast(DataTypes.DoubleType).alias("depth_sum"),
                sum("strong_earthquake_count").cast(DataTypes.LongType).alias("strong_earthquake_count"),
                sum("tsunami_flagged_count").cast(DataTypes.LongType).alias("tsunami_flagged_count"),
                sum("alerted_event_count").cast(DataTypes.LongType).alias("alerted_event_count"),
                sum("intensity_reported_count").cast(DataTypes.LongType).alias("intensity_reported_count"),
                sum("offshore_or_unknown_count").cast(DataTypes.LongType).alias("offshore_or_unknown_count"),
                max("gold_run_id").alias("gold_run_id")
        ).select(
                col("year_month_jst"),
                col("year_jst"),
                col("month_jst"),
                col("region_key"),
                col("region_name"),
                col("region_category"),
                col("magnitude_band_code"),
                col("depth_band_code"),
                col("source_coverage_code"),
                col("event_count"),
                col("magnitude_non_null_count"),
                col("magnitude_sum"),
                when(col("magnitude_non_null_count").gt(0), col("magnitude_sum").divide(col("magnitude_non_null_count")))
                        .otherwise(lit(null).cast(DataTypes.DoubleType)).alias("magnitude_avg"),
                col("magnitude_max"),
                col("depth_non_null_count"),
                col("depth_sum"),
                when(col("depth_non_null_count").gt(0), col("depth_sum").divide(col("depth_non_null_count")))
                        .otherwise(lit(null).cast(DataTypes.DoubleType)).alias("depth_avg"),
                col("strong_earthquake_count"),
                col("tsunami_flagged_count"),
                col("alerted_event_count"),
                col("intensity_reported_count"),
                col("offshore_or_unknown_count"),
                col("gold_run_id"),
                lit(java.sql.Timestamp.from(updatedAtUtc)).alias("aggregated_at_utc")
        );

        // Verification & reconciliations
        long expectedDailyCount = dailyAggregates.count();
        if (expectedDailyCount > 0) {
            Row dailyTotalRow = dailyAggregates.agg(sum("event_count")).head();
            Row monthlyTotalRow = monthly.agg(sum("event_count")).head();
            long dailyTotal = dailyTotalRow.isNullAt(0) ? 0L : dailyTotalRow.getLong(0);
            long monthlyTotal = monthlyTotalRow.isNullAt(0) ? 0L : monthlyTotalRow.getLong(0);
            if (dailyTotal != monthlyTotal) {
                throw new IllegalArgumentException("Monthly aggregate count mismatch: expected " + dailyTotal + ", got " + monthlyTotal);
            }
            long duplicates = monthly.groupBy("year_month_jst", "region_key", "magnitude_band_code", "depth_band_code", "source_coverage_code")
                    .count().filter(col("count").gt(1)).count();
            if (duplicates > 0) {
                throw new IllegalArgumentException("Duplicate grain found in monthly aggregate: " + duplicates);
            }
        }

        return monthly;
    }
}
