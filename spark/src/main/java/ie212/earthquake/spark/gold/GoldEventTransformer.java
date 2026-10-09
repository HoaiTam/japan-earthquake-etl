package ie212.earthquake.spark.gold;

import static org.apache.spark.sql.functions.*;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/** CON-03 transformation using already assigned SLV-07 IDs and primary memberships. */
public final class GoldEventTransformer {
    public static final StructType MEMBERSHIP_SCHEMA = new StructType()
            .add("canonical_event_id", DataTypes.StringType, false)
            .add("source_observation_id", DataTypes.StringType, false)
            .add("membership_status", DataTypes.StringType, false)
            .add("source_link_id", DataTypes.StringType, true)
            .add("canonical_model_version", DataTypes.StringType, false)
            .add("assigned_at_utc", DataTypes.TimestampType, false);
    public static final StructType LINK_SCHEMA = new StructType()
            .add("left_observation_id", DataTypes.StringType, false)
            .add("right_observation_id", DataTypes.StringType, false)
            .add("link_decision", DataTypes.StringType, false);
    public static final StructType REGION_SCHEMA = new StructType()
            .add("source_observation_id", DataTypes.StringType, false)
            .add("region_key", DataTypes.StringType, false)
            .add("region_name", DataTypes.StringType, false)
            .add("region_category", DataTypes.StringType, false);

    public GoldTransformationResult transform(Dataset<Row> observations, Dataset<Row> memberships,
            Dataset<Row> links, Dataset<Row> regions, GoldRunContext context, Instant updatedAtUtc) {
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(updatedAtUtc, "updatedAtUtc");
        SparkSession spark = observations.sparkSession();
        if (!"UTC".equals(spark.conf().get("spark.sql.session.timeZone"))) {
            throw new IllegalArgumentException("Spark session timezone must be UTC");
        }
        Dataset<Row> current = observations.filter(col("is_current_source_revision"));
        unique(current, "source_observation_id");
        unique(memberships, "source_observation_id");
        unique(regions, "source_observation_id");
        requireEmpty(current.filter(not(col("quality_status").isin("VALID", "WARNING"))
                .or(col("quality_status").isNull()).or(not(col("source_system").isin("USGS", "JMA_BULLETIN")))
                .or(col("source_system").isNull())), "invalid current observation");
        requireEmpty(current.filter(col("event_time_utc").isNull()
                .or(col("latitude").isNull()).or(isnan(col("latitude"))).or(not(col("latitude").between(-90, 90)))
                .or(col("longitude").isNull()).or(isnan(col("longitude"))).or(not(col("longitude").between(-180, 180)))
                .or(nonFinite("depth_km")).or(nonFinite("magnitude"))
                .or(blank("source_record_key")).or(blank("bronze_manifest_id")).or(blank("raw_object_uri"))
                .or(not(col("event_type_code").isin("EARTHQUAKE", "ARTIFICIAL", "ERUPTION", "OTHER", "UNKNOWN")))
                .or(col("event_type_code").isNull()).or(col("is_in_study_area").isNull())), "invalid required Gold fields");
        requireEmpty(memberships.filter(blank("canonical_event_id").or(blank("source_observation_id"))
                .or(blank("canonical_model_version")).or(col("assigned_at_utc").isNull())
                .or(not(col("membership_status").isin("PRIMARY", "SUPPORTING")))
                .or(col("membership_status").isNull())), "invalid canonical membership");
        requireEmpty(current.join(memberships, "source_observation_id", "left_anti"), "current observation missing membership");
        requireEmpty(memberships.join(current, "source_observation_id", "left_anti"), "membership is not a current observation");
        requireEmpty(memberships.groupBy("canonical_event_id").agg(
                sum(when(col("membership_status").equalTo("PRIMARY"), 1).otherwise(0)).alias("primary_count"),
                countDistinct("canonical_model_version").alias("versions"))
                .filter(col("primary_count").notEqual(1).or(col("versions").notEqual(1))), "exactly one primary/version required");
        requireEmpty(regions.filter(blank("region_key").or(blank("region_name"))
                .or(not(col("region_category").isin("PREFECTURE", "OFFSHORE", "UNKNOWN")))
                .or(col("region_category").isNull())
                .or(col("region_category").isin("OFFSHORE", "UNKNOWN").and(not(col("region_key").equalTo(col("region_category")))))
                .or(col("region_category").equalTo("PREFECTURE").and(not(col("region_key").startsWith("PREFECTURE:"))))),
                "invalid region classification");
        Dataset<Row> members = current.join(memberships, "source_observation_id");
        requireEmpty(members.groupBy("canonical_event_id", "source_system").count().filter(col("count").gt(1)),
                "multiple current members from same source");
        Dataset<Row> primary = members.filter(col("membership_status").equalTo("PRIMARY"));
        Dataset<Row> usgs = members.filter(col("source_system").equalTo("USGS")).select(
                col("canonical_event_id"), col("alert_level").alias("usgs_alert"), col("significance").alias("usgs_significance"));
        Dataset<Row> jma = members.filter(col("source_system").equalTo("JMA_BULLETIN")).select(
                col("canonical_event_id"), col("max_intensity_code").alias("jma_intensity"),
                col("determining_agency_code").alias("jma_agency"), col("catalog_era").alias("jma_era"));
        Dataset<Row> coverage = members.groupBy("canonical_event_id").agg(count(lit(1)).cast("int").alias("source_count"),
                max(when(col("source_system").equalTo("USGS"), 1).otherwise(0)).alias("usgs_count"),
                max(when(col("source_system").equalTo("JMA_BULLETIN"), 1).otherwise(0)).alias("jma_count"),
                max(when(col("tsunami_flag").equalTo(true), 2).when(col("tsunami_flag").equalTo(false), 1).otherwise(0)).alias("tsunami_code"),
                max(when(col("quality_status").equalTo("WARNING"), 1).otherwise(0)).alias("warning_count"));
        Dataset<Row> ambiguousIds = links.filter(col("link_decision").equalTo("AMBIGUOUS"))
                .select(col("left_observation_id").alias("source_observation_id"))
                .union(links.filter(col("link_decision").equalTo("AMBIGUOUS"))
                        .select(col("right_observation_id").alias("source_observation_id"))).distinct();
        Dataset<Row> ambiguous = members.join(ambiguousIds, "source_observation_id")
                .select("canonical_event_id").distinct().withColumn("ambiguous", lit(true));
        Dataset<Row> enriched = primary.drop("alert_level", "significance", "max_intensity_code", "determining_agency_code", "catalog_era", "quality_status", "tsunami_flag")
                .join(usgs, "canonical_event_id", "left").join(jma, "canonical_event_id", "left")
                .join(coverage, "canonical_event_id").join(ambiguous, "canonical_event_id", "left")
                .join(regions, "source_observation_id", "left");
        Dataset<Row> events = enriched.select(
                lit("1.0").alias("schema_version"), col("canonical_event_id"), col("source_observation_id").alias("primary_observation_id"),
                col("source_system").alias("canonical_source_system"), col("event_time_utc"),
                from_utc_timestamp(col("event_time_utc"), "Asia/Tokyo").alias("event_time_jst"),
                to_date(col("event_time_utc")).alias("event_date_utc"),
                to_date(from_utc_timestamp(col("event_time_utc"), "Asia/Tokyo")).alias("event_date_jst"),
                date_format(col("event_time_utc"), "yyyyMMdd").cast("int").alias("event_date_key_utc"),
                date_format(from_utc_timestamp(col("event_time_utc"), "Asia/Tokyo"), "yyyyMMdd").cast("int").alias("event_date_key_jst"),
                col("latitude"), col("longitude"), col("depth_km"), col("magnitude"), col("magnitude_type"), col("event_type_code"),
                col("event_type_code").equalTo("EARTHQUAKE").alias("is_natural_earthquake"), col("place_name"), col("is_in_study_area"),
                coalesce(col("region_key"), lit("UNKNOWN")).alias("region_key"),
                coalesce(col("region_name"), lit("Unknown")).alias("region_name"),
                coalesce(col("region_category"), lit("UNKNOWN")).alias("region_category"),
                magnitudeBand(col("magnitude")).alias("magnitude_band_code"), depthBand(col("depth_km")).alias("depth_band_code"),
                when(col("tsunami_code").equalTo(2), lit(true)).when(col("tsunami_code").equalTo(1), lit(false))
                        .otherwise(lit(null).cast("boolean")).alias("tsunami_flag"),
                col("usgs_alert").alias("alert_level"), col("usgs_significance").alias("significance"),
                col("jma_intensity").alias("max_intensity_code"), col("jma_agency").alias("determining_agency_code"), col("jma_era").alias("catalog_era"),
                col("source_count"), col("usgs_count").gt(0).alias("has_usgs"), col("jma_count").gt(0).alias("has_jma"),
                when(col("source_count").equalTo(2), "USGS_JMA").when(col("usgs_count").gt(0), "USGS_ONLY").otherwise("JMA_ONLY").alias("source_coverage_code"),
                when(col("ambiguous").equalTo(true), "AMBIGUOUS").when(col("source_count").gt(1), "MATCHED").otherwise("SINGLE_SOURCE").alias("link_status"),
                when(col("warning_count").gt(0), "WARNING").otherwise("VALID").alias("quality_status"),
                col("canonical_model_version"), lit(context.runId()).alias("gold_run_id"),
                lit(java.sql.Timestamp.from(updatedAtUtc)).alias("record_updated_at_utc"));
        Dataset<Row> bridge = members.select(col("canonical_event_id"), col("source_observation_id"), col("source_system"),
                col("source_record_key"), col("membership_status").equalTo("PRIMARY").alias("is_primary"), col("source_link_id"),
                col("raw_object_uri"), col("bronze_manifest_id"), col("catalog_release"), col("source_updated_at_utc"));
        Dataset<Row> dates = events.select(col("event_date_utc").alias("calendar_date"))
                .union(events.select(col("event_date_jst").alias("calendar_date"))).distinct();
        Dataset<Row> dimDate = dates.select(date_format(col("calendar_date"), "yyyyMMdd").cast("int").alias("date_key"),
                col("calendar_date"), year(col("calendar_date")).alias("year"), quarter(col("calendar_date")).alias("quarter"),
                month(col("calendar_date")).alias("month"), date_format(col("calendar_date"), "MMMM").alias("month_name"),
                dayofmonth(col("calendar_date")).alias("day_of_month"), dayofweek(col("calendar_date")).alias("day_of_week"),
                dayofweek(col("calendar_date")).isin(1, 7).alias("is_weekend"));
        Dataset<Row> dimRegion = events.select("region_key", "region_name", "region_category").distinct();
        unique(dimRegion, "region_key");
        long observationCount = current.count();
        long canonicalCount = memberships.select("canonical_event_id").distinct().count();
        long bridgeCount = bridge.count();
        if (events.count() != canonicalCount || bridgeCount != observationCount) {
            throw new IllegalArgumentException("Gold count reconciliation failed");
        }
        return new GoldTransformationResult(events, events.filter(col("is_natural_earthquake").and(col("is_in_study_area"))),
                bridge, dimDate, dimRegion, bandDimension(spark, true), bandDimension(spark, false),
                observationCount, canonicalCount, bridgeCount);
    }

    public static Column magnitudeBand(Column magnitude) {
        return when(magnitude.isNull(), "UNKNOWN").when(magnitude.lt(3), "LT_3")
                .when(magnitude.lt(4), "M3_TO_LT4").when(magnitude.lt(5), "M4_TO_LT5")
                .when(magnitude.lt(6), "M5_TO_LT6").when(magnitude.lt(7), "M6_TO_LT7").otherwise("GE_7");
    }

    public static Column depthBand(Column depth) {
        return when(depth.isNull(), "UNKNOWN").when(depth.lt(0), "NEGATIVE").when(depth.lt(70), "SHALLOW")
                .when(depth.lt(300), "INTERMEDIATE").otherwise("DEEP");
    }

    private static Dataset<Row> bandDimension(SparkSession spark, boolean magnitude) {
        String[] codes = magnitude ? new String[]{"UNKNOWN", "LT_3", "M3_TO_LT4", "M4_TO_LT5", "M5_TO_LT6", "M6_TO_LT7", "GE_7"}
                : new String[]{"UNKNOWN", "NEGATIVE", "SHALLOW", "INTERMEDIATE", "DEEP"};
        String[] labels = magnitude ? new String[]{"Unknown", "Below 3.0", "3.0–<4.0", "4.0–<5.0", "5.0–<6.0", "6.0–<7.0", "7.0 or greater"}
                : new String[]{"Unknown", "Negative/above datum", "Shallow", "Intermediate", "Deep"};
        java.util.ArrayList<Row> rows = new java.util.ArrayList<>();
        for (int index = 0; index < codes.length; index++) rows.add(org.apache.spark.sql.RowFactory.create(codes[index], labels[index], index));
        return spark.createDataFrame(rows, new StructType().add("band_code", DataTypes.StringType, false)
                .add("band_label", DataTypes.StringType, false).add("sort_order", DataTypes.IntegerType, false));
    }

    private static Column blank(String name) { return col(name).isNull().or(length(trim(col(name))).equalTo(0)); }
    private static Column nonFinite(String name) {
        return col(name).isNotNull().and(isnan(col(name)).or(abs(col(name)).gt(Double.MAX_VALUE)));
    }
    private static void unique(Dataset<Row> data, String key) {
        requireEmpty(data.filter(blank(key)), "blank " + key);
        requireEmpty(data.groupBy(key).count().filter(col("count").gt(1)), "duplicate " + key);
    }
    private static void requireEmpty(Dataset<Row> failures, String reason) {
        if (failures.limit(1).count() != 0) throw new IllegalArgumentException(reason);
    }
}
