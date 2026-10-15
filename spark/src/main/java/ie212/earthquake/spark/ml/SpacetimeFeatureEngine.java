package ie212.earthquake.spark.ml;

import static org.apache.spark.sql.functions.*;
import java.util.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;

/** MLD-04: distributed, fail-closed relative 4-D features with explicit invalid-row evidence. */
public final class SpacetimeFeatureEngine {
    public static final List<String> VECTOR = List.of("x_scaled", "y_scaled", "z_scaled", "time_scaled");
    private static final String[] KEYS = {"dataset_id", "mainshock_event_id"};
    private static Column finite(Column c) { return c.isNotNull().and(not(isnan(c))).and(abs(c).notEqual(Double.POSITIVE_INFINITY)); }
    private static void require(Dataset<Row> data, String name, org.apache.spark.sql.types.DataType type) {
        if (!Arrays.asList(data.columns()).contains(name) || !data.schema().apply(name).dataType().equals(type))
            throw new IllegalArgumentException("ML_FEATURE_SCHEMA_MISMATCH:" + name);
    }
    public record Result(Dataset<Row> features, Dataset<Row> invalidRows, long inputCount, long validCount,
            long invalidCount, Map<String, Long> reasonCounts) {
        public void assertValid() {
            if (inputCount != validCount + invalidCount || invalidCount != 0)
                throw new IllegalStateException("ML_FEATURE_GATE_FAILED:" + reasonCounts);
        }
    }
    public Result execute(Dataset<Row> mainshocks, Dataset<Row> candidates, FeatureScalingConfig config) {
        Objects.requireNonNull(config);
        if (!"UTC".equals(candidates.sparkSession().conf().get("spark.sql.session.timeZone")))
            throw new IllegalArgumentException("ML_FEATURE_UTC_REQUIRED");
        for (String key : KEYS) { require(mainshocks, key, DataTypes.StringType); require(candidates, key, DataTypes.StringType); }
        require(candidates, "candidate_event_id", DataTypes.StringType);
        require(candidates, "is_mainshock", DataTypes.BooleanType);
        require(candidates, "relative_time_role", DataTypes.StringType);
        require(candidates, "candidate_time_utc", DataTypes.TimestampType);
        require(mainshocks, "event_time_utc", DataTypes.TimestampType);
        require(mainshocks, "resource_guard_status", DataTypes.StringType);
        require(mainshocks, "candidate_count", DataTypes.LongType);
        for (String field : List.of("latitude", "longitude", "depth_km", "magnitude", "mc_value",
                "pre_window_hours", "post_window_hours", "search_radius_km")) require(mainshocks, field, DataTypes.DoubleType);
        for (String field : List.of("candidate_latitude", "candidate_longitude", "candidate_depth_km", "candidate_magnitude",
                "mainshock_magnitude", "mc_value", "dx_km", "dy_km", "dz_km", "distance_3d_km", "delta_time_hours"))
            require(candidates, field, DataTypes.DoubleType);
        require(candidates, "window_model_version", DataTypes.StringType);
        require(mainshocks, "window_model_version", DataTypes.StringType);
        if (mainshocks.groupBy("dataset_id", "mainshock_event_id").count().filter(col("count").notEqual(1)).limit(1).count() != 0)
            throw new IllegalArgumentException("ML_DUPLICATE_MAINSHOCK_GRAIN");
        // Prefix upstream metadata to avoid accidentally binding candidate columns in the join.
        var mainColumns = new ArrayList<Column>();
        for (String name : mainshocks.columns()) mainColumns.add(col(name).alias(Arrays.asList(KEYS).contains(name) ? name : "_m_" + name));
        mainColumns.add(lit(true).alias("_main_present"));
        Dataset<Row> checked = candidates.join(mainshocks.select(mainColumns.toArray(Column[]::new)), KEYS, "left");
        var window = Window.partitionBy("dataset_id", "mainshock_event_id");
        Column self = col("candidate_event_id").equalTo(col("mainshock_event_id"));
        checked = checked.withColumn("_observed_count", count(lit(1)).over(window))
            .withColumn("_self_count", sum(when(self, 1L).otherwise(0L)).over(window))
            .withColumn("_grain_count", count(lit(1)).over(Window.partitionBy("dataset_id", "mainshock_event_id", "candidate_event_id")))
            .withColumn("_dx", lit(111.32).multiply(cos(radians(col("_m_latitude"))))
                .multiply(col("candidate_longitude").minus(col("_m_longitude"))))
            .withColumn("_dy", lit(110.57).multiply(col("candidate_latitude").minus(col("_m_latitude"))))
            .withColumn("_dz", col("candidate_depth_km").minus(col("_m_depth_km")))
            .withColumn("_dt", unix_micros(col("candidate_time_utc")).minus(unix_micros(col("_m_event_time_utc")))
                .cast("double").divide(3_600_000_000.0))
            .withColumn("_distance", sqrt(pow(col("_dx"), 2).plus(pow(col("_dy"), 2)).plus(pow(col("_dz"), 2))));
        Column expectedRole = when(self, "MAINSHOCK").when(col("_dt").lt(0), "PRE").otherwise("POST");
        Column invalidNumeric = lit(false);
        for (String field : List.of("candidate_latitude", "candidate_longitude", "candidate_depth_km", "candidate_magnitude",
                "mainshock_magnitude", "mc_value", "_m_latitude", "_m_longitude", "_m_depth_km", "_m_magnitude", "_m_mc_value",
                "_m_pre_window_hours", "_m_post_window_hours", "_m_search_radius_km", "dx_km", "dy_km", "dz_km", "distance_3d_km", "delta_time_hours"))
            invalidNumeric = invalidNumeric.or(not(finite(col(field))));
        Column relativeConflict = abs(col("dx_km").minus(col("_dx"))).gt(config.coordinateToleranceKm())
            .or(abs(col("dy_km").minus(col("_dy"))).gt(config.coordinateToleranceKm()))
            .or(abs(col("dz_km").minus(col("_dz"))).gt(config.coordinateToleranceKm()))
            .or(abs(col("distance_3d_km").minus(col("_distance"))).gt(config.coordinateToleranceKm()))
            .or(abs(col("delta_time_hours").minus(col("_dt"))).gt(config.timeToleranceHours()));
        Column mainRule = col("_m_depth_km").between(50.0, 200.0).and(col("_m_magnitude").gt(5.5))
            .and(col("_m_post_window_hours").gt(0)).and(col("_m_pre_window_hours").geq(0)).and(col("_m_search_radius_km").gt(0))
            .and(col("_m_latitude").between(-90.0, 90.0)).and(col("_m_longitude").between(-180.0, 180.0));
        Column membership = col("_dt").geq(col("_m_pre_window_hours").multiply(-1).minus(config.timeToleranceHours()))
            .and(col("_dt").leq(col("_m_post_window_hours").plus(config.timeToleranceHours())))
            .and(sqrt(pow(col("_dx"), 2).plus(pow(col("_dy"), 2))).leq(col("_m_search_radius_km").plus(0.00001)))
            .and(col("candidate_latitude").between(-90.0, 90.0)).and(col("candidate_longitude").between(-180.0, 180.0));
        Column selfConflict = self.and(not(col("candidate_time_utc").eqNullSafe(col("_m_event_time_utc")))
            .or(not(col("candidate_latitude").eqNullSafe(col("_m_latitude"))))
            .or(not(col("candidate_longitude").eqNullSafe(col("_m_longitude"))))
            .or(not(col("candidate_depth_km").eqNullSafe(col("_m_depth_km"))))
            .or(not(col("candidate_magnitude").eqNullSafe(col("_m_magnitude")))));
        var conditions = new LinkedHashMap<String, Column>();
        Column invalidId = lit(false);
        for (String field : List.of("dataset_id", "mainshock_event_id", "candidate_event_id"))
            invalidId = invalidId.or(col(field).isNull()).or(length(trim(col(field))).equalTo(0));
        conditions.put("ML_INVALID_ID", invalidId);
        conditions.put("ML_MISSING_MAINSHOCK", col("_main_present").isNull());
        conditions.put("ML_DUPLICATE_GRAIN", col("_grain_count").notEqual(1));
        conditions.put("ML_SELF_PRESENCE_INVALID", col("_self_count").notEqual(1));
        conditions.put("ML_CANDIDATE_COUNT_CONFLICT", not(col("_observed_count").eqNullSafe(col("_m_candidate_count"))));
        conditions.put("ML_RESOURCE_LIMIT_EXCEEDED", col("_observed_count").gt(config.maxCandidatesPerWindow())
            .or(not(col("_m_resource_guard_status").eqNullSafe(lit("PASS")))));
        conditions.put("ML_NON_FINITE_FEATURE", invalidNumeric);
        conditions.put("ML_MAINSHOCK_RULE_INVALID", not(coalesce(mainRule, lit(false))));
        conditions.put("ML_SELF_SOURCE_MISMATCH", selfConflict);
        conditions.put("ML_RELATIVE_COORDINATE_CONFLICT", coalesce(relativeConflict, lit(true)));
        conditions.put("ML_EVENT_ROLE_MISMATCH", not(col("is_mainshock").eqNullSafe(self))
            .or(not(col("relative_time_role").eqNullSafe(expectedRole))).or(col("candidate_time_utc").isNull()));
        conditions.put("ML_WINDOW_MEMBERSHIP_INVALID", not(coalesce(membership, lit(false))));
        conditions.put("ML_CANDIDATE_LINEAGE_CONFLICT", not(col("mainshock_magnitude").eqNullSafe(col("_m_magnitude")))
            .or(not(col("mc_value").eqNullSafe(col("_m_mc_value")))).or(col("candidate_magnitude").lt(col("_m_mc_value")))
            .or(not(col("window_model_version").eqNullSafe(col("_m_window_model_version")))));
        checked = checked.withColumn("space_scale_km", config.mode().equals("GLOBAL") ? lit(config.spaceScaleKm()) : col("_m_search_radius_km"))
            .withColumn("depth_scale_km", lit(config.depthScaleKm()))
            .withColumn("time_scale_hours", config.mode().equals("GLOBAL") ? lit(config.timeScaleHours()) : col("_m_post_window_hours"));
        String[] relative = {"_dx", "_dy", "_dz", "_dt"}; String[] scales = {"space_scale_km", "space_scale_km", "depth_scale_km", "time_scale_hours"};
        for (int i = 0; i < 4; i++) checked = checked.withColumn(VECTOR.get(i), when(self, lit(0.0)).otherwise(col(relative[i]).divide(col(scales[i]))));
        Column invalidScaled = lit(false);
        for (String field : VECTOR) invalidScaled = invalidScaled.or(not(finite(col(field))));
        for (String field : new HashSet<>(Arrays.asList(scales))) invalidScaled = invalidScaled.or(not(finite(col(field)))).or(col(field).leq(0));
        conditions.put("ML_INVALID_SCALED_FEATURE", invalidScaled);
        var reasonColumns = conditions.entrySet().stream().map(e -> when(e.getValue(), lit(e.getKey())).otherwise(lit(null).cast("string"))).toArray(Column[]::new);
        checked = checked.withColumn("feature_reason_codes", filter(array(reasonColumns), x -> x.isNotNull()));
        Dataset<Row> invalid = checked.filter(size(col("feature_reason_codes")).gt(0));
        var output = new ArrayList<Column>();
        Map<String, String> relativeFields = Map.of("dx_km", "_dx", "dy_km", "_dy", "dz_km", "_dz",
            "distance_3d_km", "_distance", "delta_time_hours", "_dt");
        Set<String> replaced = new HashSet<>(VECTOR);
        replaced.addAll(List.of("feature_version", "scaling_config_version", "space_scale_km", "depth_scale_km", "time_scale_hours"));
        for (String name : candidates.columns()) if (!replaced.contains(name))
            output.add(col(relativeFields.getOrDefault(name, name)).alias(name));
        for (String name : List.of("x_scaled", "y_scaled", "z_scaled", "time_scaled", "space_scale_km", "depth_scale_km", "time_scale_hours")) output.add(col(name));
        output.add(lit(config.featureVersion()).alias("feature_version"));
        output.add(lit(config.scalingConfigVersion()).alias("scaling_config_version"));
        Dataset<Row> valid = checked.filter(size(col("feature_reason_codes")).equalTo(0)).select(output.toArray(Column[]::new));
        Map<String, Long> reasons = new TreeMap<>();
        for (Row row : invalid.select(explode(col("feature_reason_codes")).alias("reason")).groupBy("reason").count().collectAsList())
            reasons.put(row.getString(0), row.getLong(1));
        // Mainshocks with no candidate row must not disappear from the diagnostic gate.
        long missingWindows = mainshocks.join(candidates.select(KEYS[0], KEYS[1]).distinct(), KEYS, "left_anti").count();
        if (missingWindows != 0) throw new IllegalArgumentException("ML_EMPTY_WINDOW:" + missingWindows);
        long input = candidates.count(), bad = invalid.count(), good = valid.count();
        return new Result(valid, invalid, input, good, bad, Collections.unmodifiableMap(reasons));
    }
}
