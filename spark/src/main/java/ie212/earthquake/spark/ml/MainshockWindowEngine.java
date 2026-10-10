package ie212.earthquake.spark.ml;

import static org.apache.spark.sql.functions.*;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * Mainshock Selection and Candidate Window Engine (MLD-03).
 * Filters baseline mainshocks (depth 50–200 km, magnitude > 5.5, within dataset period)
 * and executes range-join (time -> spatial bounding box -> fine local distance)
 * without full catalog cross joins.
 */
public final class MainshockWindowEngine {

    public MainshockWindowResult execute(
            Dataset<Row> auditedEvents,
            MainshockSelectionConfig selectionConfig,
            WindowModelConfig windowConfig,
            double mcValue,
            String datasetId,
            Instant createdAtUtc
    ) {
        Objects.requireNonNull(auditedEvents, "auditedEvents");
        Objects.requireNonNull(selectionConfig, "selectionConfig");
        Objects.requireNonNull(windowConfig, "windowConfig");
        Objects.requireNonNull(datasetId, "datasetId");
        Objects.requireNonNull(createdAtUtc, "createdAtUtc");

        SparkSession spark = auditedEvents.sparkSession();
        if (!"UTC".equals(spark.conf().get("spark.sql.session.timeZone"))) {
            throw new IllegalArgumentException("Spark session timezone must be UTC");
        }

        // Validate required columns in auditedEvents
        requireColumns(auditedEvents, "canonical_event_id", "event_time_utc", "latitude", "longitude",
                "depth_km", "magnitude", "is_eligible");

        WindowModelResolver resolver = new WindowModelResolver(windowConfig);

        // Filter mainshocks according to baseline criteria
        Column isEligibleCol = col("is_eligible");
        Column depthCol = col("depth_km");
        Column magCol = col("magnitude");
        Column timeCol = col("event_time_utc");

        Column mainshockCondition = isEligibleCol
                .and(depthCol.isNotNull())
                .and(depthCol.geq(lit(selectionConfig.minDepthKm())))
                .and(depthCol.leq(lit(selectionConfig.maxDepthKm())))
                .and(magCol.isNotNull())
                .and(magCol.gt(lit(selectionConfig.minMagnitudeExclusive())))
                .and(timeCol.isNotNull())
                .and(timeCol.geq(lit(Timestamp.from(selectionConfig.periodStartUtc()))))
                .and(timeCol.lt(lit(Timestamp.from(selectionConfig.periodEndUtcExclusive()))));

        Dataset<Row> mainshocksFiltered = auditedEvents.filter(mainshockCondition);
        long mainshockCount = mainshocksFiltered.count();

        if (mainshockCount == 0) {
            return createEmptyResult(spark, selectionConfig, windowConfig, mcValue, datasetId, createdAtUtc);
        }

        // Collect distinct mainshock event IDs
        List<Row> mainshockIdRows = mainshocksFiltered.select("canonical_event_id").collectAsList();
        Set<String> mainshockIdSet = new HashSet<>(mainshockIdRows.size());
        for (Row r : mainshockIdRows) {
            mainshockIdSet.add(r.getString(0));
        }

        // Register UDFs for window dimensions
        UserDefinedFunction preHoursUdf = udf((Double mag, Double lat) -> resolver.resolve(mag, lat).preWindowHours(), DataTypes.DoubleType);
        UserDefinedFunction postHoursUdf = udf((Double mag, Double lat) -> resolver.resolve(mag, lat).postWindowHours(), DataTypes.DoubleType);
        UserDefinedFunction radiusUdf = udf((Double mag, Double lat) -> resolver.resolve(mag, lat).searchRadiusKm(), DataTypes.DoubleType);
        UserDefinedFunction deltaLatUdf = udf((Double mag, Double lat) -> resolver.resolve(mag, lat).deltaLatDeg(), DataTypes.DoubleType);
        UserDefinedFunction deltaLonUdf = udf((Double mag, Double lat) -> resolver.resolve(mag, lat).deltaLonDeg(), DataTypes.DoubleType);

        // Enrich mainshocks with window bounds
        Dataset<Row> mainshocksWithWindows = mainshocksFiltered
                .withColumn("mainshock_event_id", col("canonical_event_id"))
                .withColumn("mainshock_time_utc", col("event_time_utc"))
                .withColumn("mainshock_lat", col("latitude"))
                .withColumn("mainshock_lon", col("longitude"))
                .withColumn("mainshock_depth_km", col("depth_km"))
                .withColumn("mainshock_magnitude", col("magnitude"))
                .withColumn("pre_window_hours", preHoursUdf.apply(col("magnitude"), col("latitude")))
                .withColumn("post_window_hours", postHoursUdf.apply(col("magnitude"), col("latitude")))
                .withColumn("search_radius_km", radiusUdf.apply(col("magnitude"), col("latitude")))
                .withColumn("delta_lat_deg", deltaLatUdf.apply(col("magnitude"), col("latitude")))
                .withColumn("delta_lon_deg", deltaLonUdf.apply(col("magnitude"), col("latitude")))
                .withColumn("window_start_utc",
                        from_unixtime(unix_timestamp(col("mainshock_time_utc")).minus(col("pre_window_hours").multiply(3600.0))).cast(DataTypes.TimestampType))
                .withColumn("window_end_utc",
                        from_unixtime(unix_timestamp(col("mainshock_time_utc")).plus(col("post_window_hours").multiply(3600.0))).cast(DataTypes.TimestampType))
                .withColumn("min_lat", col("mainshock_lat").minus(col("delta_lat_deg")))
                .withColumn("max_lat", col("mainshock_lat").plus(col("delta_lat_deg")))
                .withColumn("min_lon", col("mainshock_lon").minus(col("delta_lon_deg")))
                .withColumn("max_lon", col("mainshock_lon").plus(col("delta_lon_deg")));

        // Prepare candidate pool from eligible events:
        // Candidates must have magnitude >= mcValue OR be one of the mainshocks themselves
        Column isMainshockEvent = col("canonical_event_id").isin(mainshockIdSet.toArray());
        Column candidateCondition = col("is_eligible")
                .and(col("magnitude").isNotNull())
                .and(col("magnitude").geq(lit(mcValue)).or(isMainshockEvent));

        Dataset<Row> candidates = auditedEvents.filter(candidateCondition)
                .select(
                        col("canonical_event_id").alias("candidate_event_id"),
                        col("event_time_utc").alias("candidate_time_utc"),
                        col("latitude").alias("candidate_latitude"),
                        col("longitude").alias("candidate_longitude"),
                        col("depth_km").alias("candidate_depth_km"),
                        col("magnitude").alias("candidate_magnitude")
                );

        // Range Join: Time Window AND Bounding Box Prefilter
        Column rangeJoinCondition = candidates.col("candidate_time_utc").geq(mainshocksWithWindows.col("window_start_utc"))
                .and(candidates.col("candidate_time_utc").leq(mainshocksWithWindows.col("window_end_utc")))
                .and(candidates.col("candidate_latitude").geq(mainshocksWithWindows.col("min_lat")))
                .and(candidates.col("candidate_latitude").leq(mainshocksWithWindows.col("max_lat")))
                .and(candidates.col("candidate_longitude").geq(mainshocksWithWindows.col("min_lon")))
                .and(candidates.col("candidate_longitude").leq(mainshocksWithWindows.col("max_lon")));

        Dataset<Row> rangeJoined = mainshocksWithWindows.join(candidates, rangeJoinCondition);

        // Fine distance calculations (local planar projection relative to mainshock)
        Column cosLat = cos(radians(rangeJoined.col("mainshock_lat")));
        Column dx = lit(WindowModelResolver.KM_PER_DEG_LON_EQUATOR).multiply(cosLat)
                .multiply(rangeJoined.col("candidate_longitude").minus(rangeJoined.col("mainshock_lon")));
        Column dy = lit(WindowModelResolver.KM_PER_DEG_LAT)
                .multiply(rangeJoined.col("candidate_latitude").minus(rangeJoined.col("mainshock_lat")));
        Column dz = rangeJoined.col("candidate_depth_km").minus(rangeJoined.col("mainshock_depth_km"));
        Column distHoriz = sqrt(dx.multiply(dx).plus(dy.multiply(dy)));
        Column dist3d = sqrt(dx.multiply(dx).plus(dy.multiply(dy)).plus(dz.multiply(dz)));

        // Filter within search radius (with numerical tolerance 1e-5 km)
        Column withinRadius = distHoriz.leq(rangeJoined.col("search_radius_km").plus(lit(1e-5)));
        Dataset<Row> withinDistance = rangeJoined.filter(withinRadius);

        // Mainshock identification and zero-vector guarantee
        Column isMainshock = withinDistance.col("candidate_event_id").equalTo(withinDistance.col("mainshock_event_id"));

        Column relativeRole = when(isMainshock, lit("MAINSHOCK"))
                .when(withinDistance.col("candidate_time_utc").lt(withinDistance.col("mainshock_time_utc")), lit("PRE"))
                .otherwise(lit("POST"));

        // Delta time hours with signed conventions (PRE < 0, MAINSHOCK = 0, POST > 0)
        Column dtHours = when(isMainshock, lit(0.0))
                .otherwise(
                        withinDistance.col("candidate_time_utc").cast(DataTypes.LongType)
                                .minus(withinDistance.col("mainshock_time_utc").cast(DataTypes.LongType))
                                .cast(DataTypes.DoubleType)
                                .divide(3600.0)
                );

        Column finalDx = when(isMainshock, lit(0.0)).otherwise(dx);
        Column finalDy = when(isMainshock, lit(0.0)).otherwise(dy);
        Column finalDz = when(isMainshock, lit(0.0)).otherwise(dz);
        Column finalDist3d = when(isMainshock, lit(0.0)).otherwise(dist3d);

        // Build candidate snapshot (window stage of ml.sequence_candidate_snapshot)
        Dataset<Row> candidateSnapshot = withinDistance
                .select(
                        lit("1.0").alias("schema_version"),
                        lit(datasetId).alias("dataset_id"),
                        col("mainshock_event_id"),
                        col("candidate_event_id"),
                        col("candidate_time_utc"),
                        col("candidate_latitude"),
                        col("candidate_longitude"),
                        col("candidate_depth_km"),
                        col("candidate_magnitude"),
                        col("mainshock_magnitude"),
                        relativeRole.alias("relative_time_role"),
                        isMainshock.alias("is_mainshock"),
                        finalDx.alias("dx_km"),
                        finalDy.alias("dy_km"),
                        finalDz.alias("dz_km"),
                        dtHours.alias("delta_time_hours"),
                        finalDist3d.alias("distance_3d_km"),
                        lit(mcValue).alias("mc_value"),
                        lit(windowConfig.windowModelVersion()).alias("window_model_version"),
                        lit(Timestamp.from(createdAtUtc)).alias("created_at_utc")
                )
                .dropDuplicates("dataset_id", "mainshock_event_id", "candidate_event_id");

        long totalCandidateRowCount = candidateSnapshot.count();
        long distinctCandidateEventCount = candidateSnapshot.select("candidate_event_id").distinct().count();

        // Window-level aggregations
        Map<String, Long> candidatesPerWindow = new TreeMap<>();
        List<Row> windowCountRows = candidateSnapshot.groupBy("mainshock_event_id").count().collectAsList();
        for (Row r : windowCountRows) {
            candidatesPerWindow.put(r.getString(0), r.getLong(1));
        }

        // Nested mainshocks per window (candidates that are also mainshocks, excluding self)
        Map<String, Integer> nestedMainshocksPerWindow = new TreeMap<>();
        List<Row> nestedRows = candidateSnapshot
                .filter(col("candidate_event_id").isin(mainshockIdSet.toArray()).and(not(col("is_mainshock"))))
                .groupBy("mainshock_event_id").count().collectAsList();
        for (Row r : nestedRows) {
            nestedMainshocksPerWindow.put(r.getString(0), (int) r.getLong(1));
        }

        // Resource guard evaluation
        Map<String, String> resourceGuardStatusPerWindow = new TreeMap<>();
        long flaggedCount = 0;
        long rejectedCount = 0;

        for (String mid : mainshockIdSet) {
            long count = candidatesPerWindow.getOrDefault(mid, 0L);
            if (count > selectionConfig.maxCandidatesPerWindow()) {
                if (selectionConfig.failOnResourceExceeded()) {
                    resourceGuardStatusPerWindow.put(mid, "REJECTED");
                    rejectedCount++;
                } else {
                    resourceGuardStatusPerWindow.put(mid, "FLAGGED");
                    flaggedCount++;
                }
            } else {
                resourceGuardStatusPerWindow.put(mid, "PASS");
            }
        }

        // Assemble mainshock snapshot (ml.mainshock_candidate_snapshot)
        Dataset<Row> windowCountsDf = spark.createDataFrame(
                candidatesPerWindow.entrySet().stream().map(e -> org.apache.spark.sql.RowFactory.create(e.getKey(), e.getValue())).toList(),
                new StructType()
                        .add("mid", DataTypes.StringType, false)
                        .add("candidate_count", DataTypes.LongType, false)
        );

        Dataset<Row> mainshocksWithCounts = mainshocksWithWindows.join(
                windowCountsDf,
                mainshocksWithWindows.col("mainshock_event_id").equalTo(windowCountsDf.col("mid")),
                "left"
        ).withColumn("candidate_count", coalesce(col("candidate_count"), lit(0L)));

        Column guardStatusCol = when(col("candidate_count").gt(lit(selectionConfig.maxCandidatesPerWindow())),
                lit(selectionConfig.failOnResourceExceeded() ? "REJECTED" : "FLAGGED"))
                .otherwise(lit("PASS"));

        Column reasonCodeCol = when(col("candidate_count").gt(lit(selectionConfig.maxCandidatesPerWindow())),
                lit("ML_RESOURCE_LIMIT_EXCEEDED"))
                .otherwise(lit(null).cast(DataTypes.StringType));

        Dataset<Row> mainshockSnapshot = mainshocksWithCounts
                .select(
                        lit("1.0").alias("schema_version"),
                        lit(datasetId).alias("dataset_id"),
                        col("mainshock_event_id"),
                        col("mainshock_time_utc").alias("event_time_utc"),
                        col("mainshock_lat").alias("latitude"),
                        col("mainshock_lon").alias("longitude"),
                        col("mainshock_depth_km").alias("depth_km"),
                        col("mainshock_magnitude").alias("magnitude"),
                        lit(mcValue).alias("mc_value"),
                        lit(selectionConfig.selectionRuleVersion()).alias("selection_rule_version"),
                        lit(windowConfig.windowModelVersion()).alias("window_model_version"),
                        col("pre_window_hours"),
                        col("post_window_hours"),
                        col("search_radius_km"),
                        col("candidate_count"),
                        guardStatusCol.alias("resource_guard_status"),
                        reasonCodeCol.alias("resource_reason_code"),
                        lit(Timestamp.from(createdAtUtc)).alias("created_at_utc")
                );

        MainshockWindowResult result = new MainshockWindowResult(
                mainshockSnapshot,
                candidateSnapshot,
                mainshockCount,
                totalCandidateRowCount,
                distinctCandidateEventCount,
                Collections.unmodifiableMap(candidatesPerWindow),
                Collections.unmodifiableMap(nestedMainshocksPerWindow),
                Collections.unmodifiableMap(resourceGuardStatusPerWindow),
                flaggedCount,
                rejectedCount,
                datasetId,
                selectionConfig,
                windowConfig,
                mcValue,
                createdAtUtc
        );

        return result;
    }

    private MainshockWindowResult createEmptyResult(
            SparkSession spark,
            MainshockSelectionConfig selectionConfig,
            WindowModelConfig windowConfig,
            double mcValue,
            String datasetId,
            Instant createdAtUtc
    ) {
        StructType mainshockSchema = new StructType()
                .add("schema_version", DataTypes.StringType, false)
                .add("dataset_id", DataTypes.StringType, false)
                .add("mainshock_event_id", DataTypes.StringType, false)
                .add("event_time_utc", DataTypes.TimestampType, false)
                .add("latitude", DataTypes.DoubleType, false)
                .add("longitude", DataTypes.DoubleType, false)
                .add("depth_km", DataTypes.DoubleType, false)
                .add("magnitude", DataTypes.DoubleType, false)
                .add("mc_value", DataTypes.DoubleType, false)
                .add("selection_rule_version", DataTypes.StringType, false)
                .add("window_model_version", DataTypes.StringType, false)
                .add("pre_window_hours", DataTypes.DoubleType, false)
                .add("post_window_hours", DataTypes.DoubleType, false)
                .add("search_radius_km", DataTypes.DoubleType, false)
                .add("candidate_count", DataTypes.LongType, false)
                .add("resource_guard_status", DataTypes.StringType, false)
                .add("resource_reason_code", DataTypes.StringType, true)
                .add("created_at_utc", DataTypes.TimestampType, false);

        StructType candidateSchema = new StructType()
                .add("schema_version", DataTypes.StringType, false)
                .add("dataset_id", DataTypes.StringType, false)
                .add("mainshock_event_id", DataTypes.StringType, false)
                .add("candidate_event_id", DataTypes.StringType, false)
                .add("candidate_time_utc", DataTypes.TimestampType, false)
                .add("candidate_latitude", DataTypes.DoubleType, false)
                .add("candidate_longitude", DataTypes.DoubleType, false)
                .add("candidate_depth_km", DataTypes.DoubleType, false)
                .add("candidate_magnitude", DataTypes.DoubleType, false)
                .add("mainshock_magnitude", DataTypes.DoubleType, false)
                .add("relative_time_role", DataTypes.StringType, false)
                .add("is_mainshock", DataTypes.BooleanType, false)
                .add("dx_km", DataTypes.DoubleType, false)
                .add("dy_km", DataTypes.DoubleType, false)
                .add("dz_km", DataTypes.DoubleType, false)
                .add("delta_time_hours", DataTypes.DoubleType, false)
                .add("distance_3d_km", DataTypes.DoubleType, false)
                .add("mc_value", DataTypes.DoubleType, false)
                .add("window_model_version", DataTypes.StringType, false)
                .add("created_at_utc", DataTypes.TimestampType, false);

        Dataset<Row> emptyMainshock = spark.createDataFrame(Collections.emptyList(), mainshockSchema);
        Dataset<Row> emptyCandidate = spark.createDataFrame(Collections.emptyList(), candidateSchema);

        return new MainshockWindowResult(
                emptyMainshock,
                emptyCandidate,
                0L,
                0L,
                0L,
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                0L,
                0L,
                datasetId,
                selectionConfig,
                windowConfig,
                mcValue,
                createdAtUtc
        );
    }

    private static void requireColumns(Dataset<Row> df, String... colNames) {
        Set<String> existing = new HashSet<>(Arrays.asList(df.columns()));
        for (String col : colNames) {
            if (!existing.contains(col)) {
                throw new IllegalArgumentException("Missing required column in Dataset: " + col);
            }
        }
    }
}
