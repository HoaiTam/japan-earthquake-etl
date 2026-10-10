package ie212.earthquake.spark.silver;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * Standard Spark StructType schemas for Silver tier datasets (CON-03 1.0).
 */
public final class SilverSchemas {

    public static final StructType OBSERVATION_SCHEMA = new StructType()
            .add("schema_version", DataTypes.StringType, false)
            .add("source_observation_id", DataTypes.StringType, false)
            .add("source_system", DataTypes.StringType, false)
            .add("source_record_key", DataTypes.StringType, false)
            .add("source_revision_key", DataTypes.StringType, false)
            .add("source_updated_at_utc", DataTypes.TimestampType, true)
            .add("catalog_release", DataTypes.StringType, true)
            .add("catalog_release_at_utc", DataTypes.TimestampType, true)
            .add("is_current_source_revision", DataTypes.BooleanType, false)
            .add("event_time_utc", DataTypes.TimestampType, false)
            .add("event_time_jst", DataTypes.TimestampType, false)
            .add("event_date_utc", DataTypes.DateType, false)
            .add("event_date_jst", DataTypes.DateType, false)
            .add("event_year_utc", DataTypes.IntegerType, false)
            .add("event_month_utc", DataTypes.IntegerType, false)
            .add("latitude", DataTypes.DoubleType, false)
            .add("longitude", DataTypes.DoubleType, false)
            .add("depth_km", DataTypes.DoubleType, true)
            .add("magnitude", DataTypes.DoubleType, true)
            .add("magnitude_type", DataTypes.StringType, true)
            .add("event_type_code", DataTypes.StringType, false)
            .add("place_name", DataTypes.StringType, true)
            .add("tsunami_flag", DataTypes.BooleanType, true)
            .add("alert_level", DataTypes.StringType, true)
            .add("significance", DataTypes.IntegerType, true)
            .add("max_intensity_code", DataTypes.StringType, true)
            .add("determining_agency_code", DataTypes.StringType, true)
            .add("catalog_era", DataTypes.StringType, true)
            .add("source_status", DataTypes.StringType, true)
            .add("source_url", DataTypes.StringType, true)
            .add("is_in_study_area", DataTypes.BooleanType, false)
            .add("quality_status", DataTypes.StringType, false)
            .add("quality_flags", DataTypes.createArrayType(DataTypes.StringType), false)
            .add("bronze_manifest_id", DataTypes.StringType, false)
            .add("raw_object_uri", DataTypes.StringType, false)
            .add("raw_sha256", DataTypes.StringType, false)
            .add("raw_record_locator", DataTypes.StringType, false)
            .add("raw_record_hash", DataTypes.StringType, false)
            .add("ingest_run_id", DataTypes.StringType, false)
            .add("parser_name", DataTypes.StringType, false)
            .add("parser_version", DataTypes.StringType, false)
            .add("processed_at_utc", DataTypes.TimestampType, false);

    public static final StructType REJECT_SCHEMA = new StructType()
            .add("schema_version", DataTypes.StringType, false)
            .add("source_system", DataTypes.StringType, false)
            .add("source_record_key_candidate", DataTypes.StringType, true)
            .add("bronze_manifest_id", DataTypes.StringType, false)
            .add("raw_object_uri", DataTypes.StringType, false)
            .add("raw_sha256", DataTypes.StringType, false)
            .add("raw_record_locator", DataTypes.StringType, false)
            .add("raw_record_hash", DataTypes.StringType, false)
            .add("reject_stage", DataTypes.StringType, false)
            .add("reject_reason_codes", DataTypes.createArrayType(DataTypes.StringType), false)
            .add("ingest_run_id", DataTypes.StringType, false)
            .add("parser_version", DataTypes.StringType, false)
            .add("rejected_at_utc", DataTypes.TimestampType, false);

    public static final StructType SOURCE_LINK_SCHEMA = new StructType()
            .add("source_link_id", DataTypes.StringType, false)
            .add("left_observation_id", DataTypes.StringType, false)
            .add("right_observation_id", DataTypes.StringType, false)
            .add("time_delta_seconds", DataTypes.DoubleType, false)
            .add("distance_km", DataTypes.DoubleType, false)
            .add("depth_delta_km", DataTypes.DoubleType, true)
            .add("magnitude_delta", DataTypes.DoubleType, true)
            .add("match_score", DataTypes.DoubleType, false)
            .add("link_decision", DataTypes.StringType, false)
            .add("decision_reason_codes", DataTypes.createArrayType(DataTypes.StringType), false)
            .add("match_model_version", DataTypes.StringType, false)
            .add("decided_at_utc", DataTypes.TimestampType, false);

    public static final StructType CANONICAL_MEMBERSHIP_SCHEMA = new StructType()
            .add("canonical_event_id", DataTypes.StringType, false)
            .add("source_observation_id", DataTypes.StringType, false)
            .add("membership_status", DataTypes.StringType, false)
            .add("source_link_id", DataTypes.StringType, true)
            .add("canonical_model_version", DataTypes.StringType, false)
            .add("assigned_at_utc", DataTypes.TimestampType, false);

    private SilverSchemas() {
    }

    /**
     * Creates a Spark Dataset of Rows from a list of SilverObservation records.
     */
    public static Dataset<Row> toObservationDataset(SparkSession spark, List<SilverObservation> observations) {
        List<Row> rows = observations.stream().map(SilverObservation::toRow).collect(Collectors.toList());
        return spark.createDataFrame(rows, OBSERVATION_SCHEMA);
    }

    /**
     * Creates a Spark Dataset of Rows from a list of SilverRejectRecord records.
     */
    public static Dataset<Row> toRejectDataset(SparkSession spark, List<SilverRejectRecord> rejects) {
        List<Row> rows = rejects.stream().map(SilverRejectRecord::toRow).collect(Collectors.toList());
        return spark.createDataFrame(rows, REJECT_SCHEMA);
    }

    /**
     * Creates a Spark Dataset of Rows from a list of SilverSourceLink records.
     */
    public static Dataset<Row> toSourceLinkDataset(SparkSession spark, List<SilverSourceLink> links) {
        List<Row> rows = links.stream().map(SilverSourceLink::toRow).collect(Collectors.toList());
        return spark.createDataFrame(rows, SOURCE_LINK_SCHEMA);
    }

    /**
     * Creates a Spark Dataset of Rows from a list of SilverCanonicalMembership records.
     */
    public static Dataset<Row> toCanonicalMembershipDataset(SparkSession spark, List<SilverCanonicalMembership> memberships) {
        List<Row> rows = memberships.stream().map(SilverCanonicalMembership::toRow).collect(Collectors.toList());
        return spark.createDataFrame(rows, CANONICAL_MEMBERSHIP_SCHEMA);
    }
}
