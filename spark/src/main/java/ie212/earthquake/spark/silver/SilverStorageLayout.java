package ie212.earthquake.spark.silver;

import java.util.Objects;

/**
 * Standard storage layout definitions for the Silver layer (CON-03 1.0, SLV-08).
 * Manages partition prefixes, staging prefixes, publish markers, and parquet naming.
 */
public final class SilverStorageLayout {

    public static final String DEFAULT_SILVER_PREFIX = "silver";
    public static final String SOURCE_OBSERVATION_DATASET = "source_observation";
    public static final String REJECT_RECORD_DATASET = "reject_record";
    public static final String STAGING_PREFIX = "_staging";
    public static final String SUCCESS_MARKER_FILE = "_SUCCESS";
    public static final String MANIFEST_FILE = "manifest.json";

    private SilverStorageLayout() {
    }

    /**
     * Resolves the relative partition path for source observations:
     * source_observation/event_year_utc=YYYY/event_month_utc=MM/source_system=SOURCE
     */
    public static String observationPartitionPath(SilverPartitionKey key) {
        Objects.requireNonNull(key, "key");
        return SOURCE_OBSERVATION_DATASET + "/" + key.partitionPath();
    }

    /**
     * Resolves the staging partition path for source observations in a given run:
     * _staging/<runId>/source_observation/event_year_utc=YYYY/event_month_utc=MM/source_system=SOURCE
     */
    public static String stagingObservationPartitionPath(String runId, SilverPartitionKey key) {
        validateRunId(runId);
        Objects.requireNonNull(key, "key");
        return STAGING_PREFIX + "/" + runId + "/" + observationPartitionPath(key);
    }

    /**
     * Resolves the root staging path for a given run:
     * _staging/<runId>
     */
    public static String stagingRootPath(String runId) {
        validateRunId(runId);
        return STAGING_PREFIX + "/" + runId;
    }

    /**
     * Resolves the standard Parquet file name for a partition:
     * part-00000-<runId>.parquet
     */
    public static String parquetFileName(String runId, int partitionIndex) {
        validateRunId(runId);
        if (partitionIndex < 0) {
            throw new IllegalArgumentException("partitionIndex must not be negative: " + partitionIndex);
        }
        return String.format("part-%05d-%s.parquet", partitionIndex, runId);
    }

    /**
     * Resolves the success marker path for an observation partition:
     * source_observation/event_year_utc=YYYY/event_month_utc=MM/source_system=SOURCE/_SUCCESS
     */
    public static String successMarkerPath(SilverPartitionKey key) {
        return observationPartitionPath(key) + "/" + SUCCESS_MARKER_FILE;
    }

    /**
     * Resolves the manifest path for an observation partition:
     * source_observation/event_year_utc=YYYY/event_month_utc=MM/source_system=SOURCE/manifest.json
     */
    public static String manifestPath(SilverPartitionKey key) {
        return observationPartitionPath(key) + "/" + MANIFEST_FILE;
    }

    public static final String SOURCE_LINK_DATASET = "source_link";
    public static final String CANONICAL_MEMBERSHIP_DATASET = "canonical_membership";

    /**
     * Resolves the partition path for reject records:
     * reject_record/source_system=SOURCE/run_id=RUN_ID
     */
    public static String rejectPartitionPath(String sourceSystem, String runId) {
        Objects.requireNonNull(sourceSystem, "sourceSystem");
        validateRunId(runId);
        return REJECT_RECORD_DATASET + "/source_system=" + sourceSystem + "/run_id=" + runId;
    }

    /**
     * Resolves the staging path for reject records in a given run:
     * _staging/<runId>/reject_record/source_system=SOURCE/run_id=RUN_ID
     */
    public static String stagingRejectPartitionPath(String sourceSystem, String runId) {
        return STAGING_PREFIX + "/" + runId + "/" + rejectPartitionPath(sourceSystem, runId);
    }

    /**
     * Resolves the partition path for cross-source link records:
     * source_link/run_id=RUN_ID
     */
    public static String linkPartitionPath(String runId) {
        validateRunId(runId);
        return SOURCE_LINK_DATASET + "/run_id=" + runId;
    }

    /**
     * Resolves the staging path for source link records in a given run:
     * _staging/<runId>/source_link/run_id=RUN_ID
     */
    public static String stagingLinkPartitionPath(String runId) {
        return STAGING_PREFIX + "/" + runId + "/" + linkPartitionPath(runId);
    }

    /**
     * Resolves the success marker path for a source link partition:
     * source_link/run_id=RUN_ID/_SUCCESS
     */
    public static String linkSuccessMarkerPath(String runId) {
        return linkPartitionPath(runId) + "/" + SUCCESS_MARKER_FILE;
    }

    /**
     * Resolves the partition path for canonical membership records:
     * canonical_membership/run_id=RUN_ID
     */
    public static String membershipPartitionPath(String runId) {
        validateRunId(runId);
        return CANONICAL_MEMBERSHIP_DATASET + "/run_id=" + runId;
    }

    /**
     * Resolves the staging path for canonical membership records in a given run:
     * _staging/<runId>/canonical_membership/run_id=RUN_ID
     */
    public static String stagingMembershipPartitionPath(String runId) {
        return STAGING_PREFIX + "/" + runId + "/" + membershipPartitionPath(runId);
    }

    /**
     * Resolves the success marker path for a canonical membership partition:
     * canonical_membership/run_id=RUN_ID/_SUCCESS
     */
    public static String membershipSuccessMarkerPath(String runId) {
        return membershipPartitionPath(runId) + "/" + SUCCESS_MARKER_FILE;
    }

    private static void validateRunId(String runId) {
        Objects.requireNonNull(runId, "runId");
        if (runId.isBlank() || runId.contains("/") || runId.contains("..")) {
            throw new IllegalArgumentException("runId must be a safe, non-blank string without slashes: " + runId);
        }
    }
}
