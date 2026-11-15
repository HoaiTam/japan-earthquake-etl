package ie212.earthquake.spark.ml;

import static org.apache.spark.sql.functions.*;
import static ie212.earthquake.spark.ml.FeatureExportBundle.*;
import java.nio.file.*;
import java.time.Instant;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import com.fasterxml.jackson.databind.JsonNode;

/** Synthetic resource benchmark only: never a source/Gold/export or scientific acceptance. */
public final class PreparedMl40kProfileJob {
    private PreparedMl40kProfileJob() { }
    public static void main(String[] args) throws Exception {
        require(args.length == 2, "PROFILE_ROOT_AND_RUNNER_REQUIRED"); Path root = safeRoot(Path.of(args[0])), runner = Path.of(args[1]);
        require(!Files.exists(root.resolve("result.json")), "NEW_PROFILE_RESULT_REQUIRED");
        var spark = SparkSession.builder().master("local[1]").appName("dat03-40k-component-profile").config("spark.ui.enabled", "false")
                .config("spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", "2").config("spark.sql.adaptive.enabled", "false").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        try {
            require("17".equals(System.getProperty("java.specification.version")) && spark.version().startsWith("3.5."), "PROFILE_PINNED_RUNTIME_REQUIRED");
            JsonNode scaling = JSON.readTree(Files.readAllBytes(root.resolve("feature-scaling.json")));
            var r = JSON.createObjectNode().put("version", PreparedDatasetBuildJob.VERSION).put("resource_profile_version", PreparedDatasetBuildJob.PROFILE)
                    .put("runner_sha256", WindowArtifactInput.sha(runner)).put("observation_cutoff_utc", "2026-11-15T00:00:00Z");
            var config = PreparedDatasetBuildJob.config(r, "REPRODUCTION", scaling);
            var publication = JSON.createObjectNode().put("verify_status", "PASSED").put("gold_snapshot_id", 17L)
                    .put("gold_table_name", "iceberg.capacity_fixture.earthquake_event_current").put("physical_gold_table_name", "iceberg.capacity_fixture.event_current")
                    .put("gold_publication_id", "pub_" + "a".repeat(64)).put("publication_bundle_sha256", "a".repeat(64));
            var parent = new DatasetIdentityPlan(publication, config, "synthetic-profile", Instant.parse("2026-11-15T00:00:00Z")).manifest();
            var input = spark.range(40000).select(format_string("evt_profile_%05d", col("id")).alias("canonical_event_id"),
                    timestamp_seconds(lit(1137283200L).plus(col("id"))).alias("event_time_utc"), lit(35.0).alias("latitude"), lit(140.0).alias("longitude"),
                    lit(80.0).alias("depth_km"), when(col("id").equalTo(0), lit(6.0)).otherwise(lit(1.5)).alias("magnitude"), lit(true).alias("is_eligible"))
                    .persist(StorageLevel.DISK_ONLY());
            var engine = new MainshockWindowEngine(); var window = WindowModelConfig.expandedV1(); Instant created = Instant.parse(parent.path("created_at_utc").asText());
            System.out.println("event=prepared_ml_profile_phase step=window_equivalence");
            var old = engine.execute(input, MainshockSelectionConfig.reproductionBaseline(), window, 1.0, parent.path("dataset_id").asText(), created, WindowCoveragePolicy.STRICT);
            var next = engine.execute(input, MainshockWindowJob.selectionConfig(parent), window, 1.0, parent.path("dataset_id").asText(), created, WindowCoveragePolicy.STRICT);
            require(old.mainshockSnapshot().filter(col("resource_guard_status").equalTo("FLAGGED")).count() == 1
                    && next.mainshockSnapshot().filter(col("resource_guard_status").equalTo("PASS")).count() == 1, "PROFILE_GUARDS_NOT_MEASURED");
            require(old.candidateSnapshot().count() == 40000 && next.candidateSnapshot().count() == 40000
                    && MainshockWindowJob.fingerprint(old.candidateSnapshot()).equals(MainshockWindowJob.fingerprint(next.candidateSnapshot())), "PROFILE_CHANGED_CANDIDATE_SEMANTICS");
            var coverage = JSON.createObjectNode().put("coverage_version", ie212.earthquake.spark.gold.PublishedPreparedCoverage.VERSION)
                    .put("status", "COMPLETE").put("required_month_count", 225).put("source_system", "JMA_BULLETIN").put("source_cutoff_utc", "2026-10-25T00:00:00Z");
            for (String k : java.util.List.of("prepared_catalog_sha256", "prepared_coverage_sha256", "provenance_sha256", "published_commit_sha256", "publication_sha256", "gold_request_sha256")) coverage.put(k, "b".repeat(64));
            String mcJson = "{\"applied_mc\":1.0}";
            var mc = JSON.createObjectNode().put("scope", "FULL_PINNED_SPLIT").put("mc_value", 1.0).put("mc_config_json", mcJson).put("mc_config_sha256", DatasetIdentityPlan.sha(mcJson));
            for (String k : java.util.List.of("dataset_id", "gold_snapshot_id", "period_start_utc", "period_end_utc_exclusive")) mc.set(k, parent.path(k));
            System.out.println("event=prepared_ml_profile_phase step=materialize_and_readback");
            var receipt = ResearchCollectionJob.materialize(parent, coverage, mc, next, FeatureScalingConfig.fromJson(scaling), ResearchCollectionPlan.Limits.defaults(), root.resolve("prepared"), () -> {});
            require(receipt.path("feature_row_count").asLong() == 40000 && receipt.path("shard_count").asInt() == 1 && !receipt.path("official_exported").asBoolean(true), "PROFILE_SHARD_READBACK_FAILED");
            System.out.println("event=prepared_ml_profile_phase step=immutable_rerun");
            var second = ResearchCollectionJob.materialize(parent, coverage, mc, next, FeatureScalingConfig.fromJson(scaling), ResearchCollectionPlan.Limits.defaults(), root.resolve("prepared"), () -> {});
            require(second.path("reused").asBoolean(false) && second.path("manifest_sha256").equals(receipt.path("manifest_sha256"))
                    && second.path("checksums_sha256").equals(receipt.path("checksums_sha256")), "PROFILE_RERUN_CHANGED");
            Path parquet = root.resolve("prepared").resolve(receipt.path("collection_id").asText()).resolve("shard_00000/features.parquet");
            var readback = spark.read().parquet(parquet.toString());
            var identity = JSON.createObjectNode().put("dataset_id", parent.path("dataset_id").asText()).put("feature_version", scaling.path("feature_version").asText())
                    .put("scaling_config_version", scaling.path("scaling_config_version").asText());
            System.out.println("event=prepared_ml_profile_phase step=null_fault_injection");
            for (String name : java.util.List.of("schema_version", "created_at_utc")) {
                boolean rejected = false;
                try { validate(readback.withColumn(name, lit(null).cast(readback.schema().apply(name).dataType())), next.mainshockSnapshot(), identity, FeatureScalingConfig.fromJson(scaling)); }
                catch (java.io.IOException error) { require(error.getMessage().equals("DS_EXPORT_NULL_FIELD:" + name), "PROFILE_NULL_RULE_CHANGED"); rejected = true; }
                require(rejected, "PROFILE_NULL_NOT_REJECTED");
            }
            var result = JSON.createObjectNode().put("profile_version", PreparedDatasetBuildJob.PROFILE).put("status", "PROFILE_BENCHMARK_PASSED")
                    .put("runner_sha256", WindowArtifactInput.sha(runner)).put("feature_rows", 40000).put("candidate_semantics_unchanged", true)
                    .put("scaling_sha256", WindowArtifactInput.sha(root.resolve("feature-scaling.json")))
                    .put("java_version", System.getProperty("java.version")).put("spark_version", spark.version())
                    .put("synthetic_component_qa", true).put("s3_writes", false).put("scientific_approval", false).put("official_exported", false)
                    .put("feature_parquet_bytes", Files.size(parquet)).put("parquet_sha256", WindowArtifactInput.sha(parquet))
                    .put("manifest_sha256", receipt.path("manifest_sha256").asText()).put("checksums_sha256", receipt.path("checksums_sha256").asText())
                    .put("null_guard_fault_injections_passed", 2);
            immutable(root.resolve("result.json"), bytes(result)); System.out.println("event=prepared_ml_profile_passed rows=40000 synthetic_component_qa=true s3_writes=false");
        } finally { spark.stop(); }
    }
}
