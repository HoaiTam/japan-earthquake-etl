package ie212.earthquake.spark.gold;

import java.nio.file.*;
import java.util.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import static org.apache.spark.sql.functions.*;

/** Tiny offline fixture harness runnable with the EXISTING Java17 Spark runtime. No catalog config. */
public final class PortableImporterFixtureJob {
    @FunctionalInterface interface Action { void run() throws Exception; }
    private PortableImporterFixtureJob() { }
    private static void check(boolean ok, String reason) { if (!ok) throw new AssertionError(reason); }
    private static void fail(Action action, String reason) throws Exception {
        try { action.run(); throw new AssertionError("Expected " + reason); }
        catch (java.io.IOException e) { check(e.getMessage().contains(reason), "Wrong failure: " + e.getMessage()); }
    }
    static Dataset<Row> events(SparkSession spark) {
        var schema = new StructType().add("canonical_event_id", "string").add("primary_observation_id", "string")
                .add("canonical_model_version", "string").add("event_time_utc", "timestamp").add("source_count", "integer")
                .add("has_usgs", "boolean").add("has_jma", "boolean").add("quality_status", "string");
        return spark.read().schema(schema).json(spark.createDataset(List.of(
                "{\"canonical_event_id\":\"c1\",\"primary_observation_id\":\"j1\",\"canonical_model_version\":\"v1\",\"event_time_utc\":\"2013-07-31T23:59:59Z\",\"source_count\":1,\"has_usgs\":false,\"has_jma\":true,\"quality_status\":\"VALID\"}",
                "{\"canonical_event_id\":\"c2\",\"primary_observation_id\":\"j2\",\"canonical_model_version\":\"v1\",\"event_time_utc\":\"2013-08-01T00:00:01Z\",\"source_count\":2,\"has_usgs\":true,\"has_jma\":true,\"quality_status\":\"VALID\"}"), Encoders.STRING()));
    }
    static Dataset<Row> bridge(SparkSession spark) {
        var schema = new StructType().add("canonical_event_id", "string").add("source_observation_id", "string")
                .add("source_system", "string").add("source_record_key", "string").add("bronze_manifest_id", "string")
                .add("raw_object_uri", "string").add("is_primary", "boolean").add("source_link_id", "string");
        return spark.createDataFrame(Arrays.asList(RowFactory.create("c1", "j1", "JMA_BULLETIN", "k-j1", "m1", "s3://lake/bronze/j1", true, null),
                RowFactory.create("c2", "j2", "JMA_BULLETIN", "k-j2", "m2", "s3://lake/bronze/j2", true, "L"),
                RowFactory.create("c2", "u2", "USGS", "k-u2", "m3", "s3://lake/bronze/u2", false, "L")), schema);
    }
    static Dataset<Row> links(SparkSession spark) {
        var schema = new StructType().add("source_link_id", "string").add("left_observation_id", "string")
                .add("right_observation_id", "string").add("link_decision", "string");
        return spark.createDataFrame(List.of(RowFactory.create("L", "u2", "j2", "ACCEPTED")), schema);
    }
    public static int runAll(SparkSession spark, Path root) throws Exception {
        var e = events(spark); var b = bridge(spark); var l = links(spark); int cases = 0;
        try (var valid = PortableCatalogReconciler.reconcile(e, b, l, 2, 3, 1)) {
            check(valid.report().path("balanced").asBoolean() && valid.report().path("counts").path("canonical_events").asLong() == 2, "Valid boundary data failed"); cases++;
        }
        try (var duplicate = PortableCatalogReconciler.reconcile(e.unionByName(e), b.unionByName(b), l.unionByName(l), 4, 6, 2)) {
            check(duplicate.report().path("counts").path("exact_event_duplicates").asLong() == 2
                    && duplicate.report().path("counts").path("exact_bridge_duplicates").asLong() == 3, "Exact duplicate accounting failed"); cases++;
        }
        try (var empty = PortableCatalogReconciler.reconcile(e.limit(0), b.limit(0), l.limit(0), 0, 0, 0)) {
            check(empty.events().count() == 0, "Valid empty input failed"); cases++;
        }
        fail(() -> PortableCatalogReconciler.reconcile(e.withColumn("canonical_event_id", lit("c2")), b, l, 2, 3, 1), "BOUNDARY_CANONICAL_CONFLICT"); cases++;
        fail(() -> PortableCatalogReconciler.reconcile(e, b.withColumn("source_observation_id", when(col("source_observation_id").equalTo("u2"), "j1").otherwise(col("source_observation_id"))), l, 2, 3, 1), "BOUNDARY_OBSERVATION_CONFLICT"); cases++;
        fail(() -> PortableCatalogReconciler.reconcile(e, b.withColumn("source_record_key", when(col("source_observation_id").equalTo("j2"), "k-j1").otherwise(col("source_record_key"))), l, 2, 3, 1), "GLOBAL_REVISION_CONFLICT"); cases++;
        fail(() -> PortableCatalogReconciler.reconcile(e.withColumn("primary_observation_id", lit(null).cast("string")), b, l, 2, 3, 1), "INVALID_GLOBAL_EVENT"); cases++;
        fail(() -> PortableCatalogReconciler.reconcile(e, b, l.withColumn("left_observation_id", lit("missing")), 2, 3, 1), "GLOBAL_LINK_ENDPOINT_MISSING"); cases++;
        fail(() -> PortableCatalogReconciler.reconcile(e, b, l.withColumn("right_observation_id", lit("j1")), 2, 3, 1), "GLOBAL_LINK_DECISION_MEMBERSHIP_CONFLICT"); cases++;
        fail(() -> PortableCatalogReconciler.reconcile(e, b, l.withColumn("link_decision", lit("AMBIGUOUS")), 2, 3, 1), "GLOBAL_LINK_DECISION_MEMBERSHIP_CONFLICT"); cases++;
        fail(() -> PortableCatalogReconciler.reconcile(e, b, l, 3, 3, 1), "GLOBAL_INPUT_COUNT_PIN_MISMATCH"); cases++;
        var observation = spark.createDataFrame(List.of(RowFactory.create("original", "s3://lake/old", "a")),
                new StructType().add("bronze_manifest_id", "string").add("raw_object_uri", "string").add("raw_sha256", "string"));
        var map = spark.createDataFrame(List.of(RowFactory.create("original", "s3://lake/old", "a", "physical", "s3://lake/new")),
                new StructType().add("_original_id", "string").add("_original_uri", "string").add("_original_sha", "string").add("_physical_id", "string").add("_physical_uri", "string"));
        var relocated = PortableCatalogImportJob.relocate(observation, map).head();
        check(relocated.getAs("bronze_manifest_id").equals("physical") && relocated.getAs("raw_sha256").equals("a"), "Lineage relocation changed content identity"); cases++;
        fail(() -> PortableCatalogImportJob.relocate(observation.withColumn("raw_sha256", lit("wrong")), map), "OBSERVATION_PHYSICAL_LINEAGE_NOT_RESOLVED"); cases++;
        Files.createDirectories(root);
        var descriptor = PortablePreparedCatalog.describeDataset(root, root.resolve("parquet-fixture"), e);
        check(PortablePreparedCatalog.verifyDataset(root, descriptor) == 2, "Independent footer readback failed"); cases++;
        spark.catalog().clearCache(); return cases;
    }
    public static void main(String[] args) throws Exception {
        if (args.length != 1) throw new IllegalArgumentException("NEW_FIXTURE_ROOT_REQUIRED"); Path root = Path.of(args[0]);
        if (Files.exists(root)) throw new IllegalArgumentException("NEW_FIXTURE_ROOT_REQUIRED");
        var spark = SparkSession.builder().master("local[1]").appName("dat03-portable-component-fixtures")
                .config("spark.ui.enabled", "false").config("spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", "1")
                .config("spark.sql.adaptive.enabled", "false").config("spark.sql.codegen.wholeStage", "false").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        try { int cases = runAll(spark, root); System.out.println("event=portable_import_fixtures_passed cases=" + cases + " synthetic_component_qa=true gold_published=false"); }
        finally { spark.stop(); }
    }
}
