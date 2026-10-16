package ie212.earthquake.spark.ml;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Real local Spark/Iceberg regression: a Trino blocker cannot expose CANDIDATE. */
class MlResultPublicationIcebergTest {
    private static final Path FIXTURE = Path.of("src/test/resources/ml/results/success");
    private static SparkSession spark;
    @TempDir static Path root;

    @BeforeAll static void start() {
        spark = SparkSession.builder().master("local[1]").appName("mli-03-publication-gate")
            .config("spark.ui.enabled", "false").config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1").config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "1").config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.codegen.wholeStage", "false").config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lake.type", "hadoop").config("spark.sql.catalog.lake.warehouse", root.resolve("warehouse").toUri().toString())
            .getOrCreate();
        if (System.getProperty("os.name").startsWith("Windows")) {
            spark.sparkContext().hadoopConfiguration().set("fs.file.impl", NioTestFileSystem.class.getName());
            spark.sparkContext().hadoopConfiguration().setBoolean("fs.file.impl.disable.cache", true);
        }
        spark.sparkContext().setLogLevel("ERROR");
    }

    @AfterAll static void stop() { if (spark != null) spark.stop(); }

    public static class NioTestFileSystem extends org.apache.hadoop.fs.RawLocalFileSystem {
        @Override public String getScheme() { return "file"; }
        @Override public void setPermission(org.apache.hadoop.fs.Path path, org.apache.hadoop.fs.permission.FsPermission mode) { }
        @Override public boolean mkdirs(org.apache.hadoop.fs.Path path, org.apache.hadoop.fs.permission.FsPermission mode)
                throws java.io.IOException { java.nio.file.Files.createDirectories(pathToFile(path).toPath()); return true; }
    }

    @Test void trinoBlockerLeavesOnlyRecoverableResultTables(@TempDir Path journal) throws Exception {
        var pins = new ResultBundleValidator.DatasetPins("registry/ds.json", "a".repeat(64), "b".repeat(64), "r1", "c1", "m1");
        var context = new ResultBundleValidator.Context("ds_fixture", "EXPORTED", "exp_fixture", "WINDOW", "1",
            ResultBundleContractTest.CANDIDATES, null, pins, null);
        String namespace = "lake.ml_qa_gate";
        var writer = new SparkMlIcebergWriter(spark, namespace, () -> { }, journal);
        assertEquals("IMP_TRINO_QUALITY_BLOCKER", assertThrows(IOException.class, () ->
            MlResultImportJob.importBundle(FIXTURE, journal, context, writer,
                (commit, value, fingerprint) -> { throw new IOException("IMP_TRINO_QUALITY_BLOCKER"); }, namespace)).getMessage());
        assertTrue(spark.table(namespace + ".sequence_membership").count() > 0);
        assertFalse(spark.catalog().tableExists(namespace + ".experiment_run")
            && spark.table(namespace + ".experiment_run").filter("experiment_status = 'CANDIDATE'").count() > 0);
    }

    @Test void retryReusesMembershipSnapshotAfterUnknownOutcome(@TempDir Path journal) throws Exception {
        var pins = new ResultBundleValidator.DatasetPins("registry/ds.json", "c".repeat(64), "d".repeat(64), "r2", "c2", "m2");
        var context = new ResultBundleValidator.Context("ds_fixture", "EXPORTED", "exp_fixture", "WINDOW", "1",
            ResultBundleContractTest.CANDIDATES, null, pins, null);
        String namespace = "lake.ml_qa_retry";
        var failing = new SparkMlIcebergWriter(spark, namespace, () -> { }, journal,
            table -> { if (table.equals("sequence_membership")) throw new IllegalStateException("IMP_COMMIT_OUTCOME_UNKNOWN"); });
        assertThrows(IllegalStateException.class, () -> MlResultImportJob.importBundle(FIXTURE, journal, context, failing,
            (commit, value, fingerprint) -> Map.of("sequence_membership", 2L, "sequence_summary", 1L), namespace));
        assertEquals("IMPORT_VALIDATING", new ObjectMapper().readTree(java.nio.file.Files.readString(
            journal.resolve("experiment-run-exp_fixture.json"))).path("experiment_status").asText());
        var retry = new SparkMlIcebergWriter(spark, namespace, () -> { }, journal);
        var report = MlResultImportJob.importBundle(FIXTURE, journal, context, retry,
            (commit, value, fingerprint) -> Map.of("sequence_membership", 2L, "sequence_summary", 1L), namespace);
        assertEquals("CANDIDATE", report.path("experiment_status").asText());
        assertEquals(1L, spark.table(namespace + ".sequence_membership.snapshots").count());
        var columns = java.util.Arrays.stream(spark.table(namespace + ".experiment_run").schema().fieldNames()).toList();
        for (String required : java.util.List.of("experiment_config_json", "experiment_config_sha256",
                "requirements_lock_sha256", "runtime_environment_json", "import_run_id", "created_at_utc",
                "result_ready_at_utc", "imported_at_utc", "membership_snapshot_id", "summary_snapshot_id"))
            assertTrue(columns.contains(required), "missing experiment_run field: " + required);
    }
}
