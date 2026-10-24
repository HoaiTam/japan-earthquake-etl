package ie212.earthquake.spark.ml;

import static org.junit.jupiter.api.Assertions.*;
import static org.apache.spark.sql.functions.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.*;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.*;

/** Real local Iceberg snapshots and materialized Parquet. Synthetic data, no research approval. */
class FeatureExportBundleTest {
    @org.junit.jupiter.api.io.TempDir static Path warehouse;
    @org.junit.jupiter.api.io.TempDir Path temporary;
    static SparkSession spark; static Dataset<Row> main,features; static FeatureScalingConfig scaling;
    static final String ID = "ds_" + "a".repeat(64), ID2 = "ds_" + "b".repeat(64), SHA = "c".repeat(64);
    @BeforeAll static void start() throws Exception {
        warehouse = warehouse.toRealPath();
        spark = SparkSession.builder().master("local[1]").appName("mld05-component")
            .config("spark.ui.enabled","false").config("spark.driver.host","127.0.0.1").config("spark.driver.bindAddress","127.0.0.1")
            .config("spark.sql.session.timeZone","UTC").config("spark.sql.shuffle.partitions","1").config("spark.sql.adaptive.enabled","false")
            .config("spark.sql.codegen.wholeStage","false").config("spark.sql.parquet.outputTimestampType","TIMESTAMP_MICROS")
            .config("spark.sql.catalog.lake","org.apache.iceberg.spark.SparkCatalog").config("spark.sql.catalog.lake.type","hadoop")
            .config("spark.sql.catalog.lake.warehouse",warehouse.resolve("warehouse").toUri().toString()).getOrCreate();
        if (System.getProperty("os.name").startsWith("Windows")) {
            spark.sparkContext().hadoopConfiguration().set("fs.file.impl",NioFileSystem.class.getName());
            spark.sparkContext().hadoopConfiguration().setBoolean("fs.file.impl.disable.cache",true);
        }
        spark.sparkContext().setLogLevel("ERROR"); spark.sql("CREATE NAMESPACE lake.export_test");
        var schema = new StructType().add("canonical_event_id","string",false).add("event_time_utc","timestamp",false)
            .add("latitude","double").add("longitude","double").add("depth_km","double").add("magnitude","double").add("is_eligible","boolean");
        Instant time = Instant.parse("2000-07-01T00:00:00Z");
        var events = spark.createDataFrame(List.of(RowFactory.create("main",Timestamp.from(time),35.,140.,80.,6.,true),
            RowFactory.create("post",Timestamp.from(time.plusSeconds(3600)),35.01,140.01,81.,3.,true)),schema);
        var windows = new MainshockWindowEngine().execute(events,MainshockSelectionConfig.reproductionBaseline(),WindowModelConfig.expandedV1(),1.,ID,time);
        main = windows.mainshockSnapshot().localCheckpoint(true);
        scaling = FeatureScalingConfigTest.config("GLOBAL",50,10000);
        var result = new SpacetimeFeatureEngine().execute(main,windows.candidateSnapshot(),scaling); result.assertValid();
        features = result.features().localCheckpoint(true);
    }
    @BeforeEach void canonicalTemporaryDirectory() throws Exception {
        // macOS exposes its system temp directory through /var -> /private/var.
        // Pass the real fixture path; keep the production no-symlink guard intact.
        temporary = temporary.toRealPath();
    }
    @AfterAll static void stop() { if (spark != null) spark.stop(); }
    public static class NioFileSystem extends org.apache.hadoop.fs.RawLocalFileSystem {
        @Override public String getScheme() { return "file"; }
        @Override public boolean mkdirs(org.apache.hadoop.fs.Path path, org.apache.hadoop.fs.permission.FsPermission mode) throws java.io.IOException {
            Files.createDirectories(pathToFile(path).toPath()); return true;
        }
        @Override public void setPermission(org.apache.hadoop.fs.Path path, org.apache.hadoop.fs.permission.FsPermission mode) { }
    }
    static ObjectNode manifest() throws Exception {
        var m = FeatureExportBundle.JSON.createObjectNode().put("schema_version","1.0").put("dataset_id",ID)
            .put("feature_version",scaling.featureVersion()).put("scaling_config_version",scaling.scalingConfigVersion());
        var quality = FeatureExportBundle.validate(features,main,m,scaling); m.setAll(quality);
        m.put("dataset_status","VALIDATED").put("export_identity_sha256",SHA); return m;
    }
    @Test void materializesPortableBundleAndRerunKeepsAllBytes() throws Exception {
        var m = manifest(); var first = FeatureExportBundle.materialize(features.withColumn("private_unused",lit("must_not_export")),m,temporary.resolve("one"));
        Path dir = temporary.resolve("one/dataset_id=" + ID); var hashes = LocalFeatureJob.hashes(dir);
        assertEquals(first,FeatureExportBundle.verify(spark,dir));
        assertEquals(first,FeatureExportBundle.materialize(features,m,temporary.resolve("one")));
        assertEquals(hashes,LocalFeatureJob.hashes(dir));
        assertFalse(Files.readString(dir.resolve("dataset_manifest.json")).contains("private_unused"));
        assertEquals(4,hashes.size()); assertEquals(2,first.path("feature_row_count").asInt());
        assertFalse(Files.exists(dir.resolve("_SUCCESS.json")));
        var second = FeatureExportBundle.materialize(features.repartition(2),m,temporary.resolve("two"));
        assertEquals(first,second); assertEquals(hashes,LocalFeatureJob.hashes(temporary.resolve("two/dataset_id=" + ID)));
    }
    @Test void requiresValidatedStatusAndRejectsConflictingIdentity() throws Exception {
        var m = manifest(); m.put("dataset_status","BUILDING");
        assertThrows(java.io.IOException.class,()->FeatureExportBundle.materialize(features,m,temporary));
        assertFalse(Files.exists(temporary.resolve("dataset_id=" + ID)));
        m.put("dataset_status","VALIDATED"); FeatureExportBundle.materialize(features,m,temporary);
        m.put("export_identity_sha256","d".repeat(64));
        assertThrows(java.io.IOException.class,()->FeatureExportBundle.materialize(features,m,temporary));
    }
    @Test void corruptMissingAndUnindexedPartsAreRejectedWithoutRepair() throws Exception {
        FeatureExportBundle.materialize(features,manifest(),temporary); Path dir = temporary.resolve("dataset_id=" + ID);
        Path extra = dir.resolve("features-part-00001.parquet"); Files.writeString(extra,"unindexed");
        assertThrows(java.io.IOException.class,()->FeatureExportBundle.verify(spark,dir)); Files.delete(extra);
        Files.writeString(dir.resolve("features-part-00000.parquet"),"corrupt");
        assertThrows(java.io.IOException.class,()->FeatureExportBundle.materialize(features,manifest(),temporary));
        assertEquals("corrupt",Files.readString(dir.resolve("features-part-00000.parquet")));
    }
    @Test void checksumIndexCannotEscapeOrReferToItself() throws Exception {
        for (String line : List.of(SHA + "  ../escape", SHA + "  /escape", SHA + "  checksums.sha256", SHA + "  a\n" + SHA + "  a")) {
            Path file = Files.createTempFile(temporary,"index-",".txt"); Files.writeString(file,line);
            assertThrows(java.io.IOException.class,()->FeatureExportBundle.index(file));
        }
    }
    @Test void wrongScaledValueNullDuplicateMissingSelfMcAndResourcesBlock() throws Exception {
        var m = manifest();
        assertThrows(Exception.class,()->FeatureExportBundle.validate(features.withColumn("x_scaled",lit(4.)),main,m,scaling));
        assertThrows(Exception.class,()->FeatureExportBundle.validate(features.withColumn("candidate_magnitude",lit(Double.NaN)),main,m,scaling));
        assertThrows(Exception.class,()->FeatureExportBundle.validate(features.withColumn("candidate_time_utc",lit(null).cast("timestamp")),main,m,scaling));
        assertThrows(Exception.class,()->FeatureExportBundle.validate(features.union(features),main,m,scaling));
        assertThrows(Exception.class,()->FeatureExportBundle.validate(features.filter(not(col("is_mainshock"))),main,m,scaling));
        assertThrows(Exception.class,()->FeatureExportBundle.validate(features.withColumn("mc_value",lit(99.)),main,m,scaling));
        assertThrows(Exception.class,()->FeatureExportBundle.validate(features,main,m,FeatureScalingConfigTest.config("GLOBAL",50,1)));
    }
    @Test void snapshotRetryAfterPartialWriteReusesPinAndPreservesOtherDataset() throws Exception {
        String table = "lake.export_test.sequence_candidate_snapshot";
        long pin = FeatureSnapshotWriter.commit(features,table,ID,SHA,()->{});
        long outside = FeatureSnapshotWriter.commit(features.withColumn("dataset_id",lit(ID2)),table,ID2,"d".repeat(64),()->{});
        assertEquals(pin,FeatureSnapshotWriter.commit(features,table,ID,SHA,()->{}));
        assertEquals(2,FeatureSnapshotWriter.read(spark,table,pin,ID).count());
        assertEquals(2,FeatureSnapshotWriter.read(spark,table,outside,ID2).count());
        assertEquals(4,spark.table(table).count()); assertEquals(2,spark.table(table + ".snapshots").filter(col("summary").getItem("ml.dataset_id").isNotNull()).count());
        assertThrows(Exception.class,()->FeatureSnapshotWriter.commit(features,table,ID,"f".repeat(64),()->{}));
        assertThrows(Exception.class,()->FeatureSnapshotWriter.commit(features,"lake.export_test.mainshock_candidate_snapshot",ID,SHA,()->{throw new IllegalStateException("lease lost");}));
        assertFalse(spark.catalog().tableExists("lake.export_test.mainshock_candidate_snapshot"));
    }
    @Test void symlinksNeverBecomeExports() throws Exception {
        Path real = Files.createDirectory(temporary.resolve("real")), link = temporary.resolve("link"); Files.createSymbolicLink(link,real);
        assertThrows(java.io.IOException.class,()->FeatureExportBundle.materialize(features,manifest(),link));
    }
    @Test void rehashedManifestCannotSubstituteFeatureVersionOnReuse() throws Exception {
        var original = manifest(); FeatureExportBundle.materialize(features,original,temporary); Path dir = temporary.resolve("dataset_id=" + ID);
        var changed = (ObjectNode)FeatureExportBundle.JSON.readTree(Files.readAllBytes(dir.resolve("dataset_manifest.json")));
        changed.put("feature_version","forged-version"); Files.write(dir.resolve("dataset_manifest.json"),FeatureExportBundle.bytes(changed));
        var index = FeatureExportBundle.index(dir.resolve("checksums.sha256"));
        index.put("dataset_manifest.json",WindowArtifactInput.sha(dir.resolve("dataset_manifest.json")));
        var text = new StringBuilder(); index.forEach((name,hash)->text.append(hash).append("  ").append(name).append('\n'));
        Files.writeString(dir.resolve("checksums.sha256"),text);
        assertEquals("DS_EXPORT_EXISTING_MANIFEST_CONFLICT",assertThrows(java.io.IOException.class,
            ()->FeatureExportBundle.materialize(features,original,temporary)).getMessage());
    }
    @Test void largerBoundedExportUsesMultipleStableParts() throws Exception {
        var many = spark.range(10001).crossJoin(features.filter(not(col("is_mainshock"))))
            .withColumn("candidate_event_id",concat(lit("synthetic-"),col("id"))).drop("id");
        var m = manifest().put("feature_row_count",10001).put("candidate_row_count",10001)
            .put("feature_content_sha256",MainshockWindowJob.fingerprint(many));
        var exported = FeatureExportBundle.materialize(many,m,temporary);
        assertEquals(2,exported.path("parts").size());
        assertEquals(10001,FeatureExportBundle.verify(spark,temporary.resolve("dataset_id="+ID)).path("feature_row_count").asInt());
    }
    @Test void strictExclusionReportIsBoundToExportIdentityAndCannotBeDroppedAfterRehash() throws Exception {
        var m=manifest().put("identity_payload_sha256",SHA).put("dataset_split","REPRODUCTION")
                .put("period_start_utc",MainshockSelectionConfig.REPRODUCTION_START.toString())
                .put("period_end_utc_exclusive",MainshockSelectionConfig.REPRODUCTION_END.toString());
        var filter=FeatureExportBundle.JSON.createObjectNode().put("window_coverage_policy",WindowCoveragePolicy.STRICT);
        var scalingJson=FeatureExportBundle.JSON.createObjectNode().put("feature_version",scaling.featureVersion())
                .put("scaling_config_version",scaling.scalingConfigVersion()).put("mode",scaling.mode()).put("space_scale_km",scaling.spaceScaleKm())
                .put("depth_scale_km",scaling.depthScaleKm()).put("time_scale_hours",scaling.timeScaleHours())
                .put("max_candidates_per_window",scaling.maxCandidatesPerWindow()).put("coordinate_tolerance_km",scaling.coordinateToleranceKm())
                .put("time_tolerance_hours",scaling.timeToleranceHours());
        filter.set("feature_scaling_config",scalingJson); m.put("filter_config_json",filter.toString());
        var report=FeatureExportBundle.JSON.createObjectNode().put("policy_version",WindowCoveragePolicy.STRICT)
                .put("dataset_split","REPRODUCTION").put("period_start_utc",MainshockSelectionConfig.REPRODUCTION_START.toString())
                .put("period_end_utc_exclusive",MainshockSelectionConfig.REPRODUCTION_END.toString()).put("eligible_mainshock_count",1)
                .put("included_mainshock_count",1).put("excluded_mainshock_count",0).put("windows_clipped",false).put("cross_split_data_used",false);
        report.putArray("excluded_mainshocks"); report.putObject("reason_counts"); m.set("window_coverage",report); m.putArray("coverage");
        var identity=FeatureExportBundle.JSON.createObjectNode();
        for(String key:List.of("dataset_id","identity_payload_sha256","feature_content_sha256","feature_schema_sha256")) identity.set(key,m.path(key));
        identity.put("scaling_sha256",FeatureExportBundle.digest(filter.path("feature_scaling_config")))
                .put("coverage_sha256",FeatureExportBundle.digest(m.path("coverage"))).put("window_coverage_sha256",FeatureExportBundle.digest(report));
        m.put("export_identity_sha256",FeatureExportBundle.digest(identity));
        FeatureExportBundle.materialize(features,m,temporary); Path dir=temporary.resolve("dataset_id="+ID);
        var changed=(ObjectNode)FeatureExportBundle.JSON.readTree(Files.readAllBytes(dir.resolve("dataset_manifest.json")));
        changed.remove("window_coverage"); Files.write(dir.resolve("dataset_manifest.json"),FeatureExportBundle.bytes(changed));
        var index=FeatureExportBundle.index(dir.resolve("checksums.sha256")); index.put("dataset_manifest.json",WindowArtifactInput.sha(dir.resolve("dataset_manifest.json")));
        var text=new StringBuilder(); index.forEach((name,hash)->text.append(hash).append("  ").append(name).append('\n'));
        Files.writeString(dir.resolve("checksums.sha256"),text);
        assertThrows(IllegalArgumentException.class,()->FeatureExportBundle.verify(spark,dir));
    }
}
