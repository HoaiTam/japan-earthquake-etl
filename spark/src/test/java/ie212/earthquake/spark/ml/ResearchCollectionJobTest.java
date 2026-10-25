package ie212.earthquake.spark.ml;

import static org.junit.jupiter.api.Assertions.*;
import static ie212.earthquake.spark.ml.FeatureExportBundle.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.*;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import org.apache.spark.sql.*;
import org.junit.jupiter.api.*;

/** Synthetic full-split component API only; not evidence of real research coverage or official export. */
class ResearchCollectionJobTest {
    static SparkSession spark; static MainshockWindowResult windows; static ObjectNode parent;
    static FeatureScalingConfig scaling; static String id;
    @org.junit.jupiter.api.io.TempDir Path temporary;
    @BeforeAll static void start() throws Exception {
        spark=LocalFeatureJob.session();
        if(System.getProperty("os.name").startsWith("Windows")) {
            spark.sparkContext().hadoopConfiguration().set("fs.file.impl",FeatureExportBundleTest.NioFileSystem.class.getName());
            spark.sparkContext().hadoopConfiguration().setBoolean("fs.file.impl.disable.cache",true);
        }
        scaling=FeatureScalingConfigTest.config("GLOBAL",50,10000);
        var c=DatasetManifestTest.config("REPRODUCTION");
        var scalingJson=JSON.createObjectNode().put("feature_version",scaling.featureVersion()).put("scaling_config_version",scaling.scalingConfigVersion())
                .put("mode",scaling.mode()).put("space_scale_km",scaling.spaceScaleKm()).put("depth_scale_km",scaling.depthScaleKm())
                .put("time_scale_hours",scaling.timeScaleHours()).put("max_candidates_per_window",scaling.maxCandidatesPerWindow())
                .put("coordinate_tolerance_km",scaling.coordinateToleranceKm()).put("time_tolerance_hours",scaling.timeToleranceHours());
        ((ObjectNode)c.path("filter_config")).put("window_coverage_policy",WindowCoveragePolicy.STRICT).set("feature_scaling_config",scalingJson);
        c.put("feature_version",scaling.featureVersion()).put("scaling_config_version",scaling.scalingConfigVersion());
        parent=(ObjectNode)new DatasetIdentityPlan(DatasetManifestTest.publication(),c,"collection-fixture",Instant.parse("2026-10-24T00:00:00Z")).manifest();
        id=parent.path("dataset_id").asText(); Instant t=Instant.parse("2005-06-01T00:00:00Z");
        var events=spark.createDataFrame(List.of(RowFactory.create("main-a",Timestamp.from(t),35.,140.,80.,6.,true),
                RowFactory.create("main-b",Timestamp.from(t.plusSeconds(3600)),35.,140.,81.,6.,true),
                RowFactory.create("post",Timestamp.from(t.plusSeconds(7200)),35.,140.,81.,3.,true)),MainshockWindowEngineTest.AUDITED_SCHEMA);
        windows=new MainshockWindowEngine().execute(events,MainshockSelectionConfig.reproductionBaseline(),WindowModelConfig.expandedV1(),1.,id,t,WindowCoveragePolicy.STRICT);
        windows.mainshockSnapshot().cache(); windows.candidateSnapshot().cache(); windows.assertInvariants();
    }
    @BeforeEach void realTemp() throws Exception {temporary=temporary.toRealPath();}
    @AfterAll static void stop() {if(spark!=null) spark.stop();}
    ObjectNode prepare(Path root) throws Exception {
        return ResearchCollectionJob.materialize(parent,ResearchCollectionPlanTest.coverage(),ResearchCollectionPlanTest.mc(parent),windows,
                scaling,new ResearchCollectionPlan.Limits(1,100000,64),root,()->{});
    }
    @Test void materializesWholeWindowsWithGlobalMcAndRerunPreservesExactBytesThenFailsOnCorruption() throws Exception {
        var first=prepare(temporary); assertEquals(2,first.path("shard_count").asInt());
        assertEquals(2,first.path("mainshock_count").asInt()); assertFalse(first.path("official_exported").asBoolean(true));
        assertFalse(first.path("research_ready").asBoolean(true)); assertEquals("BUILDING",parent.path("dataset_status").asText());
        Path folder=temporary.resolve(first.path("collection_id").asText());
        var report=ResearchCollectionJob.verify(spark,folder,first.path("manifest_sha256").asText(),first.path("checksums_sha256").asText());
        assertEquals(windows.totalCandidateRowCount(),report.path("plan").path("feature_row_count").asLong());
        assertEquals(1.,report.path("plan").path("global_mc").path("mc_value").asDouble());
        var receiptBefore=Files.readAllBytes(temporary.resolve("receipts/"+first.path("collection_id").asText()+".json"));
        var second=prepare(temporary); assertTrue(second.path("reused").asBoolean());
        assertEquals(first.path("manifest_sha256"),second.path("manifest_sha256"));
        assertArrayEquals(receiptBefore,Files.readAllBytes(temporary.resolve("receipts/"+first.path("collection_id").asText()+".json")));
        assertFalse(Files.exists(folder.resolve("dataset_manifest.json"))); assertFalse(Files.exists(folder.resolve("_SUCCESS.json")));
        assertThrows(Exception.class,()->ResearchCollectionJob.verify(spark,folder,"0".repeat(64),first.path("checksums_sha256").asText()));
        var feature=folder.resolve("shard_00000/features.parquet"); Files.writeString(feature,"corrupt");
        assertThrows(Exception.class,()->prepare(temporary)); assertEquals("corrupt",Files.readString(feature));
    }
    @Test void partialCoverageOrWrongGlobalMcDoesNotCreatePreparation() throws Exception {
        var c=ResearchCollectionPlanTest.coverage(); c.put("required_month_count",1);
        Path output=temporary.resolve("not-created");
        assertThrows(Exception.class,()->ResearchCollectionJob.materialize(parent,c,ResearchCollectionPlanTest.mc(parent),windows,scaling,
                ResearchCollectionPlan.Limits.defaults(),output,()->{}));
        assertFalse(Files.exists(output));
        var mc=ResearchCollectionPlanTest.mc(parent); mc.put("scope","SHARD");
        assertThrows(Exception.class,()->ResearchCollectionJob.materialize(parent,ResearchCollectionPlanTest.coverage(),mc,windows,scaling,
                ResearchCollectionPlan.Limits.defaults(),output,()->{}));
        assertFalse(Files.exists(output));
    }
}
