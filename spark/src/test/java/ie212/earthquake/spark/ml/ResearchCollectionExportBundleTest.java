package ie212.earthquake.spark.ml;

import static org.junit.jupiter.api.Assertions.*;
import static ie212.earthquake.spark.ml.FeatureExportBundle.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.*;
import org.junit.jupiter.api.*;

/** Synthetic component API only. No real source coverage, lake lifecycle, or research acceptance claimed. */
class ResearchCollectionExportBundleTest {
    @org.junit.jupiter.api.io.TempDir Path temporary;
    @BeforeAll static void start() throws Exception {ResearchCollectionJobTest.setup(true);}
    @AfterAll static void stop() {ResearchCollectionJobTest.stop();}
    @BeforeEach void real() throws Exception {temporary=temporary.toRealPath();}
    ObjectNode prepare() throws Exception {
        var pin=ResearchCollectionJob.materialize(ResearchCollectionJobTest.parent,ResearchCollectionPlanTest.coverage(),
                ResearchCollectionPlanTest.mc(ResearchCollectionJobTest.parent),ResearchCollectionJobTest.windows,
                ResearchCollectionJobTest.scaling,new ResearchCollectionPlan.Limits(1,100000,64),temporary.resolve("prepared"),()->{});
        return ResearchCollectionJob.verify(ResearchCollectionJobTest.spark,temporary.resolve("prepared/"+pin.path("collection_id").asText()),
                pin.path("manifest_sha256").asText(),pin.path("checksums_sha256").asText());
    }
    ObjectNode validated(ObjectNode prepared) throws Exception {
        var spark=ResearchCollectionJobTest.spark;var plan=prepared.path("plan");
        Path root=temporary.resolve("prepared/"+plan.path("collection_id").asText());
        var main=ResearchCollectionExportBundle.read(spark,root,plan,"mainshocks.parquet");
        var features=ResearchCollectionExportBundle.read(spark,root,plan,"features.parquet");
        String mainSha=MainshockWindowJob.fingerprint(main),featureSha=MainshockWindowJob.fingerprint(features);
        var manifest=ResearchCollectionJobTest.parent.deepCopy().put("dataset_status","VALIDATED")
                .put("mainshock_count",plan.path("mainshock_count").asLong()).put("feature_row_count",plan.path("feature_row_count").asLong())
                .put("collection_id",plan.path("collection_id").asText())
                .put("mainshock_content_sha256",mainSha).put("feature_content_sha256",featureSha)
                .put("mainshock_snapshot_id","101").put("candidate_snapshot_id","102")
                .put("export_identity_sha256",digest(ResearchCollectionExportBundle.identity(prepared,mainSha,featureSha)));
        manifest.set("feature_schema",LocalFeatureJob.schema(features));manifest.put("feature_schema_sha256",digest(manifest.path("feature_schema")));
        manifest.set("window_coverage",plan.path("window_coverage").deepCopy());return manifest;
    }
    @Test void immutableTwoShardExportRoundTripAndCorruptionDoesNotRegenerate() throws Exception {
        var prepared=prepare();var manifest=validated(prepared);var plan=prepared.path("plan");
        Path from=temporary.resolve("prepared/"+plan.path("collection_id").asText()),output=temporary.resolve("export");
        var first=ResearchCollectionExportBundle.materialize(ResearchCollectionJobTest.spark,from,prepared,manifest,output);
        assertEquals("EXPORTED",first.path("dataset_status").asText());assertEquals(2,first.path("collection_plan").path("shards").size());
        assertFalse(first.has("candidates"));assertEquals(ResearchCollectionJobTest.id,first.path("dataset_id").asText());
        var second=ResearchCollectionExportBundle.materialize(ResearchCollectionJobTest.spark,from,prepared,manifest,output);
        assertEquals(digest(first),digest(second));
        Path target=output.resolve("dataset_id="+ResearchCollectionJobTest.id);
        assertEquals(digest(first),digest(ResearchCollectionExportBundle.verify(ResearchCollectionJobTest.spark,target)));
        Files.writeString(target.resolve("shard_00000/features.parquet"),"corrupt");
        assertThrows(Exception.class,()->ResearchCollectionExportBundle.materialize(ResearchCollectionJobTest.spark,from,prepared,manifest,output));
        assertEquals("corrupt",Files.readString(target.resolve("shard_00000/features.parquet")));
    }
    @Test void rejectsWrongLifecycleLayoutAndUnindexedArtifacts() throws Exception {
        var prepared=prepare();var manifest=validated(prepared);Path from=temporary.resolve("prepared/"+prepared.path("plan").path("collection_id").asText());
        manifest.put("dataset_status","BUILDING");
        assertThrows(Exception.class,()->ResearchCollectionExportBundle.materialize(ResearchCollectionJobTest.spark,from,prepared,manifest,temporary.resolve("blocked")));
        assertFalse(Files.exists(temporary.resolve("blocked")));
        manifest.put("dataset_status","VALIDATED");var out=temporary.resolve("export");
        ResearchCollectionExportBundle.materialize(ResearchCollectionJobTest.spark,from,prepared,manifest,out);
        Path target=out.resolve("dataset_id="+ResearchCollectionJobTest.id);Files.createDirectory(target.resolve("extra"));
        assertThrows(Exception.class,()->ResearchCollectionExportBundle.verify(ResearchCollectionJobTest.spark,target));
        ((ObjectNode)manifest).put("filter_config_json",ResearchCollectionJobTest.parent.path("filter_config_json").asText().replace("dat03-collection-export-v1","unsupported"));
        assertThrows(Exception.class,()->ResearchCollectionExportBundle.config(manifest));
    }
}
