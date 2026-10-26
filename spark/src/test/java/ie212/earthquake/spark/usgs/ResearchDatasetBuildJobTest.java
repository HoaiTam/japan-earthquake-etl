package ie212.earthquake.spark.usgs;

import static org.junit.jupiter.api.Assertions.*;
import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import ie212.earthquake.spark.ml.*;
import java.nio.file.*;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ResearchDatasetBuildJobTest {
    @Test void fullSplitConfigsReuseVersionedSelectionWindowScalingAndNeverCallerMc() throws Exception {
        var scaling=JSON.readTree(Files.readAllBytes(Path.of("../config/ml/feature_scaling_global_dev_v1.json")));
        for(String split:java.util.List.of("REPRODUCTION","EXTENSION")) {
            var c=ResearchDatasetBuildJob.config(split,Instant.parse("2026-10-25T00:00:00Z"),"a".repeat(64),scaling);
            assertEquals("wm_expanded_v1.0",c.path("window_model_version").asText());
            assertEquals(scaling,c.path("filter_config").path("feature_scaling_config"));
            assertEquals(WindowCoveragePolicy.STRICT,c.path("filter_config").path("window_coverage_policy").asText());
            assertFalse(c.path("filter_config").has("fixed_mc"));assertFalse(c.path("filter_config").has("synthetic_component_qa"));
            var identity=new DatasetIdentityPlan(DatasetManifestForResearchTest.publication(),c,"component-config-test",Instant.EPOCH);
            assertTrue(identity.manifest().path("dataset_id").asText().matches("ds_[a-f0-9]{64}"));
            var manifest=identity.manifest();MainshockWindowJob.windowConfig(manifest);MainshockWindowJob.selectionConfig(manifest);
        }
    }
    @Test void partialCatalogCannotCreateMlArtifactsOrAClaimedExport(@TempDir Path temporary) throws Exception {
        Path root=temporary.toRealPath();var request=ResearchCatalogStateTest.request();
        assertEquals("RESEARCH_FULL_288_MONTH_CATALOG_REQUIRED",assertThrows(Exception.class,
                ()->ResearchDatasetBuildJob.completeCatalog(root,request)).getMessage());
        assertFalse(Files.exists(root.resolve("months")));assertEquals(0,Files.list(root).count());
    }
    /** Small pure identity reference only; no publication/runtime claim. */
    static final class DatasetManifestForResearchTest {
        static com.fasterxml.jackson.databind.node.ObjectNode publication() {
            return JSON.createObjectNode().put("gold_table_name","iceberg.dat03_catalog_"+"1".repeat(32)+".earthquake_event_current")
                    .put("physical_gold_table_name","iceberg.dat03_catalog_"+"1".repeat(32)+".event_current").put("gold_snapshot_id",99L)
                    .put("gold_publication_id","pub_"+"b".repeat(64)).put("publication_bundle_sha256","b".repeat(64)).put("verify_status","PASSED");
        }
    }
}
