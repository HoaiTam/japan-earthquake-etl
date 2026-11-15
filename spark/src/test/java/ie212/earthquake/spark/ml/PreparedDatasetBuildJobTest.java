package ie212.earthquake.spark.ml;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class PreparedDatasetBuildJobTest {
    static ObjectNode request() {
        var r = FeatureExportBundle.JSON.createObjectNode().put("version", PreparedDatasetBuildJob.LEGACY_VERSION).put("gold_run_id", "dat03-gold-" + "a".repeat(32))
                .put("observation_cutoff_utc", "2026-11-15T00:00:00Z");
        for (String k : java.util.List.of("runner_sha256", "gold_commit_sha256", "gold_publication_sha256", "scaling_sha256")) r.put(k, "a".repeat(64)); return r;
    }
    static Path root() { return Path.of("/opt/pipeline/staging/dat03-prepared-ml/" + "a".repeat(32)); }
    @Test void exactRequestIsAccepted() { assertDoesNotThrow(() -> PreparedDatasetBuildJob.validate(request(), root())); }
    @Test void noCallerMcSampleOrReadiness() {
        for (String key : java.util.List.of("mc_value", "research_ready", "skip_coverage", "sample_size")) {
            var r = request().put(key, true); assertThrows(Exception.class, () -> PreparedDatasetBuildJob.validate(r, root()));
        }
    }
    @Test void noForeignRootOrUnpinnedCode() {
        assertThrows(Exception.class, () -> PreparedDatasetBuildJob.validate(request(), Path.of("/tmp/arbitrary")));
        assertThrows(Exception.class, () -> PreparedDatasetBuildJob.validate(request().put("runner_sha256", "latest"), root()));
    }
    @Test void strictSplitAndResourceLimitsAreNotRelaxed() throws Exception {
        var scale = FeatureExportBundle.JSON.readTree(Files.readAllBytes(Path.of("../config/ml/feature_scaling_global_dev_v1.json")));
        for (String split : java.util.List.of("REPRODUCTION", "EXTENSION")) {
            var c = PreparedDatasetBuildJob.config(request(), split, scale); var f = c.path("filter_config");
            assertEquals("dat03-strict-split-v1", f.path("window_coverage_policy").asText());
            assertEquals(10000, f.path("mainshock_selection_config").path("max_candidates_per_window").asInt());
            assertEquals(100000, f.path("collection_export").path("layout_limits").path("rows_per_shard").asInt());
            assertEquals(64, f.path("collection_export").path("layout_limits").path("max_shards").asInt());
            assertEquals("dat03-published-prepared-coverage-v1", f.path("source_coverage_contract").asText());
        }
    }
    @Test void approved40kProfileChangesOnlyResourceBoundNotVectorOrWindow() throws Exception {
        var r = request().put("version", PreparedDatasetBuildJob.VERSION).put("resource_profile_version", PreparedDatasetBuildJob.PROFILE).put("profile_receipt_sha256", "b".repeat(64));
        PreparedDatasetBuildJob.validate(r, root());
        var oldScale = FeatureExportBundle.JSON.readTree(Files.readAllBytes(Path.of("../config/ml/feature_scaling_global_dev_v1.json")));
        var newScale = FeatureExportBundle.JSON.readTree(Files.readAllBytes(Path.of("../config/ml/feature_scaling_global_research_40k_v1.json")));
        var old = PreparedDatasetBuildJob.config(request(), "REPRODUCTION", oldScale).path("filter_config");
        var next = PreparedDatasetBuildJob.config(r, "REPRODUCTION", newScale).path("filter_config");
        assertEquals(old.path("window_config"), next.path("window_config")); assertEquals(old.path("completeness_config"), next.path("completeness_config"));
        assertEquals(40000, next.path("mainshock_selection_config").path("max_candidates_per_window").asInt());
        var publication = DatasetManifestTest.publication();
        var manifest = new DatasetIdentityPlan(publication, PreparedDatasetBuildJob.config(r, "REPRODUCTION", newScale), "profile-test", java.time.Instant.EPOCH).manifest();
        assertEquals(40000, MainshockWindowJob.selectionConfig(manifest).maxCandidatesPerWindow());
        var noOptIn = (ObjectNode) manifest.deepCopy(); noOptIn.put("dataset_config_version", PreparedDatasetBuildJob.LEGACY_VERSION);
        assertThrows(Exception.class, () -> MainshockWindowJob.selectionConfig(noOptIn));
        assertEquals(old.path("collection_export"), next.path("collection_export"));
        for (String k : java.util.List.of("space_scale_km", "depth_scale_km", "time_scale_hours", "mode", "coordinate_tolerance_km", "time_tolerance_hours")) assertEquals(oldScale.path(k), newScale.path(k));
        ((ObjectNode)newScale).put("time_scale_hours", 1);
        assertThrows(Exception.class, () -> PreparedDatasetBuildJob.config(r, "REPRODUCTION", newScale));
    }
    @Test void unmeasuredOrLargerProfileCannotBeSelected() {
        var r = request().put("version", PreparedDatasetBuildJob.VERSION);
        assertThrows(Exception.class, () -> PreparedDatasetBuildJob.validate(r, root()));
        r.put("resource_profile_version", "arbitrary-100k").put("profile_receipt_sha256", "a".repeat(64));
        assertThrows(Exception.class, () -> PreparedDatasetBuildJob.validate(r, root()));
    }
}
