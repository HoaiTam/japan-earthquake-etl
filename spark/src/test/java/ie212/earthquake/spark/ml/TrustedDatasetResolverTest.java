package ie212.earthquake.spark.ml;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class TrustedDatasetResolverTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    @Test void resolvesOnlyPinnedExportedManifest(@TempDir Path root) throws Exception {
        Path export = root.resolve("exports/checksums.sha256"); Files.createDirectories(export.getParent()); Files.writeString(export, "fixture\n");
        String exportSha = TrustedDatasetResolver.sha256(Files.readAllBytes(export));
        ObjectNode manifest = JSON.createObjectNode().put("dataset_id", "ds_fixture").put("dataset_status", "EXPORTED")
            .put("export_manifest_ref", "exports/checksums.sha256").put("export_manifest_sha256", exportSha)
            .put("registry_snapshot_id", "registry-1").put("candidate_snapshot_id", "candidate-1").put("mainshock_snapshot_id", "mainshock-1");
        manifest.putArray("candidates").addObject().put("mainshock_event_id", "main").put("candidate_event_id", "main").put("role", "MAINSHOCK");
        Path path = root.resolve("registry/ds_fixture.json"); Files.createDirectories(path.getParent()); Files.writeString(path, JSON.writeValueAsString(manifest));
        ObjectNode request = JSON.createObjectNode().put("dataset_id", "ds_fixture").put("dataset_manifest_ref", "registry/ds_fixture.json")
            .put("dataset_manifest_sha256", TrustedDatasetResolver.sha256(Files.readAllBytes(path))).put("experiment_run_id", "exp_fixture")
            .put("algorithm_name", "WINDOW").put("model_config_version", "1");
        var context = TrustedDatasetResolver.resolve(request, root, null);
        assertEquals("EXPORTED", context.datasetStatus());
        assertEquals("candidate-1", context.datasetPins().candidateSnapshotId());
        assertEquals("MAINSHOCK", context.candidates().get(0).role());
    }

    @Test void rejectsUnpinnedOrPilotManifest(@TempDir Path root) throws Exception {
        Path path = root.resolve("registry/ds.json"); Files.createDirectories(path.getParent());
        ObjectNode manifest = JSON.createObjectNode().put("dataset_id", "DEVELOPMENT_PILOT").put("dataset_status", "EXPORTED");
        Files.writeString(path, JSON.writeValueAsString(manifest));
        ObjectNode request = JSON.createObjectNode().put("dataset_id", "DEVELOPMENT_PILOT").put("dataset_manifest_ref", "registry/ds.json")
            .put("dataset_manifest_sha256", TrustedDatasetResolver.sha256(Files.readAllBytes(path))).put("experiment_run_id", "exp_fixture")
            .put("algorithm_name", "WINDOW").put("model_config_version", "1");
        assertEquals("IMP_UNKNOWN_DATASET", assertThrows(IllegalArgumentException.class,
            () -> TrustedDatasetResolver.resolve(request, root, null)).getMessage());
    }
}
