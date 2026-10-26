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

    @Test void runtimeQaRejectsBothDatasetStatesWithoutOverwritingEvidence(@TempDir Path temporary) throws Exception {
        Path root=temporary.toRealPath();
        ObjectNode manifest = JSON.createObjectNode().put("dataset_id", "ds_fixture");
        ObjectNode request = JSON.createObjectNode().put("dataset_id", "ds_fixture");
        ObjectNode checks = JSON.createObjectNode();
        MlResultImportAcceptanceJob.rejectNonExported(root, manifest, request, checks);
        assertEquals("IMP_UNKNOWN_DATASET", checks.path("BUILDING").asText());
        assertEquals("IMP_UNKNOWN_DATASET", checks.path("DEVELOPMENT_PILOT").asText());
        assertTrue(Files.isRegularFile(root.resolve("blocked-BUILDING.json")));
        assertTrue(Files.isRegularFile(root.resolve("blocked-DEVELOPMENT_PILOT.json")));
    }

    @Test void resolvesOnlyPinnedExportedManifest(@TempDir Path temporary) throws Exception {
        Path root=temporary.toRealPath();
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

    @Test void rejectsUnpinnedOrPilotManifest(@TempDir Path temporary) throws Exception {
        Path root=temporary.toRealPath();
        Path path = root.resolve("registry/ds.json"); Files.createDirectories(path.getParent());
        ObjectNode manifest = JSON.createObjectNode().put("dataset_id", "DEVELOPMENT_PILOT").put("dataset_status", "EXPORTED");
        Files.writeString(path, JSON.writeValueAsString(manifest));
        ObjectNode request = JSON.createObjectNode().put("dataset_id", "DEVELOPMENT_PILOT").put("dataset_manifest_ref", "registry/ds.json")
            .put("dataset_manifest_sha256", TrustedDatasetResolver.sha256(Files.readAllBytes(path))).put("experiment_run_id", "exp_fixture")
            .put("algorithm_name", "WINDOW").put("model_config_version", "1");
        assertEquals("IMP_UNKNOWN_DATASET", assertThrows(IllegalArgumentException.class,
            () -> TrustedDatasetResolver.resolve(request, root, null)).getMessage());
    }
    ObjectNode collection(Path root) throws Exception {
        String id="ds_"+"1".repeat(64),collection="col_"+"2".repeat(64);
        Path exports=root.resolve("exports/"+id);Files.createDirectories(exports);Files.writeString(exports.resolve("export-checksums.sha256"),"fixture\n");
        var lineage=JSON.createObjectNode().put("dataset_id",id).put("collection_id",collection).put("shard_id","shard_00000");
        lineage.putArray("candidates").addObject().put("mainshock_event_id","main").put("candidate_event_id","main").put("role","MAINSHOCK");
        Files.write(exports.resolve("shard_00000.json"),FeatureExportBundle.bytes(lineage));
        var manifest=JSON.createObjectNode().put("dataset_id",id).put("dataset_status","EXPORTED").put("export_version",ResearchCollectionExportBundle.VERSION)
                .put("export_manifest_ref","exports/"+id+"/export-checksums.sha256")
                .put("export_manifest_sha256",TrustedDatasetResolver.sha256(Files.readAllBytes(exports.resolve("export-checksums.sha256"))))
                .put("registry_snapshot_id","101").put("candidate_snapshot_id","102").put("mainshock_snapshot_id","103");
        var plan=manifest.putObject("collection_plan").put("collection_id",collection);
        plan.putArray("shards").addObject().put("shard_id","shard_00000").putArray("mainshocks").addObject()
                .put("mainshock_event_id","main").put("candidate_row_count",1);
        manifest.putObject("collection_shards").putObject("shard_00000").put("lineage_ref","exports/"+id+"/shard_00000.json")
                .put("lineage_sha256",TrustedDatasetResolver.sha256(Files.readAllBytes(exports.resolve("shard_00000.json"))))
                .put("mainshock_count",1).put("feature_row_count",1);
        Path file=root.resolve(id+".json");Files.write(file,FeatureExportBundle.bytes(manifest));
        return JSON.createObjectNode().put("dataset_id",id).put("dataset_manifest_ref",id+".json")
                .put("dataset_manifest_sha256",TrustedDatasetResolver.sha256(Files.readAllBytes(file))).put("experiment_run_id","exp_shard")
                .put("algorithm_name","WINDOW").put("model_config_version","1").put("collection_id",collection).put("collection_shard_id","shard_00000");
    }
    @Test void collectionRequiresExplicitWholeWindowScopeAndNeverFlattensAllShards(@TempDir Path temporary) throws Exception {
        Path root=temporary.toRealPath();
        var request=collection(root);var context=TrustedDatasetResolver.resolve(request,root,null);
        assertEquals(1,context.candidates().size());assertEquals("WHOLE_WINDOW_SHARD",context.collectionScope().toJson().path("scope").asText());
        assertEquals(context.collectionScope(),context.withMetadata(null).collectionScope());
        request.remove("collection_shard_id");assertThrows(Exception.class,()->TrustedDatasetResolver.resolve(request,root,null));
        request.put("collection_shard_id","shard_00001");assertThrows(Exception.class,()->TrustedDatasetResolver.resolve(request,root,null));
    }
    @Test void collectionRejectsCorruptionAndReassignment(@TempDir Path temporary) throws Exception {
        Path root=temporary.toRealPath();
        var request=collection(root);String id=request.path("dataset_id").asText();Path lineage=root.resolve("exports/"+id+"/shard_00000.json");
        byte[] original=Files.readAllBytes(lineage);Files.writeString(lineage,"corrupt");
        assertThrows(Exception.class,()->TrustedDatasetResolver.resolve(request,root,null));Files.write(lineage,original);
        Path manifest=root.resolve(id+".json");var m=(ObjectNode)JSON.readTree(Files.readAllBytes(manifest));
        ((ObjectNode)m.path("collection_plan").path("shards").get(0).path("mainshocks").get(0)).put("candidate_row_count",2);
        Files.write(manifest,FeatureExportBundle.bytes(m));request.put("dataset_manifest_sha256",TrustedDatasetResolver.sha256(Files.readAllBytes(manifest)));
        assertThrows(Exception.class,()->TrustedDatasetResolver.resolve(request,root,null));
    }
}
