package ie212.earthquake.spark.ml;
import static org.junit.jupiter.api.Assertions.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DatasetBuildJobTest {
    @TempDir Path temporary;
    ObjectNode request() {
        var r = FeatureExportBundle.JSON.createObjectNode().put("contract_version", DatasetBuildJob.VERSION)
            .put("step", "plan").put("attempt", 1).put("dag_id", "mli_02_build_dataset")
            .put("run_id", "manual__2026-10-24T00:00:00+00:00").put("build_run_id", "build-1").put("ml_namespace", "iceberg.ml");
        r.set("config", DatasetManifestTest.config("EXTENSION")); r.set("publication_reference", DatasetManifestTest.publication()); return r;
    }
    @Test void planReadOnlyStableJavaIdentity() throws Exception {
        var req = request(); var sql = new DatasetManifestTest.Fake(req.path("config"));
        var first = DatasetBuildJob.execute(req, sql, null, null, () -> {}); req.put("build_run_id", "build-2");
        assertEquals(first, DatasetBuildJob.execute(req, sql, null, null, () -> {}));
        assertFalse(sql.queries.stream().anyMatch(q -> !q.startsWith("SELECT")));
        assertEquals("ds_" + first.path("identity_payload_sha256").asText(), first.path("dataset_id").asText());
    }
    @Test void changedPinOrNamespaceCannotWrite() {
        for (int i=0; i<3; i++) {
            var req = request();
            if(i==0) ((ObjectNode)req.path("publication_reference")).put("gold_snapshot_id", 100);
            if(i==1) req.put("ml_namespace", "iceberg.gold"); if(i==2) req.put("ml_namespace", "other.ml");
            var sql = new DatasetManifestTest.Fake(req.path("config"));
            assertThrows(Exception.class, () -> DatasetBuildJob.execute(req,sql,null,null,()->{}));
            assertFalse(sql.queries.stream().anyMatch(q -> !q.startsWith("SELECT")));
        }
    }
    @Test void failedPublicationAndMissingLeaseStopBeforeManifest() throws Exception {
        var req = request(); var sql = new DatasetManifestTest.Fake(req.path("config")); sql.publications=0;
        assertThrows(Exception.class, () -> DatasetBuildJob.execute(req,sql,null,null,()->{}));
        assertThrows(Exception.class, () -> DatasetBuildJob.owner(req,temporary).run());
        Files.writeString(temporary.resolve("owner.json"), "{\"dag_id\":\"other\",\"run_id\":\"foreign\"}");
        assertThrows(Exception.class, () -> DatasetBuildJob.owner(req,temporary).run());
        assertTrue(Files.readString(temporary.resolve("owner.json")).contains("foreign"));
    }
    @Test void laterStepRequiresPredecessorBytes() throws Exception {
        var req = request(); var sql = new DatasetManifestTest.Fake(req.path("config"));
        var plan = DatasetBuildJob.execute(req,sql,null,null,()->{}); req.set("plan", plan); req.put("step", "resolve_gold_snapshot");
        Path root = temporary.toRealPath(); req.put("run_root", root.toString()); Files.createDirectories(root.resolve("receipts"));
        var previous = FeatureExportBundle.JSON.createObjectNode().put("step", "wait_gold_published")
            .put("run_id", req.path("run_id").asText()).put("plan_sha256", FeatureExportBundle.digest(plan));
        Files.write(root.resolve("receipts/wait_gold_published.json"), FeatureExportBundle.bytes(previous)); req.put("previous_receipt_sha256", "a".repeat(64));
        assertThrows(Exception.class, () -> DatasetBuildJob.execute(req,sql,null,null,()->{}));
        assertFalse(sql.queries.stream().anyMatch(q -> q.startsWith("INSERT")));
    }
}
