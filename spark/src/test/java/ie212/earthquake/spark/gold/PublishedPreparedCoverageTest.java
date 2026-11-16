package ie212.earthquake.spark.gold;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class PublishedPreparedCoverageTest {
    private static ObjectNode coverage() {
        var c = PortablePreparedCatalog.JSON.createObjectNode().put("coverage_version", PublishedPreparedCoverage.VERSION).put("source_cutoff_utc", "2026-10-25T06:13:04.747Z");
        for (String k : java.util.List.of("prepared_catalog_sha256", "prepared_coverage_sha256", "provenance_sha256", "published_commit_sha256", "publication_sha256", "gold_request_sha256")) c.put(k, "a".repeat(64));
        return c;
    }
    private static JsonNode[] binding() throws Exception {
        var r = PreparedGoldProofTest.request(); var c = PreparedGoldProofTest.commit().put("gold_run_id", r.path("run_id").asText());
        // Normalize persisted integer representation, as the real S3 readers do.
        c = (ObjectNode)PortablePreparedCatalog.JSON.readTree(PortablePreparedCatalog.JSON.writeValueAsBytes(c));
        var p = PortablePreparedCatalog.JSON.createObjectNode().put("gold_run_id", r.path("run_id").asText()).put("namespace", PreparedGoldProof.NAMESPACE)
                .put("verify_status", "PASSED").put("published", true).put("publication_id", "pub_" + "a".repeat(64));
        p.set("snapshot_bundle", c.path("tables")); p.set("identity_sha256", c.path("identity_sha256"));
        var m = PortablePreparedCatalog.JSON.createObjectNode().put("physical_gold_table_name", PreparedGoldProof.NAMESPACE + ".event_current")
                .put("observation_cutoff_utc", "2026-11-15T08:00:00Z");
        m.set("gold_publication_id", p.path("publication_id")); m.set("gold_snapshot_id", c.path("tables").path("event_current").path("snapshot_id"));
        return new JsonNode[]{c, r, p, m};
    }
    @Test void explicitProofHasNoFabricatedLegacyRegistry() { assertDoesNotThrow(() -> PublishedPreparedCoverage.validateProof(coverage())); }
    @Test void incompleteHashesAreNotProof() { var c = coverage(); c.remove("provenance_sha256"); assertThrows(Exception.class, () -> PublishedPreparedCoverage.validateProof(c)); }
    @Test void legacyAliasCannotBeInvented() { var c = coverage().put("registry_sha256", "a".repeat(64)); assertThrows(Exception.class, () -> PublishedPreparedCoverage.validateProof(c)); }
    @Test void matchingActualPublicationShapeBindsInput() throws Exception { var x = binding(); assertDoesNotThrow(() -> PublishedPreparedCoverage.binding(x[0], x[1], x[2], x[3])); }
    @Test void movedGoldSnapshotIsRejected() throws Exception { var x = binding(); ((ObjectNode)x[3]).put("gold_snapshot_id", 42); assertThrows(Exception.class, () -> PublishedPreparedCoverage.binding(x[0], x[1], x[2], x[3])); }
    @Test void unpublishedMarkerIsNotPublication() throws Exception { var x = binding(); ((ObjectNode)x[2]).put("published", false); assertThrows(Exception.class, () -> PublishedPreparedCoverage.binding(x[0], x[1], x[2], x[3])); }
    @Test void changedCountCannotReusePreparedProof() throws Exception { var x = binding(); ((ObjectNode)x[1].path("expected_counts")).put("event_current", 2); assertThrows(Exception.class, () -> PublishedPreparedCoverage.binding(x[0], x[1], x[2], x[3])); }
    @Test void sourceCutoffCannotBeMovedEarlier() throws Exception { var x = binding(); ((ObjectNode)x[3]).put("observation_cutoff_utc", "2026-01-01T00:00:00Z"); assertThrows(Exception.class, () -> PublishedPreparedCoverage.binding(x[0], x[1], x[2], x[3])); }
}
