package ie212.earthquake.spark.ml;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class GoldAuditAcceptanceTest {
    private final ObjectMapper json = new ObjectMapper();
    private ObjectNode request() {
        String suffix = "a".repeat(32);
        return json.createObjectNode().put("dag_id", "gold_input_audit_qa").put("run_id", "mld02-qa-" + suffix)
            .put("gold_namespace", "iceberg.gold_qa_" + suffix).put("ml_namespace", "iceberg.ml_qa_" + suffix)
            .put("config_version", "1").put("code_version", "artifact_" + "b".repeat(64));
    }
    private JsonNode reference() throws Exception {
        var reference = json.createObjectNode().put("run_id", "fixture-silver-qa").put("status", "VERIFIED_BOUNDED_SAMPLE").put("rerun_unchanged", true);
        var attempt = reference.putArray("attempts").addObject().put("run_id", "fixture-silver-qa")
            .put("silver_status", "SilverReady").put("gold_handoff_verified", true).put("bundle_manifest_sha256", "c".repeat(64));
        attempt.putObject("datasets").putObject("source_observation").put("record_count", 528);
        attempt.putObject("run_context").put("window_start_utc", "1999-12-31T15:00:00Z").put("window_end_utc", "2024-01-01T00:00:00Z");
        return reference;
    }
    @Test void boundedReferenceAndFreshQaRequestAreAccepted() throws Exception {
        GoldAuditAcceptanceJob.validateRequest(request(), reference());
    }
    @Test void productionNamespaceMismatchedMlAndUnsafeRunAreRejectedBeforeStorage() throws Exception {
        for (String field : new String[]{"gold_namespace", "ml_namespace", "run_id", "dag_id", "code_version"}) {
            var bad = request(); bad.put(field, "iceberg.gold"); var ref = reference();
            assertThrows(IllegalArgumentException.class, () -> GoldAuditAcceptanceJob.validateRequest(bad, ref));
        }
    }
    @Test void unverifiedOrOversizedSilverCannotSeedQa() throws Exception {
        var bad = (ObjectNode) reference(); bad.put("status", "FIXTURE");
        assertThrows(IllegalArgumentException.class, () -> GoldAuditAcceptanceJob.validateRequest(request(), bad));
        var huge = reference(); ((ObjectNode)huge.path("attempts").path(0).path("datasets").path("source_observation")).put("record_count", 1_000_000);
        assertThrows(IllegalArgumentException.class, () -> GoldAuditAcceptanceJob.validateRequest(request(), huge));
    }
    @Test void twoSplitsPinSeparatePeriodsButIdenticalPredeclaredMethodParameters() {
        var time = Instant.parse("2026-10-11T12:00:00Z");
        var repro = GoldAuditAcceptanceJob.datasetConfig("REPRODUCTION", time, "artifact_" + "a".repeat(64));
        var extension = GoldAuditAcceptanceJob.datasetConfig("EXTENSION", time, "artifact_" + "a".repeat(64));
        assertEquals(GoldAuditConfig.REPRODUCTION_END, repro.path("period_end_utc_exclusive").asText());
        assertEquals(GoldAuditConfig.EXTENSION_START, extension.path("period_start_utc").asText());
        assertEquals(repro.path("filter_config"), extension.path("filter_config"));
        assertEquals(50, repro.path("filter_config").path("completeness_config").path("min_events").asInt());
    }
}
