package ie212.earthquake.spark.ml;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.gold.GoldPublisher;
import ie212.earthquake.spark.silver.*;
import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class MainshockRuntimeTest {
    private final ObjectMapper json = new ObjectMapper();
    private ObjectNode manifest() {
        var filter = json.createObjectNode(); filter.set("window_config", WindowModelConfig.expandedV1().toJsonNode());
        return json.createObjectNode().put("dataset_id", "ds_" + "a".repeat(64)).put("gold_snapshot_id", 1)
            .put("gold_publication_id", "fixture-publication").put("dataset_status", "BUILDING")
            .put("window_model_version", "wm_expanded_v1.0").put("filter_config_json", filter.toString());
    }
    private GoldAuditResult audit(boolean reliable) {
        var estimator = new MagnitudeCompletenessEstimator().estimate(Collections.nCopies(reliable ? 50 : 1, 2.0));
        String mc = "{\"applied_mc\":2.0}";
        return new GoldAuditResult(null, null, null, 50, 50, 0, Map.of(), Map.of(), estimator, Map.of(), new TreeMap<>(), new TreeMap<>(),
            new TreeMap<>(), new TreeMap<>(), new TreeMap<>(), List.of(), GoldAuditConfig.forReproduction("fixture"), Instant.now(), mc,
            MagnitudeCompletenessEstimator.sha256(mc), reliable ? 2.0 : Double.NaN);
    }
    @Test void acceptedAuditHandoffMatchesImmutableManifest() {
        var manifest = manifest(); var audit = audit(true);
        MainshockWindowJob.validateHandoff(manifest, audit.enrichManifest(manifest.deepCopy()), audit);
    }
    @Test void changedIdentityPinsLifecycleMcOrCountsCannotFeedWindow() {
        var manifest = manifest(); var audit = audit(true);
        for (String field : List.of("dataset_id", "gold_snapshot_id", "gold_publication_id", "dataset_status", "mc_config_sha256", "mc_config_json", "mc_value", "eligible_event_count")) {
            var bad = audit.enrichManifest(manifest.deepCopy()); bad.put(field, "changed");
            assertThrows(IllegalArgumentException.class, () -> MainshockWindowJob.validateHandoff(manifest, bad, audit), field);
        }
        var fractional = audit.enrichManifest(manifest.deepCopy()); fractional.put("input_event_count", 50.5);
        assertThrows(IllegalArgumentException.class, () -> MainshockWindowJob.validateHandoff(manifest, fractional, audit));
    }
    @Test void unreliableSampleCannotFeedCandidates() {
        var manifest = manifest(); var audit = audit(false);
        assertThrows(IllegalArgumentException.class, () -> MainshockWindowJob.validateHandoff(manifest, audit.enrichManifest(manifest.deepCopy()), audit));
    }
    @Test void versionAndFullWindowParametersMustBePinned() throws Exception {
        var manifest = manifest(); assertEquals(WindowModelConfig.expandedV1(), MainshockWindowJob.windowConfig(manifest));
        manifest.put("window_model_version", "window-model-v1");
        assertThrows(IllegalArgumentException.class, () -> MainshockWindowJob.windowConfig(manifest));
        manifest.put("window_model_version", "wm_expanded_v1.0"); manifest.put("filter_config_json", "{}");
        assertThrows(IllegalArgumentException.class, () -> MainshockWindowJob.windowConfig(manifest));
    }
    @Test void fullSelectionParametersSurviveJsonIntegerRoundTripButChangedValuesFail() throws Exception {
        for (String split : List.of("REPRODUCTION", "EXTENSION")) {
            var selection = split.equals("REPRODUCTION") ? MainshockSelectionConfig.reproductionBaseline() : MainshockSelectionConfig.extensionBaseline();
            var filter = json.createObjectNode(); filter.set("mainshock_selection_config", selection.toJsonNode());
            var manifest = manifest().put("dataset_split", split).put("filter_config_json", filter.toString());
            assertEquals(selection, MainshockWindowJob.selectionConfig(json.readTree(manifest.toString())));
            var changed = json.readTree(filter.toString());
            ((ObjectNode) changed.path("mainshock_selection_config")).put("max_candidates_per_window", 10001);
            manifest.put("filter_config_json", changed.toString());
            assertThrows(IllegalArgumentException.class, () -> MainshockWindowJob.selectionConfig(manifest));
            manifest.put("filter_config_json", filter.toString()).put("dataset_split", "UNKNOWN");
            assertThrows(IllegalArgumentException.class, () -> MainshockWindowJob.selectionConfig(manifest));
        }
    }
    @Test void unpublishedOrUnsafeDatasetStopsBeforeSnapshotAccess() {
        String id = "ds_" + "a".repeat(64);
        GoldPublisher.Sql empty = sql -> new GoldPublisher.Result(List.of(), List.of());
        assertThrows(IllegalArgumentException.class, () -> PinnedAuditResolver.resolve("iceberg.ml_qa", id, "qa", empty, () -> {}));
        assertThrows(IllegalArgumentException.class, () -> PinnedAuditResolver.resolve("iceberg.ml_qa;DROP", id, "qa", sql -> { fail("SQL must not run"); return null; }, () -> {}));
    }
    @Test void oracleUsesIntegerMicrosInsteadOfUnsupportedUnitOrLossyUnixDoubles() {
        String expression = MainshockWindowAcceptanceJob.oracleMicros("c.event_time_utc");
        assertTrue(expression.contains("date_trunc('second'")); assertTrue(expression.contains("to_iso8601"));
        assertFalse(expression.contains("date_diff('microsecond'")); assertFalse(expression.contains("to_unixtime("));
        assertThrows(IllegalArgumentException.class, () -> MainshockWindowAcceptanceJob.oracleMicros("unsafe;DROP"));
    }
    @Test void oracleUsesRoundedIntegerTimeBoundsAndTheSameBoundingBoxAndCentimetreTolerance() throws Exception {
        var manifest = manifest().put("physical_gold_table_name", "iceberg.gold_qa.event_current")
            .put("period_start_utc", "2000-01-01T00:00:00Z").put("period_end_utc_exclusive", "2018-10-01T00:00:00Z");
        var query = new java.util.concurrent.atomic.AtomicReference<String>();
        MainshockWindowAcceptanceJob.oracle(sql -> { query.set(sql); return new GoldPublisher.Result(List.of(), List.of()); }, manifest, 2.0);
        assertTrue(query.get().contains("BETWEEN -CAST(round("));
        assertTrue(query.get().contains("c.latitude BETWEEN")); assertTrue(query.get().contains("c.longitude BETWEEN"));
        assertTrue(query.get().contains(")+0.00001")); assertFalse(query.get().contains("/CAST(3600000000 AS double)"));
    }
    @Test void blockedDiagnosticRetainsGateReasonsWithoutRawPayloadOrSuccess() throws Exception {
        var rejects = new ArrayList<SilverRejectRecord>();
        for (int i = 1; i <= 12; i++) rejects.add(new SilverRejectRecord("1.0", "JMA_BULLETIN", null,
            "fixture-manifest", "s3://fixture/archive.zip", "a".repeat(64), "member=h2023;line=" + i,
            "b".repeat(64), "PARSE", List.of("INVALID_NUMBER"), "fixture-ingest", "fixture-parser", Instant.now()));
        var integration = new SilverMultiSourceIntegrationRunner().run("fixture-run", List.of(), rejects,
            SilverLinkConfig.defaultConfig(), null, false, Instant.now(), Map.of("fixture-manifest", "fixture-ingest"));
        var report = MainshockWindowAcceptanceJob.blockedDiagnostics(integration);
        assertFalse(report.path("publishable").asBoolean()); assertTrue(report.path("quality_blocked").asBoolean());
        assertEquals(12, report.path("rejected").asInt()); assertEquals(12, report.path("reason_counts").path("INVALID_NUMBER").asInt());
        assertEquals(8, report.path("reject_samples").size()); assertTrue(report.path("parsed_balanced").asBoolean());
        assertFalse(report.toString().contains("rawObjectUri")); assertFalse(report.toString().contains("VERIFIED_PUBLISHED"));
    }
    @Test void qaCannotWriteProductionSchemaOrChangeSamplingYears() throws Exception {
        String suffix = "a".repeat(32);
        var request = json.createObjectNode().put("run_id", "mld03-qa-" + suffix).put("dag_id", "mainshock_window_qa")
            .put("gold_namespace", "iceberg.gold_qa_" + suffix).put("ml_namespace", "iceberg.ml_qa_" + suffix)
            .put("config_version", "1").put("code_version", "artifact_" + "a".repeat(64));
        var pins = json.readTree("{\"bronze_inputs\":[{\"source_system\":\"USGS\"},{\"source_system\":\"JMA_BULLETIN\",\"year\":2000},{\"source_system\":\"JMA_BULLETIN\",\"year\":2023}]}");
        MainshockWindowAcceptanceJob.validate(request, pins);
        request.put("gold_namespace", "iceberg.gold");
        assertThrows(IllegalArgumentException.class, () -> MainshockWindowAcceptanceJob.validate(request, pins));
    }
    @Test void resumeRequiresExactPublishedSourceCodeSilverDigestAndOriginalBronzePins() throws Exception {
        var request = json.createObjectNode().put("run_id", "fixture-run").put("gold_namespace", "iceberg.gold_qa_fixture").put("code_version", "fixture-source-code");
        var pins = json.readTree("{\"bronze_inputs\":[{\"source_system\":\"USGS\",\"sha256\":\"fixture-sha\"}]}");
        var commit = json.createObjectNode().put("gold_run_id", "fixture-run").put("namespace", "iceberg.gold_qa_fixture");
        commit.putObject("scope").put("code_version", "fixture-source-code").put("silver_bundle_sha256", "c".repeat(64));
        var silver = json.createObjectNode(); var context = silver.putObject("context").put("quality_passed", true).put("reconciliation_balanced", true);
        context.putArray("sources").addObject().set("pin", pins.path("bronze_inputs").get(0).deepCopy());
        var publication = json.createObjectNode().put("published", true).put("verify_status", "PASSED")
            .put("gold_run_id", "fixture-run").put("namespace", "iceberg.gold_qa_fixture");
        MainshockWindowAcceptanceJob.validateResumeSource(request, pins, commit, silver, "c".repeat(64), publication);
        publication.put("published", false);
        assertThrows(IllegalArgumentException.class, () -> MainshockWindowAcceptanceJob.validateResumeSource(request, pins, commit, silver, "c".repeat(64), publication));
        publication.put("published", true); request.put("code_version", "other-code");
        assertThrows(IllegalArgumentException.class, () -> MainshockWindowAcceptanceJob.validateResumeSource(request, pins, commit, silver, "c".repeat(64), publication));
        request.put("code_version", "fixture-source-code");
        assertThrows(IllegalArgumentException.class, () -> MainshockWindowAcceptanceJob.validateResumeSource(request, pins, commit, silver, "d".repeat(64), publication));
        ((ObjectNode) pins.path("bronze_inputs").get(0)).put("sha256", "changed");
        assertThrows(IllegalArgumentException.class, () -> MainshockWindowAcceptanceJob.validateResumeSource(request, pins, commit, silver, "c".repeat(64), publication));
    }
}
