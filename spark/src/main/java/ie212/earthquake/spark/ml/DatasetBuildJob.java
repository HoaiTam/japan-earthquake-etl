package ie212.earthquake.spark.ml;

import static ie212.earthquake.spark.ml.FeatureExportBundle.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.GoldCommitJob;
import ie212.earthquake.spark.gold.*;
import ie212.earthquake.spark.silver.*;
import java.net.URI;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import org.apache.spark.sql.SparkSession;

/** MLI-02 fixed step adapter. Real pinned reads/engines/export, never mock data or caller Mc. */
public final class DatasetBuildJob {
    public static final String VERSION = "mli-02-real-v1";
    public static final List<String> STEPS = List.of("wait_gold_published", "resolve_gold_snapshot", "audit_gold_input",
        "resolve_mc", "select_mainshocks", "build_candidate_windows", "build_spacetime_features",
        "validate_feature_snapshot", "export_parquet", "mark_dataset_exported");
    private DatasetBuildJob() { }

    public static ObjectNode plan(DatasetManifestWriter writer, JsonNode request) throws Exception {
        var reference = request.path("publication_reference");
        String physical = reference.path("physical_gold_table_name").asText();
        require(physical.matches("[a-z][a-z0-9_]*\\.[a-z][a-z0-9_]*\\.event_current"), "MLI02_EXACT_GOLD_REQUIRED");
        String gold = physical.substring(0, physical.lastIndexOf('.'));
        String ml = request.path("ml_namespace").asText();
        require(ml.matches("[a-z][a-z0-9_]*\\.[a-z][a-z0-9_]*") && !ml.equals(gold)
            && ml.split("\\.")[0].equals(gold.split("\\.")[0]), "MLI02_NAMESPACE_CONFLICT");
        var publication = writer.resolvePublication(gold, reference.path("gold_publication_id").asText());
        require(reference.path("gold_snapshot_id").isIntegralNumber()
            && publication.path("gold_snapshot_id").asText().equals(reference.path("gold_snapshot_id").asText()), "MLI02_PUBLICATION_PIN_CHANGED");
        for (String key : List.of("gold_publication_id", "physical_gold_table_name", "gold_table_name", "publication_bundle_sha256"))
            require(publication.path(key).equals(reference.path(key)), "MLI02_PUBLICATION_PIN_CHANGED");
        var identity = new DatasetIdentityPlan(publication, request.path("config"), request.path("build_run_id").asText(), Instant.now());
        var result = JSON.createObjectNode().put("dataset_id", identity.manifest().path("dataset_id").asText())
            .put("identity_payload_sha256", identity.manifest().path("identity_payload_sha256").asText());
        result.set("gold_publication", publication); return result;
    }

    static Runnable owner(JsonNode request, Path guard) throws Exception {
        String run = request.path("run_id").asText(), dag = request.path("dag_id").asText();
        require("mli_02_build_dataset".equals(dag) && run.matches("[A-Za-z0-9][A-Za-z0-9_.:+@/-]{0,255}")
            && !run.contains(".."), "MLI02_EXACT_OWNER_REQUIRED");
        return () -> { try {
            var held = JSON.readTree(Files.readAllBytes(guard.resolve("owner.json")));
            require(run.equals(held.path("run_id").asText()) && dag.equals(held.path("dag_id").asText()), "MLI02_LEASE_NOT_HELD");
        } catch (Exception error) { throw new IllegalStateException("MLI02_LEASE_NOT_HELD"); } };
    }

    static JsonNode bounded(Path path) throws Exception {
        require(!Files.isSymbolicLink(path) && Files.isRegularFile(path) && Files.size(path) <= 65536, "MLI02_BOUNDED_METADATA_REQUIRED");
        return JSON.readTree(Files.readAllBytes(path));
    }

    static ObjectNode phase(SparkSession spark, GoldPublisher.Sql sql, SilverObjectStore store, JsonNode request,
            ObjectNode plan, Runnable lease) throws Exception {
        String step = request.path("step").asText(), ns = request.path("ml_namespace").asText();
        String id = plan.path("dataset_id").asText(), run = request.path("build_run_id").asText();
        var out = JSON.createObjectNode();
        if (step.equals("wait_gold_published")) return out.put("status", "VERIFIED_PUBLISHED");
        if (step.equals("resolve_gold_snapshot")) {
            var pub = plan.path("gold_publication"); String physical = pub.path("physical_gold_table_name").asText();
            var manifest = new DatasetManifestWriter(sql, lease).build(physical.substring(0, physical.lastIndexOf('.')),
                pub.path("gold_publication_id").asText(), ns, request.path("config"), run, Instant.now());
            require(id.equals(manifest.path("dataset_id").asText()), "MLI02_DATASET_ID_CHANGED");
            return out.put("status", "SNAPSHOT_PINNED").put("dataset_status", manifest.path("dataset_status").asText());
        }
        // Reruns may already be VALIDATED/EXPORTED; do not require BUILDING or downgrade them.
        var manifest = FeatureExportJob.manifest(sql, ns, id);
        DatasetManifestWriter.pinnedGoldSql(manifest);
        Path output = Path.of(request.path("export_root").asText()), registry = Path.of(request.path("registry_root").asText());
        if (step.equals("validate_feature_snapshot"))
            return FeatureExportJob.prepare(spark, sql, store, ns, id, run, output, registry, lease);
        if (step.equals("export_parquet") || step.equals("mark_dataset_exported")) {
            var result = FeatureExportJob.export(spark, sql, store, ns, id, run, output, registry, lease);
            if (step.equals("mark_dataset_exported")) {
                var trusted = result.deepCopy().put("algorithm_name", "WINDOW").put("model_config_version", "1")
                    .put("experiment_run_id", "exp_mli02_handoff_check");
                var resolved = TrustedDatasetResolver.resolve(trusted, registry, null);
                require(resolved.candidates().size() == result.path("feature_row_count").asLong(), "MLI02_TRUSTED_HANDOFF_CHANGED");
                result.put("trusted_handoff_verified", true);
            }
            result.put("export_artifact_uri", output.resolve("dataset_id=" + id).toString());
            return result;
        }
        // Coverage is receipt-bound; no boolean from config can replace full-period history.
        String physical = manifest.path("physical_gold_table_name").asText(), gold = physical.substring(0, physical.lastIndexOf('.'));
        var pubs = DatasetManifestWriter.records(sql.execute("SELECT * FROM " + gold + ".publication_status WHERE publication_id="
            + GoldVerificationPlan.quote(manifest.path("gold_publication_id").asText())));
        require(pubs.size() == 1, "DS_GOLD_NOT_PUBLISHED");
        var coverage = FeatureExportCoverage.resolve(store, pubs.get(0), manifest);
        var audit = GoldInputAuditJob.runPinnedAudit(spark, manifest, GoldAuditConfig.fromManifest(manifest, run, "audit-report.json"), Instant.now());
        audit.assertReconciled();
        require(audit.completenessResult().isReliable() && Double.isFinite(audit.mcApplied()), "DS_EXPORT_MC_UNRELIABLE");
        if (step.equals("audit_gold_input")) {
            out.set("counts", audit.toJsonReport().path("counts")); out.set("coverage", coverage);
            return out.put("status", "AUDIT_PASSED");
        }
        if (step.equals("resolve_mc")) return out.put("status", "MC_RESOLVED").put("mc_value", audit.mcApplied())
            .put("mc_config_sha256", audit.mcConfigSha256());
        // A phase has many invariant/null/vector actions. Cache only its exact pin's
        // bounded intermediates so every column check does not rebuild the Gold/window DAG.
        var audited = audit.auditedEvents().cache();
        try {
            var windows = new MainshockWindowEngine().execute(audited, MainshockWindowJob.selectionConfig(manifest),
                MainshockWindowJob.windowConfig(manifest), audit.mcApplied(), id, Instant.parse(manifest.path("created_at_utc").asText()));
            var main = windows.mainshockSnapshot().cache(); var candidatesFrame = windows.candidateSnapshot().cache();
            try {
                windows.assertInvariants();
                long mainCount = main.count(), candidates = candidatesFrame.count();
                require(mainCount > 0 && candidates > 0 && mainCount <= 128 && candidates <= MAX_ROWS, "DS_EXPORT_RESOURCE_LIMIT");
                out.put("mainshock_count", mainCount).put("candidate_row_count", candidates);
                if (step.equals("select_mainshocks")) return out.put("status", "MAINSHOCKS_SELECTED");
                if (step.equals("build_candidate_windows")) return out.put("status", "WINDOWS_BUILT")
                    .put("candidate_content_sha256", MainshockWindowJob.fingerprint(candidatesFrame));
                require(step.equals("build_spacetime_features"), "MLI02_UNKNOWN_STEP");
                var filter = JSON.readTree(manifest.path("filter_config_json").asText());
                var scaling = FeatureScalingConfig.fromJson(filter.path("feature_scaling_config"));
                var features = new SpacetimeFeatureEngine().execute(main, candidatesFrame, scaling);
                features.assertValid(); var featureRows = features.features().cache();
                try {
                    var quality = validate(featureRows, main, manifest, scaling);
                    out.put("status", "FEATURES_BUILT").put("feature_row_count", features.validCount());
                    out.set("quality", quality); return out;
                } finally { featureRows.unpersist(); }
            } finally { main.unpersist(); candidatesFrame.unpersist(); }
        } finally { audited.unpersist(); }
    }

    public static ObjectNode execute(JsonNode request, GoldPublisher.Sql sql, SilverObjectStore store,
            SparkSession spark, Runnable lease) throws Exception {
        require(VERSION.equals(request.path("contract_version").asText()) && request.path("attempt").isIntegralNumber()
            && request.path("attempt").asInt() > 0, "MLI02_PROTOCOL_REQUIRED");
        String step = request.path("step").asText(), ns = request.path("ml_namespace").asText();
        require((step.equals("plan") || STEPS.contains(step)) && ns.matches("[a-z][a-z0-9_]*\\.[a-z][a-z0-9_]*"), "MLI02_EXACT_STEP_NAMESPACE_REQUIRED");
        lease.run(); var planned = plan(new DatasetManifestWriter(sql, lease), request);
        if (step.equals("plan")) return planned;
        require(digest(planned).equals(digest(request.path("plan"))), "MLI02_RESOLVED_PLAN_CHANGED");
        Path receipts = safeRoot(Path.of(request.path("run_root").asText()).resolve("receipts"));
        int index = STEPS.indexOf(step);
        if (index > 0) {
            var previous = bounded(receipts.resolve(STEPS.get(index - 1) + ".json"));
            require(previous.path("step").asText().equals(STEPS.get(index - 1))
                && previous.path("run_id").equals(request.path("run_id")) && previous.path("plan_sha256").asText().equals(digest(planned))
                && WindowArtifactInput.sha(receipts.resolve(STEPS.get(index - 1) + ".json"))
                    .equals(request.path("previous_receipt_sha256").asText()), "MLI02_PREVIOUS_STEP_REQUIRED");
        }
        var result = phase(spark, sql, store, request, planned, lease);
        result.put("contract_version", VERSION).put("step", step).put("run_id", request.path("run_id").asText())
            .put("dataset_id", planned.path("dataset_id").asText()).put("identity_payload_sha256", planned.path("identity_payload_sha256").asText())
            .put("plan_sha256", digest(planned)).put("attempt", request.path("attempt").asInt());
        result.set("gold_snapshot_id", planned.path("gold_publication").path("gold_snapshot_id"));
        return result;
    }

    public static void main(String[] args) {
        SparkSession spark = null;
        try {
            require(args.length == 1, "MLI02_REQUEST_REQUIRED"); Path path = Path.of(args[0]); var request = bounded(path);
            var lease = owner(request, Path.of(Objects.requireNonNull(System.getenv("SOURCE_GUARD_ROOT")))); lease.run();
            String host = System.getenv("TRINO_HOST"), port = System.getenv("TRINO_INTERNAL_PORT");
            require(host != null && host.matches("[a-z0-9][a-z0-9.-]*") && port != null && port.matches("[0-9]{1,5}"), "MLI02_TRINO_CONFIG_REQUIRED");
            String step = request.path("step").asText();
            if (!Set.of("plan", "wait_gold_published", "resolve_gold_snapshot").contains(step)) {
                spark = GoldCommitJob.configured(System.getenv()).appName("mli-02-" + step).getOrCreate(); spark.sparkContext().setLogLevel("ERROR");
            }
            try (var store = MinioSilverObjectStore.fromEnvironment()) {
                var result = execute(request, new TrinoSqlClient(URI.create("http://" + host + ":" + port)), store, spark, lease);
                immutable(path.getParent().resolve("receipt.json"), bytes(result)); System.out.println(result);
            }
        } catch (Exception error) {
            String reason = error.getMessage();
            if (reason == null || !reason.matches("[A-Z][A-Z0-9_:-]{0,180}")) reason = "MLI02_STEP_FAILED";
            System.err.println("event=mli02_failed reason=" + reason); System.exit(2);
        } finally { if (spark != null) spark.stop(); }
    }
}
