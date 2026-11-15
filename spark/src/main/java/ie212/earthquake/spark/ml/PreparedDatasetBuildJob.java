package ie212.earthquake.spark.ml;

import static ie212.earthquake.spark.ml.FeatureExportBundle.*;
import static org.apache.spark.sql.functions.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.GoldCommitJob;
import ie212.earthquake.spark.gold.*;
import ie212.earthquake.spark.silver.*;
import ie212.earthquake.spark.usgs.ResearchDatasetBuildJob;
import java.net.URI;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import org.apache.spark.storage.StorageLevel;

/** Explicit Published-PREPARED adapter. No synthetic legacy catalog/FullMonthReady proof. */
public final class PreparedDatasetBuildJob {
    public static final String VERSION = "dat03-prepared-ml-v2";
    public static final String LEGACY_VERSION = "dat03-prepared-ml-v1";
    public static final String PROFILE = "dat03-whole-window-40k-v1";
    public static final String PROFILE_90K = "dat03-whole-window-90k-v1";
    private PreparedDatasetBuildJob() { }
    public static long profileCap(JsonNode r) throws Exception {
        return switch (r.path("resource_profile_version").asText()) {
            case PROFILE -> 40000L;
            case PROFILE_90K -> 90000L;
            default -> throw new java.io.IOException("PREPARED_ML_PROFILE_REQUIRED");
        };
    }
    public static void validate(JsonNode r, Path root) throws Exception {
        var keys = new HashSet<String>(); r.fieldNames().forEachRemaining(keys::add);
        var expected = new HashSet<>(Set.of("version", "runner_sha256", "gold_run_id", "gold_commit_sha256", "gold_publication_sha256", "scaling_sha256", "observation_cutoff_utc"));
        if (VERSION.equals(r.path("version").asText())) expected.addAll(Set.of("resource_profile_version", "profile_receipt_sha256"));
        require(keys.equals(expected) && Set.of(VERSION, LEGACY_VERSION).contains(r.path("version").asText()) && root.toString().matches("/opt/pipeline/staging/dat03-prepared-ml/[a-f0-9]{32}")
                && r.path("gold_run_id").asText().matches("dat03-gold-[a-f0-9]{32}"), "PREPARED_ML_EXACT_REQUEST_REQUIRED");
        for (String key : List.of("runner_sha256", "gold_commit_sha256", "gold_publication_sha256", "scaling_sha256"))
            require(r.path(key).asText().matches("[a-f0-9]{64}"), "PREPARED_ML_SHA_REQUIRED");
        if (VERSION.equals(r.path("version").asText())) require(profileCap(r) <= 90000
                && r.path("profile_receipt_sha256").asText().matches("[a-f0-9]{64}"), "PREPARED_ML_PROFILE_REQUIRED");
        require(!Instant.parse(r.path("observation_cutoff_utc").asText()).isAfter(Instant.now()), "PREPARED_ML_FUTURE_CUTOFF");
    }
    static ObjectNode config(JsonNode r, String split, JsonNode scaling) throws Exception {
        var c = ResearchDatasetBuildJob.config(split, Instant.parse(r.path("observation_cutoff_utc").asText()), r.path("runner_sha256").asText(), scaling);
        c.put("dataset_config_version", r.path("version").asText());
        ((ObjectNode)c.path("filter_config")).put("source_coverage_contract", PublishedPreparedCoverage.VERSION);
        if (VERSION.equals(r.path("version").asText())) {
            var s = FeatureScalingConfig.fromJson(scaling);
            long cap = profileCap(r);
            require(s.maxCandidatesPerWindow() == cap
                    && ("scaling-global-research-" + cap / 1000 + "k-v1").equals(s.scalingConfigVersion()) && "GLOBAL".equals(s.mode())
                    && s.spaceScaleKm() == 50 && s.depthScaleKm() == 30 && s.timeScaleHours() == 720
                    && s.coordinateToleranceKm() == 1e-8 && s.timeToleranceHours() == 1e-12, "PREPARED_ML_PROFILE_CHANGED_SCIENCE");
            ((ObjectNode)c.path("filter_config")).put("resource_profile_version", r.path("resource_profile_version").asText());
            ((ObjectNode)c.path("filter_config").path("mainshock_selection_config")).put("max_candidates_per_window", cap);
        } else require(scaling.path("max_candidates_per_window").asLong() == 10000, "PREPARED_ML_LEGACY_CAP_CHANGED");
        return c;
    }
    public static void main(String[] args) throws Exception {
        require(args.length == 7 && Set.of("REPRODUCTION", "EXTENSION").contains(args[2])
                && Set.of("preflight", "build", "rerun").contains(args[3]) && args[4].matches("[a-f0-9]{32}"), "PREPARED_ML_ARGS_REQUIRED");
        Path file = PortableInputMaterializer.safe(Path.of(args[0])), root = file.getParent();
        require(WindowArtifactInput.sha(file).equals(args[1]), "PREPARED_ML_REQUEST_CHANGED");
        var r = JSON.readTree(Files.readAllBytes(file)); validate(r, root);
        require(LocalFeatureJob.codeSha().equals(r.path("runner_sha256").asText()) && WindowArtifactInput.sha(root.resolve("feature-scaling.json")).equals(r.path("scaling_sha256").asText()), "PREPARED_ML_CODE_OR_SCALE_CHANGED");
        if (VERSION.equals(r.path("version").asText())) {
            Path profile = root.resolve("resource-profile.json"); require(WindowArtifactInput.sha(profile).equals(r.path("profile_receipt_sha256").asText()), "PREPARED_ML_PROFILE_RECEIPT_CHANGED");
            var measured = JSON.readTree(Files.readAllBytes(profile));
            require(r.path("resource_profile_version").equals(measured.path("profile_version")) && "PROFILE_BENCHMARK_PASSED".equals(measured.path("status").asText())
                    && measured.path("runner_sha256").equals(r.path("runner_sha256")) && measured.path("scaling_sha256").equals(r.path("scaling_sha256")) && measured.path("feature_rows").asLong() == profileCap(r)
                    && measured.path("candidate_semantics_unchanged").asBoolean(false) && measured.path("synthetic_component_qa").asBoolean(false)
                    && measured.path("null_guard_fault_injections_passed").asInt() == 2
                    && measured.path("s3_writes").isBoolean() && !measured.path("s3_writes").asBoolean(), "PREPARED_ML_MEASURED_PROFILE_REQUIRED");
        }
        int seconds = Integer.parseInt(args[5]); require(seconds >= 600 && seconds <= 6000, "PREPARED_ML_BUDGET_REQUIRED");
        long deadline = System.nanoTime() + seconds * 1_000_000_000L;
        String split = args[2], mode = args[3], session = args[4], run = "dat03-ml-" + root.getFileName(), ns = "iceberg.dat03_ml_" + root.getFileName();
        var ownerRequest = JSON.createObjectNode().put("dag_id", "mli_02_build_dataset").put("run_id", run);
        Runnable held = DatasetBuildJob.owner(ownerRequest, Path.of(Objects.requireNonNull(System.getenv("SOURCE_GUARD_ROOT")))); held.run();
        Path splitRoot = root.resolve(split); Files.createDirectories(splitRoot); Files.createDirectories(root.resolve("results"));
        Path output = root.resolve("results/" + session + ".json"); require(!Files.exists(output), "IMMUTABLE_ML_RESULT_REQUIRED");
        var builder = GoldCommitJob.configured(System.getenv()).master("local[1]").appName("dat03-prepared-ml-" + split)
                .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "16").config("spark.sql.adaptive.enabled", "false")
                .config("spark.local.dir", root.resolve("spill").toString());
        PortableGoldCapacityJob.readCompatibilityOptions().forEach(builder::config);
        var spark = builder.getOrCreate(); spark.sparkContext().setLogLevel("ERROR");
        int[] queries = {0};
        try (var store = MinioSilverObjectStore.fromEnvironment()) {
            byte[] raw = store.read("gold-commits/" + r.path("gold_run_id").asText() + "/commit.json"); JsonNode commit = JSON.readTree(raw);
            byte[] pubRaw = store.read("gold-verification/" + r.path("gold_run_id").asText() + "/publication.json"); JsonNode pub = JSON.readTree(pubRaw);
            require(SilverBundlePublisher.sha(raw).equals(r.path("gold_commit_sha256").asText()) && SilverBundlePublisher.sha(pubRaw).equals(r.path("gold_publication_sha256").asText()), "PREPARED_ML_TRUSTED_GOLD_CHANGED");
            PreparedGoldProof.validateCommit(commit);
            var real = TrinoSqlClient.researchCatalog(URI.create("http://" + System.getenv("TRINO_HOST") + ":" + System.getenv("TRINO_INTERNAL_PORT")));
            GoldPublisher.Sql sql = q -> { held.run(); queries[0]++; if ((mode.equals("preflight") || mode.equals("rerun")) && !q.stripLeading().toUpperCase(Locale.ROOT).startsWith("SELECT ")) throw new java.io.IOException("PREPARED_ML_READ_ONLY_SQL_REQUIRED"); return real.execute(q); };
            var writer = new DatasetManifestWriter(sql, held); var reference = writer.resolvePublication(PreparedGoldProof.NAMESPACE, pub.path("publication_id").asText());
            var configuration = config(r, split, JSON.readTree(Files.readAllBytes(root.resolve("feature-scaling.json"))));
            var manifest = new DatasetIdentityPlan(reference, configuration, run, Instant.now()).manifest();
            var rows = DatasetManifestWriter.records(sql.execute("SELECT * FROM " + PreparedGoldProof.NAMESPACE + ".publication_status WHERE publication_id=" + GoldVerificationPlan.quote(pub.path("publication_id").asText())));
            require(rows.size() == 1, "PREPARED_ML_PUBLISHED_ROW_REQUIRED");
            var coverage = FeatureExportCoverage.resolve(store, rows.get(0), manifest);
            var report = JSON.createObjectNode().put("version", VERSION).put("mode", mode).put("dataset_split", split).put("dataset_id", manifest.path("dataset_id").asText())
                    .put("request_sha256", args[1]).put("runner_sha256", r.path("runner_sha256").asText()).put("scientific_approval", false).put("research_ready", false);
            report.set("coverage", coverage); report.set("gold_publication", reference);
            if (mode.equals("preflight")) {
                System.out.println("event=prepared_ml_phase split=" + split + " step=audit_gold_input");
                var audit = GoldInputAuditJob.runPinnedAudit(spark, manifest, GoldAuditConfig.fromManifest(manifest, run, "audit.json"), Instant.now());
                audit.assertReconciled(); require(audit.completenessResult().isReliable() && Double.isFinite(audit.mcApplied()), "DS_EXPORT_MC_UNRELIABLE");
                report.set("audit", audit.toJsonReport()); report.put("mc_value", audit.mcApplied());
                // Persist only local metadata first, so resource failures remain diagnosable without any ML lifecycle write.
                immutable(splitRoot.resolve("preflight-audit-" + session + ".json"), bytes(report));
                var audited = audit.auditedEvents().select("canonical_event_id", "event_time_utc", "latitude", "longitude", "depth_km", "magnitude", "is_eligible").persist(StorageLevel.DISK_ONLY());
                try {
                    require(audited.filter(col("is_eligible").and(col("magnitude").gt(5.5)).and(col("depth_km").between(50, 200)))
                            .limit(4097).count() <= 4096, "COLLECTION_MAINSHOCK_METADATA_LIMIT");
                    System.out.println("event=prepared_ml_phase split=" + split + " step=build_candidate_windows");
                    var windows = new MainshockWindowEngine().execute(audited, MainshockWindowJob.selectionConfig(manifest), MainshockWindowJob.windowConfig(manifest),
                            audit.mcApplied(), manifest.path("dataset_id").asText(), Instant.parse(manifest.path("created_at_utc").asText()), WindowCoveragePolicy.STRICT);
                    windows.assertInvariants(); var all = windows.mainshockSnapshot().select("mainshock_event_id", "candidate_count", "resource_guard_status").orderBy("mainshock_event_id").limit(4097).collectAsList();
                    var blocked = report.putArray("blocked_windows"); var summaries = new ArrayList<ResearchCollectionPlan.Window>();
                    for (var row : all) { summaries.add(new ResearchCollectionPlan.Window(row.getString(0), row.getLong(1))); if (!"PASS".equals(row.getString(2))) blocked.addObject().put("mainshock_event_id", row.getString(0)).put("candidate_count", row.getLong(1)).put("status", row.getString(2)); }
                    report.set("window_coverage", windows.windowCoverageReport()); report.put("mainshock_count", all.size());
                    report.put("candidate_row_count", windows.candidateSnapshot().count());
                    if (!blocked.isEmpty()) report.put("status", "BLOCKED_RESOURCE");
                    else { var plan = ResearchCollectionPlan.create(manifest, coverage, ResearchCollectionJob.mcProof(manifest, audit), windows.windowCoverageReport(), summaries, ResearchCollectionPlan.Limits.defaults()); report.set("capacity_plan", plan); report.put("status", "PREFLIGHT_PASSED"); }
                } finally { audited.unpersist(); }
                report.put("ml_writes", false);
            } else {
                Path gate = splitRoot.resolve("preflight-accepted.json");
                require(args[6].matches("[a-f0-9]{64}") && WindowArtifactInput.sha(gate).equals(args[6]), "PREPARED_ML_CAPACITY_PIN_REQUIRED");
                JsonNode accepted = JSON.readTree(Files.readAllBytes(gate));
                require("PREFLIGHT_PASSED".equals(accepted.path("status").asText()) && args[1].equals(accepted.path("request_sha256").asText())
                        && manifest.path("dataset_id").equals(accepted.path("dataset_id")), "PREPARED_ML_ACCEPTED_CAPACITY_REQUIRED");
                var request = JSON.createObjectNode().put("contract_version", DatasetBuildJob.COLLECTION_VERSION).put("dag_id", "mli_02_build_dataset").put("run_id", run)
                        .put("build_run_id", run).put("ml_namespace", ns).put("run_root", splitRoot.toString()).put("attempt", 1)
                        .put("export_root", splitRoot.resolve("export").toString()).put("registry_root", splitRoot.resolve("registry").toString());
                request.set("config", configuration); request.set("publication_reference", reference); request.put("step", "plan");
                var plan = DatasetBuildJob.execute(request, sql, store, spark, held); request.set("plan", plan);
                Path receipts = splitRoot.resolve("receipts"), chain = splitRoot.resolve("receipt-chain"); Files.createDirectories(receipts); Files.createDirectories(chain);
                JsonNode previous = null; String prevSha = null; int completed = 0;
                for (String step : DatasetBuildJob.STEPS) {
                    Path receipt = receipts.resolve(step + ".json"), bound = chain.resolve(step + ".json");
                    if (Files.exists(receipt)) {
                        var old = JSON.readTree(Files.readAllBytes(receipt)); var link = JSON.readTree(Files.readAllBytes(bound));
                        require(old.path("dataset_id").equals(plan.path("dataset_id")) && step.equals(old.path("step").asText()) && old.path("plan_sha256").asText().equals(digest(plan))
                                && WindowArtifactInput.sha(receipt).equals(link.path("receipt_sha256").asText()) && Objects.equals(prevSha, link.path("previous_sha256").isNull() ? null : link.path("previous_sha256").asText()), "PREPARED_ML_CHECKPOINT_CHANGED");
                        previous = old; prevSha = WindowArtifactInput.sha(bound); completed++; continue;
                    }
                    require(!mode.equals("rerun"), "PREPARED_ML_RERUN_REQUIRES_COMPLETE_PREFIX");
                    require(DatasetBuildJob.STEPS.subList(completed + 1, 10).stream().noneMatch(x -> Files.exists(receipts.resolve(x + ".json"))), "PREPARED_ML_NONCONTIGUOUS_PREFIX");
                    if ((deadline - System.nanoTime()) / 1_000_000_000L < 3600) break;
                    request.put("step", step); if (previous == null) request.putNull("previous_receipt_sha256"); else request.put("previous_receipt_sha256", WindowArtifactInput.sha(receipts.resolve(DatasetBuildJob.STEPS.get(completed - 1) + ".json")));
                    System.out.println("event=prepared_ml_phase split=" + split + " step=" + step + " ordinal=" + (completed + 1) + "/10");
                    previous = DatasetBuildJob.execute(request, sql, store, spark, held); immutable(receipt, bytes(previous));
                    var link = JSON.createObjectNode().put("receipt_sha256", WindowArtifactInput.sha(receipt)); if (prevSha == null) link.putNull("previous_sha256"); else link.put("previous_sha256", prevSha);
                    immutable(bound, bytes(link)); prevSha = WindowArtifactInput.sha(bound); completed++;
                }
                report.put("completed_steps", completed).put("status", completed == 10 ? "DATASET_EXPORTED" : "CHECKPOINTED"); report.set("last_receipt", previous);
                if (completed == 10) { request.put("step", "mark_dataset_exported").put("previous_receipt_sha256", WindowArtifactInput.sha(receipts.resolve("export_parquet.json"))); report.set("final_readback", DatasetBuildJob.execute(request, sql, store, spark, held)); }
            }
            for (String table : GoldIcebergWriter.TABLES) require(FeatureExportJob.scalar(sql, "SELECT snapshot_id FROM " + PreparedGoldProof.NAMESPACE + ".\"" + table + "$refs\" WHERE name='main'") == commit.path("tables").path(table).path("snapshot_id").asLong(), "PREPARED_ML_GOLD_SNAPSHOT_MOVED");
            report.put("gold_snapshots_unchanged", true).put("sql_calls", queries[0]); held.run(); immutable(output, bytes(report));
            System.out.println("event=prepared_ml_completed split=" + split + " status=" + report.path("status").asText());
        } finally { spark.stop(); }
    }
}
