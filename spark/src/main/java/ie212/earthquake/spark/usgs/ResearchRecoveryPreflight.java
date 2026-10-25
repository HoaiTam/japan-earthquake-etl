package ie212.earthquake.spark.usgs;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.gold.*;
import ie212.earthquake.spark.ml.LocalFeatureJob;
import ie212.earthquake.spark.orchestration.RealEtlAdapter;
import ie212.earthquake.spark.silver.*;
import java.net.URI;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;

/** Metadata-only preflight for the retained DAT-03 failure. No Spark, ETL or publication writes. */
public final class ResearchRecoveryPreflight {
    public static final String VERSION = "dat03-recovery-preflight-v1";
    public static final List<String> COMPLETED = List.of("2000-01", "2011-04");
    public static final String INCOMPLETE = "2016-04";
    private ResearchRecoveryPreflight() { }

    static JsonNode read(SilverObjectStore store, String key) throws Exception {
        byte[] bytes = store.read(key);
        require(bytes.length > 0 && bytes.length <= 4 * 1024 * 1024, "DAT03_RECOVERY_METADATA_LIMIT");
        return JSON.readTree(bytes);
    }
    static Path safe(Path path) throws Exception {
        path = path.toAbsolutePath().normalize();
        for (Path p = path; p != null; p = p.getParent()) require(!Files.isSymbolicLink(p), "DAT03_RECOVERY_SYMLINK_REJECTED");
        return path;
    }
    static void localEquals(Path root, String op, String name, JsonNode actual) throws Exception {
        require(ResearchBoundedE2EJob.metadataLarge(safe(root.resolve(op).resolve(name))).equals(actual), "DAT03_RECOVERY_LOCAL_REMOTE_CHANGED");
    }
    static JsonNode chain(SilverObjectStore store, JsonNode context, int phases, JsonNode expectedReceipts,
            Path root, Runnable lease) throws Exception {
        RealEtlAdapter.validateContext(context);
        String op = text(context, "operation_id"), prefix = "real-etl/" + op + "/";
        require(read(store, prefix + "context.json").equals(context), "DAT03_RECOVERY_CONTEXT_CHANGED");
        JsonNode prior = null;
        for (int i = 0; i < phases; i++) {
            lease.run(); String phase = RealEtlAdapter.PHASES.get(i);
            var receipt = read(store, prefix + phase + "-receipt.json");
            var artifact = read(store, prefix + phase + ".json");
            require("orc-07-v1".equals(text(receipt, "contract_version")) && "real".equals(text(receipt, "mode"))
                    && "ok".equals(text(receipt, "status")) && phase.equals(text(receipt, "phase"))
                    && op.equals(text(receipt, "operation_id")) && text(context, "context_sha256").equals(text(receipt, "context_sha256"))
                    && (prior == null ? receipt.path("upstream_sha256").isNull() : digest(prior).equals(text(receipt, "upstream_sha256")))
                    && store.uriForKey(prefix + phase + ".json").equals(text(receipt, "artifact_uri"))
                    && digest(artifact).equals(text(receipt, "artifact_sha256")), "DAT03_RECOVERY_PHASE_CHAIN_CHANGED");
            if (expectedReceipts != null) require(expectedReceipts.path(i).equals(receipt), "DAT03_RECOVERY_RECEIPT_CHANGED");
            localEquals(root, op, phase + ".json", artifact);
            prior = receipt;
        }
        return prior;
    }

    /** All writes are deliberately absent, including lease release and acceptance generation. */
    static ObjectNode inspect(JsonNode request, Path original, SilverObjectStore store, GoldPublisher.Sql sql, Runnable lease) throws Exception {
        safe(original);
        ResearchBoundedE2EJob.validateRequest(request);
        lease.run(); String ns = text(request, "namespace"), run = text(request, "run_id");
        Map<String, Long> baseline = new TreeMap<>();
        for (String table : GoldIcebergWriter.TABLES) baseline.put(table, 0L);
        String cutoff = null; JsonNode parent = null;
        for (String month : List.of("2000-01", "2011-04", INCOMPLETE)) {
            String op = RealEtlAdapter.operation("dat03_bounded_e2e", run + "-" + month);
            var context = ResearchBoundedE2EJob.metadata(safe(original.resolve(op).resolve("context.json")));
            var resolver = context.path("resolver_request");
            require("dat03_bounded_e2e".equals(text(context, "dag_id")) && (run + "-" + month).equals(text(context, "run_id"))
                    && op.equals(text(context, "operation_id")) && "manual".equals(text(context, "trigger"))
                    && start(month).toString().equals(text(context, "window_start_utc")) && end(month).toString().equals(text(context, "window_end_utc"))
                    && "orc-06-v2".equals(text(resolver, "resolver_version")) && ns.equals(text(resolver, "namespace"))
                    && ResearchBoundedE2EJob.VERSION.equals(text(resolver, "config_version"))
                    && ("jar-" + text(request, "runner_sha256").substring(0, 16)).equals(text(resolver, "code_version"))
                    && months(resolver.path("target_months")).equals(Set.of(month))
                    && snapshots(resolver.path("baseline_snapshots")).equals(baseline), "DAT03_RECOVERY_SCOPE_CHANGED");
            if (cutoff == null) cutoff = text(resolver, "source_cutoff_utc");
            require(cutoff.equals(text(resolver, "source_cutoff_utc")) && cutoff.equals(text(resolver, "processed_at_utc")), "DAT03_RECOVERY_CUTOFF_CHANGED");
            require(parent == null ? resolver.path("baseline").isNull() : resolver.path("baseline").equals(parent), "DAT03_RECOVERY_PARENT_CHANGED");
            int year = Integer.parseInt(month.substring(0, 4));
            var inputs = resolver.path("bronze_inputs");
            var capture = ResearchBoundedE2EJob.metadata(safe(original.resolve("inputs/h" + year + "-capture.json")));
            require(inputs.isArray() && inputs.size() == 1 && "JMA_BULLETIN".equals(text(inputs.get(0), "source_system"))
                    && inputs.get(0).path("year").asInt() == year && "full-year".equals(text(inputs.get(0), "segment"))
                    && text(capture, "sha256").equals(text(inputs.get(0), "sha256"))
                    && text(capture, "catalog_release").equals(text(inputs.get(0), "catalog_release")), "DAT03_RECOVERY_SOURCE_CHANGED");
            var commitKey = "gold-commits/" + op + "/commit.json";
            byte[] commitBytes = store.read(commitKey);
            require(commitBytes.length <= 4 * 1024 * 1024, "DAT03_RECOVERY_METADATA_LIMIT");
            var commit = JSON.readTree(commitBytes); var plan = new GoldVerificationPlan(commit);
            require(op.equals(text(commit, "gold_run_id")) && ns.equals(plan.namespace())
                    && snapshots(commit.path("scope").path("baseline_snapshots")).equals(baseline)
                    && months(commit.path("scope").path("affected_months")).equals(Set.of(month)), "DAT03_RECOVERY_COMMIT_CHANGED");
            localEquals(original, op, "gold.json", commit);
            JsonNode progress = null;
            if (COMPLETED.contains(month)) {
                progress = ResearchBoundedE2EJob.metadataLarge(safe(original.resolve("progress-" + month + ".json")));
                require(month.equals(text(progress, "month")) && progress.path("rerun_identical").asBoolean(false)
                        && progress.path("retention_verified").asBoolean(false) && progress.path("first").equals(progress.path("rerun"))
                        && context.equals(progress.path("first").path("context")) && commit.equals(progress.path("first").path("commit")), "DAT03_RECOVERY_PROGRESS_CHANGED");
            }
            var prior = chain(store, context, progress == null ? 4 : 6,
                    progress == null ? null : progress.path("first").path("receipts"), original, lease);
            var committed = JSON.createObjectNode();
            for (String table : GoldIcebergWriter.TABLES) committed.put(table, plan.snapshot(table));
            require(snapshots(prior.path("snapshots")).equals(snapshots(committed)), "DAT03_RECOVERY_SNAPSHOTS_CHANGED");
            if (progress == null) {
                // This contract only recovers the known pre-publication boundary. Unknown partial writes fail closed.
                for (String name : List.of("verify.json", "verify-receipt.json", "publish.json", "publish-receipt.json"))
                    require(!store.exists("real-etl/" + op + "/" + name), "DAT03_RECOVERY_UNKNOWN_LATER_PHASE");
                require(!store.exists("gold-verification/" + op + "/publication.json"), "DAT03_RECOVERY_ALREADY_PUBLISHED");
                var checks = JSON.createObjectNode();
                for (String table : GoldIcebergWriter.TABLES) {
                    checked(sql, lease, checks, "current:" + table, "SELECT snapshot_id FROM " + plan.metadata(table, "refs") + " WHERE name='main'", plan.snapshot(table));
                    checked(sql, lease, checks, "identity:" + table, "SELECT count(*) FROM " + plan.metadata(table, "snapshots")
                            + " WHERE snapshot_id=" + plan.snapshot(table) + " AND summary['gold.operation_id']=" + GoldVerificationPlan.quote(op)
                            + " AND summary['gold.identity_sha256']=" + GoldVerificationPlan.quote(text(commit, "identity_sha256")), 1);
                }
                checked(sql, lease, checks, "publication_absent", "SELECT count(*) FROM " + ns
                        + ".publication_status WHERE gold_run_id=" + GoldVerificationPlan.quote(op), 0);
                lease.run();
                var result = JSON.createObjectNode().put("version", VERSION).put("status", "READY_FOR_PUBLICATION_RECOVERY")
                        .put("run_id", run).put("namespace", ns).put("month", month).put("operation_id", op)
                        .put("context_sha256", text(context, "context_sha256")).put("commit_sha256", sha(commitBytes))
                        .put("original_runner_sha256", text(request, "runner_sha256")).put("source_cutoff_utc", cutoff)
                        .put("recovered", false).put("acceptance_passed", false).put("research_ready", false).put("can_release_lease", false);
                result.set("snapshots", committed); result.set("checks", checks);
                result.set("next_phases", JSON.valueToTree(List.of("verify", "publish")));
                result.set("do_not_replay", JSON.valueToTree(List.of("readiness", "bronze", "silver", "gold")));
                result.put("remaining_case", "2023-12"); return result;
            }
            baseline = snapshots(committed);
            String registry = ie212.earthquake.spark.orchestration.FullMonthStateResolver.root(op) + "/registry.json";
            parent = JSON.createObjectNode().put("run_id", op).put("registry_sha256", sha(store.read(registry)))
                    .put("commit_key", commitKey).put("commit_sha256", sha(commitBytes));
        }
        throw new IllegalStateException("DAT03_RECOVERY_INCOMPLETE_CASE_REQUIRED");
    }
    static JsonNode validateFiles(JsonNode input, Path original) throws Exception {
        safe(original); var fields = new HashSet<String>(); input.fieldNames().forEachRemaining(fields::add);
        require(fields.equals(Set.of("version", "original_root", "request_sha256", "execution_sha256", "completed_progress_sha256",
                "incomplete_context_file_sha256", "incomplete_commit_file_sha256", "incomplete_snapshots", "preflight_runner_sha256"))
                && VERSION.equals(text(input, "version")), "DAT03_RECOVERY_EXACT_REQUEST_REQUIRED");
        for (String name : List.of("request", "execution")) {
            Path file = safe(original.resolve(name + ".json"));
            ResearchBoundedE2EJob.metadata(file);
            require(sha(Files.readAllBytes(file)).equals(text(input, name + "_sha256")), "DAT03_RECOVERY_ORIGINAL_CHANGED");
        }
        var request = ResearchBoundedE2EJob.metadata(original.resolve("request.json"));
        ResearchBoundedE2EJob.validateRequest(request);
        var execution = ResearchBoundedE2EJob.metadata(original.resolve("execution.json"));
        require(execution.path("exit_code").isInt() && execution.path("exit_code").asInt() != 0
                && execution.path("lease_held_at_execution_end").isBoolean() && execution.path("lease_held_at_execution_end").asBoolean(), "DAT03_RECOVERY_FAILED_EXECUTION_REQUIRED");
        for (String name : List.of("report.json", "acceptance.json", "progress-2016-04.json", "progress-2023-12.json"))
            require(!Files.exists(original.resolve(name), LinkOption.NOFOLLOW_LINKS), "DAT03_RECOVERY_UNEXPECTED_COMPLETION");
        var pins = input.path("completed_progress_sha256");
        require(pins.isObject() && pins.size() == 2, "DAT03_RECOVERY_COMPLETED_SET_CHANGED");
        for (String month : COMPLETED) {
            Path file = safe(original.resolve("progress-" + month + ".json")); ResearchBoundedE2EJob.metadataLarge(file);
            require(sha(Files.readAllBytes(file)).equals(text(pins, month)), "DAT03_RECOVERY_PROGRESS_CHANGED");
        }
        String op = RealEtlAdapter.operation("dat03_bounded_e2e", text(request, "run_id") + "-" + INCOMPLETE);
        Path context = safe(original.resolve(op + "/context.json")); ResearchBoundedE2EJob.metadata(context);
        require(sha(Files.readAllBytes(context)).equals(text(input, "incomplete_context_file_sha256")), "DAT03_RECOVERY_CONTEXT_CHANGED");
        Path commitFile = safe(original.resolve(op + "/gold.json"));
        var commit = ResearchBoundedE2EJob.metadataLarge(commitFile);
        require(sha(Files.readAllBytes(commitFile)).equals(text(input, "incomplete_commit_file_sha256")), "DAT03_RECOVERY_COMMIT_CHANGED");
        var committed = new TreeMap<String, Long>();
        for (String table : GoldIcebergWriter.TABLES) committed.put(table, new GoldVerificationPlan(commit).snapshot(table));
        require(committed.equals(snapshots(input.path("incomplete_snapshots"))), "DAT03_RECOVERY_SNAPSHOTS_CHANGED");
        for (String file : List.of("runner.jar", "baseline.csv", "boundary.csv")) {
            Path path = safe(original.resolve(file));
            require(Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) && Files.size(path) <= 64 * 1024 * 1024, "DAT03_RECOVERY_BOUNDED_INPUT_REQUIRED");
            String field = file.equals("runner.jar") ? "runner_sha256" : file.equals("baseline.csv") ? "baseline_inventory_sha256" : "boundary_inventory_sha256";
            require(sha(Files.readAllBytes(path)).equals(text(request, field)), "DAT03_RECOVERY_INPUT_CHANGED");
        }
        Instant cutoff = Instant.parse(text(ResearchBoundedE2EJob.metadata(context).path("resolver_request"), "source_cutoff_utc"));
        for (var pin : request.path("jma_captures")) {
            int year = pin.path("year").asInt();
            var capture = ResearchBoundedE2EJob.metadata(safe(original.resolve("inputs/h" + year + "-capture.json")));
            require(digest(capture).equals(text(pin, "capture_sha256")) && text(pin, "sha256").equals(text(capture, "sha256"))
                    && capture.path("http_status").asInt() == 200 && !capture.path("inventory_metadata_drift").asBoolean(true)
                    && !Instant.parse(text(capture, "retrieved_at_utc")).isAfter(cutoff), "DAT03_RECOVERY_CAPTURE_CHANGED");
            safe(original.resolve("inputs/h" + year + ".zip"));
            ResearchBoundedE2EJob.raw(original, "h" + year + ".zip", text(pin, "sha256"), capture.path("bytes").asLong());
        }
        var usgs = ResearchBoundedE2EJob.metadata(safe(original.resolve("inputs/usgs-capture.json")));
        require(digest(usgs).equals(text(request, "usgs_capture_sha256")), "DAT03_RECOVERY_CAPTURE_CHANGED");
        ResearchBoundedE2EJob.validateUsgs(usgs, cutoff); safe(original.resolve("inputs/usgs-december2023.geojson"));
        ResearchBoundedE2EJob.raw(original, "usgs-december2023.geojson", text(usgs, "sha256"), usgs.path("bytes").asLong());
        return request;
    }
    private static void checked(GoldPublisher.Sql sql, Runnable lease, ObjectNode checks, String name, String query, long expected) throws Exception {
        lease.run(); var result = sql.execute(query);
        require(result.rows().size() == 1 && result.rows().get(0).size() == 1 && result.rows().get(0).get(0).isIntegralNumber()
                && result.rows().get(0).get(0).canConvertToLong() && result.rows().get(0).get(0).asLong() == expected, "DAT03_RECOVERY_LIVE_PIN_CHANGED");
        checks.putObject(name).put("sql", query).put("actual", expected).put("expected", expected);
    }
    public static void main(String[] args) throws Exception {
        require(args.length == 1, "DAT03_RECOVERY_REQUEST_REQUIRED");
        var input = ResearchBoundedE2EJob.metadata(Path.of(args[0]));
        require(VERSION.equals(text(input, "version")) && LocalFeatureJob.codeSha().equals(text(input, "preflight_runner_sha256")), "DAT03_RECOVERY_RUNNER_CHANGED");
        Path original = safe(Path.of(text(input, "original_root")));
        require(original.toString().matches("/opt/pipeline/staging/dat03-e2e/[a-f0-9]{32}"), "DAT03_RECOVERY_EXACT_ROOT_REQUIRED");
        var request = validateFiles(input, original);
        require(original.getFileName().toString().equals(text(request, "run_id").substring(6)), "DAT03_RECOVERY_RUN_CHANGED");
        Path guard = Path.of(Objects.requireNonNull(System.getenv("SOURCE_GUARD_ROOT")), "owner.json");
        Runnable lease = () -> { try { require(ResearchBoundedE2EJob.metadata(guard).equals(request.path("lease_owner")), "DAT03_RECOVERY_LEASE_NOT_HELD"); }
            catch (Exception error) { throw new IllegalStateException("DAT03_RECOVERY_LEASE_NOT_HELD"); } };
        try (var store = MinioSilverObjectStore.fromEnvironment()) {
            var sql = new TrinoSqlClient(URI.create("http://" + System.getenv("TRINO_HOST") + ":" + System.getenv("TRINO_INTERNAL_PORT")));
            var result = inspect(request, original, store, sql, lease);
            result.put("preflight_runner_sha256", text(input, "preflight_runner_sha256"));
            result.put("request_sha256", text(input, "request_sha256")).put("execution_sha256", text(input, "execution_sha256"));
            System.out.println(JSON.writeValueAsString(result));
        }
    }
}
