package ie212.earthquake.spark.ml;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.GoldCommitJob;
import ie212.earthquake.spark.ml.TrinoMlVerifier;
import ie212.earthquake.spark.gold.GoldPublisher;
import ie212.earthquake.spark.gold.TrinoSqlClient;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import org.apache.spark.sql.SparkSession;

/** Resolve trusted dataset, commit results, verify independently, then publish. */
public final class MlResultImportJob {
    private static final ObjectMapper JSON = new ObjectMapper();
    private MlResultImportJob() { }

    @FunctionalInterface public interface IndependentVerifier {
        Map<String, Long> verify(MlIcebergWriter.ResultCommit commit, ResultBundleValidator.Context context,
                                 String fingerprint) throws Exception;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) throw new IllegalArgumentException("MLI03_REQUEST_REQUIRED");
        JsonNode request = JSON.readTree(Files.readAllBytes(Path.of(args[0])));
        Path bundle = boundedPath(request, "artifact_root");
        Path journal = boundedPath(request, "journal_root");
        Path registryRoot = Path.of(System.getenv().getOrDefault("MLI_DATASET_REGISTRY_ROOT", "/opt/pipeline/staging/ml-artifacts"));
        ResultBundleValidator.Context context = TrustedDatasetResolver.resolve(request, registryRoot, null);
        // Reuse the production Iceberg REST/S3 catalog configuration.  A plain
        // SparkSession would silently fall back to the default catalog and make
        // the runtime smoke write to a local warehouse instead of the QA
        // MinIO/Catalog namespace.
        SparkSession spark = GoldCommitJob.configured(System.getenv())
            .appName("mli-03-result-import").getOrCreate();
        try {
            Runnable assertLease = leaseAssertion(request);
            SparkMlIcebergWriter writer = new SparkMlIcebergWriter(spark, request.path("ml_namespace").asText(), assertLease, journal);
            String trinoHost = System.getenv().getOrDefault("TRINO_HOST", "trino");
            int trinoPort = Integer.parseInt(System.getenv().getOrDefault("TRINO_INTERNAL_PORT", "8080"));
            GoldPublisher.Sql sql = new TrinoSqlClient(URI.create("http://" + trinoHost + ":" + trinoPort));
            IndependentVerifier verifier = (commit, value, fingerprint) ->
                new TrinoMlVerifier(sql, request.path("ml_namespace").asText(), assertLease).verify(commit, value, fingerprint);
            ObjectNode result = importBundle(bundle, journal, context, writer, verifier, request.path("ml_namespace").asText());
            Files.writeString(journal.resolve("import-report.json"), JSON.writerWithDefaultPrettyPrinter().writeValueAsString(result) + "\n",
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            System.out.println(JSON.writeValueAsString(result));
        } finally { spark.stop(); }
    }

    public static ObjectNode importBundle(Path bundleRoot, Path journalRoot, ResultBundleValidator.Context context,
                                          MlIcebergWriter writer) throws Exception {
        return importBundle(bundleRoot, journalRoot, context, writer,
            (commit, value, fingerprint) -> writer.verify(new MlIcebergWriter.Commit(commit.membershipSnapshot(), commit.summarySnapshot(), 0), value), null);
    }

    public static ObjectNode importBundle(Path bundleRoot, Path journalRoot, ResultBundleValidator.Context originalContext,
                                          MlIcebergWriter writer, IndependentVerifier verifier, String namespace) throws Exception {
        Files.createDirectories(journalRoot);
        ResultBundleValidator.DecodedBundle bundle;
        try { bundle = MlResultBundleReader.read(bundleRoot); }
        catch (RuntimeException | IOException error) { writeRejected(journalRoot, originalContext, reason(error), true); throw error; }
        ResultBundleValidator.Context context;
        ResultBundleValidator.Receipt receipt;
        try {
            context = enrichMetadata(bundle, originalContext);
            receipt = new ResultBundleContract().validate(bundle, context);
        } catch (RuntimeException error) {
            writeRejected(journalRoot, originalContext, reason(error), true); throw error;
        }
        Path registry = journalRoot.resolve("experiment-run-" + context.runId() + ".json");
        if (Files.exists(registry)) {
            JsonNode existing = JSON.readTree(Files.readAllBytes(registry));
            if (!receipt.bundleSha256().equals(existing.path("result_bundle_sha256").asText()))
                throw new IllegalArgumentException("IMP_EXPERIMENT_RUN_REUSED");
            if (namespace != null && (!existing.has("namespace") || !namespace.equals(existing.path("namespace").asText())))
                throw new IllegalStateException("IMP_NAMESPACE_DRIFT");
            if ("REJECTED".equals(existing.path("experiment_status").asText())) throw new IllegalStateException("IMP_REJECTED_RUN_TERMINAL");
            if ("CANDIDATE".equals(existing.path("experiment_status").asText())) {
                long membership = existing.path("membership_snapshot_id").asLong();
                long summary = existing.path("summary_snapshot_id").asLong();
                long experiment = existing.path("experiment_snapshot_id").asLong();
                Map<String, Long> readback = writer.readback(new MlIcebergWriter.ResultCommit(membership, summary), experiment, context);
                ObjectNode result = (ObjectNode) existing.deepCopy();
                ObjectNode counts = result.putObject("readback"); readback.forEach(counts::put);
                return result;
            }
            if (!"IMPORT_VALIDATING".equals(existing.path("experiment_status").asText()))
                throw new IllegalStateException("IMP_INVALID_IMPORT_STATE");
        }
        ObjectNode validating = JSON.createObjectNode().put("experiment_run_id", context.runId()).put("dataset_id", context.datasetId())
            .put("experiment_status", "IMPORT_VALIDATING").put("result_bundle_sha256", receipt.bundleSha256())
            .put("created_at_utc", context.experimentMetadata().createdAtUtc());
        if (namespace != null) validating.put("namespace", namespace);
        atomicWrite(registry, validating);
        try {
            MlIcebergWriter.ResultCommit commit = writer.commitResults(bundle, context, receipt.bundleSha256());
            Map<String, Long> verified = verifier.verify(commit, context, receipt.bundleSha256());
            if (verified.getOrDefault("sequence_membership", 0L) <= 0 || verified.getOrDefault("sequence_summary", 0L) <= 0)
                throw new IllegalStateException("IMP_TRINO_QUALITY_BLOCKER");
            long experiment = writer.publishCandidate(bundle, context, receipt.bundleSha256(), commit);
            Map<String, Long> readback = writer.readback(commit, experiment, context);
            ResultBundleContract.assertTransition("IMPORT_VALIDATING", "CANDIDATE", false,
                commit.membershipSnapshot(), commit.summarySnapshot());
            ObjectNode candidate = JSON.createObjectNode().put("experiment_run_id", context.runId()).put("dataset_id", context.datasetId())
                .put("algorithm_name", context.algorithm()).put("experiment_status", "CANDIDATE")
                .put("result_bundle_sha256", receipt.bundleSha256()).put("candidate_at_utc", Instant.now().toString())
                .put("membership_snapshot_id", commit.membershipSnapshot()).put("summary_snapshot_id", commit.summarySnapshot())
                .put("experiment_snapshot_id", experiment);
            if (namespace != null) candidate.put("namespace", namespace);
            ObjectNode counts = candidate.putObject("readback"); readback.forEach(counts::put);
            atomicWrite(registry, candidate);
            return candidate;
        } catch (Exception error) {
            boolean quality = isQualityReason(reason(error));
            writeRejected(journalRoot, context, reason(error), quality);
            throw error;
        }
    }

    /** Request claims are not accepted as lineage evidence; resolve only from the trusted registry. */
    public static ResultBundleValidator.Context context(JsonNode request, String previous) {
        if (!request.has("dataset_manifest_ref") || request.has("dataset_status") || request.has("candidates"))
            throw new IllegalArgumentException("MLI03_CALLER_LINEAGE_NOT_ALLOWED");
        try { return TrustedDatasetResolver.resolve(request, Path.of(System.getenv().getOrDefault("MLI_DATASET_REGISTRY_ROOT", ".")), previous); }
        catch (IOException error) { throw new IllegalArgumentException("IMP_UNKNOWN_DATASET", error); }
    }

    private static ResultBundleValidator.Context enrichMetadata(ResultBundleValidator.DecodedBundle bundle,
                                                                ResultBundleValidator.Context context) throws IOException {
        JsonNode config = JSON.readTree(bundle.artifacts().get("experiment_config.json"));
        String configJson = new String(bundle.artifacts().get("experiment_config.json"), StandardCharsets.UTF_8);
        String requirementsHash = ResultBundleContract.sha256(bundle.artifacts().get("requirements-lock.txt"));
        String runtime = config.path("runtime_environment_json").asText();
        if (runtime.isBlank()) throw new IllegalArgumentException("IMP_EXPERIMENT_METADATA_REQUIRED");
        String created = context.experimentMetadata() == null ? Instant.now().toString() : context.experimentMetadata().createdAtUtc();
        String importRun = context.experimentMetadata() == null ? context.runId() : context.experimentMetadata().importRunId();
        String ready = bundle.success().path("completed_at_utc").asText();
        ResultBundleValidator.ExperimentMetadata metadata = new ResultBundleValidator.ExperimentMetadata(configJson,
            ResultBundleContract.sha256(bundle.artifacts().get("experiment_config.json")), requirementsHash, runtime,
            bundle.success().path("artifact_uri").asText(), importRun, created, ready, config.path("notebook_version").asText(null),
            config.hasNonNull("random_seed") ? config.path("random_seed").asLong() : null);
        return context.withMetadata(metadata);
    }

    private static Runnable leaseAssertion(JsonNode request) {
        Path owner = Path.of(System.getenv().getOrDefault("SOURCE_GUARD_ROOT", "/opt/pipeline/staging/source-guard")).resolve("owner.json");
        String dag = request.path("dag_id").asText(), run = request.path("run_id").asText();
        return () -> {
            try {
                JsonNode value = JSON.readTree(Files.readAllBytes(owner));
                if (!dag.equals(value.path("dag_id").asText()) || !run.equals(value.path("run_id").asText()))
                    throw new IllegalStateException("SOURCE_LEASE_NOT_HELD");
            } catch (IOException error) { throw new IllegalStateException("SOURCE_LEASE_NOT_HELD", error); }
        };
    }

    private static Path boundedPath(JsonNode request, String field) {
        if (!request.path(field).isTextual() || request.path(field).asText().isBlank()) throw new IllegalArgumentException("MLI03_REQUEST_FIELD_REQUIRED");
        Path path = Path.of(request.path(field).asText()); if (path.toString().contains("..")) throw new IllegalArgumentException("MLI03_PATH_INVALID");
        return path;
    }

    private static String reason(Throwable error) {
        String value = error.getMessage();
        if (value == null || !value.matches("[A-Z0-9_:-]{3,128}")) return "IMP_IMPORT_FAILED";
        return value;
    }

    private static boolean isQualityReason(String reason) {
        return reason.startsWith("IMP_CHECKSUM") || reason.startsWith("IMP_SCHEMA") || reason.startsWith("IMP_UNKNOWN")
            || reason.startsWith("IMP_DATASET") || reason.startsWith("IMP_EVENT") || reason.startsWith("IMP_INVALID")
            || reason.startsWith("IMP_DUPLICATE") || reason.startsWith("IMP_SUMMARY") || reason.startsWith("IMP_TRINO_QUALITY")
            || reason.startsWith("IMP_TRINO_DUPLICATE") || reason.startsWith("IMP_TRINO_LINEAGE");
    }

    private static void writeRejected(Path root, ResultBundleValidator.Context context, String reason, boolean terminal) {
        try {
            Files.createDirectories(root);
            ObjectNode rejected = JSON.createObjectNode().put("experiment_run_id", context.runId()).put("dataset_id", context.datasetId())
                .put("experiment_status", "REJECTED").put("status_reason_code", reason == null ? "IMP_UNKNOWN" : reason);
            atomicWrite(root.resolve("rejected-" + context.runId() + ".json"), rejected);
            if (terminal) atomicWrite(root.resolve("experiment-run-" + context.runId() + ".json"), rejected);
        } catch (Exception ignored) { /* preserve original failure and never expose bundle contents */ }
    }

    private static void atomicWrite(Path path, JsonNode node) throws Exception {
        Files.createDirectories(path.getParent()); Path temp = Files.createTempFile(path.getParent(), ".mli03-", ".tmp");
        try { Files.writeString(temp, JSON.writerWithDefaultPrettyPrinter().writeValueAsString(node) + "\n", StandardOpenOption.TRUNCATE_EXISTING);
            Files.move(temp, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING); }
        finally { Files.deleteIfExists(temp); }
    }
}
