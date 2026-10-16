package ie212.earthquake.spark.ml;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.io.IOException;
import java.time.Instant;
import java.util.*;
import org.apache.spark.sql.SparkSession;

/** Import entrypoint: exact bundle -> validation -> Iceberg commit -> pinned readback. */
public final class MlResultImportJob {
    private static final ObjectMapper JSON = new ObjectMapper();
    private MlResultImportJob() { }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) throw new IllegalArgumentException("MLI03_REQUEST_REQUIRED");
        JsonNode request = JSON.readTree(Files.readAllBytes(Path.of(args[0])));
        Path bundle = Path.of(request.path("artifact_root").asText());
        Path journal = Path.of(request.path("journal_root").asText());
        ResultBundleValidator.Context context = context(request, null);
        SparkSession spark = SparkSession.builder().appName("mli-03-result-import").config("spark.sql.session.timeZone", "UTC").getOrCreate();
        try {
            SparkMlIcebergWriter writer = new SparkMlIcebergWriter(spark, request.path("ml_namespace").asText(), () -> { });
            ObjectNode result = importBundle(bundle, journal, context, writer);
            Files.writeString(journal.resolve("import-report.json"), JSON.writerWithDefaultPrettyPrinter().writeValueAsString(result) + "\n",
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            System.out.println(JSON.writeValueAsString(result));
        } finally { spark.stop(); }
    }

    public static ObjectNode importBundle(Path bundleRoot, Path journalRoot, ResultBundleValidator.Context context,
                                          MlIcebergWriter writer) throws Exception {
        Files.createDirectories(journalRoot);
        ResultBundleValidator.DecodedBundle bundle;
        try { bundle = MlResultBundleReader.read(bundleRoot); }
        catch (RuntimeException | IOException error) {
            writeRejected(journalRoot, context, error.getMessage());
            throw error;
        }
        ResultBundleValidator.Receipt receipt;
        try { receipt = new ResultBundleContract().validate(bundle, context); }
        catch (RuntimeException error) {
            writeRejected(journalRoot, context, error.getMessage());
            throw error;
        }
        Path registry = journalRoot.resolve("experiment-run-" + context.runId() + ".json");
        if (Files.exists(registry)) {
            JsonNode existing = JSON.readTree(Files.readAllBytes(registry));
            if (!receipt.bundleSha256().equals(existing.path("result_bundle_sha256").asText())) throw new IllegalArgumentException("IMP_EXPERIMENT_RUN_REUSED");
            if ("CANDIDATE".equals(existing.path("experiment_status").asText())) return (ObjectNode) existing;
            if (!"IMPORT_VALIDATING".equals(existing.path("experiment_status").asText()))
                throw new IllegalStateException("IMP_INVALID_IMPORT_STATE");
        }
        ObjectNode validating = JSON.createObjectNode().put("experiment_run_id", context.runId()).put("dataset_id", context.datasetId())
            .put("experiment_status", "IMPORT_VALIDATING").put("result_bundle_sha256", receipt.bundleSha256());
        atomicWrite(registry, validating);
        try {
            MlIcebergWriter.Commit commit = writer.commit(bundle, context, receipt.bundleSha256());
            Map<String, Long> counts = writer.verify(commit, context);
            ResultBundleContract.assertTransition("IMPORT_VALIDATING", "CANDIDATE", false, commit.membershipSnapshot(), commit.summarySnapshot());
            ObjectNode candidate = JSON.createObjectNode().put("experiment_run_id", context.runId()).put("dataset_id", context.datasetId())
                .put("algorithm_name", context.algorithm()).put("experiment_status", "CANDIDATE")
                .put("result_bundle_sha256", receipt.bundleSha256()).put("candidate_at_utc", Instant.now().toString())
                .put("membership_snapshot_id", commit.membershipSnapshot()).put("summary_snapshot_id", commit.summarySnapshot())
                .put("experiment_snapshot_id", commit.experimentSnapshot());
            ObjectNode readback = candidate.putObject("readback"); counts.forEach(readback::put);
            atomicWrite(registry, candidate);
            return candidate;
        } catch (Exception error) {
            writeRejected(journalRoot, context, error.getMessage());
            throw error;
        }
    }

    public static ResultBundleValidator.Context context(JsonNode request, String previous) {
        List<ResultBundleValidator.Candidate> candidates = new ArrayList<>();
        request.path("candidates").forEach(candidate -> candidates.add(new ResultBundleValidator.Candidate(
            candidate.path("mainshock_event_id").asText(), candidate.path("candidate_event_id").asText(), candidate.path("role").asText())));
        if (candidates.isEmpty()) throw new IllegalArgumentException("IMP_CANDIDATE_LINEAGE_REQUIRED");
        if (!"EXPORTED".equals(request.path("dataset_status").asText())) throw new IllegalArgumentException("IMP_UNKNOWN_DATASET");
        return new ResultBundleValidator.Context(request.path("dataset_id").asText(), request.path("dataset_status").asText(),
            request.path("experiment_run_id").asText(), request.path("algorithm_name").asText(), request.path("model_config_version").asText(), candidates, previous);
    }

    private static void writeRejected(Path root, ResultBundleValidator.Context context, String reason) {
        try {
            Files.createDirectories(root);
            ObjectNode rejected = JSON.createObjectNode().put("experiment_run_id", context.runId()).put("dataset_id", context.datasetId())
                .put("experiment_status", "REJECTED").put("status_reason_code", reason == null ? "IMP_UNKNOWN" : reason);
            atomicWrite(root.resolve("rejected-" + context.runId() + ".json"), rejected);
        } catch (Exception ignored) { /* preserve original failure and never expose bundle contents */ }
    }

    private static void atomicWrite(Path path, JsonNode node) throws Exception {
        Files.createDirectories(path.getParent());
        Path temp = Files.createTempFile(path.getParent(), ".mli03-", ".tmp");
        try {
            Files.writeString(temp, JSON.writerWithDefaultPrettyPrinter().writeValueAsString(node) + "\n", StandardOpenOption.TRUNCATE_EXISTING);
            Files.move(temp, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } finally { Files.deleteIfExists(temp); }
    }
}
