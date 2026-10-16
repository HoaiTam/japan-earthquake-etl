package ie212.earthquake.spark.ml;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

/**
 * Resolves an exported dataset from the operator configured registry.  The
 * request can pin a reference and an expected digest, but cannot supply the
 * dataset status, candidate IDs, or roles used by validation.
 */
public final class TrustedDatasetResolver {
    private static final ObjectMapper JSON = new ObjectMapper();
    private TrustedDatasetResolver() { }

    public static ResultBundleValidator.Context resolve(JsonNode request, Path registryRoot,
                                                         String previousBundleSha256) throws IOException {
        if (registryRoot == null || registryRoot.toString().isBlank()) throw new IllegalArgumentException("IMP_DATASET_REGISTRY_REQUIRED");
        String reference = text(request, "dataset_manifest_ref");
        if (Path.of(reference).isAbsolute() || reference.contains("..")) throw new IllegalArgumentException("IMP_DATASET_REFERENCE_INVALID");
        Path root = registryRoot.toAbsolutePath().normalize();
        Path manifestPath = root.resolve(reference).normalize();
        requireInside(root, manifestPath);
        if (Files.isSymbolicLink(manifestPath) || !Files.isRegularFile(manifestPath)) throw new IllegalArgumentException("IMP_UNKNOWN_DATASET");
        String manifestSha = sha256(Files.readAllBytes(manifestPath));
        if (!manifestSha.equals(text(request, "dataset_manifest_sha256"))) throw new IllegalArgumentException("IMP_CHECKSUM_MISMATCH");
        JsonNode manifest = JSON.readTree(Files.readAllBytes(manifestPath));
        if (!manifest.isObject() || !"EXPORTED".equals(manifest.path("dataset_status").asText())
                || manifest.path("dataset_id").asText().isBlank()
                || manifest.path("dataset_id").asText().equals("DEVELOPMENT_PILOT")
                || manifest.path("dataset_id").asText().contains("pilot"))
            throw new IllegalArgumentException("IMP_UNKNOWN_DATASET");
        if (!manifest.path("dataset_id").asText().equals(text(request, "dataset_id"))) throw new IllegalArgumentException("IMP_DATASET_LINEAGE_MISMATCH");
        requirePinned(manifest);
        Path exportManifest = resolveExportManifest(root, manifest);
        String exportSha = sha256(Files.readAllBytes(exportManifest));
        if (!exportSha.equals(manifest.path("export_manifest_sha256").asText())) throw new IllegalArgumentException("IMP_CHECKSUM_MISMATCH");
        List<ResultBundleValidator.Candidate> candidates = candidates(manifest.path("candidates"));
        if (candidates.isEmpty()) throw new IllegalArgumentException("IMP_CANDIDATE_LINEAGE_REQUIRED");
        String runId = text(request, "experiment_run_id");
        return new ResultBundleValidator.Context(text(request, "dataset_id"), "EXPORTED", runId,
            text(request, "algorithm_name"), text(request, "model_config_version"), candidates, previousBundleSha256,
            new ResultBundleValidator.DatasetPins(reference, manifestSha, exportSha,
                manifest.path("registry_snapshot_id").asText(), manifest.path("candidate_snapshot_id").asText(),
                manifest.path("mainshock_snapshot_id").asText()), null);
    }

    private static List<ResultBundleValidator.Candidate> candidates(JsonNode values) {
        List<ResultBundleValidator.Candidate> result = new ArrayList<>();
        if (!values.isArray()) return result;
        values.forEach(item -> {
            if (item.isObject() && item.path("mainshock_event_id").isTextual()
                    && item.path("candidate_event_id").isTextual() && item.path("role").isTextual())
                result.add(new ResultBundleValidator.Candidate(item.path("mainshock_event_id").asText(),
                    item.path("candidate_event_id").asText(), item.path("role").asText()));
        });
        return result;
    }

    private static Path resolveExportManifest(Path root, JsonNode manifest) throws IOException {
        String ref = manifest.path("export_manifest_ref").asText();
        if (ref.isBlank() || Path.of(ref).isAbsolute() || ref.contains("..")) throw new IllegalArgumentException("IMP_DATASET_PINS_REQUIRED");
        Path path = root.resolve(ref).normalize(); requireInside(root, path);
        if (Files.isSymbolicLink(path) || !Files.isRegularFile(path)) throw new IllegalArgumentException("IMP_UNKNOWN_DATASET");
        return path;
    }

    private static void requirePinned(JsonNode manifest) {
        for (String key : List.of("export_manifest_sha256", "registry_snapshot_id", "candidate_snapshot_id", "mainshock_snapshot_id"))
            if (!manifest.path(key).isTextual() || manifest.path(key).asText().isBlank()) throw new IllegalArgumentException("IMP_DATASET_PINS_REQUIRED");
    }

    private static String text(JsonNode node, String key) {
        if (!node.path(key).isTextual() || node.path(key).asText().isBlank()) throw new IllegalArgumentException("IMP_REQUEST_FIELD_REQUIRED");
        return node.path(key).asText();
    }

    private static void requireInside(Path root, Path path) {
        if (!path.startsWith(root)) throw new IllegalArgumentException("IMP_DATASET_REFERENCE_INVALID");
    }

    static String sha256(byte[] bytes) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)); }
        catch (Exception error) { throw new IllegalStateException(error); }
    }
}
