package ie212.earthquake.spark.ml;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Resolves an exported dataset from the operator configured registry.  The
 * request can pin a reference and an expected digest, but cannot supply the
 * dataset status, candidate IDs, or roles used by validation.
 */
public final class TrustedDatasetResolver {
    private static final long MAX_METADATA_BYTES = 64L * 1024 * 1024;
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
        byte[] manifestBytes = metadata(manifestPath);
        String manifestSha = sha256(manifestBytes);
        if (!manifestSha.equals(text(request, "dataset_manifest_sha256"))) throw new IllegalArgumentException("IMP_CHECKSUM_MISMATCH");
        JsonNode manifest = JSON.readTree(manifestBytes);
        if (!manifest.isObject() || !"EXPORTED".equals(manifest.path("dataset_status").asText())
                || manifest.path("dataset_id").asText().isBlank()
                || manifest.path("dataset_id").asText().equals("DEVELOPMENT_PILOT")
                || manifest.path("dataset_id").asText().contains("pilot"))
            throw new IllegalArgumentException("IMP_UNKNOWN_DATASET");
        if (!manifest.path("dataset_id").asText().equals(text(request, "dataset_id"))) throw new IllegalArgumentException("IMP_DATASET_LINEAGE_MISMATCH");
        requirePinned(manifest);
        Path exportManifest = resolveExportManifest(root, manifest);
        String exportSha = sha256(metadata(exportManifest));
        if (!exportSha.equals(manifest.path("export_manifest_sha256").asText())) throw new IllegalArgumentException("IMP_CHECKSUM_MISMATCH");
        String algorithm = text(request, "algorithm_name");
        String datasetId = text(request, "dataset_id");
        ResultBundleValidator.CollectionScope scope = null;
        List<ResultBundleValidator.Candidate> candidates;
        if (ResearchCollectionExportBundle.VERSION.equals(manifest.path("export_version").asText())) {
            String shard = text(request, "collection_shard_id");
            String collection = text(request, "collection_id");
            scope = new ResultBundleValidator.CollectionScope(ResearchCollectionExportBundle.VERSION, collection, shard);
            JsonNode plan = manifest.path("collection_plan");
            JsonNode entries = manifest.path("collection_shards");
            if (!collection.equals(plan.path("collection_id").asText()) || !entries.isObject()
                    || !plan.path("shards").isArray())
                throw new IllegalArgumentException("IMP_COLLECTION_SCOPE_REQUIRED");
            Map<String, JsonNode> plannedShards = new HashMap<>();
            for (JsonNode planned : plan.path("shards")) {
                String shardId = planned.path("shard_id").asText();
                if (!shardId.matches("shard_[0-9]{5}") || plannedShards.putIfAbsent(shardId, planned) != null)
                    throw new IllegalArgumentException("IMP_DATASET_LINEAGE_MISMATCH");
            }
            if (plannedShards.size() != entries.size() || !plannedShards.containsKey(shard)
                    || plannedShards.keySet().stream().anyMatch(id -> !entries.path(id).isObject()))
                throw new IllegalArgumentException("IMP_COLLECTION_SCOPE_REQUIRED");
            candidates = shardCandidates(root, datasetId, collection, shard, entries.path(shard), plannedShards.get(shard));
            if ("HDBSCAN_ADAPTIVE".equals(algorithm) && plannedShards.size() > 1) {
                Map<String, List<ResultBundleValidator.Candidate>> siblings = new HashMap<>();
                for (var planned : plannedShards.entrySet()) {
                    String sibling = planned.getKey();
                    if (sibling.equals(shard)) continue;
                    siblings.put(sibling, shardCandidates(root, datasetId, collection, sibling,
                        entries.path(sibling), planned.getValue()));
                }
                scope = new ResultBundleValidator.CollectionScope(scope.version(), collection, shard, siblings);
            }
        } else {
            if (request.has("collection_shard_id") || request.has("collection_id"))
                throw new IllegalArgumentException("IMP_COLLECTION_SCOPE_REQUIRED");
            candidates = candidates(manifest.path("candidates"));
        }
        if (candidates.isEmpty()) throw new IllegalArgumentException("IMP_CANDIDATE_LINEAGE_REQUIRED");
        String runId = text(request, "experiment_run_id");
        return new ResultBundleValidator.Context(datasetId, "EXPORTED", runId,
            algorithm, text(request, "model_config_version"), candidates, previousBundleSha256,
            new ResultBundleValidator.DatasetPins(reference, manifestSha, exportSha,
                manifest.path("registry_snapshot_id").asText(), manifest.path("candidate_snapshot_id").asText(),
                manifest.path("mainshock_snapshot_id").asText()), null,scope);
    }

    private static List<ResultBundleValidator.Candidate> shardCandidates(Path root, String datasetId,
            String collection, String shard, JsonNode entry, JsonNode planned) throws IOException {
        long featureRows = entry.path("feature_row_count").asLong();
        long mainshocks = entry.path("mainshock_count").asLong();
        if (featureRows < 1 || featureRows > FeatureExportBundle.MAX_ROWS || mainshocks < 1 || mainshocks > 128)
            throw new IllegalArgumentException("IMP_COLLECTION_SCOPE_REQUIRED");
        String reference = text(entry, "lineage_ref");
        if (!reference.equals("exports/" + datasetId + "/" + shard + ".json"))
            throw new IllegalArgumentException("IMP_DATASET_REFERENCE_INVALID");
        Path path = root.resolve(reference).normalize();
        requireInside(root, path);
        if (!Files.isRegularFile(path))
            throw new IllegalArgumentException("IMP_DATASET_REFERENCE_INVALID");
        byte[] bytes = metadata(path);
        if (!sha256(bytes).equals(text(entry, "lineage_sha256")))
            throw new IllegalArgumentException("IMP_CHECKSUM_MISMATCH");
        JsonNode lineage = JSON.readTree(bytes);
        if (lineage.size() != 4 || !collection.equals(text(lineage, "collection_id"))
                || !shard.equals(text(lineage, "shard_id")) || !datasetId.equals(text(lineage, "dataset_id")))
            throw new IllegalArgumentException("IMP_DATASET_LINEAGE_MISMATCH");
        var rows = candidates(lineage.path("candidates"));
        if (!lineage.path("candidates").isArray() || rows.size() != lineage.path("candidates").size())
            throw new IllegalArgumentException("IMP_CANDIDATE_LINEAGE_REQUIRED");
        Map<String, Long> assigned = new HashMap<>();
        for (var mainshock : planned.path("mainshocks")) {
            if (assigned.putIfAbsent(text(mainshock, "mainshock_event_id"),
                    mainshock.path("candidate_row_count").asLong()) != null)
                throw new IllegalArgumentException("IMP_DUPLICATE_GRAIN");
        }
        Map<String, Long> actual = new HashMap<>();
        Set<CandidateGrain> keys = new HashSet<>();
        for (var candidate : rows) {
            if (candidate.eventId().isBlank() || candidate.eventId().length() > 256
                    || !Set.of("PRE", "MAINSHOCK", "POST").contains(candidate.role())
                    || candidate.mainshockId().equals(candidate.eventId()) != candidate.role().equals("MAINSHOCK"))
                throw new IllegalArgumentException("IMP_EVENT_ROLE_MISMATCH");
            if (!keys.add(new CandidateGrain(candidate.mainshockId(), candidate.eventId())))
                throw new IllegalArgumentException("IMP_DUPLICATE_GRAIN");
            actual.merge(candidate.mainshockId(), 1L, Long::sum);
        }
        if (rows.size() != featureRows || actual.size() != mainshocks || !actual.equals(assigned))
            throw new IllegalArgumentException("IMP_CANDIDATE_LINEAGE_REQUIRED");
        return rows;
    }

    private record CandidateGrain(String mainshock, String event) {}

    private static byte[] metadata(Path path) throws IOException {
        if (Files.size(path) > MAX_METADATA_BYTES) throw new IllegalArgumentException("IMP_DATASET_METADATA_LIMIT");
        try (var stream = Files.newInputStream(path)) {
            byte[] bytes = stream.readNBytes((int) MAX_METADATA_BYTES + 1);
            if (bytes.length > MAX_METADATA_BYTES) throw new IllegalArgumentException("IMP_DATASET_METADATA_LIMIT");
            return bytes;
        }
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
        for(Path p=path;p!=null;p=p.getParent()) if(Files.isSymbolicLink(p)) throw new IllegalArgumentException("IMP_DATASET_REFERENCE_INVALID");
    }

    static String sha256(byte[] bytes) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)); }
        catch (Exception error) { throw new IllegalStateException(error); }
    }
}
