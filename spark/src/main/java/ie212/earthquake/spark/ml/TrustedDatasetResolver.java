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
        if(Files.size(manifestPath)>64*1024*1024) throw new IllegalArgumentException("IMP_DATASET_METADATA_LIMIT");
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
        ResultBundleValidator.CollectionScope scope=null;
        List<ResultBundleValidator.Candidate> candidates;
        if(ResearchCollectionExportBundle.VERSION.equals(manifest.path("export_version").asText())) {
            String shard=text(request,"collection_shard_id"),collection=text(request,"collection_id");
            scope=new ResultBundleValidator.CollectionScope(ResearchCollectionExportBundle.VERSION,collection,shard);
            JsonNode plan=manifest.path("collection_plan"),entry=manifest.path("collection_shards").path(shard);
            if(!collection.equals(plan.path("collection_id").asText()) || !entry.isObject()
                    || entry.path("feature_row_count").asLong()<1 || entry.path("feature_row_count").asLong()>FeatureExportBundle.MAX_ROWS
                    || entry.path("mainshock_count").asLong()<1 || entry.path("mainshock_count").asLong()>128)
                throw new IllegalArgumentException("IMP_COLLECTION_SCOPE_REQUIRED");
            String ref=text(entry,"lineage_ref");
            if(!ref.equals("exports/"+text(request,"dataset_id")+"/"+shard+".json"))throw new IllegalArgumentException("IMP_DATASET_REFERENCE_INVALID");
            Path file=root.resolve(ref);requireInside(root,file);
            if(!Files.isRegularFile(file) || Files.size(file)>64*1024*1024)throw new IllegalArgumentException("IMP_DATASET_METADATA_LIMIT");
            byte[] bytes=Files.readAllBytes(file);
            if(!sha256(bytes).equals(text(entry,"lineage_sha256")))throw new IllegalArgumentException("IMP_CHECKSUM_MISMATCH");
            var lineage=JSON.readTree(bytes);
            if(lineage.size()!=4 || !collection.equals(text(lineage,"collection_id")) || !shard.equals(text(lineage,"shard_id"))
                    || !text(request,"dataset_id").equals(text(lineage,"dataset_id")))throw new IllegalArgumentException("IMP_DATASET_LINEAGE_MISMATCH");
            candidates=candidates(lineage.path("candidates"));
            if(!lineage.path("candidates").isArray() || candidates.size()!=lineage.path("candidates").size())
                throw new IllegalArgumentException("IMP_CANDIDATE_LINEAGE_REQUIRED");
            var assigned=new java.util.TreeMap<String,Long>();
            for(var p:plan.path("shards"))if(shard.equals(p.path("shard_id").asText()))for(var m:p.path("mainshocks"))
                if(assigned.put(m.path("mainshock_event_id").asText(),m.path("candidate_row_count").asLong())!=null)throw new IllegalArgumentException("IMP_DUPLICATE_GRAIN");
            var actual=new java.util.TreeMap<String,Long>();var keys=new java.util.HashSet<String>();
            for(var candidate:candidates) {
                if(candidate.eventId().isBlank() || candidate.eventId().length()>256
                        || !java.util.Set.of("PRE","MAINSHOCK","POST").contains(candidate.role())
                        || candidate.mainshockId().equals(candidate.eventId())!=candidate.role().equals("MAINSHOCK"))
                    throw new IllegalArgumentException("IMP_EVENT_ROLE_MISMATCH");
                if(!keys.add(candidate.mainshockId()+"\u0000"+candidate.eventId()))throw new IllegalArgumentException("IMP_DUPLICATE_GRAIN");
                actual.merge(candidate.mainshockId(),1L,Long::sum);
            }
            if(candidates.size()!=entry.path("feature_row_count").asLong() || actual.size()!=entry.path("mainshock_count").asLong()
                    || !actual.equals(assigned))throw new IllegalArgumentException("IMP_CANDIDATE_LINEAGE_REQUIRED");
        } else {
            if(request.has("collection_shard_id") || request.has("collection_id"))throw new IllegalArgumentException("IMP_COLLECTION_SCOPE_REQUIRED");
            candidates=candidates(manifest.path("candidates"));
        }
        if (candidates.isEmpty()) throw new IllegalArgumentException("IMP_CANDIDATE_LINEAGE_REQUIRED");
        String runId = text(request, "experiment_run_id");
        return new ResultBundleValidator.Context(text(request, "dataset_id"), "EXPORTED", runId,
            text(request, "algorithm_name"), text(request, "model_config_version"), candidates, previousBundleSha256,
            new ResultBundleValidator.DatasetPins(reference, manifestSha, exportSha,
                manifest.path("registry_snapshot_id").asText(), manifest.path("candidate_snapshot_id").asText(),
                manifest.path("mainshock_snapshot_id").asText()), null,scope);
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
