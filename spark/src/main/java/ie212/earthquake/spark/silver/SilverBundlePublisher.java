package ie212.earthquake.spark.silver;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.jma.JmaBronzeWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.parquet.schema.MessageType;

/**
 * Immutable, marker-last four-dataset handoff. Callers must hold the whole-run
 * source guard when sharing a store: S3 put/move is NOT an atomic rename/CAS.
 * Never replaces or deletes a committed bundle or any month partition.
 */
public final class SilverBundlePublisher {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Map<String, MessageType> SCHEMAS = Map.of(
            "source_observation", SilverParquetSerializer.OBSERVATION_PARQUET_SCHEMA,
            "reject_record", SilverParquetSerializer.REJECT_PARQUET_SCHEMA,
            "source_link", SilverParquetSerializer.SOURCE_LINK_PARQUET_SCHEMA,
            "canonical_membership", SilverParquetSerializer.CANONICAL_MEMBERSHIP_PARQUET_SCHEMA);
    private final SilverObjectStore store;

    public SilverBundlePublisher(SilverObjectStore store) { this.store = java.util.Objects.requireNonNull(store); }

    public static String root(String runId) {
        if (runId == null || !runId.matches("[A-Za-z0-9][A-Za-z0-9_-]{0,127}")) {
            throw new IllegalArgumentException("SAFE_BUNDLE_RUN_ID_REQUIRED");
        }
        return "bundles/" + runId;
    }

    public SilverWriteResult publish(SilverWriteRequest request, JsonNode context) throws IOException {
        if (context == null || !context.path("quality_passed").asBoolean(false)
                || !context.path("reconciliation_balanced").asBoolean(false) || !request.rejects().isEmpty()) {
            throw new IOException("BUNDLE_QUALITY_GATE_BLOCKED");
        }
        String root = root(request.runId());
        var serializer = new SilverParquetSerializer();
        var datasets = JSON.createObjectNode();
        var payloads = new LinkedHashMap<String, byte[]>();
        var partitions = new ArrayList<SilverPartitionManifest>();
        var groups = new TreeMap<String, List<SilverObservation>>();
        for (var observation : request.observations()) {
            if (observation.eventTimeUtc() == null) { throw new IOException("UTC_PARTITION_REQUIRED"); }
            var utc = observation.eventTimeUtc().atZone(java.time.ZoneOffset.UTC);
            if (utc.getYear() != observation.eventYearUtc() || utc.getMonthValue() != observation.eventMonthUtc()) {
                throw new IOException("UTC_PARTITION_MISMATCH");
            }
            if (!java.util.Set.of("USGS", "JMA_BULLETIN").contains(observation.sourceSystem())) {
                throw new IOException("BUNDLE_SOURCE_REQUIRED");
            }
            groups.computeIfAbsent(SilverStorageLayout.observationPartitionPath(SilverPartitionKey.from(observation)),
                    ignored -> new ArrayList<>()).add(observation);
        }
        var obsFiles = new ArrayList<SilverFileMetadata>();
        for (var entry : groups.entrySet()) {
            String path = root + "/" + entry.getKey();
            byte[] bytes = serializer.serializeObservations(entry.getValue());
            var file = file(path + "/part-00000.parquet", bytes, entry.getValue().size());
            payloads.put(file.relativePath(), bytes); obsFiles.add(file);
            var first = entry.getValue().get(0);
            partitions.add(new SilverPartitionManifest("1.0", "SilverReady", first.sourceSystem(),
                    first.eventYearUtc(), first.eventMonthUtc(), path, request.runId(), file.recordCount(),
                    List.of(file), Map.of("valid", file.recordCount()), request.publishedAtUtc(), "slv-09-bundle-v1"));
        }
        if (obsFiles.isEmpty()) {
            byte[] bytes = serializer.serializeObservations(List.of());
            var file = file(root + "/source_observation/part-00000.parquet", bytes, 0);
            payloads.put(file.relativePath(), bytes); obsFiles.add(file);
        }
        dataset(datasets, "source_observation", obsFiles, payloads, root, request);
        var rejectFiles = single(datasets, payloads, root, request, "reject_record",
                serializer.serializeRejects(request.rejects()), request.rejects().size());
        var linkFiles = single(datasets, payloads, root, request, "source_link",
                serializer.serializeSourceLinks(request.links()), request.links().size());
        var membershipFiles = single(datasets, payloads, root, request, "canonical_membership",
                serializer.serializeCanonicalMemberships(request.memberships()), request.memberships().size());

        ObjectNode bundle = JSON.createObjectNode();
        bundle.put("bundle_version", "slv-09-bundle-v1"); bundle.put("run_id", request.runId());
        bundle.put("serialization_version", "slv-09-parquet-stable-footer-v1");
        bundle.put("silver_status", "SilverReady"); bundle.put("published_at_utc", request.publishedAtUtc().toString());
        bundle.set("context", java.util.Objects.requireNonNull(context).deepCopy()); bundle.set("datasets", datasets);
        String fingerprint = sha(JSON.writeValueAsBytes(bundle));
        bundle.put("identity_sha256", fingerprint);
        byte[] manifest = JSON.writeValueAsBytes(bundle);
        String markerKey = root + "/_SUCCESS";
        boolean reuse = store.exists(markerKey);
        if (reuse) {
            var committed = verify(request.runId());
            if (!JSON.readTree(JSON.writeValueAsBytes(context)).equals(committed.path("context"))) {
                throw new IOException("BUNDLE_CONTEXT_CONFLICT");
            }
            for (var name : SCHEMAS.keySet()) {
                if (!datasets.path(name).equals(committed.path("datasets").path(name))) {
                    throw new IOException("BUNDLE_DATASET_BYTES_CONFLICT_" + name);
                }
            }
            if (!fingerprint.equals(committed.path("identity_sha256").asText())) {
                throw new IOException("BUNDLE_JSON_ORDER_CONFLICT");
            }
        }
        // A failed attempt retains a reservation; another input cannot take its run ID.
        String reservation = root + "/identity.sha256";
        if (!store.exists(reservation) && !store.list(root + "/").isEmpty()) {
            throw new IOException("UNRESERVED_BUNDLE_REFUSED");
        }
        putImmutable(reservation, fingerprint.getBytes(StandardCharsets.US_ASCII), "text/plain");
        if (!reuse) {
            for (var entry : payloads.entrySet()) {
                putImmutable(entry.getKey(), entry.getValue(), entry.getKey().endsWith(".parquet")
                        ? "application/octet-stream" : "application/json");
            }
            putImmutable(root + "/manifest.json", manifest, "application/json");
            // Verify FINAL objects, not staging aliases, before consumers can see a marker.
            verifyManifest(bundle, root);
            putImmutable(markerKey, sha(manifest).getBytes(StandardCharsets.US_ASCII), "text/plain");
        }
        JsonNode verified = verify(request.runId());
        if (!fingerprint.equals(verified.path("identity_sha256").asText())) {
            throw new IOException("BUNDLE_IDENTITY_CONFLICT");
        }
        return new SilverWriteResult("SilverReady", request.runId(), partitions, request.observations().size(),
                request.rejects().size(), request.links().size(), request.memberships().size(), reuse,
                rejectFiles, linkFiles, membershipFiles, root + "/manifest.json");
    }

    /** Fresh readback receipt; never discovers inputs through wildcard/latest listings. */
    public JsonNode verify(String runId) throws IOException {
        String root = root(runId);
        if (!store.exists(root + "/_SUCCESS")) { throw new IOException("BUNDLE_NOT_READY"); }
        byte[] bytes = store.read(root + "/manifest.json");
        if (!Arrays.equals(store.read(root + "/_SUCCESS"), sha(bytes).getBytes(StandardCharsets.US_ASCII))) {
            throw new IOException("BUNDLE_MARKER_MISMATCH");
        }
        JsonNode bundle = JSON.readTree(bytes);
        if (!"slv-09-bundle-v1".equals(bundle.path("bundle_version").asText())
                || !runId.equals(bundle.path("run_id").asText())
                || !"SilverReady".equals(bundle.path("silver_status").asText())) { throw new IOException("BUNDLE_CONTEXT_MISMATCH"); }
        ObjectNode identity = ((ObjectNode) bundle).deepCopy(); identity.remove("identity_sha256");
        String fingerprint = sha(JSON.writeValueAsBytes(identity));
        if (!fingerprint.equals(bundle.path("identity_sha256").asText())
                || !Arrays.equals(store.read(root + "/identity.sha256"), fingerprint.getBytes(StandardCharsets.US_ASCII))) {
            throw new IOException("BUNDLE_IDENTITY_MISMATCH");
        }
        verifyManifest(bundle, root);
        return bundle;
    }

    private void verifyManifest(JsonNode bundle, String root) throws IOException {
        JsonNode datasets = bundle.path("datasets");
        if (!datasets.isObject() || datasets.size() != SCHEMAS.size()) { throw new IOException("FOUR_DATASETS_REQUIRED"); }
        for (var entry : SCHEMAS.entrySet()) {
            JsonNode reference = datasets.path(entry.getKey());
            String key = root + "/" + entry.getKey() + "/manifest.json";
            byte[] bytes = store.read(key);
            if (!sha(bytes).equals(reference.path("sha256").asText())
                    || !store.uriForKey(key).equals(reference.path("manifest_uri").asText())) {
                throw new IOException("DATASET_MANIFEST_MISMATCH");
            }
            JsonNode manifest = JSON.readTree(bytes);
            if (!entry.getKey().equals(manifest.path("dataset").asText())
                    || !bundle.path("run_id").asText().equals(manifest.path("run_id").asText())
                    || !schemaSha(entry.getValue()).equals(manifest.path("schema_sha256").asText())
                    || !manifest.path("files").isArray() || manifest.path("files").isEmpty()) {
                throw new IOException("DATASET_SCHEMA_MISMATCH");
            }
            long count = 0;
            var seen = new java.util.HashSet<String>();
            for (JsonNode file : manifest.path("files")) {
                String fileKey = file.path("relative_path").asText();
                if (!fileKey.startsWith(root + "/" + entry.getKey() + "/") || fileKey.contains("..")
                        || !seen.add(fileKey) || !store.uriForKey(fileKey).equals(file.path("uri").asText())
                        || !file.path("record_count").canConvertToInt() || file.path("record_count").asInt() < 0) {
                    throw new IOException("EXACT_DATASET_FILES_REQUIRED");
                }
                byte[] data = store.read(fileKey);
                if (data.length != file.path("byte_size").asLong(-1) || !sha(data).equals(file.path("sha256").asText())) {
                    throw new IOException("DATASET_CHECKSUM_MISMATCH");
                }
                SilverParquetSerializer.verifyParquet(data, file.path("record_count").asInt(), entry.getValue());
                count += file.path("record_count").asInt();
            }
            if (count != manifest.path("record_count").asLong(-1) || count != reference.path("record_count").asLong(-1)) {
                throw new IOException("DATASET_COUNT_MISMATCH");
            }
        }
    }

    private List<String> single(ObjectNode datasets, Map<String, byte[]> payloads, String root,
            SilverWriteRequest request, String name, byte[] bytes, int count) throws IOException {
        var file = file(root + "/" + name + "/part-00000.parquet", bytes, count);
        payloads.put(file.relativePath(), bytes); dataset(datasets, name, List.of(file), payloads, root, request);
        return List.of(file.relativePath());
    }

    private void dataset(ObjectNode datasets, String name, List<SilverFileMetadata> files,
            Map<String, byte[]> payloads, String root, SilverWriteRequest request) throws IOException {
        ObjectNode manifest = JSON.createObjectNode();
        manifest.put("dataset", name); manifest.put("schema_version", "1.0"); manifest.put("run_id", request.runId());
        manifest.put("schema_sha256", schemaSha(SCHEMAS.get(name)));
        manifest.put("record_count", files.stream().mapToInt(SilverFileMetadata::recordCount).sum());
        var rows = manifest.putArray("files");
        for (var file : files) {
            var row = rows.addObject(); row.put("relative_path", file.relativePath());
            row.put("uri", store.uriForKey(file.relativePath())); row.put("sha256", file.sha256());
            row.put("byte_size", file.byteSize()); row.put("record_count", file.recordCount());
        }
        String key = root + "/" + name + "/manifest.json";
        byte[] bytes = JSON.writeValueAsBytes(manifest); payloads.put(key, bytes);
        var reference = datasets.putObject(name); reference.put("manifest_uri", store.uriForKey(key));
        reference.put("sha256", sha(bytes)); reference.put("record_count", manifest.path("record_count").asInt());
        // Visible only after final verification and the bundle marker.
        reference.put("readback_verified", true);
    }

    private void putImmutable(String key, byte[] bytes, String type) throws IOException {
        if (store.exists(key)) {
            if (!Arrays.equals(store.read(key), bytes)) { throw new IOException("IMMUTABLE_BUNDLE_CONFLICT"); }
        } else { store.put(key, bytes, type); }
        if (!Arrays.equals(store.read(key), bytes)) { throw new IOException("FINAL_READBACK_MISMATCH"); }
    }
    private static SilverFileMetadata file(String key, byte[] bytes, int count) {
        return new SilverFileMetadata("part-00000.parquet", key, sha(bytes), bytes.length, count);
    }
    private static String schemaSha(MessageType schema) { return sha(schema.toString().getBytes(StandardCharsets.UTF_8)); }
    public static String sha(byte[] bytes) { return JmaBronzeWriter.sha256(bytes); }
}
