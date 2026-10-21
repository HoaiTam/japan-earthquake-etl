package ie212.earthquake.spark.ml;

import static org.apache.spark.sql.functions.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import org.apache.spark.sql.*;

/** MLD-05 portable materialization; never copies Iceberg physical files. */
public final class FeatureExportBundle {
    static final ObjectMapper JSON = WindowArtifactInput.JSON;
    public static final long MAX_ROWS = 100_000;
    public static final List<String> COLUMNS = List.of("schema_version", "dataset_id", "mainshock_event_id", "candidate_event_id",
        "candidate_time_utc", "candidate_latitude", "candidate_longitude", "candidate_depth_km", "candidate_magnitude",
        "mainshock_magnitude", "relative_time_role", "is_mainshock", "dx_km", "dy_km", "dz_km", "delta_time_hours",
        "distance_3d_km", "x_scaled", "y_scaled", "z_scaled", "time_scaled", "space_scale_km", "depth_scale_km",
        "time_scale_hours", "mc_value", "window_model_version", "feature_version", "scaling_config_version", "created_at_utc");
    private FeatureExportBundle() { }
    static void require(boolean ok, String reason) throws IOException { if (!ok) throw new IOException(reason); }
    static String digest(JsonNode node) { return DatasetIdentityPlan.sha(DatasetIdentityPlan.canonical(node).toString()); }
    static void immutable(Path path, byte[] bytes) throws Exception {
        if (Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
            require(Files.isRegularFile(path) && !Files.isSymbolicLink(path) && Arrays.equals(bytes, Files.readAllBytes(path)), "DS_EXISTING_BUNDLE_CORRUPT");
        } else Files.write(path, bytes, StandardOpenOption.CREATE_NEW);
    }
    static byte[] bytes(JsonNode node) throws Exception { return JSON.writeValueAsBytes(DatasetIdentityPlan.canonical(node)); }
    public static Path safeRoot(Path root) throws Exception {
        root = root.toAbsolutePath().normalize();
        for (Path p = root; p != null; p = p.getParent()) require(!Files.isSymbolicLink(p), "DS_EXPORT_SYMLINK_REJECTED");
        Files.createDirectories(root); require(root.toRealPath().equals(root), "DS_EXPORT_UNSAFE_ROOT"); return root;
    }
    public static Dataset<Row> columns(Dataset<Row> rows) throws Exception {
        require(Arrays.asList(rows.columns()).containsAll(COLUMNS), "DS_EXPORT_SCHEMA_MISMATCH");
        return rows.select(COLUMNS.stream().map(org.apache.spark.sql.functions::col).toArray(Column[]::new));
    }
    public static ObjectNode validate(Dataset<Row> features, Dataset<Row> main, JsonNode manifest, FeatureScalingConfig scaling) throws Exception {
        var selected = columns(features); String id = manifest.path("dataset_id").asText(); long count = selected.count(), mainCount = main.count();
        require(id.matches("ds_[a-f0-9]{64}") && count > 0 && count <= MAX_ROWS && mainCount > 0 && mainCount <= 128, "DS_EXPORT_RESOURCE_LIMIT");
        require(scaling.featureVersion().equals(manifest.path("feature_version").asText())
            && scaling.scalingConfigVersion().equals(manifest.path("scaling_config_version").asText()), "DS_EXPORT_SCALING_CONFLICT");
        require(selected.filter(not(col("dataset_id").eqNullSafe(id))).count() == 0
            && main.filter(not(col("dataset_id").eqNullSafe(id))).count() == 0, "DS_EXPORT_DATASET_LINEAGE_MISMATCH");
        for (String name : COLUMNS) require(selected.filter(col(name).isNull()).limit(1).count() == 0, "DS_EXPORT_NULL_FIELD:" + name);
        require(selected.filter(not(col("schema_version").equalTo("1.0"))).limit(1).count() == 0, "DS_EXPORT_SCHEMA_VERSION");
        // Recompute through the shared engine: supplied scale/vector/version fields are not caller assertions.
        var checked = new SpacetimeFeatureEngine().execute(main, selected, scaling); checked.assertValid();
        var recomputed = columns(checked.features());
        require(MainshockWindowJob.fingerprint(selected).equals(MainshockWindowJob.fingerprint(recomputed)), "DS_EXPORT_FEATURE_VALUE_MISMATCH");
        var result = JSON.createObjectNode().put("status", "PASSED").put("quality_gate_status", "PASSED")
            .put("invalid_feature_count", 0).put("candidate_row_count", count).put("feature_row_count", count).put("mainshock_count", mainCount)
            .put("feature_content_sha256", MainshockWindowJob.fingerprint(selected));
        result.set("feature_schema", LocalFeatureJob.schema(selected));
        result.put("feature_schema_sha256", digest(result.path("feature_schema"))); return result;
    }
    /** Public checksum index includes the manifest; the referenced data-only index avoids a hash cycle. */
    public static ObjectNode materialize(Dataset<Row> pinned, ObjectNode manifest, Path outputRoot) throws Exception {
        require("VALIDATED".equals(manifest.path("dataset_status").asText()), "DS_VALIDATED_DATASET_REQUIRED");
        String id = manifest.path("dataset_id").asText(); require(id.matches("ds_[a-f0-9]{64}"), "DS_EXACT_DATASET_REQUIRED");
        Path root = safeRoot(outputRoot), target = root.resolve("dataset_id=" + id);
        if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
            var existing = verify(pinned.sparkSession(), target);
            require(existing.path("export_identity_sha256").equals(manifest.path("export_identity_sha256")), "DS_IDENTITY_CONFLICT");
            for (var fields = manifest.fields(); fields.hasNext();) {
                var field = fields.next();
                if (Set.of("dataset_status", "export_manifest_sha256", "export_manifest_ref").contains(field.getKey())) continue;
                require(existing.has(field.getKey()) && digest(field.getValue()).equals(digest(existing.path(field.getKey()))), "DS_EXPORT_EXISTING_MANIFEST_CONFLICT");
            }
            return existing;
        }
        require("UTC".equals(pinned.sparkSession().conf().get("spark.sql.session.timeZone")), "DS_EXPORT_UTC_REQUIRED");
        pinned.sparkSession().conf().set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS");
        var rows = columns(pinned); long count = rows.count();
        require(count > 0 && count <= MAX_ROWS && count == manifest.path("feature_row_count").asLong(-1), "DS_EXPORT_COUNT_MISMATCH");
        Path temporary = Files.createTempDirectory(root, ".mld05-attempt-");
        int parts = (int)Math.max(1, (count + 9999) / 10000); var hashes = new TreeMap<String,String>();
        var bucket = pmod(xxhash64(col("dataset_id"), col("mainshock_event_id"), col("candidate_event_id")), lit(parts));
        for (int i = 0; i < parts; i++) {
            var subset = rows.filter(bucket.equalTo(i)); if (subset.limit(1).count() == 0) continue;
            String name = String.format(Locale.ROOT, "features-part-%05d.parquet", i);
            LocalFeatureJob.parquet(subset, temporary.resolve(name), "dataset_id", "mainshock_event_id", "candidate_event_id");
            hashes.put(name, WindowArtifactInput.sha(temporary.resolve(name)));
        }
        StringBuilder dataIndex = new StringBuilder(); hashes.forEach((name,sha) -> dataIndex.append(sha).append("  ").append(name).append('\n'));
        immutable(temporary.resolve("export-checksums.sha256"), dataIndex.toString().getBytes(java.nio.charset.StandardCharsets.US_ASCII));
        var exported = manifest.deepCopy(); exported.put("dataset_status", "EXPORTED").put("export_version", "mld05-export-v1")
            .put("export_manifest_sha256", WindowArtifactInput.sha(temporary.resolve("export-checksums.sha256")))
            .put("export_manifest_ref", "exports/" + id + "/export-checksums.sha256");
        exported.set("parts", JSON.valueToTree(hashes));
        immutable(temporary.resolve("dataset_manifest.json"), bytes(exported));
        StringBuilder index = new StringBuilder(); LocalFeatureJob.hashes(temporary).forEach((name,sha) -> index.append(sha).append("  ").append(name).append('\n'));
        immutable(temporary.resolve("checksums.sha256"), index.toString().getBytes(java.nio.charset.StandardCharsets.US_ASCII));
        verify(rows.sparkSession(), temporary); // Read back Parquet/manifest/indices before exposing the directory.
        require(!Files.exists(target, LinkOption.NOFOLLOW_LINKS), "DS_EXPORT_TARGET_APPEARED");
        Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE); return verify(rows.sparkSession(), target);
    }
    public static ObjectNode verify(SparkSession spark, Path directory) throws Exception {
        require(Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS), "DS_EXPORT_DIRECTORY_REQUIRED");
        Path root = safeRoot(directory); var expected = index(root.resolve("checksums.sha256"));
        Set<String> found = new TreeSet<>();
        try (var files = Files.list(root)) { for (Path file : files.toList()) {
            require(Files.isRegularFile(file) && !Files.isSymbolicLink(file) && Files.size(file) <= 67_108_864, "DS_EXPORT_UNEXPECTED_FILE");
            found.add(file.getFileName().toString());
        } }
        Set<String> indexed = new TreeSet<>(expected.keySet()); indexed.add("checksums.sha256");
        require(found.equals(indexed) && expected.containsKey("dataset_manifest.json") && expected.containsKey("export-checksums.sha256"), "DS_EXPORT_FILE_SET_MISMATCH");
        for (var entry : expected.entrySet()) require(WindowArtifactInput.sha(root.resolve(entry.getKey())).equals(entry.getValue()), "DS_EXPORT_CHECKSUM_MISMATCH");
        var manifest = (ObjectNode)JSON.readTree(Files.readAllBytes(root.resolve("dataset_manifest.json")));
        require("mld05-export-v1".equals(manifest.path("export_version").asText()) && "EXPORTED".equals(manifest.path("dataset_status").asText()), "DS_EXPORT_MANIFEST_INVALID");
        require(WindowArtifactInput.sha(root.resolve("export-checksums.sha256")).equals(manifest.path("export_manifest_sha256").asText()), "DS_EXPORT_INDEX_MISMATCH");
        var dataIndex = index(root.resolve("export-checksums.sha256"));
        Set<String> parts = new TreeSet<>();
        for (String name : expected.keySet()) if (name.matches("features-part-[0-9]{5}\\.parquet")) parts.add(name);
        require(!parts.isEmpty() && parts.size() <= 10 && dataIndex.keySet().equals(parts)
            && found.size() == parts.size() + 3, "DS_EXPORT_PART_SET_MISMATCH");
        for (String name : parts) require(expected.get(name).equals(dataIndex.get(name))
            && expected.get(name).equals(manifest.path("parts").path(name).asText()), "DS_EXPORT_PART_IDENTITY_MISMATCH");
        var read = spark.read().parquet(parts.stream().map(name -> root.resolve(name).toString()).toArray(String[]::new));
        require(Arrays.asList(read.columns()).equals(COLUMNS) && read.count() == manifest.path("feature_row_count").asLong(-1)
            && MainshockWindowJob.fingerprint(read).equals(manifest.path("feature_content_sha256").asText())
            && digest(LocalFeatureJob.schema(read)).equals(manifest.path("feature_schema_sha256").asText()), "DS_EXPORT_PARQUET_READBACK_FAILED");
        return manifest;
    }
    static SortedMap<String,String> index(Path file) throws Exception {
        require(Files.isRegularFile(file) && !Files.isSymbolicLink(file) && Files.size(file) <= 65536, "DS_EXPORT_INDEX_REQUIRED");
        var result = new TreeMap<String,String>();
        for (String line : Files.readAllLines(file)) {
            require(line.matches("[a-f0-9]{64}  [A-Za-z0-9_.-]+"), "DS_EXPORT_INDEX_INVALID");
            String name = line.substring(66); require(!name.equals(".") && !name.equals("..") && !name.equals("checksums.sha256")
                && result.put(name,line.substring(0,64)) == null, "DS_EXPORT_INDEX_INVALID");
        }
        require(!result.isEmpty() && result.size() <= 32, "DS_EXPORT_INDEX_INVALID"); return result;
    }
}
