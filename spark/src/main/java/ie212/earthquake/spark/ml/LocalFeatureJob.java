package ie212.earthquake.spark.ml;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.gold.TrinoSqlClient;
import java.net.URI;
import java.nio.file.*;
import java.util.*;
import org.apache.spark.sql.*;

/** Explicit local preparation/readback, not an official dataset lifecycle transition. */
public final class LocalFeatureJob {
    static final ObjectMapper JSON = WindowArtifactInput.JSON;
    public static SparkSession session() {
        var spark = SparkSession.builder().master("local[1]").appName("MLD-04-local-features")
            .config("spark.ui.enabled", "false").config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1").config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "1").config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR"); return spark;
    }
    public static String codeSha() throws Exception {
        Path code = Path.of(LocalFeatureJob.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        if (!Files.isRegularFile(code)) throw new IllegalArgumentException("ML_PREPARED_JAR_REQUIRED");
        return WindowArtifactInput.sha(code);
    }
    public static void json(Path path, JsonNode value) throws Exception {
        Files.writeString(path, DatasetIdentityPlan.canonical(value).toString() + "\n", StandardOpenOption.CREATE_NEW);
    }
    public static JsonNode schema(Dataset<Row> rows) {
        var result = JSON.createArrayNode();
        for (var field : rows.schema().fields()) {
            var entry = result.addObject().put("name", field.name());
            entry.put("type", field.dataType().typeName());
        }
        return result;
    }
    /** One stable part, sorted by full grain; fail rather than overwrite an existing destination. */
    public static void parquet(Dataset<Row> rows, Path file, String... keys) throws Exception {
        Path temporary = file.resolveSibling("." + file.getFileName() + "-spark");
        rows.coalesce(1).sortWithinPartitions(keys[0], Arrays.copyOfRange(keys, 1, keys.length))
            .write().mode(SaveMode.ErrorIfExists).option("compression", "snappy").parquet(temporary.toString());
        List<Path> parts;
        try (var stream = Files.list(temporary)) { parts = stream.filter(p -> p.getFileName().toString().matches("part-.*\\.parquet")).toList(); }
        if (parts.size() != 1) throw new IllegalStateException("ML_SINGLE_PART_EXPECTED");
        Files.move(parts.get(0), file);
        if (Files.size(file) > 67_108_864) throw new IllegalStateException("ML_LOCAL_PARQUET_SIZE_LIMIT");
        // Reuse the existing Silver serializer's stable-footer rule. Enum identity-hash
        // iteration otherwise changes Parquet encoding-list order across JVM submits.
        Files.write(file, ie212.earthquake.spark.silver.SilverParquetSerializer.canonicalFooter(Files.readAllBytes(file)),
            StandardOpenOption.TRUNCATE_EXISTING);
        // Only files created by this method in its newly-owned temporary folder.
        try (var stream = Files.list(temporary)) { for (Path path : stream.toList()) Files.delete(path); }
        Files.delete(temporary);
        var read = rows.sparkSession().read().parquet(file.toString());
        if (!MainshockWindowJob.fingerprint(rows).equals(MainshockWindowJob.fingerprint(read)) || rows.count() != read.count())
            throw new IllegalStateException("ML_FEATURE_PARQUET_READBACK_FAILED");
    }
    public static SortedMap<String, String> hashes(Path directory) throws Exception {
        var hashes = new TreeMap<String, String>();
        try (var files = Files.list(directory)) {
            for (Path path : files.toList()) {
                if (!Files.isRegularFile(path) || Files.isSymbolicLink(path)) throw new IllegalStateException("ML_BUNDLE_UNEXPECTED_PATH");
                hashes.put(path.getFileName().toString(), WindowArtifactInput.sha(path));
            }
        }
        return hashes;
    }
    public static boolean install(Path prepared, Path destination) throws Exception {
        if (Files.exists(destination)) {
            if (Files.isSymbolicLink(destination) || !hashes(prepared).equals(hashes(destination)))
                throw new IllegalStateException("ML_EXISTING_BUNDLE_CONFLICT");
            try (var paths = Files.list(prepared)) { for (Path path : paths.toList()) Files.delete(path); }
            Files.delete(prepared); return true;
        }
        Files.move(prepared, destination); return false;
    }
    public static void main(String[] args) throws Exception {
        if (args.length != 5) throw new IllegalArgumentException("usage: LocalFeatureJob INPUT NATIVE_SHA SCALING_JSON OUTPUT_ROOT TRINO_URL");
        String code = codeSha(); var configJson = JSON.readTree(Files.readAllBytes(Path.of(args[2])));
        var config = FeatureScalingConfig.fromJson(configJson);
        SparkSession spark = session();
        try {
            var input = WindowArtifactInput.load(spark, Path.of(args[0]), args[1], new TrinoSqlClient(URI.create(args[4])));
            var result = new SpacetimeFeatureEngine().execute(input.mainshocks(), input.candidates(), config);
            result.assertValid();
            ObjectNode identity = JSON.createObjectNode().put("native_report_sha256", args[1]).put("code_sha256", code)
                .put("spark_version", spark.version()).put("java_version", System.getProperty("java.version"))
                .put("scaling_config_sha256", DatasetIdentityPlan.sha(DatasetIdentityPlan.canonical(configJson).toString()))
                .put("source_dataset_id", input.dataset().path("manifest").path("dataset_id").asText());
            String id = "feature_" + DatasetIdentityPlan.sha(DatasetIdentityPlan.canonical(identity).toString());
            Path root = Path.of(args[3]).toAbsolutePath(); Files.createDirectories(root);
            Path temp = Files.createTempDirectory(root, ".feature-prepare-");
            parquet(result.features(), temp.resolve("features.parquet"), "dataset_id", "mainshock_event_id", "candidate_event_id");
            var report = identity.deepCopy().put("status", "FEATURE_READY_LOCAL").put("feature_id", id)
                .put("input_count", result.inputCount()).put("output_count", result.validCount()).put("invalid_count", result.invalidCount())
                .put("parquet_readback_verified", true).put("source_status_unchanged", "BUILDING").put("research_ready", false);
            report.set("reason_counts", JSON.valueToTree(result.reasonCounts())); report.set("scaling_config", configJson);
            report.set("schema", schema(result.features())); report.set("vector_columns", JSON.valueToTree(SpacetimeFeatureEngine.VECTOR));
            report.put("feature_content_sha256", MainshockWindowJob.fingerprint(result.features()));
            report.put("parquet_sha256", WindowArtifactInput.sha(temp.resolve("features.parquet")));
            json(temp.resolve("feature-report.json"), report);
            boolean reused = install(temp, root.resolve(id));
            System.out.println("event=feature_ready feature_id=" + id + " rows=" + result.validCount() + " invalid=0 reused=" + reused);
        } finally { spark.stop(); }
    }
}
