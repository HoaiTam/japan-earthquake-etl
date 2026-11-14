package ie212.earthquake.spark.gold;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.GoldCommitJob;
import ie212.earthquake.spark.silver.SilverBundlePublisher;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;

/** Isolated read-only fingerprint capacity gate on an exact existing snapshot, never a research dataset. */
public final class PortableGoldCapacityJob {
    private static final ObjectMapper JSON = new ObjectMapper();
    private PortableGoldCapacityJob() { }
    static Map<String, String> readCompatibilityOptions() {
        // Trino-written publication_status may use DELTA_LENGTH_BYTE_ARRAY.
        // Change only this isolated reader session, never table/catalog properties.
        return Map.of("spark.sql.iceberg.vectorization.enabled", "false");
    }
    static void validate(JsonNode r) {
        if (!"dat03-portable-gold-capacity-v1".equals(r.path("version").asText())
                || !r.path("source_namespace").asText().matches("iceberg\\.dat03_catalog_[a-f0-9]{32}")
                || !Set.of("event_current", "event_source_bridge").contains(r.path("table").asText())
                || !r.path("source_publication_id").asText().matches("pub_[a-f0-9]{64}")
                || !r.path("source_bundle_sha256").asText().matches("[a-f0-9]{64}"))
            throw new IllegalArgumentException("EXACT_CAPACITY_SOURCE_REQUIRED");
        for (String key : List.of("snapshot_id", "source_count", "copies", "required_rows"))
            if (!r.path(key).isIntegralNumber() || !r.path(key).canConvertToLong() || r.path(key).asLong() <= 0)
                throw new IllegalArgumentException("TYPED_POSITIVE_CAPACITY_LIMIT_REQUIRED");
        long source = r.path("source_count").asLong(), copies = r.path("copies").asLong(), required = r.path("required_rows").asLong();
        if (source > 5_000_000 || copies > 8 || required > 10_000_000
                || Math.multiplyExact(source, copies) > 10_000_000 || Math.multiplyExact(source, copies) < required)
            throw new IllegalArgumentException("BOUNDED_FULL_WORKLOAD_REQUIRED");
    }
    public static void main(String[] args) throws Exception {
        if (args.length != 3) throw new IllegalArgumentException("Usage: PortableGoldCapacityJob <request> <expected-sha256> <new-report>");
        Path input = Path.of(args[0]);
        if (Files.isSymbolicLink(input) || Files.size(input) > 65536) throw new IOException("BOUNDED_REQUEST_REQUIRED");
        byte[] raw = Files.readAllBytes(input);
        if (!SilverBundlePublisher.sha(raw).equals(args[1])) throw new IOException("CAPACITY_REQUEST_PIN_MISMATCH");
        JsonNode request = JSON.readTree(raw); validate(request);
        Path output = Path.of(args[2]);
        if (Files.exists(output, LinkOption.NOFOLLOW_LINKS)) throw new IOException("NEW_CAPACITY_REPORT_REQUIRED");
        var builder = GoldCommitJob.configured(System.getenv()).master("local[1]")
                .appName("dat03-portable-read-only-capacity").config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "4").config("spark.sql.adaptive.enabled", "false")
                .config("spark.sql.codegen.wholeStage", "false");
        readCompatibilityOptions().forEach(builder::config);
        SparkSession spark = builder.getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        try {
            long start = System.nanoTime(); String table = request.path("table").asText();
            String namespace = request.path("source_namespace").asText();
            String publication = request.path("source_publication_id").asText();
            Dataset<Row> status = spark.table(namespace + ".publication_status").filter(col("publication_id").equalTo(publication)
                    .and(col("verify_status").equalTo("PASSED")).and(col("bundle_sha256").equalTo(request.path("source_bundle_sha256").asText())));
            if (status.count() != 1) throw new IOException("PINNED_PUBLICATION_REQUIRED");
            JsonNode bundle = JSON.readTree(status.select("snapshot_bundle_json").head().getString(0));
            JsonNode pin = bundle.path(table);
            if (!pin.path("snapshot_id").equals(request.path("snapshot_id")) || pin.path("count").asLong(-1) != request.path("source_count").asLong())
                throw new IOException("PUBLICATION_SNAPSHOT_PIN_MISMATCH");
            Dataset<Row> source = spark.read().format("iceberg").option("snapshot-id", request.path("snapshot_id").asText()).load(namespace + "." + table);
            if (source.count() != request.path("source_count").asLong()) throw new IOException("SOURCE_COUNT_CHANGED");
            Dataset<Row> rows = source.crossJoin(spark.range(request.path("copies").asLong()).withColumnRenamed("id", "_capacity_copy"));
            for (String id : List.of("canonical_event_id", "source_observation_id")) if (Arrays.asList(source.columns()).contains(id))
                rows = rows.withColumn(id, concat(col(id), lit("-capacity-"), col("_capacity_copy").cast("string")));
            rows = rows.drop("_capacity_copy");
            long expected = Math.multiplyExact(request.path("source_count").asLong(), request.path("copies").asLong());
            String key = table.equals("event_current") ? "canonical_event_id" : "source_observation_id";
            if (rows.count() != expected || rows.select(key).distinct().count() != expected) throw new IOException("STRESS_COUNT_UNBALANCED");
            var method = GoldIcebergWriter.class.getDeclaredMethod("fingerprint", Dataset.class); method.setAccessible(true);
            String first = (String) method.invoke(null, rows), second = (String) method.invoke(null, rows);
            if (!first.equals(second)) throw new IOException("FINGERPRINT_RERUN_MISMATCH");
            ObjectNode report = JSON.createObjectNode().put("version", "dat03-portable-gold-capacity-v1")
                    .put("status", "READ_ONLY_FINGERPRINT_CAPACITY_PASSED").put("request_sha256", args[1])
                    .put("table", table).put("stress_rows", expected).put("required_rows", request.path("required_rows").asLong())
                    .put("fingerprint_sha256", first).put("repeat_fingerprint_sha256", second)
                    .put("elapsed_seconds", (System.nanoTime() - start) / 1e9).put("synthetic_stress_only", true)
                    .put("reader_profile", "session-row-reader-delta-compatible-v1")
                    .put("gold_published", false).put("full_capacity_accepted", false).put("research_ready", false);
            Files.write(output, JSON.writerWithDefaultPrettyPrinter().writeValueAsBytes(report), StandardOpenOption.CREATE_NEW);
            System.out.println("event=portable_gold_capacity_passed table=" + table + " stress_rows=" + expected + " gold_published=false");
        } finally { spark.stop(); }
    }
}
