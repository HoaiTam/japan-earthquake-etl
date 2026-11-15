package ie212.earthquake.spark.gold;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.silver.*;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.*;
import java.io.*;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;

/** New prepared contract. Never pretends to be a legacy SilverReady/FullMonthReady publication. */
public final class PortablePreparedCatalog {
    static final ObjectMapper JSON = new ObjectMapper().enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    private PortablePreparedCatalog() { }
    public static Path relative(Path root, String name) throws Exception {
        PortableImportContract.require(name.matches("[A-Za-z0-9][A-Za-z0-9._/-]*") && !name.contains("..") && !name.contains("//"), "SAFE_RELATIVE_ARTIFACT_REQUIRED");
        Path path = PortableInputMaterializer.safe(root.resolve(name));
        PortableImportContract.require(path.startsWith(root.toAbsolutePath().normalize()), "ARTIFACT_OUTSIDE_PREPARED_ROOT"); return path;
    }
    public static JsonNode read(Path path, int limit) throws Exception {
        path = PortableInputMaterializer.safe(path); PortableImportContract.require(Files.size(path) <= limit, "PREPARED_METADATA_BOUND");
        return JSON.readTree(Files.readAllBytes(path));
    }
    public static void save(Path path, JsonNode value) throws Exception {
        path = PortableInputMaterializer.safe(path); Files.write(path, JSON.writerWithDefaultPrettyPrinter().writeValueAsBytes(value), StandardOpenOption.CREATE_NEW);
    }
    public static boolean persistedJsonEquals(Path path, JsonNode value) throws Exception {
        // Compare the exact deterministic persisted representation, not IntNode vs LongNode identity.
        return Arrays.equals(Files.readAllBytes(path), JSON.writerWithDefaultPrettyPrinter().writeValueAsBytes(value));
    }
    public static ObjectNode reference(Path root, Path path) throws Exception {
        return JSON.createObjectNode().put("relative_path", root.relativize(path).toString()).put("sha256", PortableInputMaterializer.fileSha(path))
                .put("byte_size", Files.size(path));
    }
    public static JsonNode verifyReference(Path root, JsonNode ref) throws Exception {
        Path path = relative(root, ref.path("relative_path").asText());
        PortableImportContract.require(ref.path("byte_size").isIntegralNumber() && ref.path("byte_size").canConvertToLong()
                && Files.size(path) == ref.path("byte_size").asLong() && PortableInputMaterializer.fileSha(path).equals(ref.path("sha256").asText()), "PREPARED_FILE_PIN_MISMATCH");
        return ref;
    }
    public static long parquetRows(Path path) throws Exception {
        Path file = PortableInputMaterializer.safe(path);
        InputFile input = new InputFile() {
            public long getLength() throws IOException { return Files.size(file); }
            public SeekableInputStream newStream() throws IOException {
                var random = new RandomAccessFile(file.toFile(), "r");
                return new DelegatingSeekableInputStream(new InputStream() {
                    public int read() throws IOException { return random.read(); }
                    public int read(byte[] b, int o, int n) throws IOException { return random.read(b, o, n); }
                    public void close() throws IOException { random.close(); }
                }) {
                    public long getPos() throws IOException { return random.getFilePointer(); }
                    public void seek(long pos) throws IOException { random.seek(pos); }
                };
            }
        };
        try (var reader = ParquetFileReader.open(input)) {
            return reader.getFooter().getBlocks().stream().mapToLong(b -> b.getRowCount()).sum();
        }
    }
    public static ObjectNode describeDataset(Path root, Path folder, Dataset<Row> data) throws Exception {
        long expected = data.count(); String[] names = data.columns().clone(); Arrays.sort(names);
        data.select(Arrays.stream(names).map(org.apache.spark.sql.functions::col).toArray(Column[]::new))
                .coalesce(1).write().mode("errorifexists").parquet(folder.toString());
        var ref = JSON.createObjectNode().put("row_count", expected).put("schema_json", data.select(Arrays.stream(names).map(org.apache.spark.sql.functions::col).toArray(Column[]::new)).schema().json());
        var files = ref.putArray("files"); long actual = 0;
        try (var walk = Files.walk(folder)) {
            for (var path : walk.filter(p -> p.getFileName().toString().endsWith(".parquet")).sorted().toList()) {
                path = PortableInputMaterializer.safe(path); long n = parquetRows(path); actual += n;
                var file = reference(root, path).put("row_count", n); files.add(file);
            }
        }
        PortableImportContract.require(!files.isEmpty() && actual == expected, "PREPARED_PARQUET_COUNT_MISMATCH"); return ref;
    }
    public static long verifyDataset(Path root, JsonNode descriptor) throws Exception {
        PortableImportContract.require(descriptor.path("row_count").isIntegralNumber() && descriptor.path("row_count").canConvertToLong()
                && descriptor.path("row_count").asLong() >= 0 && descriptor.path("files").isArray()
                && !descriptor.path("files").isEmpty() && descriptor.path("files").size() <= 512, "BOUNDED_PREPARED_DATASET_REQUIRED");
        var seen = new HashSet<String>(); long rows = 0;
        for (var f : descriptor.path("files")) {
            PortableImportContract.require(seen.add(f.path("relative_path").asText()), "DUPLICATE_PREPARED_FILE"); verifyReference(root, f);
            long actual = parquetRows(relative(root, f.path("relative_path").asText()));
            PortableImportContract.require(f.path("row_count").isIntegralNumber() && f.path("row_count").canConvertToLong()
                    && actual == f.path("row_count").asLong(), "PREPARED_FOOTER_COUNT_CHANGED"); rows = Math.addExact(rows, actual);
        }
        PortableImportContract.require(rows == descriptor.path("row_count").asLong(), "PREPARED_ROW_COUNT_CHANGED"); return rows;
    }
    public static JsonNode verify(Path root, String expected) throws Exception {
        root = PortableInputMaterializer.safe(root); Path manifest = root.resolve("catalog.json");
        PortableImportContract.require(expected.matches("[a-f0-9]{64}") && PortableInputMaterializer.fileSha(manifest).equals(expected), "TRUSTED_PREPARED_SHA_REQUIRED");
        JsonNode value = read(manifest, 8 * 1024 * 1024);
        PortableImportContract.require(PortableImportContract.PREPARED_VERSION.equals(value.path("version").asText())
                && "PREPARED".equals(value.path("status").asText()) && value.path("complete_months").equals(JSON.valueToTree(PortableImportContract.ALL_MONTHS)), "EXACT_PREPARED_CATALOG_REQUIRED");
        for (String flag : List.of("gold_published", "official_export_ready", "research_ready", "ml_upstream_eligible"))
            PortableImportContract.require(value.path(flag).isBoolean() && !value.path(flag).asBoolean(), "PREPARED_IS_NOT_A_PUBLICATION");
        verifyReference(root, value.path("request")); verifyReference(root, value.path("coverage")); verifyReference(root, value.path("reconciliation"));
        JsonNode coverage = read(relative(root, value.path("coverage").path("relative_path").asText()), 1024 * 1024);
        PortableImportContract.require(PortableImportContract.COVERAGE_VERSION.equals(coverage.path("version").asText())
                && "PREPARED_SOURCE_COVERAGE".equals(coverage.path("status").asText()) && coverage.path("months").size() == 288, "PREPARED_COVERAGE_REQUIRED");
        for (int i = 0; i < 288; i++) {
            var m = coverage.path("months").get(i); String month = PortableImportContract.ALL_MONTHS.get(i);
            PortableImportContract.require(month.equals(m.path("month").asText())
                    && (i < 225 ? "REPRODUCTION" : "EXTENSION").equals(m.path("split").asText())
                    && m.path("jma_physical_manifest_uris").isArray() && !m.path("jma_physical_manifest_uris").isEmpty(), "PREPARED_COVERAGE_GAP_OR_SPLIT_CHANGED");
        }
        verifyReference(root, value.path("provenance"));
        PortableImportContract.require(coverage.path("provenance_receipt_sha256").equals(value.path("provenance").path("sha256")), "PREPARED_PROVENANCE_PIN_MISMATCH");
        JsonNode reconciliation = read(relative(root, value.path("reconciliation").path("relative_path").asText()), 65536);
        PortableImportContract.require(reconciliation.path("balanced").isBoolean() && reconciliation.path("balanced").asBoolean()
                && reconciliation.path("global_checks_passed").isBoolean() && reconciliation.path("global_checks_passed").asBoolean(), "PREPARED_RECONCILIATION_REQUIRED");
        PortableImportContract.require(value.path("tables").size() == 6, "SIX_PREPARED_TABLES_REQUIRED");
        for (String name : GoldIcebergWriter.TABLES) verifyDataset(root, value.path("tables").path(name));
        verifyDataset(root, value.path("source_links"));
        JsonNode request = read(relative(root, value.path("request").path("relative_path").asText()), 1024 * 1024);
        PortableImportContract.validate(request);
        PortableImportContract.require(value.path("checkpoints").isArray() && value.path("checkpoints").size() == 14 + request.path("worker_batches").size(), "COMPLETE_CHECKPOINT_CHAIN_REQUIRED");
        String previous = null; var months = new ArrayList<String>();
        for (var ref : value.path("checkpoints")) {
            verifyReference(root, ref); JsonNode cp = read(relative(root, ref.path("relative_path").asText()), 1024 * 1024);
            PortableImportContract.require(cp.path("request_sha256").equals(value.path("request").path("sha256"))
                    && Objects.equals(previous, cp.path("previous_checkpoint_sha256").isNull() ? null : cp.path("previous_checkpoint_sha256").asText()), "PREPARED_CHECKPOINT_CHAIN_CHANGED");
            for (var month : cp.path("months")) months.add(month.asText());
            for (String table : GoldIcebergWriter.TABLES) verifyDataset(root, cp.path("tables").path(table));
            verifyDataset(root, cp.path("tables").path("source_link")); previous = ref.path("sha256").asText();
        }
        PortableImportContract.require(months.equals(PortableImportContract.ALL_MONTHS), "PREPARED_CHECKPOINT_COVERAGE_CHANGED");
        PortableImportContract.require(value.path("tables").path("event_current").path("row_count").equals(reconciliation.path("counts").path("canonical_events"))
                && value.path("tables").path("event_source_bridge").path("row_count").equals(reconciliation.path("counts").path("bridge_rows"))
                && value.path("source_links").path("row_count").equals(reconciliation.path("counts").path("source_links")), "PREPARED_RECONCILIATION_COUNT_MISMATCH");
        return value;
    }
    public static Dataset<Row> dataset(SparkSession spark, Path root, JsonNode descriptor) throws Exception {
        var files = new ArrayList<String>(); for (var f : descriptor.path("files")) files.add(relative(root, f.path("relative_path").asText()).toString());
        return spark.read().parquet(files.toArray(String[]::new));
    }
    public static GoldTransformationResult asGoldInput(SparkSession spark, Path root, String sha) throws Exception {
        JsonNode value = verify(root, sha); JsonNode t = value.path("tables");
        Dataset<Row> events = dataset(spark, root, t.path("event_current"));
        long canonical = t.path("event_current").path("row_count").asLong(), bridge = t.path("event_source_bridge").path("row_count").asLong();
        return new GoldTransformationResult(events, events.filter(col("is_natural_earthquake").and(col("is_in_study_area"))),
                dataset(spark, root, t.path("event_source_bridge")).drop("event_month_utc"), dataset(spark, root, t.path("dim_date")),
                dataset(spark, root, t.path("dim_region")), dataset(spark, root, t.path("dim_magnitude_band")), dataset(spark, root, t.path("dim_depth_band")),
                bridge, canonical, bridge);
    }
}
