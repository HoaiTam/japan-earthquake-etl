package ie212.earthquake.spark;

import com.fasterxml.jackson.databind.*;
import ie212.earthquake.spark.gold.*;
import ie212.earthquake.spark.silver.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.net.URI;
import org.apache.spark.sql.SparkSession;

/** Production GLD-03 entry point. Shared source lease must be acquired by the control plane. */
public final class GoldCommitJob {
    private static final ObjectMapper JSON = new ObjectMapper();
    private GoldCommitJob() { }
    public static SparkSession.Builder configured(Map<String, String> env) {
        String catalog = require(env, "ICEBERG_CATALOG_NAME");
        if (!catalog.matches("[a-z][a-z0-9_]*")) throw new IllegalArgumentException("SAFE_CATALOG_REQUIRED");
        String prefix = "spark.sql.catalog."+catalog;
        String catalogUri = endpoint(env, "ICEBERG_CATALOG_URI");
        String minioUri = endpoint(env, "MINIO_ENDPOINT");
        URI warehouse = URI.create(require(env, "WAREHOUSE_PATH"));
        if (!"s3".equals(warehouse.getScheme()) || warehouse.getHost() == null
                || warehouse.getUserInfo() != null || warehouse.getQuery() != null || warehouse.getFragment() != null)
            throw new IllegalArgumentException("S3_WAREHOUSE_REQUIRED");
        return SparkSession.builder().appName("gld-03-iceberg-commit")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.redaction.regex", "(?i)secret|password|token|access[._-]?key|credential")
            .config(prefix, "org.apache.iceberg.spark.SparkCatalog").config(prefix+".type", "rest")
            .config(prefix+".uri", catalogUri)
            .config(prefix+".warehouse", require(env,"WAREHOUSE_PATH"))
            .config(prefix+".io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config(prefix+".s3.endpoint", minioUri)
            .config(prefix+".s3.path-style-access", "true")
            .config(prefix+".client.region", require(env,"S3_REGION"))
            .config(prefix+".s3.access-key-id", require(env,"MINIO_ACCESS_KEY"))
            .config(prefix+".s3.secret-access-key", require(env,"MINIO_SECRET_KEY"))
            .config(prefix+".rest.vended-credentials-enabled", "false");
    }
    public static void main(String[] args) throws Exception {
        if (args.length != 1 || Files.size(Path.of(args[0])) > 65536) throw new IllegalArgumentException("EXACT_REQUEST_REQUIRED");
        JsonNode request = JSON.readTree(Files.readAllBytes(Path.of(args[0])));
        if (!"COMPLETE_MONTHS".equals(request.path("scope_completeness").asText()))
            throw new IllegalArgumentException("COMPLETE_MONTH_HANDOFF_REQUIRED");
        var run = request.path("run_context"); var manifestUris = new ArrayList<String>();
        if (!request.path("dag_id").isTextual() || request.path("dag_id").asText().isBlank()
                || !run.path("is_backfill").isBoolean() || !run.path("input_manifest_uris").isArray()
                || !request.path("affected_months").isArray() || !request.path("baseline_snapshots").isObject())
            throw new IllegalArgumentException("TYPED_RUN_REQUEST_REQUIRED");
        run.path("input_manifest_uris").forEach(uri -> manifestUris.add(uri.asText()));
        GoldRunContext context = new GoldRunContext(run.path("run_id").asText(), Instant.parse(run.path("window_start_utc").asText()),
                Instant.parse(run.path("window_end_utc").asText()), LocalDate.parse(run.path("processing_date").asText()),
                run.path("is_backfill").asBoolean(), run.path("config_version").asText(), manifestUris);
        Path guard = Path.of(require(System.getenv(),"SOURCE_GUARD_ROOT"), "owner.json");
        Runnable lease = () -> {
            try { var owner = JSON.readTree(Files.readAllBytes(guard));
                if (!context.runId().equals(owner.path("run_id").asText())
                        || !request.path("dag_id").asText().equals(owner.path("dag_id").asText()))
                    throw new IllegalStateException("GOLD_SHARED_LEASE_NOT_HELD");
            } catch (Exception error) { throw new IllegalStateException("GOLD_SHARED_LEASE_NOT_HELD"); }
        };
        lease.run(); SparkSession spark = configured(System.getenv()).getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        try (var store = MinioSilverObjectStore.fromEnvironment()) {
            var output = GoldInputReader.read(spark, store, request.path("silver_run_id").asText(),
                request.path("silver_bundle_sha256").asText(), context, Instant.parse(request.path("processed_at_utc").asText()),
                Path.of(args[0]).toAbsolutePath().getParent().resolve("readback"));
            var writer = new GoldIcebergWriter(spark, request.path("namespace").asText(), store, lease);
            var months = new ArrayList<String>(); request.path("affected_months").forEach(month -> months.add(month.asText()));
            Map<String,Long> baseline = new TreeMap<>(); request.path("baseline_snapshots").fields().forEachRemaining(
                entry -> { if(!entry.getValue().isIntegralNumber()) throw new IllegalArgumentException("EXACT_BASELINES_REQUIRED");
                    baseline.put(entry.getKey(),entry.getValue().asLong()); });
            var receipt = writer.commit(output, new GoldCommitScope(context, request.path("silver_bundle_sha256").asText(),
                    request.path("code_version").asText(), months, baseline));
            Files.write(Path.of(args[0]).toAbsolutePath().getParent().resolve("commit.json"), JSON.writeValueAsBytes(receipt));
            System.out.println(JSON.writeValueAsString(receipt));
        } finally { spark.stop(); }
    }
    private static String require(Map<String,String> env, String key) {
        String value = env.get(key);
        if(value == null || value.isBlank() || value.startsWith("change-me")) throw new IllegalArgumentException("CONFIG_REQUIRED:"+key);
        return value;
    }
    private static String endpoint(Map<String,String> env, String key) {
        String value = require(env, key); URI uri = URI.create(value);
        if (!Set.of("http", "https").contains(uri.getScheme()) || uri.getHost() == null
                || uri.getUserInfo() != null || uri.getQuery() != null || uri.getFragment() != null)
            throw new IllegalArgumentException("SAFE_ENDPOINT_REQUIRED:"+key);
        return value;
    }
}
