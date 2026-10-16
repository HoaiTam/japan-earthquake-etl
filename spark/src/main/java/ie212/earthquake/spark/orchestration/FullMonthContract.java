package ie212.earthquake.spark.orchestration;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import ie212.earthquake.spark.gold.GoldIcebergWriter;
import ie212.earthquake.spark.silver.SilverBundlePublisher;
import ie212.earthquake.spark.silver.SilverLinkConfig;
import java.io.IOException;
import java.net.URI;
import java.time.*;
import java.util.*;

/** ORC-06 metadata only. Fixed source policy; completeness is computed, never a caller flag. */
public final class FullMonthContract {
    public static final ObjectMapper JSON = new ObjectMapper().enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    public static final String VERSION = "orc-06-v1";
    public static final Instant USGS_START = Instant.parse("2023-01-01T00:00:00Z");
    public static final int MAX_ROWS = 100_000;
    public static final long HALO_SECONDS = (long)Math.ceil(SilverLinkConfig.DEFAULT_MAX_TIME_DELTA_SECONDS);
    private FullMonthContract() { }

    public static void require(boolean value, String reason) throws IOException {
        if (!value) throw new IOException(reason);
    }
    public static String text(JsonNode node, String key) throws IOException {
        require(node.path(key).isTextual() && !node.path(key).asText().isBlank() && node.path(key).asText().length()<=2048
                && node.path(key).asText().equals(node.path(key).asText().trim())
                && node.path(key).asText().chars().noneMatch(Character::isISOControl), "TYPED_METADATA_REQUIRED");
        return node.path(key).asText();
    }
    public static String sha(byte[] bytes) { return SilverBundlePublisher.sha(bytes); }
    public static String digest(JsonNode node) throws IOException { return sha(JSON.writeValueAsBytes(canonical(node))); }
    public static JsonNode canonical(JsonNode node) {
        if (node.isObject()) {
            ObjectNode out = JSON.createObjectNode(); var keys = new TreeSet<String>(); node.fieldNames().forEachRemaining(keys::add);
            keys.forEach(k -> out.set(k, canonical(node.get(k)))); return out;
        }
        if (node.isArray()) { var out = JSON.createArrayNode(); node.forEach(v -> out.add(canonical(v))); return out; }
        return node;
    }
    public static Set<String> months(JsonNode node) throws IOException {
        require(node.isArray() && !node.isEmpty() && node.size() <= 24, "BOUNDED_EXACT_MONTHS_REQUIRED");
        var out = new TreeSet<String>();
        for (var value : node) {
            require(value.isTextual() && value.asText().matches("[12][0-9]{3}-(0[1-9]|1[0-2])"), "UTC_MONTH_REQUIRED");
            require(out.add(value.asText()), "DUPLICATE_MONTH"); YearMonth.parse(value.asText());
        }
        return out;
    }
    public static Instant start(String month) { return YearMonth.parse(month).atDay(1).atStartOfDay(ZoneOffset.UTC).toInstant(); }
    public static Instant end(String month) { return YearMonth.parse(month).plusMonths(1).atDay(1).atStartOfDay(ZoneOffset.UTC).toInstant(); }
    public static String month(Instant time) { return YearMonth.from(time.atZone(ZoneOffset.UTC)).toString(); }
    public static Map<String, Long> snapshots(JsonNode node) throws IOException {
        require(node.isObject() && node.size() == GoldIcebergWriter.TABLES.size(), "SIX_BASELINE_SNAPSHOTS_REQUIRED");
        var out = new TreeMap<String, Long>();
        for (String table : GoldIcebergWriter.TABLES) {
            var value = node.path(table);
            require(value.isIntegralNumber() && value.canConvertToLong() && value.asLong() >= 0, "TYPED_SNAPSHOT_REQUIRED");
            out.put(table, value.asLong());
        }
        return out;
    }
    public static String key(String uri, java.util.function.Function<String,String> uriForKey) throws IOException {
        URI parsed = URI.create(uri);
        require(Set.of("s3", "file").contains(parsed.getScheme()) && parsed.getUserInfo() == null
                && parsed.getQuery() == null && parsed.getFragment() == null, "SAFE_STORE_URI_REQUIRED");
        String key;
        if("s3".equals(parsed.getScheme())) {
            require(parsed.getHost()!=null && parsed.getRawPath()!=null && parsed.getRawPath().startsWith("/"),"EXACT_STORE_REQUIRED");
            // Bronze adapter validates full keys under its configured prefix; do not probe
            // a made-up key outside that prefix or strip the real bronze/ prefix.
            key=parsed.getRawPath().substring(1);
        } else {
            String probe = uriForKey.apply("orc06-probe"); String prefix = probe.substring(0, probe.length() - "orc06-probe".length());
            require(uri.startsWith(prefix), "EXACT_STORE_REQUIRED"); key=uri.substring(prefix.length());
        }
        require(key.matches("[A-Za-z0-9][A-Za-z0-9_./=~-]*") && !key.contains("..") && !key.contains("//")
                && uriForKey.apply(key).equals(uri), "SAFE_EXACT_KEY_REQUIRED");
        return key;
    }
    public static void validate(JsonNode request) throws IOException {
        require(request.isObject() && VERSION.equals(request.path("resolver_version").asText()), "RESOLVER_VERSION_REQUIRED");
        var allowed = Set.of("resolver_version", "run_id", "dag_id", "processed_at_utc", "source_cutoff_utc",
                "target_months", "namespace", "config_version", "code_version", "baseline_snapshots", "baseline", "bronze_inputs");
        var fields = request.fieldNames(); while (fields.hasNext()) require(allowed.contains(fields.next()), "UNKNOWN_REQUEST_FIELD");
        SilverBundlePublisher.root(text(request, "run_id"));
        require(text(request,"dag_id").matches("[A-Za-z0-9_-]{1,128}"), "SAFE_DAG_REQUIRED");
        require(text(request,"namespace").matches("[a-z][a-z0-9_]*\\.[a-z][a-z0-9_]*"), "EXACT_GOLD_NAMESPACE_REQUIRED");
        for (String k : List.of("code_version", "config_version")) require(text(request,k).matches("[A-Za-z0-9._-]{1,128}"), "VERSION_REQUIRED");
        Instant processed = utc(request,"processed_at_utc"), cutoff = utc(request,"source_cutoff_utc");
        require(!cutoff.isAfter(processed) && !processed.isAfter(Instant.now().plusSeconds(300)), "SOURCE_CUTOFF_REQUIRED");
        for (String month : months(request.path("target_months"))) require(!end(month).isAfter(cutoff), "CLOSED_MONTH_REQUIRED");
        snapshots(request.path("baseline_snapshots"));
        require(request.has("baseline") && (request.path("baseline").isNull() || request.path("baseline").isObject()), "EXPLICIT_BASELINE_REQUIRED");
        if(!request.path("baseline").isNull()) {
            var baseline=request.path("baseline"); var keys=new HashSet<String>(); baseline.fieldNames().forEachRemaining(keys::add);
            boolean committed=snapshots(request.path("baseline_snapshots")).values().stream().anyMatch(id->id!=0);
            require(keys.equals(committed?Set.of("run_id","registry_sha256","commit_key","commit_sha256"):Set.of("run_id","registry_sha256")),"EXACT_BASELINE_FIELDS_REQUIRED");
            SilverBundlePublisher.root(text(baseline,"run_id"));
            require(text(baseline,"registry_sha256").matches("[a-f0-9]{64}"),"REGISTRY_SHA_REQUIRED");
            if(committed) {
                require(text(baseline,"commit_sha256").matches("[a-f0-9]{64}"),"COMMIT_SHA_REQUIRED");
                require(text(baseline,"commit_key").matches("gold-commits/[A-Za-z0-9_-]{1,128}/commit\\.json"),"EXACT_GOLD_COMMIT_KEY_REQUIRED");
            }
        }
        require(request.path("bronze_inputs").isArray() && request.path("bronze_inputs").size() <= 64, "EXACT_INPUTS_REQUIRED");
        if (request.path("baseline").isNull()) require(!request.path("bronze_inputs").isEmpty(), "BOOTSTRAP_INPUTS_REQUIRED");
    }
    public static Instant utc(JsonNode node, String key) throws IOException {
        String value = text(node,key); require(value.endsWith("Z"), "UTC_Z_REQUIRED"); return Instant.parse(value);
    }
}
