package ie212.earthquake.spark.bronze;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.jma.JmaArchiveValidator;
import ie212.earthquake.spark.jma.JmaBronzeWriter;
import ie212.earthquake.spark.usgs.BronzeObjectStore;
import ie212.earthquake.spark.usgs.MinioBronzeObjectStore;
import ie212.earthquake.spark.usgs.UsgsGeoJsonValidator;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;

/** ORC-03 exact-key readback only. Never downloads a source or writes any lake object. */
public final class BronzeReuseVerifier {
    private static final ObjectMapper JSON = new ObjectMapper()
            .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    private static final List<String> FLAGS = List.of("object_write_completed", "raw_readback_verified",
            "checksum_verified", "source_structure_valid", "manifest_consistent");

    private BronzeReuseVerifier() { }

    public static void main(String[] args) {
        try {
            require(args.length == 2 && "--context-file".equals(args[0]));
            byte[] bytes = Files.readAllBytes(Path.of(args[1]));
            require(bytes.length <= 65536);
            try (var store = MinioBronzeObjectStore.fromEnvironment()) {
                System.out.println(JSON.writeValueAsString(verify(JSON.readTree(bytes), store)));
            }
        } catch (Exception exception) {
            System.err.println("event=bronze_reuse_failed reason=READBACK_OR_PIN_MISMATCH");
            System.exit(2);
        }
    }

    public static ObjectNode verify(JsonNode request, BronzeObjectStore store) throws IOException {
        require("orc-03-v1".equals(text(request, "contract_version"))
                && text(request, "scope_sha256").matches("[0-9a-f]{64}"));
        JsonNode inputs = request.path("bronze_inputs");
        require(inputs.isArray() && !inputs.isEmpty() && inputs.size() <= 64);
        var seen = new HashSet<String>();
        for (JsonNode pin : inputs) {
            String uri = text(pin, "manifest_uri"), source = text(pin, "source_system");
            require(seen.add(uri) && uri.endsWith("/manifest.json"));
            String key = key(store, uri);
            byte[] manifestBytes = store.read(key);
            require(manifestBytes.length <= 65536
                    && JmaBronzeWriter.sha256(manifestBytes).equals(text(pin, "manifest_sha256")));
            JsonNode manifest = JSON.readTree(manifestBytes);
            require("1.0".equals(text(manifest, "manifest_version"))
                    && "BronzeReady".equals(text(manifest, "bronze_status"))
                    && source.equals(text(manifest, "source_system"))
                    && text(pin, "sha256").matches("[0-9a-f]{64}")
                    && text(pin, "sha256").equals(text(manifest, "sha256")));
            for (String flag : FLAGS) {
                require(manifest.path("validation").path(flag).isBoolean()
                        && manifest.path("validation").path(flag).asBoolean());
            }
            String rawKey = key(store, text(manifest, "raw_object_uri"));
            require(rawKey.equals(text(manifest, "raw_object_key"))
                    && rawKey.substring(0, rawKey.lastIndexOf('/') + 1)
                        .equals(key.substring(0, key.lastIndexOf('/') + 1)));
            byte[] raw = store.read(rawKey);
            require(raw.length == number(manifest, "content_length_bytes")
                    && JmaBronzeWriter.sha256(raw).equals(text(pin, "sha256")));
            JsonNode interval = manifest.path("data_interval");
            require("[start,end)".equals(text(interval, "interval_semantics")));
            long count;
            if ("USGS".equals(source)) {
                require(rawKey.contains("/usgs/") && rawKey.endsWith("/response.geojson")
                        && Instant.parse(text(pin, "window_start_utc"))
                            .equals(Instant.parse(text(interval, "window_start_utc")))
                        && Instant.parse(text(pin, "window_end_utc"))
                            .equals(Instant.parse(text(interval, "window_end_utc"))));
                var validation = new UsgsGeoJsonValidator().validate(raw);
                require(validation.valid()); count = validation.featureCount();
            } else {
                require("JMA_BULLETIN".equals(source) && rawKey.endsWith("/archive.zip")
                        && number(pin, "year") == number(interval, "year")
                        && text(pin, "segment").equals(text(interval, "segment"))
                        && text(pin, "catalog_release").equals(text(manifest, "catalog_release"))
                        && rawKey.contains("/jma/year=" + number(pin, "year") + "/catalog_release="
                            + text(pin, "catalog_release") + "/")
                        && "Asia/Tokyo".equals(text(interval, "native_timezone")));
                var validation = new JmaArchiveValidator().validate(raw,
                        text(manifest.path("provenance"), "member_name"));
                require(validation.valid()); count = validation.recordCount();
            }
            require(count == number(manifest, "record_count_estimate"));
        }
        ObjectNode result = JSON.createObjectNode();
        result.put("contract_version", "orc-03-v1");
        result.put("scope_sha256", text(request, "scope_sha256"));
        result.put("status", "BronzeVerified"); result.put("verified", true);
        result.set("bronze_inputs", inputs.deepCopy());
        return result;
    }

    private static String key(BronzeObjectStore store, String value) throws IOException {
        URI uri = URI.create(value);
        require("s3".equals(uri.getScheme()) && uri.getHost() != null && uri.getUserInfo() == null
                && uri.getQuery() == null && uri.getFragment() == null
                && uri.getPath().matches("/[A-Za-z0-9=._~/-]+") && !uri.getPath().contains("..")
                && !uri.getPath().contains("//"));
        String key = uri.getPath().substring(1);
        require(store.uriForKey(key).equals(value)); return key;
    }

    private static String text(JsonNode node, String key) throws IOException {
        require(node.path(key).isTextual() && !node.path(key).asText().isBlank());
        return node.path(key).asText();
    }

    private static long number(JsonNode node, String key) throws IOException {
        require(node.path(key).isIntegralNumber() && node.path(key).canConvertToLong() && node.path(key).asLong() >= 0);
        return node.path(key).asLong();
    }

    private static void require(boolean condition) throws IOException {
        if (!condition) { throw new IOException("BRONZE_READBACK_OR_PIN_MISMATCH"); }
    }
}
