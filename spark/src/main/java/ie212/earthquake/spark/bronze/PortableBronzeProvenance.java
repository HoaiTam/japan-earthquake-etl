package ie212.earthquake.spark.bronze;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.jma.JmaBronzeWriter;
import ie212.earthquake.spark.usgs.*;
import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.util.*;

/** Exact original-to-physical provenance mapping; read-only and never restores original keys. */
public final class PortableBronzeProvenance {
    private static final ObjectMapper JSON = new ObjectMapper().enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    private record Physical(JsonNode manifest, JsonNode pin) { }
    private PortableBronzeProvenance() { }
    private static void require(boolean ok, String reason) throws IOException { if (!ok) throw new IOException(reason); }
    private static String key(BronzeObjectStore store, String value) throws IOException {
        URI uri = URI.create(value);
        require("s3".equals(uri.getScheme()) && uri.getHost() != null && uri.getUserInfo() == null
                && uri.getQuery() == null && uri.getFragment() == null
                && uri.getPath().matches("/bronze/[A-Za-z0-9=._~/-]+")
                && !uri.getPath().contains("..") && !uri.getPath().contains("//"), "EXACT_BRONZE_URI_REQUIRED");
        String k = uri.getPath().substring(1);
        require(store.uriForKey(k).equals(value), "BRONZE_BUCKET_MISMATCH"); return k;
    }
    static ObjectNode identity(JsonNode manifest) throws IOException {
        require("1.0".equals(manifest.path("manifest_version").asText())
                && "BronzeReady".equals(manifest.path("bronze_status").asText())
                && manifest.path("manifest_id").isTextual() && !manifest.path("manifest_id").asText().isBlank(), "BRONZE_READY_REQUIRED");
        String source = manifest.path("source_system").asText(), kind = manifest.path("source_kind").asText();
        require((source.equals("JMA_BULLETIN") && kind.equals("annual_archive"))
                || (source.equals("USGS") && kind.equals("event_api")), "SOURCE_KIND_MISMATCH");
        for (String flag : List.of("object_write_completed", "raw_readback_verified", "checksum_verified", "source_structure_valid", "manifest_consistent"))
            require(manifest.path("validation").path(flag).isBoolean() && manifest.path("validation").path(flag).asBoolean(), "BRONZE_VALIDATION_REQUIRED");
        require(manifest.path("sha256").asText().matches("[a-f0-9]{64}")
                && manifest.path("content_length_bytes").isIntegralNumber() && manifest.path("content_length_bytes").canConvertToLong()
                && manifest.path("content_length_bytes").asLong() > 0 && manifest.path("content_length_bytes").asLong() <= 256L * 1024 * 1024
                && manifest.path("record_count_estimate").isIntegralNumber() && manifest.path("record_count_estimate").canConvertToLong()
                && manifest.path("record_count_estimate").asLong() >= 0
                && manifest.path("data_interval").isObject() && manifest.path("request").isObject(), "TYPED_SOURCE_IDENTITY_REQUIRED");
        var request = manifest.path("request"); URI endpoint = URI.create(request.path("url").asText());
        require("GET".equals(request.path("method").asText()) && "https".equals(endpoint.getScheme())
                && endpoint.getHost() != null && endpoint.getUserInfo() == null && endpoint.getFragment() == null, "SOURCE_REQUEST_IDENTITY_REQUIRED");
        var id = JSON.createObjectNode();
        for (String field : List.of("source_system", "source_kind", "catalog_release", "sha256", "content_length_bytes",
                "media_type", "content_encoding", "record_count_estimate", "data_interval")) {
            require(manifest.has(field), "SOURCE_IDENTITY_FIELD_MISSING:" + field); id.set(field, manifest.get(field));
        }
        if (source.equals("JMA_BULLETIN")) {
            JsonNode provenance = manifest.path("provenance"); var logical = id.putObject("provenance");
            for (String field : List.of("dataset_title", "inventory_version", "archive_name", "member_name", "record_format",
                    "record_length_bytes", "catalog_era", "geodetic_datum", "index_url", "final_url", "citation_version",
                    "terms_url", "update_url", "errata_url")) {
                require(provenance.hasNonNull(field), "JMA_SOURCE_PROVENANCE_REQUIRED:" + field); logical.set(field, provenance.get(field));
            }
            // citation_text embeds retrieval time. Original bytes retain that citation;
            // it is not a logical archive identity and must not be rewritten.
        } else {
            // USGS Bronze contract has no required provenance object.
            id.set("provenance", manifest.has("provenance") ? manifest.get("provenance") : JSON.nullNode());
        }
        var logicalRequest = id.putObject("request");
        // Retrieval timeout/attempt/timestamp are not source identity, but request bounds are.
        for (String field : List.of("method", "url", "path", "query"))
            if (request.has(field)) logicalRequest.set(field, request.get(field));
        return id;
    }
    public static ObjectNode verify(JsonNode plan, String planSha, BronzeObjectStore store) throws Exception {
        require("dat03-bronze-relocation-plan-v1".equals(plan.path("version").asText())
                && planSha.matches("[a-f0-9]{64}"), "PINNED_RELOCATION_PLAN_REQUIRED");
        JsonNode pins = plan.path("physical_bronze_inputs"), originals = plan.path("original_sources");
        require(pins.isArray() && pins.size() > 0 && pins.size() <= 64 && originals.isArray()
                && originals.size() > 0 && originals.size() <= 128, "RELOCATION_SOURCE_BOUND");
        var request = JSON.createObjectNode().put("contract_version", "orc-03-v1").put("scope_sha256", planSha);
        request.set("bronze_inputs", pins);
        // Shared verifier re-reads raw bytes, checks SHA/size/structure and source windows/releases.
        BronzeReuseVerifier.verify(request, store);
        Map<JsonNode, Physical> physical = new HashMap<>();
        for (JsonNode pin : pins) {
            byte[] raw = store.read(key(store, pin.path("manifest_uri").asText()));
            require(raw.length <= 65536 && JmaBronzeWriter.sha256(raw).equals(pin.path("manifest_sha256").asText()), "PHYSICAL_MANIFEST_CHANGED");
            JsonNode manifest = JSON.readTree(raw), id = identity(manifest);
            require(physical.put(id, new Physical(manifest, pin)) == null, "AMBIGUOUS_PHYSICAL_SOURCE_IDENTITY");
        }
        var result = JSON.createObjectNode().put("version", "dat03-bronze-relocation-receipt-v1")
                .put("plan_sha256", planSha).put("physical_source_count", pins.size());
        var mappings = result.putArray("mappings"); Map<String, Set<JsonNode>> coverage = new TreeMap<>();
        Set<String> uris = new HashSet<>();
        for (JsonNode original : originals) {
            String owner = original.path("owner").asText(), uri = original.path("manifest_uri").asText(); key(store, uri);
            require(Set.of("ThanhTris", "rosy179").contains(owner) && uri.endsWith("/manifest.json")
                    && uris.add(uri), "DISTINCT_WORKER_SOURCE_REQUIRED");
            byte[] raw = Base64.getDecoder().decode(original.path("manifest_base64").asText());
            require(raw.length > 0 && raw.length <= 65536
                    && JmaBronzeWriter.sha256(raw).equals(original.path("manifest_sha256").asText()), "ORIGINAL_MANIFEST_PIN_MISMATCH");
            JsonNode manifest = JSON.readTree(raw), id = identity(manifest); Physical match = physical.get(id);
            require(match != null && coverage.computeIfAbsent(owner, ignored -> new HashSet<>()).add(id), "NO_UNIQUE_PHYSICAL_SOURCE_MATCH");
            JsonNode mapped = match.manifest(), pin = match.pin();
            String rawKey = key(store, manifest.path("raw_object_uri").asText());
            require(rawKey.equals(manifest.path("raw_object_key").asText())
                    && uri.substring(0, uri.lastIndexOf('/') + 1).equals(manifest.path("raw_object_uri").asText()
                        .substring(0, manifest.path("raw_object_uri").asText().lastIndexOf('/') + 1)), "ORIGINAL_SOURCE_LAYOUT_MISMATCH");
            var row = mappings.addObject().put("owner", owner).put("original_manifest_uri", uri)
                    .put("original_manifest_sha256", original.path("manifest_sha256").asText())
                    .put("original_manifest_id", manifest.path("manifest_id").asText())
                    .put("original_raw_object_uri", manifest.path("raw_object_uri").asText())
                    .put("physical_manifest_uri", pin.path("manifest_uri").asText())
                    .put("physical_manifest_sha256", pin.path("manifest_sha256").asText())
                    .put("physical_manifest_id", mapped.path("manifest_id").asText())
                    .put("physical_raw_object_uri", mapped.path("raw_object_uri").asText())
                    .put("source_system", mapped.path("source_system").asText())
                    .put("sha256", mapped.path("sha256").asText()).put("byte_size", mapped.path("content_length_bytes").asLong());
            row.set("catalog_release", mapped.path("catalog_release"));
            row.put("logical_source_identity_matched", true).put("physical_raw_readback_verified", true)
                    .put("original_s3_key_verified", false);
        }
        require(coverage.values().stream().allMatch(s -> s.equals(physical.keySet())), "COMPLETE_PHYSICAL_COVERAGE_REQUIRED");
        result.put("original_source_count", originals.size()).put("worker_count", coverage.size())
                .put("status", "ORIGINAL_TO_PHYSICAL_MAPPING_VERIFIED").put("lake_writes", false)
                .put("portable_import_ready", false).put("gold_published", false).put("official_export_ready", false);
        result.put("identity_policy", "source-kind-window-request-release-format-content-v1");
        return result;
    }
    public static void main(String[] args) throws Exception {
        require(args.length == 3, "USAGE_PLAN_SHA_NEW_RECEIPT"); Path input = Path.of(args[0]), output = Path.of(args[2]);
        require(!Files.isSymbolicLink(input) && Files.size(input) <= 4 * 1024 * 1024
                && !Files.exists(output, LinkOption.NOFOLLOW_LINKS), "BOUNDED_NEW_PATHS_REQUIRED");
        byte[] raw = Files.readAllBytes(input); require(JmaBronzeWriter.sha256(raw).equals(args[1]), "PLAN_PIN_MISMATCH");
        try (var store = MinioBronzeObjectStore.fromEnvironment()) {
            var receipt = verify(JSON.readTree(raw), args[1], store);
            Files.write(output, JSON.writerWithDefaultPrettyPrinter().writeValueAsBytes(receipt), StandardOpenOption.CREATE_NEW);
            System.out.println("event=portable_bronze_mapping_verified physical_sources=" + receipt.path("physical_source_count").asInt()
                    + " original_sources=" + receipt.path("original_source_count").asInt() + " lake_writes=false");
        }
    }
}
