package ie212.earthquake.spark.ml;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.*;

/** Immutable identity. Publication and physical table are included to disambiguate catalogs. */
public final class DatasetIdentityPlan {
    private static final ObjectMapper JSON = new ObjectMapper();
    public static final List<String> VERSIONS = List.of("dataset_config_version", "audit_rule_version",
        "mc_method_version", "window_model_version", "feature_version", "scaling_config_version", "code_version");
    private final ObjectNode identity;
    private final ObjectNode manifest;

    public DatasetIdentityPlan(JsonNode publication, JsonNode config, String run, Instant now) {
        if(publication!=null) {
            ie212.earthquake.spark.gold.OperationalGoldPolicy.researchOnly(publication.path("physical_gold_table_name").asText());
            if(publication.has("operational_policy") || "PROVISIONAL".equals(publication.path("publication_quality").asText()))
                throw new IllegalArgumentException("PROVISIONAL_GOLD_ML_FORBIDDEN");
        }
        if (publication == null || !"PASSED".equals(publication.path("verify_status").asText())
                || !publication.path("gold_snapshot_id").isIntegralNumber()
                || publication.path("gold_snapshot_id").asLong() <= 0)
            throw new IllegalArgumentException("DS_GOLD_NOT_PUBLISHED");
        if (config == null || !config.isObject() || run == null || !run.matches("[A-Za-z0-9][A-Za-z0-9_-]{0,127}") || now == null)
            throw new IllegalArgumentException("DS_INVALID_METADATA");
        String split = config.path("dataset_split").asText();
        String start, end;
        switch (split) {
            case "REPRODUCTION" -> { start = "2000-01-01T00:00:00Z"; end = "2018-10-01T00:00:00Z"; }
            case "EXTENSION" -> { start = "2018-10-01T00:00:00Z"; end = "2024-01-01T00:00:00Z"; }
            default -> throw new IllegalArgumentException("DS_INVALID_PERIOD");
        }
        if (!Instant.parse(start).equals(instant(config, "period_start_utc"))
                || !Instant.parse(end).equals(instant(config, "period_end_utc_exclusive")))
            throw new IllegalArgumentException("DS_INVALID_PERIOD");
        Instant cutoff = instant(config, "observation_cutoff_utc");
        if (cutoff.isBefore(Instant.parse(end))) throw new IllegalArgumentException("DS_CUTOFF_BEFORE_PERIOD_END");
        JsonNode filters = config.path("filter_config");
        if (!filters.isObject() || !filters.path("is_natural_earthquake").asBoolean(false)
                || !filters.path("is_in_study_area").asBoolean(false)
                || !"JMA_BULLETIN".equals(filters.path("canonical_source_system").asText())
                || !"UNIFIED".equals(filters.path("catalog_era").asText()))
            throw new IllegalArgumentException("DS_BASELINE_FILTER_REQUIRED");
        identity = JSON.createObjectNode();
        for (String key : List.of("gold_table_name", "physical_gold_table_name", "gold_publication_id", "publication_bundle_sha256")) {
            String value = publication.path(key).asText();
            if (value.isBlank()) throw new IllegalArgumentException("DS_INVALID_PUBLICATION");
            identity.put(key, value);
        }
        identity.put("gold_snapshot_id", publication.path("gold_snapshot_id").asLong());
        identity.put("dataset_split", split); identity.put("period_start_utc", start);
        identity.put("period_end_utc_exclusive", end); identity.put("observation_cutoff_utc", cutoff.toString());
        identity.put("filter_config_json", canonical(filters).toString());
        for (String key : VERSIONS) {
            String value = config.path(key).asText();
            if (!value.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}")) throw new IllegalArgumentException("DS_INVALID_VERSION:" + key);
            identity.put(key, value);
        }
        manifest = identity.deepCopy(); manifest.put("schema_version", "1.0");
        manifest.put("identity_payload_sha256", sha(canonicalPayload()));
        manifest.put("dataset_id", "ds_" + sha(canonicalPayload()));
        manifest.put("dataset_status", "BUILDING"); manifest.put("build_run_id", run);
        manifest.put("created_at_utc", now.toString()); manifest.put("updated_at_utc", now.toString());
    }
    private static Instant instant(JsonNode config, String key) {
        try { return Instant.parse(config.path(key).asText()); }
        catch (Exception e) { throw new IllegalArgumentException("DS_INVALID_UTC:" + key); }
    }
    public static JsonNode canonical(JsonNode value) {
        if (value.isObject()) {
            var sorted = new TreeMap<String, JsonNode>();
            value.fields().forEachRemaining(e -> {
                if (e.getKey().matches("(?i).*(password|credential|secret|token|access_key|private_key).*"))
                    throw new IllegalArgumentException("DS_UNSAFE_CONFIG_KEY");
                sorted.put(e.getKey(), canonical(e.getValue()));
            });
            var out = JSON.createObjectNode(); sorted.forEach(out::set); return out;
        }
        if (value.isArray()) { var out = JSON.createArrayNode(); value.forEach(v -> out.add(canonical(v))); return out; }
        if (value.isFloatingPointNumber() && !Double.isFinite(value.doubleValue()))
            throw new IllegalArgumentException("DS_NON_FINITE_CONFIG");
        if (value.isTextual() && value.textValue().matches("(?i).*(?:[a-z]:[\\\\/]|/Users/|/home/|://[^/]+@).*"))
            throw new IllegalArgumentException("DS_UNSAFE_CONFIG_PATH");
        return value.deepCopy();
    }
    public static String sha(String value) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8))); }
        catch (Exception e) { throw new IllegalStateException(e); }
    }
    public String canonicalPayload() { return canonical(identity).toString(); }
    public JsonNode manifest() { return manifest.deepCopy(); }
    public void assertRerun(JsonNode existing) {
        for (var fields = identity.fields(); fields.hasNext();) {
            var field = fields.next();
            if (!field.getValue().equals(existing.path(field.getKey()))) throw new IllegalArgumentException("DS_IDENTITY_CONFLICT");
        }
        if (!manifest.path("dataset_id").equals(existing.path("dataset_id"))
                || !manifest.path("identity_payload_sha256").equals(existing.path("identity_payload_sha256")))
            throw new IllegalArgumentException("DS_IDENTITY_CONFLICT");
        if (!Set.of("BUILDING", "VALIDATED", "EXPORTED", "REJECTED").contains(existing.path("dataset_status").asText()))
            throw new IllegalArgumentException("DS_INVALID_LIFECYCLE");
    }
}
