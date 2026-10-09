package ie212.earthquake.spark.ml;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/** Metadata-only preparation for MLD-01; this class never resolves current or writes Iceberg. */
public final class DatasetIdentityPlan {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String[] VERSIONS = {"dataset_config_version", "audit_rule_version",
        "mc_method_version", "window_model_version", "feature_version", "scaling_config_version", "code_version"};
    private final String payload;
    private final String datasetId;
    private final ObjectNode manifest;

    public record SnapshotFixture(long snapshotId, String publicationId, String status) {
        public SnapshotFixture {
            if (snapshotId <= 0 || publicationId == null || publicationId.isBlank()
                    || !"Published".equals(status)) throw new IllegalArgumentException("DS_GOLD_NOT_PUBLISHED");
        }
    }

    public DatasetIdentityPlan(SnapshotFixture snapshot, String split, Instant start, Instant end,
            Instant cutoff, JsonNode filters, Map<String, String> versions, String buildRunId, Instant created) {
        String expectedStart, expectedEnd;
        if ("REPRODUCTION".equals(split)) {
            expectedStart = "2000-01-01T00:00:00Z"; expectedEnd = "2018-10-01T00:00:00Z";
        } else if ("EXTENSION".equals(split)) {
            expectedStart = "2018-10-01T00:00:00Z"; expectedEnd = "2024-01-01T00:00:00Z";
        } else throw new IllegalArgumentException("DS_INVALID_PERIOD");
        if (!Instant.parse(expectedStart).equals(start) || !Instant.parse(expectedEnd).equals(end))
            throw new IllegalArgumentException("DS_INVALID_PERIOD");
        if (snapshot == null || cutoff == null || created == null || buildRunId == null || buildRunId.isBlank()
                || filters == null || !filters.isObject() || filters.isEmpty() || versions == null)
            throw new IllegalArgumentException("DS_INVALID_METADATA");
        JsonNode canonicalFilters = canonical(filters);
        ObjectNode identity = JSON.createObjectNode();
        identity.put("gold_table_name", "gold.earthquake_event_current");
        identity.put("gold_snapshot_id", snapshot.snapshotId());
        identity.put("dataset_split", split);
        identity.put("period_start_utc", start.toString());
        identity.put("period_end_utc_exclusive", end.toString());
        identity.put("filter_config_json", canonicalFilters.toString());
        // Cutoff changes source eligibility; include it to prevent accidental identity reuse.
        identity.put("observation_cutoff_utc", cutoff.toString());
        for (String version : VERSIONS) {
            String value = versions.get(version);
            if (value == null || value.isBlank()) throw new IllegalArgumentException("DS_INVALID_VERSION:" + version);
            identity.put(version, value);
        }
        payload = canonical(identity).toString();
        datasetId = "ds_" + sha256(payload);
        manifest = identity.deepCopy();
        manifest.put("schema_version", "1.0");
        manifest.put("dataset_id", datasetId);
        manifest.put("gold_publication_id", snapshot.publicationId());
        manifest.put("dataset_status", "BUILDING");
        manifest.put("build_run_id", buildRunId);
        manifest.put("created_at_utc", created.toString());
        manifest.put("updated_at_utc", created.toString());
    }

    private static JsonNode canonical(JsonNode value) {
        if (value.isObject()) {
            TreeMap<String, JsonNode> sorted = new TreeMap<>();
            value.fields().forEachRemaining(entry -> sorted.put(entry.getKey(), canonical(entry.getValue())));
            ObjectNode result = JSON.createObjectNode();
            sorted.forEach(result::set);
            return result;
        }
        if (value.isArray()) {
            var result = JSON.createArrayNode();
            value.forEach(child -> result.add(canonical(child)));
            return result;
        }
        if (value.isFloatingPointNumber() && !Double.isFinite(value.doubleValue()))
            throw new IllegalArgumentException("DS_NON_FINITE_CONFIG");
        if (value.isTextual() && (value.textValue().matches("(?i).*(?:[a-z]:[\\\\/]|/Users/|/home/|://[^/]+@).*")))
            throw new IllegalArgumentException("DS_UNSAFE_CONFIG_PATH");
        return value.deepCopy();
    }

    private static String sha256(String text) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8))); }
        catch (java.security.NoSuchAlgorithmException error) { throw new IllegalStateException(error); }
    }

    public String datasetId() { return datasetId; }
    public String canonicalPayload() { return payload; }
    public JsonNode buildingManifestFixture() { return manifest.deepCopy(); }
    public boolean canMaterializePublishedDataset() { return false; }
    public void assertRerun(String existingId, String existingPayload) {
        if (!datasetId.equals(existingId) || !payload.equals(existingPayload))
            throw new IllegalArgumentException("DS_IDENTITY_CONFLICT");
    }
}
