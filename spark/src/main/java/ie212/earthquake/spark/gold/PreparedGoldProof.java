package ie212.earthquake.spark.gold;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.util.List;

/** Explicit portable input proof, never a fabricated legacy Silver bundle or FullMonthReady marker. */
public record PreparedGoldProof(String namespace, String catalogSha256, String requestSha256,
        String coverageSha256, String provenanceSha256, String sourceCutoffUtc) {
    public static final String VERSION = "dat03-prepared-gold-v1";
    public static final String NAMESPACE = "iceberg.dat03_unified_20261114";
    // Actual earlier Trino 16/16 bucket capacity proof, not the larger Spark fingerprint stress size.
    public static final long MAX_FACT_ROWS = 4_527_126;
    public PreparedGoldProof {
        if (!NAMESPACE.equals(namespace)) throw new IllegalArgumentException("APPROVED_PREPARED_NAMESPACE_REQUIRED");
        for (String value : new String[]{catalogSha256, requestSha256, coverageSha256, provenanceSha256})
            if (value == null || !value.matches("[a-f0-9]{64}")) throw new IllegalArgumentException("PINNED_PREPARED_PROOF_REQUIRED");
        Instant.parse(sourceCutoffUtc);
    }
    public ObjectNode json() {
        return PortablePreparedCatalog.JSON.createObjectNode().put("version", VERSION)
                .put("input_contract", PortableImportContract.PREPARED_VERSION).put("namespace", namespace)
                .put("prepared_catalog_sha256", catalogSha256).put("prepared_request_sha256", requestSha256)
                .put("coverage_sha256", coverageSha256).put("provenance_sha256", provenanceSha256)
                .put("source_cutoff_utc", sourceCutoffUtc);
    }
    public static PreparedGoldProof from(JsonNode value) {
        if (!VERSION.equals(value.path("version").asText())
                || !PortableImportContract.PREPARED_VERSION.equals(value.path("input_contract").asText()))
            throw new IllegalArgumentException("PREPARED_PROOF_VERSION_REQUIRED");
        return new PreparedGoldProof(value.path("namespace").asText(), value.path("prepared_catalog_sha256").asText(),
                value.path("prepared_request_sha256").asText(), value.path("coverage_sha256").asText(),
                value.path("provenance_sha256").asText(), value.path("source_cutoff_utc").asText());
    }
    public static void validateCommit(JsonNode commit) {
        var scope = commit.path("scope"); var proof = from(scope.path("prepared_input"));
        if (!proof.namespace().equals(commit.path("namespace").asText()) || scope.has("silver_bundle_sha256")
                || !VERSION.equals(scope.path("code_version").asText())
                || !scope.path("affected_months").equals(PortablePreparedCatalog.JSON.valueToTree(PortableImportContract.ALL_MONTHS)))
            throw new IllegalArgumentException("EXACT_PREPARED_COMMIT_SCOPE_REQUIRED");
        for (String name : GoldIcebergWriter.TABLES) {
            var base = scope.path("baseline_snapshots").path(name);
            if (!base.isIntegralNumber() || !base.canConvertToLong() || base.asLong(-1) != 0) throw new IllegalArgumentException("PREPARED_EMPTY_NAMESPACE_BASELINE_REQUIRED");
            var count = commit.path("tables").path(name).path("count");
            if (!count.isIntegralNumber() || !count.canConvertToLong() || count.asLong() < 0
                    || count.asLong() > MAX_FACT_ROWS) throw new IllegalArgumentException("PREPARED_MEASURED_CAPACITY_EXCEEDED");
        }
    }
}
