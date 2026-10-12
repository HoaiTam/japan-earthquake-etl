package ie212.earthquake.spark.ml;

import com.fasterxml.jackson.databind.*;
import ie212.earthquake.spark.gold.*;
import java.time.Instant;

/** Shared exact persisted BUILDING manifest + PASSED publication + current cutoff gate. */
public final class PinnedAuditResolver {
    private static final ObjectMapper JSON = new ObjectMapper();
    private PinnedAuditResolver() { }
    public static JsonNode resolve(String namespace, String id, String run, GoldPublisher.Sql sql, Runnable lease) throws Exception {
        if (!namespace.matches("[a-z][a-z0-9_]*\\.[a-z][a-z0-9_]*") || !id.matches("ds_[a-f0-9]{64}")
                || !run.matches("[A-Za-z0-9][A-Za-z0-9_-]{0,127}")) throw new IllegalArgumentException("ML_EXACT_DATASET_AND_RUN_REQUIRED");
        lease.run();
        var rows = DatasetManifestWriter.records(sql.execute("SELECT * FROM " + namespace + ".dataset_manifest WHERE dataset_id=" + GoldVerificationPlan.quote(id)));
        if (rows.size() != 1 || !"BUILDING".equals(rows.get(0).path("dataset_status").asText()))
            throw new IllegalArgumentException("ML_EXACT_BUILDING_MANIFEST_REQUIRED");
        var manifest = rows.get(0); var config = JSON.createObjectNode();
        for (String key : java.util.List.of("dataset_split", "period_start_utc", "period_end_utc_exclusive", "observation_cutoff_utc")) config.set(key, manifest.path(key));
        DatasetIdentityPlan.VERSIONS.forEach(key -> config.set(key, manifest.path(key)));
        config.set("filter_config", JSON.readTree(manifest.path("filter_config_json").asText()));
        DatasetManifestWriter.pinnedGoldSql(manifest);
        String physical = manifest.path("physical_gold_table_name").asText();
        String gold = physical.substring(0, physical.lastIndexOf('.'));
        var pinned = new DatasetManifestWriter(sql, lease).build(gold, manifest.path("gold_publication_id").asText(), namespace, config, run, Instant.now());
        if (!manifest.path("dataset_id").equals(pinned.path("dataset_id"))) throw new IllegalArgumentException("ML_MANIFEST_IDENTITY_CONFLICT");
        var publications = DatasetManifestWriter.records(sql.execute("SELECT published_at_utc FROM " + gold + ".publication_status WHERE publication_id="
            + GoldVerificationPlan.quote(manifest.path("gold_publication_id").asText())));
        if (publications.size() != 1 || Instant.parse(manifest.path("observation_cutoff_utc").asText())
                .isBefore(Instant.parse(publications.get(0).path("published_at_utc").asText())))
            throw new IllegalArgumentException("ML_HISTORICAL_CUTOFF_UNPROVEN");
        return manifest;
    }
}
