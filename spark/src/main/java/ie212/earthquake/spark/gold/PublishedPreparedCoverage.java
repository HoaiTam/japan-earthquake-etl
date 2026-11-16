package ie212.earthquake.spark.gold;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.ml.GoldAuditConfig;
import ie212.earthquake.spark.silver.*;
import java.nio.file.Path;
import java.time.*;

/** Explicit proof variant: original PREPARED chain + actual Published Gold. Never legacy registry emulation. */
public final class PublishedPreparedCoverage {
    public static final String VERSION = "dat03-published-prepared-coverage-v1";
    private PublishedPreparedCoverage() { }
    private static void require(boolean ok, String reason) throws Exception { PortableImportContract.require(ok, reason); }
    static void binding(JsonNode commit, JsonNode anchor, JsonNode publication, JsonNode manifest) throws Exception {
        PortableGoldPublicationJob.validate(anchor); PreparedGoldProof.validateCommit(commit);
        JsonNode scope = PortablePreparedCatalog.JSON.readTree(PortablePreparedCatalog.JSON.writeValueAsBytes(
                GoldIcebergWriter.scopeJson(PortableGoldPublicationJob.scope(anchor))));
        String run = anchor.path("run_id").asText(), ns = PreparedGoldProof.NAMESPACE;
        require(commit.path("scope").equals(scope) && run.equals(commit.path("gold_run_id").asText())
                && run.equals(publication.path("gold_run_id").asText()) && ns.equals(publication.path("namespace").asText())
                && "PASSED".equals(publication.path("verify_status").asText()) && publication.path("published").asBoolean(false)
                && publication.path("snapshot_bundle").equals(commit.path("tables"))
                && commit.path("identity_sha256").equals(publication.path("identity_sha256"))
                && (ns + ".event_current").equals(manifest.path("physical_gold_table_name").asText())
                && commit.path("tables").path("event_current").path("snapshot_id").equals(manifest.path("gold_snapshot_id"))
                && publication.path("publication_id").equals(manifest.path("gold_publication_id")), "PREPARED_ML_PUBLICATION_BINDING_CHANGED");
        for (String table : GoldIcebergWriter.TABLES) require(commit.path("tables").path(table).path("count").equals(anchor.path("expected_counts").path(table)), "PREPARED_ML_COUNT_CHANGED");
        require(!Instant.parse(anchor.path("prepared_input").path("source_cutoff_utc").asText())
                .isAfter(Instant.parse(manifest.path("observation_cutoff_utc").asText())), "PREPARED_ML_CUTOFF_CONFLICT");
    }
    public static ObjectNode resolve(SilverObjectStore store, JsonNode statusRow, JsonNode manifest) throws Exception {
        GoldAuditConfig.fromManifest(manifest, "dat03-prepared-coverage", "audit-report.json");
        String run = statusRow.path("gold_run_id").asText();
        require(run.matches("dat03-gold-[a-f0-9]{32}"), "PREPARED_ML_EXACT_RUN_REQUIRED");
        byte[] rawCommit = store.read("gold-commits/" + run + "/commit.json");
        JsonNode commit = PortablePreparedCatalog.JSON.readTree(rawCommit);
        String digest = SilverBundlePublisher.sha(rawCommit);
        require(digest.equals(statusRow.path("bundle_sha256").asText())
                && digest.equals(manifest.path("publication_bundle_sha256").asText())
                && ("pub_" + digest).equals(statusRow.path("publication_id").asText()), "PREPARED_ML_COMMIT_PIN_CHANGED");
        byte[] rawAnchor = store.read("dat03-prepared-gold/" + run + "/request.json");
        JsonNode anchor = PortablePreparedCatalog.JSON.readTree(rawAnchor);
        byte[] rawPublication = store.read("gold-verification/" + run + "/publication.json");
        JsonNode publication = PortablePreparedCatalog.JSON.readTree(rawPublication);
        binding(commit, anchor, publication, manifest);
        require(digest.equals(publication.path("bundle_sha256").asText())
                && "PASSED".equals(statusRow.path("verify_status").asText())
                && statusRow.path("snapshot_id").equals(manifest.path("gold_snapshot_id")), "PREPARED_ML_STATUS_ROW_CHANGED");
        JsonNode catalog = PortableGoldPublicationJob.verifyInput(anchor);
        Path root = Path.of(anchor.path("prepared_root").asText());
        JsonNode coverage = PortablePreparedCatalog.read(PortablePreparedCatalog.relative(root, catalog.path("coverage").path("relative_path").asText()), 1024 * 1024);
        YearMonth start = YearMonth.from(Instant.parse(manifest.path("period_start_utc").asText()).atZone(ZoneOffset.UTC));
        YearMonth end = YearMonth.from(Instant.parse(manifest.path("period_end_utc_exclusive").asText()).atZone(ZoneOffset.UTC));
        int count = 0;
        for (JsonNode month : coverage.path("months")) {
            YearMonth m = YearMonth.parse(month.path("month").asText());
            if (!m.isBefore(start) && m.isBefore(end)) {
                require(month.path("split").equals(manifest.path("dataset_split")) && !month.path("jma_physical_manifest_uris").isEmpty(), "PREPARED_ML_SPLIT_COVERAGE_CHANGED"); count++;
            }
        }
        int required = "REPRODUCTION".equals(manifest.path("dataset_split").asText()) ? 225 : 63;
        require(count == required, "PREPARED_ML_FULL_SPLIT_REQUIRED");
        return PortablePreparedCatalog.JSON.createObjectNode().put("coverage_version", VERSION).put("status", "COMPLETE")
                .put("required_month_count", count).put("source_system", "JMA_BULLETIN")
                .put("prepared_catalog_sha256", anchor.path("prepared_catalog_sha256").asText())
                .put("prepared_coverage_sha256", catalog.path("coverage").path("sha256").asText())
                .put("provenance_sha256", catalog.path("provenance").path("sha256").asText())
                .put("published_commit_sha256", digest).put("publication_sha256", SilverBundlePublisher.sha(rawPublication))
                .put("gold_request_sha256", SilverBundlePublisher.sha(rawAnchor))
                .put("source_cutoff_utc", anchor.path("prepared_input").path("source_cutoff_utc").asText());
    }
    public static void validateProof(JsonNode coverage) throws Exception {
        require(VERSION.equals(coverage.path("coverage_version").asText()), "PREPARED_ML_COVERAGE_VERSION_REQUIRED");
        for (String key : java.util.List.of("prepared_catalog_sha256", "prepared_coverage_sha256", "provenance_sha256", "published_commit_sha256", "publication_sha256", "gold_request_sha256"))
            require(coverage.path(key).asText().matches("[a-f0-9]{64}"), "PREPARED_ML_TRUSTED_SHA_REQUIRED");
        require(!coverage.has("registry_sha256"), "PREPARED_COVERAGE_IS_NOT_LEGACY_REGISTRY");
        Instant.parse(coverage.path("source_cutoff_utc").asText());
    }
}
