package ie212.earthquake.spark.ml;

import static org.junit.jupiter.api.Assertions.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.gold.*;
import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.Test;

class DatasetManifestTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Instant NOW = Instant.parse("2026-10-11T00:00:00Z");
    private static final String PUB = "pub_" + "a".repeat(64);
    static ObjectNode config(String split) {
        var c = JSON.createObjectNode(); c.put("dataset_split", split);
        c.put("period_start_utc", split.equals("REPRODUCTION") ? "2000-01-01T00:00:00Z" : "2018-10-01T00:00:00Z");
        c.put("period_end_utc_exclusive", split.equals("REPRODUCTION") ? "2018-10-01T00:00:00Z" : "2024-01-01T00:00:00Z");
        c.put("observation_cutoff_utc", NOW.toString());
        c.putObject("filter_config").put("catalog_era", "UNIFIED").put("canonical_source_system", "JMA_BULLETIN")
            .put("is_natural_earthquake", true).put("is_in_study_area", true);
        DatasetIdentityPlan.VERSIONS.forEach(v -> c.put(v, "v1")); return c;
    }
    static ObjectNode publication() {
        var p = JSON.createObjectNode(); p.put("verify_status", "PASSED"); p.put("gold_snapshot_id", 99);
        p.put("gold_table_name", "iceberg.gold.earthquake_event_current"); p.put("physical_gold_table_name", "iceberg.gold.event_current");
        p.put("gold_publication_id", PUB); p.put("publication_bundle_sha256", "a".repeat(64)); return p;
    }
    static class Fake implements GoldPublisher.Sql {
        final List<String> queries = new ArrayList<>(); JsonNode manifest;
        JsonNode publicationRow; JsonNode insertRow; int publications = 1; long snapshotPresent = 1;
        Fake(JsonNode config) {
            var row = publication(); row.set("snapshot_id", row.remove("gold_snapshot_id"));
            row.set("publication_id", row.remove("gold_publication_id")); row.set("bundle_sha256", row.remove("publication_bundle_sha256"));
            var bundle = JSON.createObjectNode();
            for (String name : GoldIcebergWriter.TABLES) bundle.putObject(name).put("table", "iceberg.gold." + name).put("snapshot_id", 99);
            row.put("snapshot_bundle_json", bundle.toString()); publicationRow = row;
            insertRow = new DatasetIdentityPlan(publication(), config, "run-1", NOW).manifest();
        }
        public GoldPublisher.Result execute(String query) throws Exception {
            queries.add(query);
            if (query.contains(".publication_status")) return records(publications == 0 ? List.of() : publications == 1
                ? List.of(publicationRow) : List.of(publicationRow, publicationRow));
            if (query.contains("$snapshots")) return new GoldPublisher.Result(List.of(), List.of(JSON.readTree("[" + snapshotPresent + "]")));
            if (query.startsWith("INSERT")) { manifest = insertRow.deepCopy(); return records(List.of()); }
            if (query.startsWith("SELECT * FROM iceberg.ml.dataset_manifest")) return records(manifest == null ? List.of() : List.of(manifest));
            return records(List.of());
        }
        private GoldPublisher.Result records(List<JsonNode> records) {
            if (records.isEmpty()) return new GoldPublisher.Result(List.of(), List.of());
            var columns = new ArrayList<JsonNode>(); records.get(0).fieldNames().forEachRemaining(n -> columns.add(JSON.createObjectNode().put("name", n)));
            var rows = new ArrayList<JsonNode>();
            records.forEach(r -> { var array = JSON.createArrayNode(); columns.forEach(c -> array.add(r.path(c.path("name").asText()))); rows.add(array); });
            return new GoldPublisher.Result(columns, rows);
        }
    }
    @Test void stableIdentityAcrossClockRetryAndNestedKeyOrderForBothSplits() {
        for (String split : List.of("REPRODUCTION", "EXTENSION")) {
            var c = config(split); var p = new DatasetIdentityPlan(publication(), c, "run-1", NOW);
            var filters = JSON.createObjectNode().put("is_in_study_area", true).put("is_natural_earthquake", true)
                .put("canonical_source_system", "JMA_BULLETIN").put("catalog_era", "UNIFIED");
            c.set("filter_config", filters);
            var retry = new DatasetIdentityPlan(publication(), c, "run-2", NOW.plusSeconds(99));
            assertEquals(p.manifest().path("dataset_id"), retry.manifest().path("dataset_id"));
            assertEquals(p.canonicalPayload(), retry.canonicalPayload());
            assertNotEquals(p.manifest().path("created_at_utc"), retry.manifest().path("created_at_utc"));
        }
    }
    @Test void revisionConfigAndCutoffProduceDistinctIdentities() {
        var c = config("EXTENSION"); var p = new DatasetIdentityPlan(publication(), c, "run-1", NOW).manifest();
        var revision = publication(); revision.put("gold_snapshot_id", 100);
        assertNotEquals(p.path("dataset_id"), new DatasetIdentityPlan(revision, c, "run-1", NOW).manifest().path("dataset_id"));
        c.put("feature_version", "v2");
        assertNotEquals(p.path("dataset_id"), new DatasetIdentityPlan(publication(), c, "run-1", NOW).manifest().path("dataset_id"));
        c = config("EXTENSION"); c.put("observation_cutoff_utc", NOW.plusSeconds(1).toString());
        assertNotEquals(p.path("dataset_id"), new DatasetIdentityPlan(publication(), c, "run-1", NOW).manifest().path("dataset_id"));
    }
    @Test void unpublishedDuplicatePublicationAndExpiredSnapshotCannotWrite() throws Exception {
        for (int caseId = 0; caseId < 4; caseId++) {
            var c = config("EXTENSION"); var sql = new Fake(c);
            if (caseId == 0) sql.publications = 0;
            if (caseId == 1) sql.publications = 2;
            if (caseId == 2) ((ObjectNode)sql.publicationRow).put("verify_status", "FAILED");
            if (caseId == 3) sql.snapshotPresent = 0;
            assertThrows(Exception.class, () -> new DatasetManifestWriter(sql, () -> {}).build("iceberg.gold", PUB, "iceberg.ml", c, "run-1", NOW));
            assertFalse(sql.queries.stream().anyMatch(q -> q.startsWith("CREATE") || q.startsWith("INSERT")));
        }
    }
    @Test void insertThenRerunPreservesOneRowAndAdvancedLifecycle() throws Exception {
        var c = config("EXTENSION"); var sql = new Fake(c); var writer = new DatasetManifestWriter(sql, () -> {});
        var first = writer.build("iceberg.gold", PUB, "iceberg.ml", c, "run-1", NOW);
        var retry = writer.build("iceberg.gold", PUB, "iceberg.ml", c, "run-2", NOW.plusSeconds(2));
        assertEquals(first, retry); assertEquals(1, sql.queries.stream().filter(q -> q.startsWith("INSERT")).count());
        ((ObjectNode)sql.manifest).put("dataset_status", "REJECTED").put("status_reason_code", "DS_COVERAGE_INCOMPLETE");
        assertEquals("REJECTED", writer.build("iceberg.gold", PUB, "iceberg.ml", c, "run-3", NOW).path("dataset_status").asText());
        assertEquals("SELECT * FROM iceberg.gold.event_current FOR VERSION AS OF 99", DatasetManifestWriter.pinnedGoldSql(first));
        assertFalse(sql.queries.stream().anyMatch(q -> q.contains("$refs") || q.contains("FROM iceberg.gold.earthquake_event_current")));
    }
    @Test void identityCollisionOrCorruptBundleFails() throws Exception {
        var c = config("EXTENSION"); var sql = new Fake(c); sql.manifest = sql.insertRow.deepCopy();
        ((ObjectNode)sql.manifest).put("gold_snapshot_id", 100);
        assertThrows(Exception.class, () -> new DatasetManifestWriter(sql, () -> {}).build("iceberg.gold", PUB, "iceberg.ml", c, "run-1", NOW));
        assertFalse(sql.queries.stream().anyMatch(q -> q.startsWith("INSERT")));
        var invalid = new Fake(c); ((ObjectNode)invalid.publicationRow).put("snapshot_bundle_json", "{}");
        assertThrows(Exception.class, () -> new DatasetManifestWriter(invalid, () -> {}).build("iceberg.gold", PUB, "iceberg.ml", c, "run-1", NOW));
    }
    @Test void invalidPeriodFilterPrivateConfigAndUnheldLeaseReject() {
        var c = config("EXTENSION"); c.put("period_end_utc_exclusive", "2023-12-31T00:00:00Z");
        assertThrows(Exception.class, () -> new DatasetIdentityPlan(publication(), c, "run-1", NOW));
        var privateConfig = config("EXTENSION"); ((ObjectNode)privateConfig.path("filter_config")).put("access_key", "fixture");
        assertThrows(Exception.class, () -> new DatasetIdentityPlan(publication(), privateConfig, "run-1", NOW));
        var badFilter = config("EXTENSION"); ((ObjectNode)badFilter.path("filter_config")).put("catalog_era", "LEGACY");
        assertThrows(Exception.class, () -> new DatasetIdentityPlan(publication(), badFilter, "run-1", NOW));
        var valid = config("EXTENSION"); var sql = new Fake(valid);
        assertThrows(Exception.class, () -> new DatasetManifestWriter(sql, () -> { throw new IllegalStateException(); })
            .build("iceberg.gold", PUB, "iceberg.ml", valid, "run-1", NOW));
        assertTrue(sql.queries.isEmpty());
    }
    @Test void trinoTimestampNormalizesToUtcIdentity() throws Exception {
        var row = JSON.readTree("[\"2026-10-11 00:00:00.000000 UTC\"]");
        var columns = List.<JsonNode>of(JSON.createObjectNode().put("name", "observation_cutoff_utc"));
        assertEquals("2026-10-11T00:00:00Z", DatasetManifestWriter.records(new GoldPublisher.Result(columns, List.of(row)))
            .get(0).path("observation_cutoff_utc").asText());
    }
}
