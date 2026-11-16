package ie212.earthquake.spark.gold;

import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.silver.FileSilverObjectStore;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class PreparedGoldProofTest {
    @TempDir Path root;
    static PreparedGoldProof proof() { return new PreparedGoldProof(PreparedGoldProof.NAMESPACE, "a".repeat(64), "b".repeat(64), "c".repeat(64), "d".repeat(64), "2026-10-25T06:13:04.747Z"); }
    static ObjectNode request() {
        var r = PortablePreparedCatalog.JSON.createObjectNode().put("version", PreparedGoldProof.VERSION).put("run_id", "dat03-gold-" + "a".repeat(32))
                .put("dag_id", PortableGoldPublicationJob.DAG).put("namespace", PreparedGoldProof.NAMESPACE).put("runner_sha256", "e".repeat(64))
                .put("prepared_root", "/opt/pipeline/staging/dat03-portable-import/" + "b".repeat(32) + "/prepared")
                .put("prepared_catalog_sha256", "a".repeat(64)).put("processed_at_utc", "2026-11-15T01:00:00Z").put("processing_date", "2026-11-15");
        r.set("prepared_input", proof().json()); var counts = r.putObject("expected_counts"); GoldIcebergWriter.TABLES.forEach(n -> counts.put(n, 1)); return r;
    }
    static ObjectNode commit() throws Exception {
        var c = GoldPublicationTest.commit().put("namespace", PreparedGoldProof.NAMESPACE);
        c.set("scope", GoldIcebergWriter.scopeJson(PortableGoldPublicationJob.scope(request())));
        for (String n : GoldIcebergWriter.TABLES) ((ObjectNode)c.path("tables").path(n)).put("table", PreparedGoldProof.NAMESPACE + "." + n);
        return c;
    }
    @Test void explicitProofDoesNotMasqueradeAsSilverBundle() {
        var json = GoldIcebergWriter.scopeJson(PortableGoldPublicationJob.scope(request()));
        assertFalse(json.has("silver_bundle_sha256")); assertEquals(proof().json(), json.path("prepared_input"));
        assertEquals(288, json.path("affected_months").size()); assertDoesNotThrow(() -> PortableGoldPublicationJob.validate(request()));
    }
    @Test void legacyScopeSerializationRemainsLegacyOnly() {
        var s = PortableGoldPublicationJob.scope(request());
        var legacy = new GoldCommitScope(s.context(), "a".repeat(64), "legacy-v1", List.of("2023-01"), s.baselineSnapshots());
        var json = GoldIcebergWriter.scopeJson(legacy); assertEquals("a".repeat(64), json.path("silver_bundle_sha256").asText()); assertFalse(json.has("prepared_input"));
    }
    @Test void wrongNamespaceAndMissingHashFailClosed() {
        assertThrows(Exception.class, () -> new PreparedGoldProof("iceberg.gold", "a".repeat(64), "b".repeat(64), "c".repeat(64), "d".repeat(64), "2026-10-25T00:00:00Z"));
        assertThrows(Exception.class, () -> new PreparedGoldProof(PreparedGoldProof.NAMESPACE, null, "b".repeat(64), "c".repeat(64), "d".repeat(64), "2026-10-25T00:00:00Z"));
    }
    @Test void truncatedScopeAndNonzeroBaselineAreNotPortableImports() {
        var s = PortableGoldPublicationJob.scope(request());
        assertThrows(Exception.class, () -> new GoldCommitScope(s.context(), null, s.codeVersion(), List.of("2023-01"), s.baselineSnapshots(), proof()));
        var bases = new TreeMap<>(s.baselineSnapshots()); bases.put("event_current", 1L);
        assertThrows(Exception.class, () -> new GoldCommitScope(s.context(), null, s.codeVersion(), s.affectedMonths(), bases, proof()));
        assertThrows(Exception.class, () -> new GoldCommitScope(s.context(), "f".repeat(64), s.codeVersion(), s.affectedMonths(), s.baselineSnapshots(), proof()));
    }
    @Test void callerReadinessAndOversizedCountsAreRejected() {
        for (String flag : List.of("published", "research_ready", "official_export_ready", "scope_completeness", "silver_bundle_sha256")) {
            var r = request().put(flag, true); assertThrows(Exception.class, () -> PortableGoldPublicationJob.validate(r));
        }
        var r = request(); ((ObjectNode)r.path("expected_counts")).put("event_current", PreparedGoldProof.MAX_FACT_ROWS + 1);
        assertThrows(Exception.class, () -> PortableGoldPublicationJob.validate(r));
    }
    @Test void exactInt64AndScopeAreRequiredInCommit() throws Exception {
        PreparedGoldProof.validateCommit(commit()); var c = commit();
        ((ObjectNode)c.path("scope").path("baseline_snapshots")).set("event_current", PortablePreparedCatalog.JSON.readTree("18446744073709551616"));
        assertThrows(Exception.class, () -> PreparedGoldProof.validateCommit(c));
    }
    @Test void legacyPublisherCannotSilentlyAcceptPreparedInput() throws Exception {
        var c = commit(); var store = new FileSilverObjectStore(root); store.put("gold-commits/gold-test/commit.json", GoldPublicationTest.JSON.writeValueAsBytes(c), "application/json");
        var fake = new GoldPublicationTest.Fake();
        assertThrows(Exception.class, () -> new GoldPublisher(fake, store, () -> {}).publish("gold-test", PreparedGoldProof.NAMESPACE, "a".repeat(64)));
        assertTrue(fake.queries.isEmpty());
    }
    @Test void exhaustivePreparedVerifyPublishAndRerunAreMeasuredNotFabricated() throws Exception {
        var c = commit(); var store = new FileSilverObjectStore(root); store.put("gold-commits/gold-test/commit.json", GoldPublicationTest.JSON.writeValueAsBytes(c), "application/json");
        var fake = new GoldPublicationTest.Fake(); var pub = GoldPublisher.preparedCatalog(fake, store, () -> {});
        assertEquals(139, PortableGoldPublicationJob.verificationQueries(c));
        var verification = pub.verify("gold-test", PreparedGoldProof.NAMESPACE, "a".repeat(64)); assertEquals(139, fake.queries.size());
        assertEquals("PASSED", verification.path("verify_status").asText()); int start = fake.queries.size();
        var published = pub.publish("gold-test", PreparedGoldProof.NAMESPACE, "a".repeat(64)); assertTrue(published.path("published").asBoolean());
        assertEquals(28, fake.queries.size() - start); start = fake.queries.size();
        assertEquals(published, pub.publish("gold-test", PreparedGoldProof.NAMESPACE, "a".repeat(64))); assertEquals(2, fake.queries.size() - start);
        assertFalse(fake.queries.subList(start, fake.queries.size()).stream().anyMatch(q -> q.startsWith("CREATE") || q.startsWith("INSERT")));
    }
    @Test void blockerPreventsAnyPreparedPublicationWrite() throws Exception {
        var c = commit(); var store = new FileSilverObjectStore(root); store.put("gold-commits/gold-test/commit.json", GoldPublicationTest.JSON.writeValueAsBytes(c), "application/json");
        var fake = new GoldPublicationTest.Fake(); fake.failed = true;
        assertThrows(Exception.class, () -> GoldPublisher.preparedCatalog(fake, store, () -> {}).publish("gold-test", PreparedGoldProof.NAMESPACE, "a".repeat(64)));
        assertFalse(fake.queries.stream().anyMatch(q -> q.startsWith("CREATE") || q.startsWith("INSERT")));
    }
    @Test void rerunJournalCannotPutDeleteOrMoveEvenIfAnArtifactDisappears() throws Exception {
        var source = new FileSilverObjectStore(root); source.put("existing.json", new byte[]{1}, "application/json");
        var guarded = PortableGoldPublicationJob.readOnly(source); assertArrayEquals(new byte[]{1}, guarded.read("existing.json"));
        assertThrows(Exception.class, () -> guarded.put("new.json", new byte[]{2}, "application/json"));
        assertThrows(Exception.class, () -> guarded.delete("existing.json"));
        assertThrows(Exception.class, () -> guarded.deletePrefix(""));
        assertThrows(Exception.class, () -> guarded.move("existing.json", "moved.json"));
        assertTrue(source.exists("existing.json")); assertFalse(source.exists("new.json"));
    }
}
