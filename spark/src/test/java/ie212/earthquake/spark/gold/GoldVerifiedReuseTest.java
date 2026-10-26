package ie212.earthquake.spark.gold;

import static org.junit.jupiter.api.Assertions.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.silver.FileSilverObjectStore;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class GoldVerifiedReuseTest {
    @TempDir Path temporary;
    ObjectNode commit() throws Exception {
        var c=GoldPublicationTest.commit();String ns="iceberg.dat03_catalog_"+"a".repeat(32);c.put("namespace",ns);
        for(String t:GoldIcebergWriter.TABLES)((ObjectNode)c.path("tables").path(t)).put("table",ns+"."+t);return c;
    }
    FileSilverObjectStore store(ObjectNode c) throws Exception {
        var store=new FileSilverObjectStore(temporary.toRealPath());store.put("gold-commits/gold-test/commit.json",GoldPublicationTest.JSON.writeValueAsBytes(c),"application/json");return store;
    }
    @Test void sameProcessExactProofIsBoundToOriginalReportAndFreshnessStillExecutes() throws Exception {
        var c=commit();var store=store(c);var fake=new GoldPublicationTest.Fake();
        var p=GoldPublisher.researchCatalogCheckpointed(fake,store,()->{});
        var verification=p.verify("gold-test",c.path("namespace").asText(),"a".repeat(64));
        String sha=ie212.earthquake.spark.orchestration.FullMonthContract.digest(verification);int count=fake.queries.size();
        ((ObjectNode)verification.path("checks")).put("caller_mutation","not-cache-data");
        var report=p.publish("gold-test",c.path("namespace").asText(),"a".repeat(64));
        assertEquals(sha,report.path("verification_reuse").path("source_report_sha256").asText());
        assertFalse(report.path("checks").has("caller_mutation"));
        assertEquals(report.path("checks").size()-8,report.path("verification_reuse").path("reused_check_count").asInt());
        var published=fake.queries.subList(count,fake.queries.size());
        assertFalse(published.stream().anyMatch(q->q.contains("count(DISTINCT")));
        assertEquals(12,published.stream().filter(q->q.startsWith("SELECT snapshot_id FROM")).count());
        assertTrue(published.stream().anyMatch(q->q.contains(".publication_status")));
    }
    @Test void expiredProofFallsBackToAllReadbacksWithoutChangingLimits() throws Exception {
        var c=commit();var store=store(c);var fake=new GoldPublicationTest.Fake();var clock=new AtomicLong();
        var p=GoldPublisher.researchCatalogCheckpointed(fake,store,()->{},clock::get);
        p.verify("gold-test",c.path("namespace").asText(),"a".repeat(64));int n=fake.queries.size();clock.set(301_000_000_000L);
        var report=p.publish("gold-test",c.path("namespace").asText(),"a".repeat(64));
        assertFalse(report.has("verification_reuse"));assertTrue(fake.queries.subList(n,fake.queries.size()).stream().anyMatch(q->q.contains("count(DISTINCT")));
    }
    @Test void freshInstanceOrChangedCommitBytesCannotReuseAnOldProof() throws Exception {
        var c=commit();var store=store(c);var fake=new GoldPublicationTest.Fake();
        var p=GoldPublisher.researchCatalogCheckpointed(fake,store,()->{});p.verify("gold-test",c.path("namespace").asText(),"a".repeat(64));
        c.put("extra_commit_metadata","different-bytes");store.put("gold-commits/gold-test/commit.json",GoldPublicationTest.JSON.writeValueAsBytes(c),"application/json");
        assertFalse(p.publish("gold-test",c.path("namespace").asText(),"a".repeat(64)).has("verification_reuse"));
        var fresh=GoldPublisher.researchCatalogCheckpointed(fake,store,()->{});int n=fake.queries.size();
        fresh.publish("gold-test",c.path("namespace").asText(),"a".repeat(64));
        assertTrue(fake.queries.subList(n,fake.queries.size()).stream().anyMatch(q->q.contains("count(DISTINCT")));
    }
    @Test void staleCurrentPinsFailBeforeAnyPublicationMutationDespiteAValidProof() throws Exception {
        var c=commit();var store=store(c);
        class Stale extends GoldPublicationTest.Fake {
            boolean stale;
            @Override public GoldPublisher.Result execute(String query) throws Exception {
                var r=super.execute(query);
                return stale && query.startsWith("SELECT snapshot_id FROM")?new GoldPublisher.Result(List.of(),List.of(GoldPublicationTest.JSON.readTree("[2]"))):r;
            }
        }
        var fake=new Stale();var p=GoldPublisher.researchCatalogCheckpointed(fake,store,()->{});
        p.verify("gold-test",c.path("namespace").asText(),"a".repeat(64));fake.stale=true;
        assertThrows(Exception.class,()->p.publish("gold-test",c.path("namespace").asText(),"a".repeat(64)));
        assertFalse(fake.queries.stream().anyMatch(q->q.startsWith("CREATE")||q.startsWith("INSERT")));
    }
    @Test void aFailedVerificationClearsPreviousProofAndLegacyFactoriesStillRepeatChecks() throws Exception {
        var c=commit();var store=store(c);var fake=new GoldPublicationTest.Fake();
        var p=GoldPublisher.researchCatalogCheckpointed(fake,store,()->{});p.verify("gold-test",c.path("namespace").asText(),"a".repeat(64));
        fake.failed=true;assertThrows(Exception.class,()->p.verify("gold-test",c.path("namespace").asText(),"a".repeat(64)));fake.failed=false;
        assertFalse(p.publish("gold-test",c.path("namespace").asText(),"a".repeat(64)).has("verification_reuse"));
        var legacy=GoldPublisher.researchCatalog(fake,store,()->{});legacy.verify("gold-test",c.path("namespace").asText(),"a".repeat(64));int n=fake.queries.size();
        legacy.publish("gold-test",c.path("namespace").asText(),"a".repeat(64));
        assertTrue(fake.queries.subList(n,fake.queries.size()).stream().anyMatch(q->q.contains("count(DISTINCT")));
    }
}
