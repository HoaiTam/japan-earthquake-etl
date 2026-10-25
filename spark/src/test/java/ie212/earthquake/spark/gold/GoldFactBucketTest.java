package ie212.earthquake.spark.gold;

import static org.junit.jupiter.api.Assertions.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.silver.FileSilverObjectStore;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class GoldFactBucketTest {
    @TempDir Path root;
    ObjectNode commit() throws Exception {
        var c=GoldPublicationTest.commit();String ns="iceberg.dat03_catalog_"+"b".repeat(32);c.put("namespace",ns);
        for(String t:GoldIcebergWriter.TABLES)((ObjectNode)c.path("tables").path(t)).put("table",ns+"."+t);return c;
    }
    @Test void uniquenessRoutesByTheActualUniqueKeyNotACanonicalKeyThatCouldHideBridgeDuplicates() throws Exception {
        var plan=new GoldVerificationPlan(commit());
        for(char bucket:"0123456789abcdef".toCharArray()) {
            String observation=plan.factBlockerBucket("unique:event_source_bridge",String.valueOf(bucket));
            assertTrue(observation.contains("coalesce(source_observation_id,''"));assertFalse(observation.contains("coalesce(canonical_event_id"));
            assertTrue(observation.contains("count(DISTINCT source_observation_id)"));
            assertTrue(plan.factBlockerBucket("unique:event_current",String.valueOf(bucket)).contains("coalesce(canonical_event_id,''"));
        }
    }
    @Test void canonicalJoinDomainsAreIdenticalForParentsAndChildrenIncludingOrphanAndNullKeys() throws Exception {
        var plan=new GoldVerificationPlan(commit());
        for(String name:List.of("bridge_lineage","source_counts","primary_provenance"))for(char b:"0123456789abcdef".toCharArray()) {
            String q=plan.factBlockerBucket(name,String.valueOf(b));
            assertEquals(2,q.split("coalesce\\(canonical_event_id",-1).length-1);
            assertEquals(2,q.split("='"+b+"'",-1).length-1);
            assertTrue(q.contains("FOR VERSION AS OF 1"));assertTrue(q.contains("LEFT JOIN"));assertFalse(q.contains("approx"));
        }
        assertThrows(Exception.class,()->plan.factBlockerBucket("dimensions","0"));
        for(String b:Arrays.asList(null,"","00","g","A","0' OR true"))assertThrows(Exception.class,()->plan.factBlockerBucket("source_counts",b));
    }
    @Test void everyFactCheckNeedsSixteenActualReadbacksAndDerivedTotalsAreNotFakeSql() throws Exception {
        var c=commit();var store=new FileSilverObjectStore(root);store.put("gold-commits/gold-test/commit.json",GoldPublicationTest.JSON.writeValueAsBytes(c),"application/json");
        var fake=new GoldPublicationTest.Fake();
        var report=GoldPublisher.researchCatalog(fake,store,()->{}).verify("gold-test",c.path("namespace").asText(),"a".repeat(64));
        assertEquals(GoldPublisher.BUCKETED_BLOCKERS_VERSION,report.path("fact_blocker_strategy").asText());
        for(String name:GoldVerificationPlan.BUCKETED_FACT_BLOCKERS) {
            var summary=report.path("checks").path(name);assertEquals(16,summary.path("query_count").asInt());assertFalse(summary.has("sql"));
            for(char b:"0123456789abcdef".toCharArray())assertTrue(report.path("checks").path(name+":"+b).path("sql").isTextual());
        }
        assertFalse(fake.queries.stream().anyMatch(q->q.startsWith("CREATE") || q.startsWith("INSERT")));
    }
    @Test void anyBucketFailureBlocksBeforePublicationAndLegacyModeStillUsesItsOriginalQueries() throws Exception {
        var c=commit();var store=new FileSilverObjectStore(root);store.put("gold-commits/gold-test/commit.json",GoldPublicationTest.JSON.writeValueAsBytes(c),"application/json");
        var fake=new GoldPublicationTest.Fake() {
            @Override public GoldPublisher.Result execute(String query) throws Exception {
                var result=super.execute(query);
                if(query.contains("count(DISTINCT source_observation_id)") && query.contains("='f'"))
                    return new GoldPublisher.Result(List.of(),List.of(GoldPublicationTest.JSON.readTree("[1]")));
                return result;
            }
        };
        assertThrows(Exception.class,()->GoldPublisher.researchCatalog(fake,store,()->{}).publish("gold-test",c.path("namespace").asText(),"a".repeat(64)));
        assertFalse(fake.queries.stream().anyMatch(q->q.startsWith("CREATE") || q.startsWith("INSERT")));
        assertFalse(store.exists("gold-verification/gold-test/publication.json"));
        var legacy=new GoldPublicationTest.Fake();var report=GoldPublisher.researchRollout(legacy,store,()->{}).verify("gold-test",c.path("namespace").asText(),"a".repeat(64));
        assertFalse(report.has("fact_blocker_strategy"));
        assertTrue(report.path("checks").path("unique:event_source_bridge").has("sql"));
    }
    @Test void sharedNamespaceCannotOptIntoCatalogFactory() throws Exception {
        var store=new FileSilverObjectStore(root);store.put("gold-commits/gold-test/commit.json",GoldPublicationTest.JSON.writeValueAsBytes(GoldPublicationTest.commit()),"application/json");
        var fake=new GoldPublicationTest.Fake();assertThrows(Exception.class,()->GoldPublisher.researchCatalog(fake,store,()->{}).verify("gold-test","iceberg.gold","a".repeat(64)));
        assertTrue(fake.queries.isEmpty());
    }
}
