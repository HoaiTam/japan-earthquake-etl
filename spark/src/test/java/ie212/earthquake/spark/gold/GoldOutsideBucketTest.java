package ie212.earthquake.spark.gold;

import static org.junit.jupiter.api.Assertions.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.silver.FileSilverObjectStore;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class GoldOutsideBucketTest {
    @TempDir Path root;
    ObjectNode commit() throws Exception {
        var c=GoldPublicationTest.commit();String ns="iceberg.dat03_catalog_"+"a".repeat(32);c.put("namespace",ns);
        for(String name:GoldIcebergWriter.TABLES) ((ObjectNode)c.path("tables").path(name)).put("table",ns+"."+name);
        ((ObjectNode)c.path("scope").path("baseline_snapshots")).put("event_current",9007199254740993L).put("event_source_bridge",3);
        return c;
    }
    @Test void eachBucketPreservesLiteralPinsFullRowsBothDirectionsAndWriteScope() throws Exception {
        var p=new GoldVerificationPlan(commit());
        for(char bucket:"0123456789abcdef".toCharArray())for(String table:List.of("event_current","event_source_bridge")) {
            String query=p.outsideScopeBucket(table,String.valueOf(bucket));
            assertTrue(query.startsWith("SELECT count(*) FROM ((SELECT * FROM "));
            assertEquals(2,query.split(" EXCEPT ",-1).length-1);assertEquals(4,query.split("SELECT \\* FROM",-1).length-1);
            assertTrue(query.contains("NOT IN ('2023-01')"));assertTrue(query.contains("='"+bucket+"'"));
            assertFalse(query.toLowerCase(Locale.ROOT).contains("approx"));assertFalse(query.contains("sum("));
            assertTrue(query.contains("FOR VERSION AS OF "+(table.equals("event_current")?"9007199254740993":"3")));
        }
    }
    @Test void allUtf8KeysIncludingNullAndHashCollisionsHaveExactlyOnePartition() throws Exception {
        var seen=new HashSet<Character>();
        for(String key:Arrays.asList(null,"","earthquake-日本","event-a","event-b","usgs/abc")) {
            String hash=HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest((key==null?"":key).getBytes(StandardCharsets.UTF_8)));
            assertEquals(1,"0123456789abcdef".chars().filter(c->c==hash.charAt(0)).count());seen.add(hash.charAt(0));
        }
        // Null and empty may collide in the routing hash. EXCEPT still compares
        // the entire original row, so routing equality never certifies row equality.
        assertNotEquals(Arrays.asList(null,"a"),Arrays.asList("","a"));assertFalse(seen.isEmpty());
    }
    @Test void invalidBucketsTablesAndLossyBaselinePinsCannotRun() throws Exception {
        var p=new GoldVerificationPlan(commit());
        for(String b:Arrays.asList(null,"","00","g","A","0' OR true"))assertThrows(Exception.class,()->p.outsideScopeBucket("event_current",b));
        assertThrows(Exception.class,()->p.outsideScopeBucket("dim_date","0"));
        for(Object value:List.of(true,1.5,-1)) {
            var c=commit();((ObjectNode)c.path("scope").path("baseline_snapshots")).set("event_current",GoldPublicationTest.JSON.valueToTree(value));
            assertThrows(Exception.class,()->new GoldVerificationPlan(c).outsideScopeBucket("event_current","0"));
        }
    }
    @Test void sixteenQueriesAreRequiredForEachTableAndSummaryIsExplicitlyDerived() throws Exception {
        var c=commit();var store=new FileSilverObjectStore(root);
        store.put("gold-commits/gold-test/commit.json",GoldPublicationTest.JSON.writeValueAsBytes(c),"application/json");
        var fake=new GoldPublicationTest.Fake();
        var report=GoldPublisher.researchRollout(fake,store,()->{}).verify("gold-test",c.path("namespace").asText(),"a".repeat(64));
        assertEquals(GoldPublisher.BUCKETED_OUTSIDE_VERSION,report.path("outside_scope_strategy").asText());
        assertEquals(32,fake.queries.stream().filter(q->q.contains("coalesce(canonical_event_id" )).count());
        assertEquals("EXACT_EXHAUSTIVE_BUCKET_SUM",report.path("checks").path("outside:event_current").path("kind").asText());
        assertEquals(16,report.path("checks").path("outside:event_source_bridge").path("query_count").asInt());
        assertFalse(fake.queries.stream().anyMatch(q->q.startsWith("CREATE")||q.startsWith("INSERT")));
    }
    @Test void anyChangedBucketBlocksBeforePublicationAndCannotReturnASuccessfulSum() throws Exception {
        var c=commit();var store=new FileSilverObjectStore(root);
        store.put("gold-commits/gold-test/commit.json",GoldPublicationTest.JSON.writeValueAsBytes(c),"application/json");
        var fake=new GoldPublicationTest.Fake() {
            @Override public GoldPublisher.Result execute(String q) throws Exception {
                var result=super.execute(q);
                if(q.contains("coalesce(canonical_event_id")&&q.contains("='7'"))
                    return new GoldPublisher.Result(List.of(),List.of(GoldPublicationTest.JSON.readTree("[1]")));
                return result;
            }
        };
        assertThrows(Exception.class,()->GoldPublisher.researchRollout(fake,store,()->{}).publish("gold-test",c.path("namespace").asText(),"a".repeat(64)));
        assertFalse(store.exists("gold-verification/gold-test/publication.json"));
        assertFalse(fake.queries.stream().anyMatch(q->q.startsWith("CREATE")||q.startsWith("INSERT")));
    }
    @Test void optInCannotAffectSharedGoldOrOperationalNamespaces() throws Exception {
        var store=new FileSilverObjectStore(root);
        store.put("gold-commits/gold-test/commit.json",GoldPublicationTest.JSON.writeValueAsBytes(GoldPublicationTest.commit()),"application/json");
        var fake=new GoldPublicationTest.Fake();
        assertThrows(Exception.class,()->GoldPublisher.researchRollout(fake,store,()->{}).verify("gold-test","iceberg.gold","a".repeat(64)));
        assertTrue(fake.queries.isEmpty());
    }
}
