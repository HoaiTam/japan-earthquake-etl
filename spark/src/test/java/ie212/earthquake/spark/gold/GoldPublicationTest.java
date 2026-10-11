package ie212.earthquake.spark.gold;

import static org.junit.jupiter.api.Assertions.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.silver.FileSilverObjectStore;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Gate flow fixtures only. Real SQL/MinIO integration is recorded separately. */
class GoldPublicationTest {
    static final ObjectMapper JSON=new ObjectMapper();
    @TempDir Path root;
    static ObjectNode commit() throws Exception {
        var c=JSON.createObjectNode();c.put("namespace","iceberg.gold");c.put("status","COMMITTED");c.put("published",false);
        c.put("identity_sha256","a".repeat(64));c.put("gold_run_id","gold-test");
        var scope=c.putObject("scope");scope.putArray("affected_months").add("2023-01");var bases=scope.putObject("baseline_snapshots");
        var pins=c.putObject("tables");
        for(String name:GoldIcebergWriter.TABLES) {
            var pin=pins.putObject(name);pin.put("table","iceberg.gold."+name);pin.put("snapshot_id",1);pin.put("count",1);
            pin.put("committed_at_utc","2026-10-10T00:00:00Z");
            pin.put("schema_json","{\"fields\":[{\"name\":\"key\",\"type\":\"string\"}]}");bases.put(name,0);
        }
        return c;
    }
    static class Fake implements GoldPublisher.Sql {
        final List<String> queries=new ArrayList<>(); boolean failed,published; String schemaType="varchar";
        public GoldPublisher.Result execute(String query) throws Exception {
            queries.add(query);
            if(query.startsWith("SELECT *") && query.endsWith("LIMIT 0"))
                return new GoldPublisher.Result(List.of(JSON.readTree("{\"name\":\"key\",\"type\":\""+schemaType+"\"}")),List.of());
            if(query.startsWith("INSERT INTO")){published=true;return new GoldPublisher.Result(List.of(),List.of());}
            if(query.startsWith("CREATE"))return new GoldPublisher.Result(List.of(),List.of());
            long value=query.contains("$snapshots") || query.contains("$refs") || query.startsWith("SELECT snapshot_id") ? 1 : 0;
            if(query.startsWith("SELECT count(*) FROM") && (query.endsWith("FOR VERSION AS OF 1")
                    || query.contains("is_natural_earthquake AND is_in_study_area") || query.contains(".v_")
                    || query.endsWith(".earthquake_event_current")))value=1;
            if(query.contains(".publication_status"))value=published?1:0;
            if(failed && query.contains("latitude NOT BETWEEN"))value=1;
            return new GoldPublisher.Result(List.of(),List.of(JSON.readTree("["+value+"]")));
        }
    }
    @Test void wrongTimestampPrecisionOrZoneStillBlocksPublication() throws Exception {
        var c=commit();((ObjectNode)c.path("tables").path("event_current"))
            .put("schema_json","{\"fields\":[{\"name\":\"key\",\"type\":\"timestamp\"}]}");
        var store=new FileSilverObjectStore(root);store.put("gold-commits/gold-test/commit.json",JSON.writeValueAsBytes(c),"application/json");
        for(String wrong:List.of("timestamp(3) with time zone","timestamp(6)","timestamp with time zone")) {
            var fake=new Fake();fake.schemaType=wrong;
            assertEquals("TRINO_SCHEMA_MISMATCH:event_current",assertThrows(Exception.class,()->
                new GoldPublisher(fake,store,()->{}).publish("gold-test","iceberg.gold","a".repeat(64))).getMessage());
            assertFalse(fake.queries.stream().anyMatch(q->q.startsWith("CREATE") || q.startsWith("INSERT")));
        }
    }
    @Test void allBlockersBeforeAnyPublicationMutation() throws Exception {
        var store=new FileSilverObjectStore(root);store.put("gold-commits/gold-test/commit.json",JSON.writeValueAsBytes(commit()),"application/json");
        var fake=new Fake();fake.failed=true;
        assertThrows(Exception.class,()->new GoldPublisher(fake,store,()->{}).publish("gold-test","iceberg.gold","a".repeat(64)));
        assertFalse(fake.queries.stream().anyMatch(q->q.startsWith("CREATE") || q.startsWith("INSERT")));
        assertFalse(store.exists("gold-verification/gold-test/publication.json"));
    }
    @Test void rerunReusesPublishedRowAndHistoricalRerunDoesNotReplaceAlias() throws Exception {
        var store=new FileSilverObjectStore(root);store.put("gold-commits/gold-test/commit.json",JSON.writeValueAsBytes(commit()),"application/json");
        var fake=new Fake();var publisher=new GoldPublisher(fake,store,()->{});
        var first=publisher.publish("gold-test","iceberg.gold","a".repeat(64));int before=fake.queries.size();
        var same=publisher.publish("gold-test","iceberg.gold","a".repeat(64));
        assertEquals(first,same);assertTrue(first.path("published").asBoolean());
        assertTrue(fake.queries.subList(before,fake.queries.size()).stream().noneMatch(q->q.startsWith("CREATE") || q.startsWith("INSERT")));
        assertEquals(1,fake.queries.stream().filter(q->q.startsWith("INSERT")).count());
        assertThrows(Exception.class,()->publisher.publish("gold-test","iceberg.other","a".repeat(64)));
    }
    @Test void literalPinsOutsideScopeAndTypedSchema() throws Exception {
        var c=commit();c.path("scope").path("baseline_snapshots").deepCopy();
        ((ObjectNode)c.path("scope").path("baseline_snapshots")).put("event_current",99);
        var plan=new GoldVerificationPlan(c);
        assertTrue(plan.outsideScope("event_current").contains("FOR VERSION AS OF 99"));
        assertTrue(plan.outsideScope("event_current").contains("FOR VERSION AS OF 1"));
        assertEquals(Map.of("key","varchar"),plan.expectedTypes("event_current"));
        assertThrows(Exception.class,()->plan.ref("event_current; DROP TABLE x"));
        ((ObjectNode)c.path("tables").path("event_current")).put("snapshot_id",0);
        assertThrows(Exception.class,()->new GoldVerificationPlan(c));
    }
}
