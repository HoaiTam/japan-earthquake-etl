package ie212.earthquake.spark.gold;

import static org.junit.jupiter.api.Assertions.*;
import static ie212.earthquake.spark.usgs.CurrentMonthCaptureTest.*;
import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import static org.apache.spark.sql.functions.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.orchestration.*;
import ie212.earthquake.spark.silver.*;
import ie212.earthquake.spark.usgs.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

/** Real source fixtures -> Parquet -> Iceberg; SQL adapter fixture, NOT real Trino acceptance. */
class CurrentMonthGoldTest {
    @TempDir static Path warehouse;
    @TempDir Path root;
    static SparkSession spark;
    FileSilverObjectStore raw; CurrentMonthSilverResolver resolver; CurrentMonthCapture capture;
    CurrentMonthGold gold; SqlFixture sql; List<JsonNode> events; AtomicInteger calls;
    @BeforeAll static void start() {
        spark=SparkSession.builder().master("local[1]").appName("current-month-gold-test")
            .config("spark.ui.enabled","false").config("spark.driver.host","127.0.0.1").config("spark.driver.bindAddress","127.0.0.1")
            .config("spark.sql.session.timeZone","UTC").config("spark.sql.shuffle.partitions","1")
            .config("spark.sql.adaptive.enabled","false").config("spark.sql.codegen.wholeStage","false")
            .config("spark.sql.catalog.cmtest","org.apache.iceberg.spark.SparkCatalog").config("spark.sql.catalog.cmtest.type","hadoop")
            .config("spark.sql.catalog.cmtest.warehouse",warehouse.toUri().toString()).getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
    }
    @AfterAll static void stop() { if(spark!=null) spark.stop(); }
    @BeforeEach void setUp() {
        raw=new FileSilverObjectStore(root.resolve("silver"));events=new ArrayList<>();calls=new AtomicInteger();
        capture=new CurrentMonthCapture(new FileBronzeObjectStore(root.resolve("bronze")),UsgsRequestConfig.fromEnvironment(environment()),
            q->fetcher(events,calls).fetch(q),CLOCK,()->{});
        resolver=new CurrentMonthSilverResolver(raw,capture,CLOCK,()->{});
        sql=new SqlFixture(new CurrentMonthSilverResolver.OperationalStore(raw));gold=new CurrentMonthGold(raw,resolver,sql,()->{});
    }
    ObjectNode silver(ObjectNode request) throws Exception { capture.capture(request);return resolver.resolve(spark,request); }
    ObjectNode requestGold(JsonNode silver,JsonNode prior) {
        var out=JSON.createObjectNode();out.put("contract_version",OperationalGoldPolicy.VERSION);
        out.set("operation_id",silver.path("operation_id"));out.set("registry_sha256",silver.path("registry_sha256"));
        out.put("namespace","cmtest."+OperationalGoldPolicy.SCHEMA);out.put("code_version","a".repeat(40));
        if(prior==null) out.putNull("baseline_publication");
        else out.putObject("baseline_publication").set("operation_id",prior.path("operation_id"));
        if(prior!=null) ((ObjectNode)out.path("baseline_publication")).set("publication_sha256",prior.path("publication_sha256"));
        return out;
    }
    ObjectNode run(JsonNode request) throws Exception {
        sql.op=request.path("operation_id").asText();return gold.execute(spark,request,root.resolve(sql.op));
    }
    @Test void realIcebergBootstrapRevisionRolloverRetentionAndOldRerun() throws Exception {
        events.add(event("september","2026-09-01T12:00:00Z","2026-09-02T00:00:00Z"));
        var s1=silver(request("2026-10-01T00:15:00Z"));var r1=requestGold(s1,null);var p1=run(r1);
        assertTrue(p1.path("published").asBoolean());assertEquals("PROVISIONAL",p1.path("publication_quality").asText());
        assertFalse(p1.path("research_ready").asBoolean(true));assertFalse(p1.path("ml_upstream_eligible").asBoolean(true));
        int writes=sql.mutations,fetchCount=calls.get();var keys=raw.list("");
        var audit=CurrentMonthAudit.verify(r1,raw,resolver,sql);
        assertEquals("PASSED",audit.path("status").asText());assertTrue(audit.path("read_only").asBoolean());
        assertEquals(p1,audit.path("gold"));assertEquals(writes,sql.mutations);assertEquals(fetchCount,calls.get());assertEquals(keys,raw.list(""));
        int fetched=calls.get(),mutations=sql.mutations;assertEquals(p1,run(r1));assertEquals(fetched,calls.get());assertEquals(mutations,sql.mutations);
        var req2=request("2026-10-05T00:15:00Z");req2.putObject("baseline").set("operation_id",s1.path("operation_id"));
        ((ObjectNode)req2.path("baseline")).set("registry_sha256",s1.path("registry_sha256"));
        events.add(event("october","2026-10-02T12:00:00Z","2026-10-03T00:00:00Z"));
        var s2=silver(req2);var p2=run(requestGold(s2,p1));
        assertEquals(2,p2.path("snapshot_bundle").path("event_current").path("count").asInt());
        var table=spark.table("cmtest."+OperationalGoldPolicy.SCHEMA+".event_current");
        assertEquals(1,table.filter(col("gold_run_id").equalTo(s1.path("operation_id").asText())).count());
        assertEquals(1,table.filter(col("gold_run_id").equalTo(s2.path("operation_id").asText())).count());
        // Revision updates October, preserving September and the pinned old snapshots.
        events.set(1,event("october","2026-10-02T12:00:00Z","2026-10-05T01:00:00Z"));
        var req3=request("2026-10-06T00:15:00Z");req3.putObject("baseline").set("operation_id",s2.path("operation_id"));
        ((ObjectNode)req3.path("baseline")).set("registry_sha256",s2.path("registry_sha256"));
        var p3=run(requestGold(silver(req3),p2));assertEquals(2,p3.path("event_count").asInt());
        mutations=sql.mutations;assertEquals(p1,run(r1));assertEquals(mutations,sql.mutations); // no alias rollback
        assertFalse(raw.exists("gold-verification/"+s1.path("operation_id").asText()+"/publication.json"));
        for(String name:GoldIcebergWriter.TABLES) {
            var t=org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark,"cmtest."+OperationalGoldPolicy.SCHEMA+"."+name);
            assertEquals(OperationalGoldPolicy.VERSION,t.properties().get("gold.data_policy"));
            assertNotNull(t.currentSnapshot().summary().get(OperationalGoldPolicy.PROPERTY));
        }
        assertTrue(sql.queries.stream().anyMatch(q->q.contains("AS publication_quality")));
        assertTrue(sql.queries.stream().anyMatch(q->q.contains("operational_policy_json=")));
    }
    @Test void badPinsNamespaceAndLostLeaseBlockBeforeGoldWrites() throws Exception {
        var s=silver(request("2026-10-07T00:15:00Z"));var req=requestGold(s,null);
        var wrong=req.deepCopy().put("registry_sha256","b".repeat(64));assertThrows(Exception.class,()->run(wrong));
        var finalWrong=req.deepCopy().put("namespace","cmtest.gold");assertThrows(Exception.class,()->run(finalWrong));
        var unknown=req.deepCopy().put("ml_upstream_eligible",true);assertThrows(Exception.class,()->run(unknown));
        var lost=new CurrentMonthGold(raw,resolver,sql,()->{throw new IllegalStateException("LEASE_LOST");});
        assertThrows(Exception.class,()->lost.execute(spark,req,root.resolve("lost")));
        assertTrue(raw.list("current-month/gold-commits/").isEmpty());assertTrue(sql.queries.isEmpty());
    }
    @Test void failedTrinoCreatesNoPublicationThenResumesSameSnapshots() throws Exception {
        // A separate catalog shares the same Hadoop implementation but an independent QA warehouse.
        spark.conf().set("spark.sql.catalog.cmblocked","org.apache.iceberg.spark.SparkCatalog");
        spark.conf().set("spark.sql.catalog.cmblocked.type","hadoop");
        spark.conf().set("spark.sql.catalog.cmblocked.warehouse",root.resolve("warehouse").toUri().toString());
        events.add(event("blocked","2026-10-02T12:00:00Z","2026-10-03T00:00:00Z"));
        var s=silver(request("2026-10-08T00:15:00Z"));var req=requestGold(s,null).put("namespace","cmblocked."+OperationalGoldPolicy.SCHEMA);
        sql.fail=true;assertThrows(Exception.class,()->run(req));assertEquals(0,sql.mutations);
        var store=new CurrentMonthSilverResolver.OperationalStore(raw);String op=s.path("operation_id").asText();
        byte[] commit=store.read("gold-commits/"+op+"/commit.json");
        assertFalse(store.exists("gold-verification/"+op+"/publication.json"));assertFalse(store.exists("gold-state/"+op+"/_SUCCESS"));
        sql.fail=false;var p=run(req);assertTrue(p.path("published").asBoolean());assertArrayEquals(commit,store.read("gold-commits/"+op+"/commit.json"));
        var changed=req.deepCopy().put("code_version","b".repeat(40));assertThrows(Exception.class,()->run(changed));
    }
    /** Scripted Trino protocol responses; real SQL acceptance is deliberately a separate test. */
    static final class SqlFixture implements GoldPublisher.Sql {
        final SilverObjectStore store;String op;boolean fail;int mutations;final Set<String> published=new HashSet<>();
        final List<String> queries=new ArrayList<>();
        SqlFixture(SilverObjectStore store) { this.store=store; }
        public GoldPublisher.Result execute(String q) throws Exception {
            queries.add(q);var c=JSON.readTree(store.read("gold-commits/"+op+"/commit.json"));var plan=new GoldVerificationPlan(c);
            if(q.startsWith("SELECT *") && q.endsWith("LIMIT 0")) {
                for(String name:GoldIcebergWriter.TABLES) if(q.contains(plan.ref(name))) {
                    var cols=new ArrayList<JsonNode>();plan.expectedTypes(name).forEach((k,v)->cols.add(JSON.createObjectNode().put("name",k).put("type",v)));
                    return new GoldPublisher.Result(cols,List.of());
                }
            }
            if(q.startsWith("CREATE") || q.startsWith("INSERT")) { mutations++;if(q.startsWith("INSERT")) published.add(op);return new GoldPublisher.Result(List.of(),List.of()); }
            long value=0;
            if(q.contains("$snapshots")) value=1;
            else if(q.startsWith("SELECT snapshot_id")) {
                for(String n:GoldIcebergWriter.TABLES) if(q.contains(plan.metadata(n,"refs"))) value=org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark,plan.table(n)).currentSnapshot().snapshotId();
            } else if(q.contains(".publication_status")) value=published.contains(op)?1:0;
            else {
                for(String n:GoldIcebergWriter.TABLES) if(q.equals("SELECT count(*) FROM "+plan.ref(n))) value=plan.count(n);
                if(q.contains("is_natural_earthquake AND is_in_study_area") || q.startsWith("SELECT count(*) FROM "+plan.namespace()+".v_")
                        || q.equals("SELECT count(*) FROM "+plan.namespace()+".earthquake_event_current")) value=plan.count("event_current");
            }
            if(fail && q.contains("latitude NOT BETWEEN")) value=1;
            if(q.contains(" WHERE coverage_mode<>")) value=0;
            return new GoldPublisher.Result(List.of(),List.of(JSON.createArrayNode().add(value)));
        }
    }
}
