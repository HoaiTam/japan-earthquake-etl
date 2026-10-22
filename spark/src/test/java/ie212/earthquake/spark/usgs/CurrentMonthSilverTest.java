package ie212.earthquake.spark.usgs;

import static ie212.earthquake.spark.usgs.CurrentMonthCaptureTest.*;
import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import static ie212.earthquake.spark.orchestration.CurrentMonthContract.*;
import static org.junit.jupiter.api.Assertions.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.orchestration.*;
import ie212.earthquake.spark.silver.*;
import java.io.IOException;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

class CurrentMonthSilverTest {
    static SparkSession spark;
    @TempDir Path temporary;
    FileBronzeObjectStore bronze; FileSilverObjectStore silver;
    CurrentMonthCapture capture; CurrentMonthSilverResolver resolver;
    AtomicInteger fetches; List<JsonNode> events;
    @BeforeAll static void startSpark() {
        spark=SparkSession.builder().master("local[1]").appName("current-month-fixtures").config("spark.ui.enabled","false")
                .config("spark.driver.host","127.0.0.1").config("spark.driver.bindAddress","127.0.0.1")
                .config("spark.sql.session.timeZone","UTC").config("spark.sql.shuffle.partitions","1").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
    }
    @AfterAll static void stopSpark() { if(spark!=null) spark.stop(); }
    @BeforeEach void setUp() {
        bronze=new FileBronzeObjectStore(temporary.resolve("bronze")); silver=new FileSilverObjectStore(temporary.resolve("silver"));
        fetches=new AtomicInteger(); events=new ArrayList<>();
        capture=new CurrentMonthCapture(bronze,UsgsRequestConfig.fromEnvironment(environment()),q->fetcher(events,fetches).fetch(q),CLOCK,()->{});
        resolver=new CurrentMonthSilverResolver(silver,capture,CLOCK,()->{});
    }
    ObjectNode run(ObjectNode request) throws Exception { capture.capture(request); return resolver.resolve(spark,request); }
    ObjectNode next(String end,JsonNode before) {
        var r=request(end); r.putObject("baseline").put("operation_id",before.path("operation_id").asText()).put("registry_sha256",before.path("registry_sha256").asText()); return r;
    }
    @Test void bootstrapProducesIsolatedFourDatasetBundleAndReadbackOnlyRerun() throws Exception {
        events.add(event("keep","2026-10-01T12:00:00Z","2026-10-03T00:00:00Z"));
        var req=request("2026-10-05T00:15:00Z"); var first=run(req); String op=operation(req);
        assertEquals("ObservedAsOfReady",first.path("status").asText()); assertEquals(COVERAGE,first.path("coverage_mode").asText());
        for(String flag:List.of("published","research_ready","ml_upstream_eligible")) assertFalse(first.path(flag).asBoolean(true));
        assertEquals(1,first.path("reconciliation").path("current").asInt()); assertFalse(first.has("gold_request"));
        assertFalse(silver.exists("bundles/"+op+"/_SUCCESS")); assertFalse(silver.exists("full-month/"+op+"/_SUCCESS"));
        var bundle=new SilverBundlePublisher(new CurrentMonthSilverResolver.OperationalStore(silver)).verify(op);
        assertEquals(4,bundle.path("datasets").size()); assertEquals(0,bundle.path("datasets").path("source_link").path("record_count").asInt());
        int calls=fetches.get(); assertEquals(first,run(req)); assertEquals(calls,fetches.get());
        assertThrows(IOException.class,()->FullMonthContract.validate(req));
    }
    @Test void revisionRetainsHistoryAndEventOutsideThreeDayOverlapWithoutDuplicates() throws Exception {
        var old=event("revised","2026-10-02T12:00:00Z","2026-10-03T00:00:00Z");
        var untouched=event("outside-overlap","2026-10-01T12:00:00Z","2026-10-03T00:00:00Z");
        events.add(old); events.add(untouched); var first=run(request("2026-10-05T00:15:00Z"));
        var revision=old.deepCopy(); ((ObjectNode)revision.path("properties")).put("updated",Instant.parse("2026-10-05T01:00:00Z").toEpochMilli());
        ((ObjectNode)revision.path("properties")).put("mag",4.5); events.set(0,revision);
        var second=run(next("2026-10-06T00:15:00Z",first)); var r=second.path("reconciliation");
        assertEquals(2,r.path("current").asInt()); assertEquals(1,r.path("history").asInt()); assertEquals(3,r.path("output_revisions").asInt());
        assertEquals(1,r.path("reused_revision_ids").asInt()); assertEquals(first,resolver.verify(first.path("operation_id").asText(),first.path("registry_sha256").asText()));
    }
    @Test void missingIdAndOlderRevisionBlockBeforeSilverWrites() throws Exception {
        var event=event("known","2026-10-02T12:00:00Z","2026-10-04T00:00:00Z"); events.add(event);
        var before=run(request("2026-10-05T00:15:00Z"));
        events.clear(); var missing=next("2026-10-06T00:15:00Z",before); capture.capture(missing);
        assertTrue(assertThrows(IOException.class,()->resolver.resolve(spark,missing)).getMessage().contains("MISSING_SOURCE_ID"));
        assertTrue(silver.list("current-month/state/"+operation(missing)).isEmpty());
        var older=event.deepCopy(); ((ObjectNode)older.path("properties")).put("updated",Instant.parse("2026-10-03T00:00:00Z").toEpochMilli()); events.add(older);
        var stale=next("2026-10-07T00:15:00Z",before); capture.capture(stale);
        assertTrue(assertThrows(IOException.class,()->resolver.resolve(spark,stale)).getMessage().contains("STALE_OR_AMBIGUOUS"));
    }
    @Test void eventMovingOutsideAffectedMonthsBlocksRatherThanLeavingGhostCurrent() throws Exception {
        events.add(event("moved","2026-09-29T12:00:00Z","2026-09-30T00:00:00Z"));
        var before=run(request("2026-10-02T00:15:00Z"));
        events.set(0,event("moved","2026-10-03T12:00:00Z","2026-10-04T00:00:00Z"));
        var next=next("2026-10-05T00:15:00Z",before); capture.capture(next);
        assertTrue(assertThrows(IOException.class,()->resolver.resolve(spark,next)).getMessage().contains("MOVE_REQUIRES_EXPLICIT_CLOSURE"));
        assertTrue(silver.list("current-month/state/"+operation(next)).isEmpty());
    }
    @Test void moveAcrossBothCapturedMonthsRetainsRevisionHistoryAndOldMonthEvidence() throws Exception {
        events.add(event("moved","2026-09-29T12:00:00Z","2026-09-30T00:00:00Z"));
        var before=run(request("2026-10-01T00:15:00Z"));
        events.set(0,event("moved","2026-10-01T12:00:00Z","2026-10-02T00:00:00Z"));
        var after=run(next("2026-10-02T00:15:00Z",before));
        assertEquals(1,after.path("reconciliation").path("current").asInt()); assertEquals(1,after.path("reconciliation").path("history").asInt());
        assertEquals(JSON.readTree("[\"2026-09\",\"2026-10\"]"),after.path("affected_months"));
    }
    @Test void emptyCaptureNeedsExplicitProofAndDoesNotBecomeComplete() throws Exception {
        var result=run(request("2026-10-05T00:15:00Z")); assertEquals(0,result.path("reconciliation").path("current").asInt());
        assertEquals(COVERAGE,result.path("coverage_mode").asText());
        var registry=JSON.readTree(silver.read("current-month/state/"+result.path("operation_id").asText()+"/registry.json"));
        assertEquals("2026-10-05T00:00:00Z",registry.path("month_coverage").path("2026-10").path("event_end_utc_exclusive").asText());
        assertFalse(registry.has("complete_months"));
    }
    @Test void invalidBusinessFieldsAndOutsideWindowNeverProduceSilverReady() throws Exception {
        events.add(event("bad","2026-10-02T12:00:00Z","2026-10-03T00:00:00Z"));
        ((ObjectNode)events.get(0).path("geometry")).put("coordinates","invalid");
        var req=request("2026-10-05T00:15:00Z"); capture.capture(req);
        assertThrows(IOException.class,()->resolver.resolve(spark,req)); assertTrue(silver.list("").isEmpty());
        var unfiltered=new CurrentMonthCapture(bronze,UsgsRequestConfig.fromEnvironment(environment()),q->response(q,List.of(event("out","2026-10-06T00:00:00Z","2026-10-06T01:00:00Z"))),CLOCK,()->{});
        var out=request("2026-10-07T00:15:00Z"); unfiltered.capture(out);
        assertThrows(IOException.class,()->new CurrentMonthSilverResolver(silver,unfiltered,CLOCK,()->{}).resolve(spark,out));
        assertTrue(silver.list("").isEmpty());
    }
    @Test void forgedParentAndCorruptParquetCannotBeReused() throws Exception {
        var first=run(request("2026-10-05T00:15:00Z")); var forged=next("2026-10-06T00:15:00Z",first);
        ((ObjectNode)forged.path("baseline")).put("registry_sha256","a".repeat(64)); capture.capture(forged);
        assertThrows(IOException.class,()->resolver.resolve(spark,forged));
        var file=silver.list("current-month/bundles/"+first.path("operation_id").asText()+"/").stream().filter(k->k.endsWith(".parquet")).findFirst().orElseThrow();
        silver.put(file,"corrupt".getBytes(),"application/octet-stream");
        assertThrows(IOException.class,()->resolver.verify(first.path("operation_id").asText(),first.path("registry_sha256").asText()));
    }
    @Test void lossOfLeasePreventsAnySilverWriteAndStoreRejectsDestructiveMethods() throws Exception {
        var req=request("2026-10-05T00:15:00Z"); capture.capture(req);
        var lost=new CurrentMonthSilverResolver(silver,capture,CLOCK,()->{throw new IllegalStateException("LEASE_LOST");});
        assertThrows(IllegalStateException.class,()->lost.resolve(spark,req)); assertTrue(silver.list("").isEmpty());
        var scoped=new CurrentMonthSilverResolver.OperationalStore(silver);
        assertThrows(IOException.class,()->scoped.deletePrefix("")); assertThrows(IOException.class,()->scoped.move("a","b"));
    }
}
