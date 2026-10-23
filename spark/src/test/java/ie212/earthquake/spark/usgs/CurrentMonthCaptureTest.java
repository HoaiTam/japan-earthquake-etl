package ie212.earthquake.spark.usgs;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import static ie212.earthquake.spark.orchestration.CurrentMonthContract.*;
import static org.junit.jupiter.api.Assertions.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.orchestration.CurrentMonthContract;
import java.io.IOException;
import java.net.http.HttpHeaders;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

public class CurrentMonthCaptureTest {
    @TempDir Path temporary;
    public static final Clock CLOCK=Clock.fixed(Instant.parse("2026-10-22T01:00:00Z"),ZoneOffset.UTC);
    public static ObjectNode request(String end) {
        var r=JSON.createObjectNode(); r.put("contract_version",COMPONENT); r.putNull("baseline");
        var c=r.putObject("run_context"); c.put("dag_id","orc_02_daily_sources"); c.put("run_type","scheduled"); c.putObject("conf");
        Instant b=Instant.parse(end); c.put("data_interval_end",end); c.put("data_interval_start",b.minus(Duration.ofDays(1)).toString());
        c.put("run_id","scheduled__"+b.minus(Duration.ofDays(1))); return r;
    }
    public static Map<String,String> environment() {
        return Map.ofEntries(Map.entry("USGS_API_BASE_URL","https://earthquake.usgs.gov/fdsnws/event/1/query"),
                Map.entry("USGS_MIN_LATITUDE","20"),Map.entry("USGS_MAX_LATITUDE","50"),Map.entry("USGS_MIN_LONGITUDE","120"),Map.entry("USGS_MAX_LONGITUDE","155"),
                Map.entry("USGS_SEED_START_UTC","2023-01-01T00:00:00Z"),Map.entry("PIPELINE_OVERLAP_DAYS","3"),Map.entry("USGS_MAX_WINDOW_DAYS","31"),
                Map.entry("USGS_REQUEST_LIMIT","20000"),Map.entry("USGS_HTTP_TIMEOUT_MS","60000"),Map.entry("USGS_HTTP_MAX_ATTEMPTS","1"),
                Map.entry("USGS_HTTP_INITIAL_BACKOFF_MS","0"),Map.entry("USGS_HTTP_MAX_BACKOFF_MS","0"),Map.entry("USGS_MAX_RESPONSE_BYTES","1048576"),Map.entry("USGS_EVENT_TYPE","earthquake"));
    }
    public static ObjectNode event(String id,String time,String updated) {
        var f=JSON.createObjectNode(); f.put("type","Feature"); f.put("id",id);
        var p=f.putObject("properties"); p.put("time",Instant.parse(time).toEpochMilli()); p.put("updated",Instant.parse(updated).toEpochMilli());
        p.put("mag",4.1); p.put("magType","mww"); p.put("type","earthquake"); p.put("status","reviewed");
        p.put("place","Offshore Japan"); p.put("url","https://earthquake.usgs.gov/earthquakes/eventpage/"+id);
        p.put("tsunami",0); p.put("sig",20);
        var g=f.putObject("geometry"); g.put("type","Point"); g.putArray("coordinates").add(140).add(35).add(10); return f;
    }
    public static UsgsHttpResponse response(UsgsRequest query,List<? extends JsonNode> events) throws IOException {
        var b=JSON.createObjectNode(); b.put("type","FeatureCollection"); var features=b.putArray("features");
        for(var f:events) features.add(f); b.putObject("metadata").put("count",events.size());
        boolean count=query.uri().getPath().endsWith("/count");
        return new UsgsHttpResponse("fixture",query,200,HttpHeaders.of(Map.of("content-type",List.of(count?"text/plain":"application/geo+json")),(a,v)->true),
                count?Integer.toString(events.size()).getBytes():JSON.writeValueAsBytes(b),1,Duration.ZERO);
    }
    public static CurrentMonthCapture.Fetcher fetcher(List<? extends JsonNode> events,AtomicInteger calls) {
        return q->{ calls.incrementAndGet(); var selected=new ArrayList<JsonNode>();
            for(var f:events) { Instant t=Instant.ofEpochMilli(f.path("properties").path("time").asLong());
                if(!t.isBefore(q.windowStartUtc()) && t.isBefore(q.windowEndExclusiveUtc())) selected.add(f); }
            return response(q,selected);
        };
    }
    @Test void bootstrapPinsWholeMonthNotThreeDayDeltaAndRerunNeverRefetches() throws Exception {
        var store=new FileBronzeObjectStore(temporary); var calls=new AtomicInteger();
        var c=new CurrentMonthCapture(store,UsgsRequestConfig.fromEnvironment(environment()),fetcher(List.of(),calls),CLOCK,()->{});
        var req=request("2026-10-22T00:15:00Z"); var result=c.capture(req);
        assertEquals(21,result.path("parts").size()); assertEquals(63,calls.get());
        assertEquals("2026-10-01T00:00:00Z",result.path("parts").get(0).path("window_start_utc").asText());
        assertEquals("2026-10-22T01:00:00Z",result.path("observation_cutoff_utc").asText());
        assertFalse(result.path("published").asBoolean(true)); assertEquals(COVERAGE,result.path("coverage_mode").asText());
        assertEquals(result,c.capture(req)); assertEquals(63,calls.get());
    }
    @Test void interruptedCaptureReusesCompletedPartsWithoutMarkingPartialReady() throws Exception {
        var store=new FileBronzeObjectStore(temporary); var calls=new AtomicInteger(); var req=request("2026-10-05T00:15:00Z");
        var c=new CurrentMonthCapture(store,UsgsRequestConfig.fromEnvironment(environment()),q->{
            if(calls.incrementAndGet()==5) throw new IOException("fixture-interrupt"); return response(q,List.of());
        },CLOCK,()->{});
        assertThrows(IOException.class,()->c.capture(req)); assertFalse(store.exists(CurrentMonthCapture.root(operation(req))+"/_SUCCESS"));
        var resumed=new AtomicInteger(); var complete=new CurrentMonthCapture(store,UsgsRequestConfig.fromEnvironment(environment()),fetcher(List.of(),resumed),CLOCK,()->{});
        assertEquals(4,complete.capture(req).path("parts").size()); assertEquals(9,resumed.get());
    }
    @Test void saturatedPartIsSplitAndOriginalRawRemainsOutsideLeafSelection() throws Exception {
        var env=new HashMap<>(environment()); env.put("USGS_REQUEST_LIMIT","2"); var calls=new AtomicInteger();
        var c=new CurrentMonthCapture(new FileBronzeObjectStore(temporary),UsgsRequestConfig.fromEnvironment(env),q->{
            calls.incrementAndGet(); return response(q,Duration.between(q.windowStartUtc(),q.windowEndExclusiveUtc()).equals(Duration.ofDays(1))
                    ?List.of(JSON.createObjectNode().put("type","Feature"),JSON.createObjectNode().put("type","Feature")):List.of());
        },CLOCK,()->{});
        var cap=c.capture(request("2023-01-02T00:15:00Z")); assertEquals(9,calls.get()); assertEquals(2,cap.path("parts").size());
        try(var files=Files.walk(temporary)) { assertEquals(3,files.filter(p->p.getFileName().toString().equals("response.geojson")).count()); }
    }
    @Test void tamperedRawManifestMarkerAndChangedRequestAreRejected() throws Exception {
        for(String mode:List.of("raw","manifest","marker","context")) {
            var dir=temporary.resolve(mode); var store=new FileBronzeObjectStore(dir); var req=request("2023-01-02T00:15:00Z");
            var c=new CurrentMonthCapture(store,UsgsRequestConfig.fromEnvironment(environment()),fetcher(List.of(),new AtomicInteger()),CLOCK,()->{});
            var cap=c.capture(req); var pin=cap.path("parts").get(0).path("pin");
            if(mode.equals("context")) req.putObject("baseline").put("operation_id","cm1-"+"a".repeat(40)).put("registry_sha256","b".repeat(64));
            else if(mode.equals("marker")) Files.writeString(dir.resolve(CurrentMonthCapture.root(operation(req))+"/_SUCCESS"),"corrupt");
            else {
                var manifest=JSON.readTree(store.read(key(pin.path("manifest_uri").asText(),store::uriForKey)));
                var path=Path.of(java.net.URI.create(mode.equals("raw")?manifest.path("raw_object_uri").asText():pin.path("manifest_uri").asText()));
                Files.writeString(path,"corrupt");
            }
            assertThrows(Exception.class,()->c.capture(req),mode);
        }
    }
    @Test void rejectsUnknownFlagsWrongPolicyManualFutureAndOpenIntervals() throws Exception {
        var good=request("2026-10-22T00:15:00Z");
        for(String mode:List.of("manual","conf","future","short","flag","missing-baseline")) {
            var r=good.deepCopy(); var ctx=(ObjectNode)r.path("run_context");
            switch(mode) {
                case "manual"->ctx.put("run_type","manual"); case "conf"->ctx.with("conf").put("complete",true);
                case "future"->ctx.put("data_interval_end","2027-10-22T00:15:00Z");
                case "short"->ctx.put("data_interval_start","2026-10-21T12:00:00Z");
                case "flag"->r.put("research_ready",true); case "missing-baseline"->r.remove("baseline");
            }
            assertThrows(Exception.class,()->CurrentMonthContract.validate(r,CLOCK),mode);
        }
        var env=new HashMap<>(environment()); env.put("USGS_MIN_LATITUDE","21");
        var c=new CurrentMonthCapture(new FileBronzeObjectStore(temporary),UsgsRequestConfig.fromEnvironment(env),q->{throw new AssertionError();},CLOCK,()->{});
        assertThrows(IOException.class,()->c.capture(good)); assertFalse(Files.exists(temporary.resolve("bronze")));
    }
    @Test void rolloverLeapYearAndSeedScopesAgreeWithMetadataPlanner() throws Exception {
        assertEquals(List.of("2026-09","2026-10"),scopes(request("2026-10-02T00:15:00Z")).stream().map(Scope::month).toList());
        assertEquals(List.of("2026-09"),scopes(request("2026-10-01T00:15:00Z")).stream().map(Scope::month).toList());
        assertEquals(Instant.parse("2024-03-01T00:00:00Z"),scopes(request("2024-03-01T00:15:00Z")).get(0).end());
        assertEquals(USGS_START,scopes(request("2023-01-02T00:15:00Z")).get(0).start());
    }
    @Test void sourceCountMismatchDuplicateJsonAndLeaseFailureCannotMarkReady() throws Exception {
        for(String mode:List.of("count","json","lease")) {
            var store=new FileBronzeObjectStore(temporary.resolve(mode)); var req=request("2026-10-02T00:15:00Z");
            var c=new CurrentMonthCapture(store,UsgsRequestConfig.fromEnvironment(environment()),q->{
                if(q.uri().getPath().endsWith("/count")) return response(q,List.of());
                byte[] raw=(mode.equals("count")?"{\"type\":\"FeatureCollection\",\"metadata\":{\"count\":1},\"features\":[]}":
                        "{\"type\":\"FeatureCollection\",\"metadata\":{\"count\":0,\"count\":1},\"features\":[]}").getBytes();
                return new UsgsHttpResponse("bad",q,200,HttpHeaders.of(Map.of(),(a,b)->true),raw,1,Duration.ZERO);
            },CLOCK,()->{if(mode.equals("lease")) throw new IllegalStateException("LEASE_LOST");});
            assertThrows(Exception.class,()->c.capture(req)); assertFalse(store.exists(CurrentMonthCapture.root(operation(req))+"/_SUCCESS"));
        }
    }
    @Test void missingGeoJsonCountUsesPinnedIndependentCountResponses() throws Exception {
        var store=new FileBronzeObjectStore(temporary); var req=request("2023-01-02T00:15:00Z");
        var c=new CurrentMonthCapture(store,UsgsRequestConfig.fromEnvironment(environment()),q->{
            if(q.uri().getPath().endsWith("/count")) return response(q,List.of());
            var b=JSON.createObjectNode(); b.put("type","FeatureCollection"); b.putObject("metadata").put("api","2.8.2"); b.putArray("features");
            return new UsgsHttpResponse("no-count",q,200,HttpHeaders.of(Map.of(),(a,v)->true),JSON.writeValueAsBytes(b),1,Duration.ZERO);
        },CLOCK,()->{});
        var cap=c.capture(req); var pin=cap.path("parts").get(0).path("pin");
        assertEquals(0,pin.path("count_before").path("count").asInt(-1)); assertEquals(0,pin.path("count_after").path("count").asInt(-1));
        assertTrue(pin.path("count_before").path("request_url").asText().contains("/count?"));
        assertFalse(JSON.readTree(c.read(pin).raw()).path("metadata").has("count"));
        Files.writeString(Path.of(java.net.URI.create(pin.path("count_before").path("raw_uri").asText())),"1");
        assertThrows(IOException.class,()->c.verify(req));
    }
    @Test void countDriftAndSaturatedMinimumChunkCannotMarkCaptureReady() throws Exception {
        var req=request("2023-01-02T00:15:00Z"); var store=new FileBronzeObjectStore(temporary.resolve("drift")); var calls=new AtomicInteger();
        var drift=new CurrentMonthCapture(store,UsgsRequestConfig.fromEnvironment(environment()),q->{
            if(q.uri().getPath().endsWith("/count")) return response(q,calls.incrementAndGet()==1?List.of():List.of(JSON.createObjectNode()));
            return response(q,List.of());
        },CLOCK,()->{});
        assertThrows(IOException.class,()->drift.capture(req)); assertFalse(store.exists(CurrentMonthCapture.root(operation(req))+"/_SUCCESS"));
        var env=new HashMap<>(environment()); env.put("USGS_REQUEST_LIMIT","1");
        var bounded=new CurrentMonthCapture(new FileBronzeObjectStore(temporary.resolve("budget")),UsgsRequestConfig.fromEnvironment(env),
                q->response(q,List.of(JSON.createObjectNode().put("type","Feature"))),CLOCK,()->{});
        String error=assertThrows(IOException.class,()->bounded.capture(req)).getMessage();
        assertTrue(error.equals("ASOF_REQUEST_BUDGET") || error.equals("ASOF_SATURATED_MINIMUM_CHUNK"));
    }
    @Test void splitTreeStopsAtNodeBudgetWithoutTruncatingLaterDays() throws Exception {
        var env=new HashMap<>(environment()); env.put("USGS_REQUEST_LIMIT","2");
        var store=new FileBronzeObjectStore(temporary); var req=request("2026-10-04T00:15:00Z");
        var bounded=new CurrentMonthCapture(store,UsgsRequestConfig.fromEnvironment(env),q->response(q,
                Duration.between(q.windowStartUtc(),q.windowEndExclusiveUtc()).compareTo(Duration.ofHours(1))>0
                        ?List.of(JSON.createObjectNode().put("type","Feature"),JSON.createObjectNode().put("type","Feature")):List.of()),CLOCK,()->{});
        assertEquals("ASOF_REQUEST_BUDGET",assertThrows(IOException.class,()->bounded.capture(req)).getMessage());
        assertFalse(store.exists(CurrentMonthCapture.root(operation(req))+"/_SUCCESS"));
    }
}
