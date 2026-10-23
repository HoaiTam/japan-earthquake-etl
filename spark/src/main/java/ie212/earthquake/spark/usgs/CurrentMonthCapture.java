package ie212.earthquake.spark.usgs;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import static ie212.earthquake.spark.orchestration.CurrentMonthContract.*;
import ie212.earthquake.spark.orchestration.CurrentMonthContract;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.*;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.charset.StandardCharsets;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.time.*;
import java.util.*;

/** Bounded raw HTTP captures, immutable resumable parts and marker-last observed coverage. */
public final class CurrentMonthCapture {
    @FunctionalInterface public interface Fetcher { UsgsHttpResponse fetch(UsgsRequest request) throws IOException; }
    public record Input(JsonNode pin, JsonNode manifest, byte[] raw) { }
    private final BronzeObjectStore store;
    private final UsgsRequestConfig config;
    private final Fetcher fetcher;
    private final Clock clock;
    private final Runnable guard;
    public CurrentMonthCapture(BronzeObjectStore store,UsgsRequestConfig config,Fetcher fetcher,Clock clock,Runnable guard) {
        this.store=Objects.requireNonNull(store); this.config=Objects.requireNonNull(config);
        this.fetcher=Objects.requireNonNull(fetcher); this.clock=Objects.requireNonNull(clock); this.guard=Objects.requireNonNull(guard);
    }
    public static String root(String operation) {
        if(operation==null || !operation.matches("cm1-[a-f0-9]{40}")) throw new IllegalArgumentException("ASOF_SAFE_OPERATION_REQUIRED");
        return "bronze/usgs/current-month/"+operation;
    }
    private JsonNode policy() throws IOException {
        require(config.endpoint().toString().equals("https://earthquake.usgs.gov/fdsnws/event/1/query")
                && config.minLatitude()==20 && config.maxLatitude()==50 && config.minLongitude()==120 && config.maxLongitude()==155
                && config.seedStartUtc().equals(USGS_START) && config.revisionWindowDays()==3,"ASOF_FIXED_SOURCE_POLICY_REQUIRED");
        var p=JSON.createObjectNode(); p.put("endpoint",config.endpoint().toString()); p.put("request_limit",config.requestLimit());
        p.put("timeout_ms",(int)config.requestTimeout().toMillis()); p.put("max_response_bytes",config.maxResponseBytes());
        p.put("chunk_days",1); p.put("count_strategy","PRE_POST_COUNT_TEXT_V1"); return p;
    }
    public JsonNode capture(JsonNode request) throws IOException {
        CurrentMonthContract.validate(request,clock); guard.run(); String op=operation(request), root=root(op);
        JsonNode policy=policy();
        if(store.exists(root+"/_SUCCESS")) return verify(request);
        JsonNode identity;
        if(store.exists(root+"/identity.json")) {
            identity=JSON.readTree(store.read(root+"/identity.json"));
            require(identity.path("request").equals(request) && identity.path("query_policy").equals(policy),"ASOF_CAPTURE_IDENTITY_CONFLICT");
        } else {
            var i=JSON.createObjectNode(); i.set("request",canonical(request)); i.set("query_policy",policy);
            i.put("capture_started_at_utc",clock.instant().toString()); immutable(root+"/identity.json",i); identity=i;
        }
        var started=utc(identity,"capture_started_at_utc");
        require(!started.isBefore(cutoff(request)) && !started.isAfter(clock.instant()),"ASOF_CAPTURE_CLOCK_INVALID");
        var parts=JSON.createArrayNode(); int[] budget={0}; long[] bytes={0}; long[] rows={0};
        for(var scope:scopes(request)) {
            Instant cursor=scope.start();
            while(cursor.isBefore(scope.end())) {
                Instant end=cursor.plus(Duration.ofDays(1)); if(end.isAfter(scope.end())) end=scope.end();
                collect(request,started,cursor,end,parts,budget,bytes,rows); cursor=end;
            }
        }
        var selection=flags(); selection.put("status","ObservedCaptureReady"); selection.put("operation_id",op);
        selection.set("request",canonical(request)); selection.put("request_sha256",digest(request)); selection.set("query_policy",policy);
        selection.put("event_cutoff_utc",cutoff(request).toString()); selection.put("capture_started_at_utc",started.toString());
        Instant completed=started; for(var part:parts) if(utc(part,"capture_completed_at_utc").isAfter(completed)) completed=utc(part,"capture_completed_at_utc");
        selection.put("observation_cutoff_utc",completed.toString()); selection.set("parts",parts);
        // Validate all exact raw/manifest pins and contiguous monthly coverage before marking ready.
        validateSelection(request,selection);
        immutable(root+"/selection.json",selection);
        immutableBytes(root+"/_SUCCESS",sha(store.read(root+"/selection.json")).getBytes(StandardCharsets.US_ASCII));
        return verify(request);
    }
    private void collect(JsonNode request,Instant started,Instant from,Instant to,ArrayNode parts,int[] budget,long[] bytes,long[] rows) throws IOException {
        guard.run(); require(++budget[0]<=MAX_PARTS,"ASOF_REQUEST_BUDGET");
        var planned=new UsgsRequestBuilder(config).buildResolvedPlan(from,to,from,to).requests();
        require(planned.size()==1,"ASOF_SINGLE_CHUNK_REQUIRED"); var query=planned.get(0);
        String key=root(operation(request))+"/parts/"+sha(query.uri().toString().getBytes(StandardCharsets.UTF_8))+".json";
        JsonNode part;
        if(store.exists(key)) part=JSON.readTree(store.read(key));
        else {
            Instant begin=clock.instant(); var before=count(query,operation(request),"before");
            var response=fetcher.fetch(query); var after=count(query,operation(request),"after"); Instant finish=clock.instant();
            require(response.request().equals(query) && response.statusCode()==200 && response.bodyLengthBytes()<=32L*1024*1024,"ASOF_BOUNDED_EXACT_RESPONSE_REQUIRED");
            require(!begin.isBefore(started) && !finish.isBefore(begin),"ASOF_CAPTURE_CLOCK_INVALID");
            String ingest=operation(request)+"-"+sha(query.uri().toString().getBytes(StandardCharsets.UTF_8)).substring(0,20);
            var written=new UsgsBronzeWriter(store).write(new UsgsBronzeWriteRequest(response,ingest,1,
                    LocalDate.ofInstant(started,ZoneOffset.UTC),finish,false,operation(request),(int)config.requestTimeout().toMillis()));
            require(written.ready(),"ASOF_SOURCE_REJECTED");
            var p=JSON.createObjectNode(); p.put("window_start_utc",from.toString()); p.put("window_end_utc",to.toString());
            p.put("capture_started_at_utc",begin.toString()); p.put("capture_completed_at_utc",finish.toString());
            var pin=p.putObject("pin"); pin.put("source_system","USGS"); pin.put("manifest_uri",store.uriForKey(written.manifestKey()));
            pin.put("manifest_sha256",sha(store.read(written.manifestKey()))); pin.put("sha256",written.sha256());
            pin.put("window_start_utc",from.toString()); pin.put("window_end_utc",to.toString());
            pin.set("count_before",before); pin.set("count_after",after);
            p.put("saturated",before.path("count").asInt()>=query.limit()); immutable(key,p); part=p;
        }
        require(utc(part,"window_start_utc").equals(from) && utc(part,"window_end_utc").equals(to),"ASOF_PART_SCOPE_CHANGED");
        var input=read(part.path("pin"));
        require(input.manifest().path("request").path("url").asText().equals(query.uri().toString())
                && utc(input.manifest(),"retrieved_at_utc").equals(utc(part,"capture_completed_at_utc"))
                && part.path("saturated").isBoolean(),"ASOF_PART_IDENTITY_CHANGED");
        bytes[0]+=input.raw().length; require(bytes[0]<=MAX_BYTES,"ASOF_CAPTURE_BYTE_BUDGET");
        int count=input.manifest().path("record_count_estimate").asInt();
        int total=part.path("pin").path("count_before").path("count").asInt();
        require(part.path("saturated").asBoolean()==(total>=query.limit()),"ASOF_SATURATION_CHANGED");
        if(total>=query.limit()) {
            long millis=Duration.between(from,to).toMillis(); require(millis>=2000,"ASOF_SATURATED_MINIMUM_CHUNK");
            Instant split=from.plusMillis(millis/2);
            collect(request,started,from,split,parts,budget,bytes,rows); collect(request,started,split,to,parts,budget,bytes,rows);
        } else {
            require(total==count,"ASOF_SOURCE_COUNT_CHANGED");
            rows[0]+=count; require(rows[0]<=MAX_ROWS,"ASOF_CAPTURE_ROW_BUDGET"); parts.add(part);
        }
    }
    public JsonNode verify(JsonNode request) throws IOException {
        CurrentMonthContract.validate(request,clock); guard.run(); String root=root(operation(request));
        byte[] bytes=store.read(root+"/selection.json"); require(bytes.length<=256*1024,"ASOF_SELECTION_SIZE_BUDGET");
        require(Arrays.equals(store.read(root+"/_SUCCESS"),sha(bytes).getBytes(StandardCharsets.US_ASCII)),"ASOF_CAPTURE_MARKER_MISMATCH");
        JsonNode selection=JSON.readTree(bytes); validateSelection(request,selection); return selection;
    }
    public JsonNode request(String operation) throws IOException {
        guard.run();
        byte[] bytes=store.read(root(operation)+"/selection.json");
        require(bytes.length<=256*1024,"ASOF_SELECTION_SIZE_BUDGET");
        return JSON.readTree(bytes).path("request");
    }
    private void validateSelection(JsonNode request,JsonNode selection) throws IOException {
        require(selection.path("request").equals(request) && text(selection,"request_sha256").equals(digest(request))
                && operation(request).equals(text(selection,"operation_id")) && selection.path("query_policy").equals(policy())
                && "ObservedCaptureReady".equals(text(selection,"status")),"ASOF_SELECTION_IDENTITY_CHANGED");
        for(var it=flags().fields();it.hasNext();) { var f=it.next(); require(f.getValue().equals(selection.path(f.getKey())),"ASOF_POLICY_PROMOTION_BLOCKED"); }
        require(utc(selection,"event_cutoff_utc").equals(cutoff(request)) && selection.path("parts").isArray()
                && !selection.path("parts").isEmpty() && selection.path("parts").size()<=MAX_PARTS,"ASOF_SELECTION_SCOPE_REQUIRED");
        Instant begin=utc(selection,"capture_started_at_utc"), end=utc(selection,"observation_cutoff_utc"), actualEnd=begin;
        require(!begin.isBefore(cutoff(request)) && !end.isBefore(begin) && !end.isAfter(clock.instant()),"ASOF_CAPTURE_CLOCK_INVALID");
        int index=0; long bytes=0,rows=0;
        for(var scope:scopes(request)) {
            Instant cursor=scope.start();
            while(cursor.isBefore(scope.end())) {
                require(index<selection.path("parts").size(),"ASOF_MISSING_BOOTSTRAP_COVERAGE");
                JsonNode part=selection.path("parts").get(index++); Instant a=utc(part,"window_start_utc"), b=utc(part,"window_end_utc");
                require(a.equals(cursor) && b.isAfter(a) && !b.isAfter(scope.end()) && Duration.between(a,b).compareTo(Duration.ofDays(1))<=0,"ASOF_CAPTURE_GAP_OR_OVERLAP");
                var input=read(part.path("pin")); var planned=new UsgsRequestBuilder(config).buildResolvedPlan(a,b,a,b).requests().get(0);
                require(planned.uri().toString().equals(input.manifest().path("request").path("url").asText())
                        && a.equals(utc(part.path("pin"),"window_start_utc")) && b.equals(utc(part.path("pin"),"window_end_utc"))
                        && part.path("saturated").isBoolean() && !part.path("saturated").asBoolean(),"ASOF_EXACT_LEAF_REQUIRED");
                Instant pa=utc(part,"capture_started_at_utc"), pb=utc(part,"capture_completed_at_utc");
                require(!pa.isBefore(begin) && !pb.isBefore(pa) && !pb.isAfter(end)
                        && pb.equals(utc(input.manifest(),"retrieved_at_utc")),"ASOF_PART_CLOCK_CHANGED");
                if(pb.isAfter(actualEnd)) actualEnd=pb;
                int count=input.manifest().path("record_count_estimate").asInt();
                require(count<config.requestLimit() && count==part.path("pin").path("count_before").path("count").asInt(),"ASOF_TRUNCATED_LEAF");
                bytes+=input.raw().length; rows+=count; require(bytes<=MAX_BYTES && rows<=MAX_ROWS,"ASOF_CAPTURE_BUDGET"); cursor=b;
            }
        }
        require(index==selection.path("parts").size() && actualEnd.equals(end),"ASOF_UNEXPECTED_CAPTURE_PARTS_OR_CLOCK");
    }
    public Input read(JsonNode pin) throws IOException {
        guard.run(); fields(pin,Set.of("source_system","manifest_uri","manifest_sha256","sha256","window_start_utc","window_end_utc","count_before","count_after"));
        require("USGS".equals(text(pin,"source_system")),"ASOF_USGS_ONLY");
        byte[] bytes=store.read(key(text(pin,"manifest_uri"),store::uriForKey));
        require(bytes.length<=65536 && sha(bytes).equals(text(pin,"manifest_sha256")),"ASOF_MANIFEST_PIN_CHANGED");
        var m=JSON.readTree(bytes); require("1.0".equals(text(m,"manifest_version")) && "BronzeReady".equals(text(m,"bronze_status"))
                && "USGS".equals(text(m,"source_system")),"ASOF_BRONZE_READY_REQUIRED");
        for(String flag:List.of("object_write_completed","raw_readback_verified","checksum_verified","source_structure_valid","manifest_consistent"))
            require(m.path("validation").path(flag).isBoolean() && m.path("validation").path(flag).asBoolean(),"ASOF_BRONZE_VALIDATION_REQUIRED");
        require(m.path("content_length_bytes").isIntegralNumber() && m.path("content_length_bytes").canConvertToInt()
                && m.path("content_length_bytes").asInt()>0 && m.path("content_length_bytes").asInt()<=32*1024*1024,"ASOF_BOUNDED_RAW_REQUIRED");
        String rawKey=key(text(m,"raw_object_uri"),store::uriForKey); require(rawKey.equals(text(m,"raw_object_key")),"ASOF_RAW_KEY_CHANGED");
        byte[] raw=store.read(rawKey); require(raw.length==m.path("content_length_bytes").asInt()
                && sha(raw).equals(text(pin,"sha256")) && sha(raw).equals(text(m,"sha256")),"ASOF_RAW_PIN_CHANGED");
        JsonNode body=JSON.readTree(raw); var valid=new UsgsGeoJsonValidator().validate(raw);
        require(valid.valid() && m.path("record_count_estimate").isIntegralNumber() && m.path("record_count_estimate").canConvertToInt()
                && valid.featureCount()==m.path("record_count_estimate").asInt(-1),"ASOF_SOURCE_COUNT_CHANGED");
        if(body.path("metadata").has("count")) require(body.path("metadata").path("count").isIntegralNumber()
                && body.path("metadata").path("count").canConvertToInt()
                && body.path("metadata").path("count").asInt(-1)==valid.featureCount(),"ASOF_SOURCE_COUNT_CHANGED");
        require("[start,end)".equals(m.path("data_interval").path("interval_semantics").asText())
                && utc(pin,"window_start_utc").equals(utc(m.path("data_interval"),"window_start_utc"))
                && utc(pin,"window_end_utc").equals(utc(m.path("data_interval"),"window_end_utc")),"ASOF_INPUT_INTERVAL_CHANGED");
        var query=new UsgsRequestBuilder(config).buildResolvedPlan(utc(pin,"window_start_utc"),utc(pin,"window_end_utc"),
                utc(pin,"window_start_utc"),utc(pin,"window_end_utc")).requests();
        require(query.size()==1 && query.get(0).uri().toString().equals(m.path("request").path("url").asText()),"ASOF_EXACT_QUERY_REQUIRED");
        String operation=text(m,"logical_run_key"), querySha=sha(query.get(0).uri().toString().getBytes(StandardCharsets.UTF_8));
        require(text(m,"run_id").equals(operation+"-"+querySha.substring(0,20)),"ASOF_INGEST_OWNER_CHANGED");
        int before=verifyCount(pin.path("count_before"),countRequest(query.get(0)).uri(),root(operation)+"/counts/"+querySha+"-before.txt");
        int after=verifyCount(pin.path("count_after"),countRequest(query.get(0)).uri(),root(operation)+"/counts/"+querySha+"-after.txt");
        require(before==after && before>=valid.featureCount(),"ASOF_SOURCE_COUNT_CHANGED");
        return new Input(pin.deepCopy(),m,raw);
    }
    private static UsgsRequest countRequest(UsgsRequest query) {
        var values=new TreeMap<String,String>();
        for(String part:query.uri().getRawQuery().split("&")) {
            String[] pair=part.split("=",2); values.put(URLDecoder.decode(pair[0],StandardCharsets.UTF_8),URLDecoder.decode(pair[1],StandardCharsets.UTF_8));
        }
        values.put("format","text"); values.remove("limit"); values.remove("offset");
        var parameters=new ArrayList<String>(); values.forEach((k,v)->parameters.add(URLEncoder.encode(k,StandardCharsets.UTF_8)+"="+URLEncoder.encode(v,StandardCharsets.UTF_8)));
        URI uri=URI.create("https://earthquake.usgs.gov/fdsnws/event/1/count?"+String.join("&",parameters));
        return new UsgsRequest(uri,query.windowStartUtc(),query.windowEndExclusiveUtc(),query.limit(),1);
    }
    private ObjectNode count(UsgsRequest query,String op,String slot) throws IOException {
        guard.run(); var expected=countRequest(query); var response=fetcher.fetch(expected);
        require(response.request().equals(expected) && response.statusCode()==200 && response.bodyLengthBytes()<=64,"ASOF_EXACT_COUNT_RESPONSE_REQUIRED");
        byte[] raw=response.body(); int value=countValue(raw);
        String key=root(op)+"/counts/"+sha(query.uri().toString().getBytes(StandardCharsets.UTF_8))+"-"+slot+".txt";
        immutableBytes(key,raw);
        var proof=JSON.createObjectNode(); proof.put("request_url",expected.uri().toString()); proof.put("http_status",200);
        proof.put("raw_uri",store.uriForKey(key)); proof.put("sha256",sha(raw)); proof.put("count",value); return proof;
    }
    private int verifyCount(JsonNode proof,URI query,String expectedKey) throws IOException {
        fields(proof,Set.of("request_url","http_status","raw_uri","sha256","count"));
        require(query.toString().equals(text(proof,"request_url")) && proof.path("http_status").isIntegralNumber() && proof.path("http_status").canConvertToInt()
                && proof.path("http_status").asInt()==200 && proof.path("count").isIntegralNumber()
                && proof.path("count").canConvertToInt() && proof.path("count").asInt()>=0,"ASOF_COUNT_PROOF_REQUIRED");
        require(store.uriForKey(expectedKey).equals(text(proof,"raw_uri")),"ASOF_COUNT_OWNER_CHANGED");
        byte[] raw=store.read(key(text(proof,"raw_uri"),store::uriForKey));
        require(raw.length<=64 && sha(raw).equals(text(proof,"sha256")) && countValue(raw)==proof.path("count").asInt(),"ASOF_COUNT_PIN_CHANGED");
        return proof.path("count").asInt();
    }
    private static int countValue(byte[] raw) throws IOException {
        String text=new String(raw,StandardCharsets.US_ASCII).trim();
        require(text.matches("[0-9]{1,10}") && Long.parseLong(text)<=Integer.MAX_VALUE,"ASOF_TYPED_SOURCE_COUNT_REQUIRED");
        return Integer.parseInt(text);
    }
    private void immutable(String key,JsonNode node) throws IOException { immutableBytes(key,JSON.writeValueAsBytes(canonical(node))); }
    private void immutableBytes(String key,byte[] bytes) throws IOException {
        guard.run();
        if(!store.exists(key)) try { store.putIfAbsent(key,bytes,"application/json"); } catch(FileAlreadyExistsException ignored) { }
        require(Arrays.equals(store.read(key),bytes),"ASOF_IMMUTABLE_CAPTURE_CONFLICT");
    }
}
