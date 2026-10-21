package ie212.earthquake.spark.orchestration;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import ie212.earthquake.spark.gold.*;
import ie212.earthquake.spark.jma.*;
import ie212.earthquake.spark.silver.*;
import ie212.earthquake.spark.usgs.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.*;
import java.util.*;
import org.apache.spark.sql.SparkSession;

/** ORC-07 trusted data plane. Exact inputs only; every gate reads actual artifacts. */
public final class RealEtlAdapter {
    public static final String PROTOCOL="orc-07-v1";
    public static final List<String> PHASES=List.of("readiness","bronze","silver","gold","verify","publish");
    private final SparkSession spark;
    private final BronzeObjectStore bronze;
    private final SilverObjectStore silver;
    private final List<JmaArchiveEntry> inventory;
    private final GoldIcebergWriter gold;
    private final GoldPublisher publisher;
    private final Runnable lease;
    public RealEtlAdapter(SparkSession spark,BronzeObjectStore bronze,SilverObjectStore silver,
            List<JmaArchiveEntry> inventory,GoldIcebergWriter gold,GoldPublisher publisher,Runnable lease) {
        this.spark=spark; this.bronze=bronze; this.silver=silver; this.inventory=List.copyOf(inventory);
        this.gold=gold; this.publisher=publisher; this.lease=lease;
    }
    public static String operation(String dag,String run) {
        return "orc07-"+sha((dag+"|"+run).getBytes(StandardCharsets.UTF_8)).substring(0,40);
    }
    public static void validateContext(JsonNode context) throws IOException {
        var fields=new HashSet<String>(); context.fieldNames().forEachRemaining(fields::add);
        require(fields.equals(Set.of("contract_version","dag_id","run_id","operation_id","mode","trigger",
                "window_start_utc","window_end_utc","resolver_request","context_sha256","source_summary_sha256")),"EXACT_REAL_CONTEXT_REQUIRED");
        require(PROTOCOL.equals(text(context,"contract_version")) && "real".equals(text(context,"mode")),"REAL_PROTOCOL_REQUIRED");
        require(text(context,"dag_id").matches("[A-Za-z0-9_-]{1,128}") && text(context,"run_id").length()<=250,"EXACT_LEASE_OWNER_REQUIRED");
        require(Set.of("daily","manual").contains(text(context,"trigger")),"EXPLICIT_TRIGGER_REQUIRED");
        require(operation(text(context,"dag_id"),text(context,"run_id")).equals(text(context,"operation_id")),"OPERATION_ID_CHANGED");
        require(utc(context,"window_start_utc").isBefore(utc(context,"window_end_utc"))
                && !utc(context,"window_end_utc").isAfter(Instant.now()),"CLOSED_EXECUTION_INTERVAL_REQUIRED");
        require(context.path("source_summary_sha256").isNull() || text(context,"source_summary_sha256").matches("[a-f0-9]{64}"),"SOURCE_SUMMARY_PIN_REQUIRED");
        require(!"daily".equals(context.path("trigger").asText()) || !context.path("source_summary_sha256").isNull(),"DAILY_READINESS_PIN_REQUIRED");
        validate(context.path("resolver_request"));
        require(text(context,"operation_id").equals(text(context.path("resolver_request"),"run_id"))
                && text(context,"dag_id").equals(text(context.path("resolver_request"),"dag_id")),"RESOLVER_CONTEXT_CHANGED");
        var unsigned=context.deepCopy(); ((ObjectNode)unsigned).remove("context_sha256");
        require(digest(unsigned).equals(text(context,"context_sha256")),"REAL_CONTEXT_CHANGED");
        require(JSON.writeValueAsBytes(context).length<=65536,"BOUNDED_REAL_CONTEXT_REQUIRED");
    }
    public ObjectNode execute(String phase,JsonNode context,JsonNode upstream,Path work) throws Exception {
        validateContext(context); require(PHASES.contains(phase),"UNKNOWN_REAL_PHASE"); lease.run();
        var request=context.path("resolver_request"); String op=text(context,"operation_id"), root="real-etl/"+op;
        immutable(root+"/context.json",context);
        int index=PHASES.indexOf(phase);
        if(index==0) require(upstream==null || upstream.isNull(),"UNEXPECTED_REAL_UPSTREAM");
        else {
            require(upstream!=null && !upstream.isNull(),"REAL_UPSTREAM_REQUIRED");
            var stored=JSON.readTree(silver.read(root+"/"+PHASES.get(index-1)+"-receipt.json"));
            require(stored.equals(upstream) && text(context,"context_sha256").equals(stored.path("context_sha256").asText()),"REAL_UPSTREAM_CHANGED");
            artifact(stored);
        }
        JsonNode actual;
        var resolver=new FullMonthStateResolver(bronze,silver,inventory,gold::baselines,lease);
        if(index<=1) {
            var result=JSON.createObjectNode(); var inputs=result.putArray("bronze_inputs");
            var reader=new FullMonthBronzeReader(bronze,inventory);
            for(var pin:request.path("bronze_inputs")) {
                lease.run(); var input=reader.read(pin,utc(request,"source_cutoff_utc"));
                var item=inputs.addObject(); item.set("pin",pin); item.put("manifest_id",text(input.manifest(),"manifest_id"));
                item.put("raw_object_uri",text(input.manifest(),"raw_object_uri"));
                item.put("count",input.manifest().path("record_count_estimate").asLong());
                item.put("status",text(input.manifest(),"bronze_status"));
            }
            // A baseline can supply the other source; the resolver independently re-verifies it.
            result.put("status",index==0?"SourcesReady":"BronzeVerified"); actual=result;
        } else if("silver".equals(phase)) {
            if(silver.exists(FullMonthStateResolver.root(op)+"/_SUCCESS")) {
                actual=verifiedMonth(resolver,request);
                require(silver.exists("gold-commits/"+op+"/identity.json") || gold.baselines().equals(snapshots(request.path("baseline_snapshots"))),"STALE_GOLD_BASELINE");
            } else actual=resolver.resolve(spark,request);
        } else if("gold".equals(phase)) {
            var monthly=verifiedMonth(resolver,request); var g=monthly.path("gold_request"); var run=g.path("run_context");
            var ctx=new GoldRunContext(op,utc(run,"window_start_utc"),utc(run,"window_end_utc"),
                    java.time.LocalDate.parse(text(run,"processing_date")),run.path("is_backfill").asBoolean(),
                    text(run,"config_version"),List.of(monthly.path("silver_bundle_uri").asText()));
            var transformed=GoldInputReader.read(spark,silver,op,text(monthly,"silver_bundle_sha256"),ctx,
                    utc(request,"processed_at_utc"),work.resolve("gold-readback"));
            actual=gold.commit(transformed,new GoldCommitScope(ctx,text(monthly,"silver_bundle_sha256"),text(request,"code_version"),
                    new ArrayList<>(months(monthly.path("affected_months"))),snapshots(request.path("baseline_snapshots"))));
        } else {
            var commit=JSON.readTree(silver.read("gold-commits/"+op+"/commit.json"));
            // Bind the live writer journal to the exact predecessor artifact BEFORE
            // Trino can create any views/status rows. Python's post-check is too late.
            require(commit.equals(JSON.readTree(silver.read(root+"/gold.json"))),"GOLD_JOURNAL_CHANGED");
            require("COMMITTED".equals(commit.path("status").asText()) && text(request,"namespace").equals(text(commit,"namespace")),"EXACT_COMMIT_REQUIRED");
            actual="verify".equals(phase)?publisher.verify(op,text(request,"namespace"),text(commit,"identity_sha256"))
                    :publisher.publish(op,text(request,"namespace"),text(commit,"identity_sha256"));
            require("PASSED".equals(actual.path("verify_status").asText()),"TRINO_BLOCKERS_FAILED");
            if("publish".equals(phase)) require(actual.path("published").asBoolean(false),"PUBLICATION_READBACK_REQUIRED");
        }
        lease.run(); var artifactKey=root+"/"+phase+".json"; immutable(artifactKey,actual);
        var receipt=JSON.createObjectNode(); receipt.put("contract_version",PROTOCOL); receipt.put("mode","real");
        receipt.put("operation_id",op); receipt.put("context_sha256",text(context,"context_sha256"));
        receipt.put("phase",phase); receipt.put("status","ok");
        if(index==0) receipt.putNull("upstream_sha256"); else receipt.put("upstream_sha256",digest(upstream));
        receipt.put("artifact_uri",silver.uriForKey(artifactKey)); receipt.put("artifact_sha256",digest(actual));
        if("silver".equals(phase)) receipt.set("counts",actual.path("reconciliation"));
        if(index>=3) receipt.set("snapshots",snapshotIds(actual.path("tables").isObject()?actual.path("tables"):actual.path("snapshot_bundle")));
        if("publish".equals(phase)) {
            receipt.put("published",actual.path("published").asBoolean()); receipt.put("publication_id",text(actual,"publication_id"));
            receipt.put("publication_status","Published");
        }
        immutable(root+"/"+phase+"-receipt.json",receipt); return receipt;
    }
    private JsonNode verifiedMonth(FullMonthStateResolver resolver,JsonNode request) throws Exception {
        String op=text(request,"run_id"); var registry=JSON.readTree(silver.read(FullMonthStateResolver.root(op)+"/registry.json"));
        var receipt=resolver.verify(op,digest(registry));
        require(digest(request).equals(text(receipt,"request_sha256")) && "COMPLETE_MONTHS".equals(receipt.path("scope_completeness").asText()),"FULL_MONTH_PROOF_REQUIRED");
        var reader=new FullMonthBronzeReader(bronze,inventory); var inputs=new ArrayList<FullMonthBronzeReader.Input>();
        for(var pin:registry.path("active_inputs")) { lease.run(); inputs.add(reader.read(pin,utc(request,"source_cutoff_utc"))); }
        verifyRegistryCoverage(inputs,registry);
        return receipt;
    }
    static void verifyRegistryCoverage(List<FullMonthBronzeReader.Input> inputs,JsonNode registry) throws IOException {
        require(FullMonthStateResolver.coverage(inputs,registryMonths(registry.path("complete_months"))).equals(registry.path("coverage")),"FULL_MONTH_COVERAGE_CHANGED");
    }
    private ObjectNode snapshotIds(JsonNode tables) throws IOException {
        var out=JSON.createObjectNode();
        for(String name:GoldIcebergWriter.TABLES) {
            var id=tables.path(name).path("snapshot_id"); require(id.isIntegralNumber() && id.canConvertToLong() && id.asLong()>0,"SIX_COMMITTED_SNAPSHOTS_REQUIRED");
            out.set(name,id);
        }
        return out;
    }
    private void artifact(JsonNode receipt) throws Exception {
        String op=text(receipt,"operation_id"), phase=text(receipt,"phase");
        require(PHASES.contains(phase),"UNKNOWN_REAL_PHASE");
        String key="real-etl/"+op+"/"+phase+".json";
        require(silver.uriForKey(key).equals(text(receipt,"artifact_uri"))
                && digest(JSON.readTree(silver.read(key))).equals(text(receipt,"artifact_sha256")),"REAL_ARTIFACT_READBACK_FAILED");
    }
    private void immutable(String key,JsonNode value) throws IOException {
        lease.run(); byte[] bytes=JSON.writeValueAsBytes(canonical(value));
        if(silver.exists(key)) require(Arrays.equals(bytes,silver.read(key)),"REAL_OPERATION_IDENTITY_CONFLICT");
        else silver.put(key,bytes,"application/json");
        require(Arrays.equals(bytes,silver.read(key)),"REAL_ARTIFACT_READBACK_FAILED");
    }
}
