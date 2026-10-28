package ie212.earthquake.spark.usgs;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.gold.GoldIcebergWriter;
import ie212.earthquake.spark.orchestration.RealEtlAdapter;
import java.nio.file.*;
import java.time.*;
import java.util.*;

/** Exact, append-only control metadata. No data transformation or caller-supplied completeness flag. */
public final class ResearchRangeState {
    public static final String VERSION="dat03-range-worker-v1", DAG="dat03_range_worker";
    public static final List<String> MONTHS;
    static {
        var months=new ArrayList<String>();
        for(var month=YearMonth.of(2000,1);month.isBefore(YearMonth.of(2024,1));month=month.plusMonths(1)) months.add(month.toString());
        MONTHS=List.copyOf(months);
    }
    private ResearchRangeState() { }
    public static void validate(JsonNode r) throws Exception {
        var fields=new HashSet<String>();r.fieldNames().forEachRemaining(fields::add);
        require(fields.equals(Set.of("version","run_id","namespace","runner_sha256","controller_sha256","support_sha256","source_cutoff_utc",
                "baseline_inventory_sha256","boundary_inventory_sha256","plan_sha256","usgs_summary_sha256","sources","owner","start_month","end_month_exclusive","input_manifest_sha256","catalog_controller_sha256")),"CATALOG_EXACT_REQUEST_REQUIRED");
        String run=text(r,"run_id");
        require(VERSION.equals(text(r,"version")) && run.matches("dat03-range-[a-f0-9]{32}")
                && text(r,"namespace").equals("iceberg.dat03_range_"+run.substring(12)),"CATALOG_ISOLATED_SCOPE_REQUIRED");
        for(String k:List.of("runner_sha256","controller_sha256","support_sha256","baseline_inventory_sha256","boundary_inventory_sha256","plan_sha256","usgs_summary_sha256","catalog_controller_sha256"))
            require(text(r,k).matches("[a-f0-9]{64}"),"CATALOG_PIN_REQUIRED");
        require(!utc(r,"source_cutoff_utc").isAfter(Instant.now()) && !utc(r,"source_cutoff_utc").isBefore(end("2023-12")),"CATALOG_CUTOFF_INVALID");
        var expected=new TreeSet<String>();for(int y=2000;y<=2024;y++)expected.add("h"+y);
        for(int m=1;m<=12;m++)expected.add(String.format(Locale.ROOT,"usgs-2023-%02d",m));
        var seen=new TreeSet<String>();require(r.path("sources").isArray() && r.path("sources").size()==37,"CATALOG_EXACT_SOURCE_SET_REQUIRED");
        for(var source:r.path("sources")) {
            var names=new HashSet<String>();source.fieldNames().forEachRemaining(names::add);
            require(names.equals(Set.of("name","raw_sha256","metadata_sha256","bytes")) && seen.add(text(source,"name"))
                    && text(source,"raw_sha256").matches("[a-f0-9]{64}") && text(source,"metadata_sha256").matches("[a-f0-9]{64}")
                    && source.path("bytes").isIntegralNumber() && source.path("bytes").canConvertToLong()
                    && source.path("bytes").asLong()>0 && source.path("bytes").asLong()<=32L*1024*1024,"CATALOG_SOURCE_PIN_INVALID");
        }
        require(seen.equals(expected),"CATALOG_EXACT_SOURCE_SET_REQUIRED");
        require(text(r,"input_manifest_sha256").matches("[a-f0-9]{64}"),"RANGE_INPUT_MANIFEST_PIN_REQUIRED");
        String owner=text(r,"owner");
        String first=switch(owner){case "ThanhTris"->"2013-08";case "rosy179"->"2018-10";case "HoaiTam"->"2008-06";default->throw new IllegalArgumentException("RANGE_OWNER_INVALID");};
        String last=switch(owner){case "ThanhTris"->"2018-10";case "rosy179"->"2024-01";default->"2013-08";};
        require(first.equals(text(r,"start_month")) && last.equals(text(r,"end_month_exclusive")),"RANGE_ASSIGNMENT_CHANGED");
    }
    public static List<String> months(JsonNode r) throws Exception {
        validate(r);
        return MONTHS.subList(MONTHS.indexOf(text(r,"start_month")),
                text(r,"end_month_exclusive").equals("2024-01")?288:MONTHS.indexOf(text(r,"end_month_exclusive")));
    }

    public static Path safe(Path path) throws Exception {
        path=path.toAbsolutePath().normalize();for(Path p=path;p!=null;p=p.getParent())require(!Files.isSymbolicLink(p),"CATALOG_SYMLINK_REJECTED");
        return path;
    }
    public static JsonNode read(Path path,long limit) throws Exception {
        path=safe(path);require(Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS) && Files.size(path)>0 && Files.size(path)<=limit,"CATALOG_METADATA_BOUND");
        return JSON.readTree(Files.readAllBytes(path));
    }
    public static void save(Path path,JsonNode value) throws Exception {
        path=safe(path);byte[] bytes=JSON.writeValueAsBytes(canonical(value));
        if(Files.exists(path,LinkOption.NOFOLLOW_LINKS))require(Arrays.equals(bytes,Files.readAllBytes(path)),"CATALOG_CHECKPOINT_CONFLICT");
        else Files.write(path,bytes,StandardOpenOption.CREATE_NEW);
    }
    public static ObjectNode context(JsonNode r,String month,JsonNode pins,JsonNode parent,Map<String,Long> baselines) throws Exception {
        return context(r,List.of(month),pins,parent,baselines,text(r,"runner_sha256"));
    }
    public static ObjectNode context(JsonNode r,List<String> targets,JsonNode pins,JsonNode parent,Map<String,Long> baselines,String codeSha) throws Exception {
        validate(r);require(!targets.isEmpty() && targets.size()<=12 && codeSha.matches("[a-f0-9]{64}"),"CATALOG_EXACT_BATCH_REQUIRED");
        for(int i=0;i<targets.size();i++)require(months(r).contains(targets.get(i))
                && (i==0 || YearMonth.parse(targets.get(i-1)).plusMonths(1).toString().equals(targets.get(i))),"CATALOG_EXACT_MONTH_REQUIRED");
        require(!(targets.get(0).compareTo("2018-10")<0 && targets.get(targets.size()-1).compareTo("2018-10")>=0),"CATALOG_BATCH_SPLIT_BOUNDARY");
        snapshots(JSON.valueToTree(baselines));String month=targets.get(0),last=targets.get(targets.size()-1);
        String run=text(r,"run_id")+"-"+month+(targets.size()==1?"":"-to-"+last),op=RealEtlAdapter.operation(DAG,run);
        var request=JSON.createObjectNode().put("resolver_version",SCOPED_VERSION).put("run_id",op).put("dag_id",DAG)
                .put("namespace",text(r,"namespace")).put("config_version",VERSION).put("code_version","jar-"+codeSha.substring(0,16))
                .put("processed_at_utc",text(r,"source_cutoff_utc")).put("source_cutoff_utc",text(r,"source_cutoff_utc"));
        request.set("target_months",JSON.valueToTree(targets));request.set("baseline_snapshots",JSON.valueToTree(baselines));
        // Bootstrap all 37 exact source pins once. Subsequent operations reuse the
        // receipt-bound active input set; never discover latest files or releases.
        request.set("bronze_inputs",parent==null?pins:JSON.createArrayNode());
        if(parent==null)request.putNull("baseline");else request.set("baseline",parent.deepCopy());
        var out=JSON.createObjectNode().put("contract_version",RealEtlAdapter.PROTOCOL).put("mode","real").put("dag_id",DAG)
                .put("run_id",run).put("operation_id",op).put("trigger","manual")
                .put("window_start_utc",start(month).toString()).put("window_end_utc",end(last).toString());
        out.putNull("source_summary_sha256");out.set("resolver_request",request);out.put("context_sha256",digest(out));
        RealEtlAdapter.validateContext(out);return out;
    }
    /** Only a contiguous exact prefix of the research plan can be acknowledged. */
    public static List<JsonNode> completed(Path root,JsonNode request) throws Exception {
        var result=new ArrayList<JsonNode>();boolean gap=false;String previous=null;
        Path monthRoot=safe(root.resolve("months"));
        if(Files.exists(monthRoot))try(var entries=Files.list(monthRoot)) {
            for(Path entry:entries.toList())require(months(request).contains(entry.getFileName().toString())
                    && !Files.isSymbolicLink(entry) && Files.isDirectory(entry),"RANGE_FOREIGN_MONTH_REJECTED");
        }
        for(String month:months(request)) {
            Path file=safe(root.resolve("months/"+month+"/checkpoint.json"));
            if(!Files.exists(file,LinkOption.NOFOLLOW_LINKS)){gap=true;continue;}
            require(!gap,"CATALOG_NONCONTIGUOUS_CHECKPOINTS");var checkpoint=read(file,65536);
            require(VERSION.equals(checkpoint.path("version").asText()) && month.equals(checkpoint.path("month").asText())
                    && digest(request).equals(checkpoint.path("request_identity_sha256").asText())
                    && "PUBLISHED".equals(checkpoint.path("status").asText())
                    && Objects.equals(previous,checkpoint.path("previous_checkpoint_sha256").isNull()?null:checkpoint.path("previous_checkpoint_sha256").asText()),
                    "CATALOG_CHECKPOINT_CHAIN_CHANGED");
            Path record=root.resolve("months/"+month+"/record.json");read(record,4*1024*1024);
            require(sha(Files.readAllBytes(safe(record))).equals(text(checkpoint,"record_sha256")),"CATALOG_RECORD_CHANGED");
            var pin=snapshots(checkpoint.path("snapshots"));require(pin.values().stream().allMatch(v->v>0),"CATALOG_SIX_PINS_REQUIRED");
            result.add(checkpoint);previous=sha(Files.readAllBytes(file));
        }
        return result;
    }
    public static boolean mayStartPhase(long remainingSeconds) { return remainingSeconds>=1200; }
    public static ObjectNode progress(JsonNode request,List<JsonNode> done) throws Exception {
        int count=months(request).size();require(done.size()<=count,"CATALOG_PROGRESS_BOUND");
        var result=JSON.createObjectNode().put("version",VERSION).put("request_identity_sha256",digest(request))
                .put("completed_months",done.size()).put("required_months",count)
                .put("status",done.size()==count?"RANGE_PUBLISHED":"CHECKPOINTED")
                .put("full_research_catalog",false).put("official_exported",false).put("research_ready",false);
        result.put("owner",text(request,"owner"));
        result.set("months",JSON.valueToTree(done));return result;
    }
}
