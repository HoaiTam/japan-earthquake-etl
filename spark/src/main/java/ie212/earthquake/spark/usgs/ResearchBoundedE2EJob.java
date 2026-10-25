package ie212.earthquake.spark.usgs;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import static org.apache.spark.sql.functions.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.GoldCommitJob;
import ie212.earthquake.spark.gold.*;
import ie212.earthquake.spark.ml.LocalFeatureJob;
import ie212.earthquake.spark.ml.MainshockWindowJob;
import ie212.earthquake.spark.orchestration.*;
import ie212.earthquake.spark.silver.*;
import ie212.earthquake.spark.jma.*;
import java.net.URI;
import java.net.http.HttpHeaders;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import org.apache.spark.sql.SparkSession;

/** Real original captures -> Bronze/Silver/six Gold snapshots/Trino in one isolated namespace. */
public final class ResearchBoundedE2EJob {
    public static final String VERSION="dat03-bounded-e2e-v1";
    public static final List<String> MONTHS=List.of("2000-01","2011-04","2016-04","2023-12");
    private ResearchBoundedE2EJob() { }
    static JsonNode metadata(Path file) throws Exception {
        require(Files.isRegularFile(file,LinkOption.NOFOLLOW_LINKS) && !Files.isSymbolicLink(file) && Files.size(file)<=65536,"DAT03_BOUNDED_METADATA_REQUIRED");
        return JSON.readTree(Files.readAllBytes(file));
    }
    static byte[] raw(Path root,String name,String sha,long length) throws Exception {
        require(name.matches("(h(2000|2011|2016|2023|2024)\\.zip|usgs-december2023\\.geojson)") && sha.matches("[a-f0-9]{64}"),"DAT03_EXACT_CAPTURE_REQUIRED");
        Path path=root.resolve("inputs").resolve(name);
        require(Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS) && !Files.isSymbolicLink(path) && length>0 && length<=32L*1024*1024
                && Files.size(path)==length,"DAT03_BOUNDED_CAPTURE_REQUIRED");
        byte[] raw=Files.readAllBytes(path); require(sha(raw).equals(sha),"DAT03_CAPTURE_PIN_CHANGED"); return raw;
    }
    static void save(Path path,JsonNode value) throws Exception {
        var bytes=JSON.writerWithDefaultPrettyPrinter().writeValueAsBytes(value);
        if(Files.exists(path,LinkOption.NOFOLLOW_LINKS)) require(!Files.isSymbolicLink(path) && Arrays.equals(Files.readAllBytes(path),bytes),"DAT03_EVIDENCE_CONFLICT");
        else Files.write(path,bytes,StandardOpenOption.CREATE_NEW);
    }
    static ObjectNode context(String run,String month,List<JsonNode> pins,JsonNode parent,String namespace,
            String code,Instant now,GoldIcebergWriter writer,SilverObjectStore silver) throws Exception {
        String op=RealEtlAdapter.operation("dat03_bounded_e2e",run);
        var r=JSON.createObjectNode().put("resolver_version",SCOPED_VERSION).put("run_id",op).put("dag_id","dat03_bounded_e2e")
                .put("namespace",namespace).put("config_version",VERSION).put("code_version",code)
                .put("processed_at_utc",now.toString()).put("source_cutoff_utc",now.toString());
        r.set("target_months",JSON.valueToTree(List.of(month))); r.set("bronze_inputs",JSON.valueToTree(pins));
        r.set("baseline_snapshots",JSON.valueToTree(writer.baselines()));
        if(parent==null) r.putNull("baseline"); else {
            String old=text(parent,"operation_id"), key="gold-commits/"+old+"/commit.json";
            r.putObject("baseline").put("run_id",old).put("registry_sha256",sha(silver.read(FullMonthStateResolver.root(old)+"/registry.json")))
                    .put("commit_key",key).put("commit_sha256",sha(silver.read(key)));
        }
        var c=JSON.createObjectNode().put("contract_version",RealEtlAdapter.PROTOCOL).put("mode","real")
                .put("dag_id","dat03_bounded_e2e").put("run_id",run).put("operation_id",op).put("trigger","manual")
                .put("window_start_utc",start(month).toString()).put("window_end_utc",end(month).toString());
        c.putNull("source_summary_sha256"); c.set("resolver_request",r); c.put("context_sha256",digest(c)); return c;
    }
    static ObjectNode execute(RealEtlAdapter adapter,SilverObjectStore silver,JsonNode context,Path root,Runnable lease) throws Exception {
        String op=text(context,"operation_id"); Path folder=root.resolve(op); Files.createDirectories(folder);
        save(folder.resolve("context.json"),context); var record=JSON.createObjectNode(); record.set("context",context);
        var receipts=record.putArray("receipts"); ObjectNode prior=null;
        for(String phase:RealEtlAdapter.PHASES) {
            lease.run(); System.out.println("event=dat03_e2e_phase phase="+phase+" operation_id="+op);
            prior=adapter.execute(phase,context,prior,folder); receipts.add(prior);
            var bytes=silver.read(key(text(prior,"artifact_uri"),silver::uriForKey));
            require(digest(JSON.readTree(bytes)).equals(text(prior,"artifact_sha256")),"DAT03_PHASE_ARTIFACT_CHANGED");
            save(folder.resolve(phase+".json"),JSON.readTree(bytes));
        }
        require(prior.path("published").asBoolean(false),"DAT03_GOLD_NOT_PUBLISHED");
        record.put("operation_id",op); record.set("commit",JSON.readTree(silver.read("gold-commits/"+op+"/commit.json")));
        record.set("publication",metadataLarge(folder.resolve("publish.json")));
        record.set("silver",metadataLarge(folder.resolve("silver.json")));
        require(record.path("silver").path("reconciliation").path("balanced").asBoolean(false)
                && "COMPLETE_MONTHS".equals(record.path("silver").path("scope_completeness").asText()),"DAT03_RECONCILIATION_FAILED");
        var checks=record.path("publication").path("checks"); require(checks.isObject() && checks.size()>=36,"DAT03_TRINO_READBACKS_MISSING");
        for(var it=checks.fields();it.hasNext();) {var check=it.next().getValue();
            require(check.path("actual").isIntegralNumber() && check.path("expected").isIntegralNumber()
                    && check.path("actual").asLong()==check.path("expected").asLong(),"DAT03_TRINO_CHECK_FAILED");}
        save(folder.resolve("record.json"),record); return record;
    }
    static JsonNode metadataLarge(Path p) throws Exception {require(Files.size(p)<=4*1024*1024,"DAT03_REPORT_SIZE_LIMIT");return JSON.readTree(Files.readAllBytes(p));}
    static void validateRequest(JsonNode request) throws Exception {
        var fields=new HashSet<String>(); request.fieldNames().forEachRemaining(fields::add);
        require(fields.equals(Set.of("version","namespace","run_id","lease_owner","runner_sha256","baseline_inventory_sha256",
                "boundary_inventory_sha256","jma_captures","usgs_capture_sha256")) && VERSION.equals(text(request,"version")),"DAT03_EXACT_REQUEST_REQUIRED");
        for(String field:List.of("runner_sha256","baseline_inventory_sha256","boundary_inventory_sha256","usgs_capture_sha256"))
            require(text(request,field).matches("[a-f0-9]{64}"),"DAT03_SHA_REQUIRED");
        String run=text(request,"run_id"); require(run.matches("dat03-[a-f0-9]{32}")
                && text(request,"namespace").equals("iceberg.dat03_e2e_"+run.substring(6)),"DAT03_ISOLATED_SCOPE_REQUIRED");
        var owner=request.path("lease_owner"); require(owner.isObject() && owner.size()==2
                && "dat03_bounded_e2e".equals(text(owner,"dag_id")) && run.equals(text(owner,"run_id")),"DAT03_OWNER_REQUIRED");
        var years=new HashSet<Integer>(); require(request.path("jma_captures").isArray() && request.path("jma_captures").size()==5,"DAT03_EXACT_YEAR_SET_REQUIRED");
        for(var pin:request.path("jma_captures")) require(pin.isObject() && pin.size()==3 && pin.path("year").isInt()
                && years.add(pin.path("year").asInt()) && text(pin,"sha256").matches("[a-f0-9]{64}")
                && text(pin,"capture_sha256").matches("[a-f0-9]{64}"),"DAT03_EXACT_YEAR_SET_REQUIRED");
        require(years.equals(Set.of(2000,2011,2016,2023,2024)),"DAT03_EXACT_YEAR_SET_REQUIRED");
    }
    static void validateUsgs(JsonNode capture,Instant cutoff) throws Exception {
        require(capture.path("http_status").isInt() && capture.path("http_status").asInt()==200
                && Set.of("application/json","application/geo+json").contains(text(capture,"content_type").split(";")[0])
                && !Instant.parse(text(capture,"retrieved_at_utc")).isAfter(cutoff),"DAT03_USGS_PROVENANCE_INVALID");
        var uri=URI.create(text(capture,"url")); require("https".equals(uri.getScheme()) && "earthquake.usgs.gov".equals(uri.getHost())
                && uri.getPort()==-1 && uri.getUserInfo()==null && uri.getFragment()==null && "/fdsnws/event/1/query".equals(uri.getPath()),"DAT03_USGS_URL_INVALID");
        var query=FullMonthAcceptanceJob.query(uri);
        require(query.equals(Map.ofEntries(Map.entry("format","geojson"),Map.entry("eventtype","earthquake"),
                Map.entry("starttime","2023-11-30T23:59:44Z"),Map.entry("endtime","2024-01-01T00:00:15.999Z"),
                Map.entry("minlatitude","20.0"),Map.entry("maxlatitude","50.0"),Map.entry("minlongitude","120.0"),Map.entry("maxlongitude","155.0"),
                Map.entry("orderby","time-asc"),Map.entry("limit","20000"),Map.entry("offset","1"))),"DAT03_USGS_SCOPE_INVALID");
    }
    public static void main(String[] args) throws Exception {
        require(args.length==1,"DAT03_REQUEST_REQUIRED"); Path requestPath=Path.of(args[0]).toAbsolutePath();
        var request=metadata(requestPath); Path root=requestPath.getParent();
        validateRequest(request); require(LocalFeatureJob.codeSha().equals(text(request,"runner_sha256")),"DAT03_RUNNER_PIN_CHANGED");
        String namespace=text(request,"namespace"), run=text(request,"run_id");
        require(namespace.matches("[a-z][a-z0-9_]*\\.dat03_e2e_[a-f0-9]{32}") && run.matches("dat03-[a-f0-9]{32}"),"DAT03_ISOLATED_SCOPE_REQUIRED");
        var owner=request.path("lease_owner"); Path guard=Path.of(Objects.requireNonNull(System.getenv("SOURCE_GUARD_ROOT")),"owner.json");
        require(owner.path("dag_id").asText().equals("dat03_bounded_e2e") && owner.path("run_id").asText().equals(run),"DAT03_OWNER_REQUIRED");
        Runnable lease=()->{try{require(metadata(guard).equals(owner),"DAT03_LEASE_NOT_HELD");}catch(Exception error){throw new IllegalStateException("DAT03_LEASE_NOT_HELD");}};
        lease.run();
        for(String file:List.of("baseline.csv","boundary.csv")) require(sha(Files.readAllBytes(root.resolve(file))).equals(text(request,file.equals("baseline.csv")?"baseline_inventory_sha256":"boundary_inventory_sha256")),"DAT03_INVENTORY_PIN_CHANGED");
        var inventory=new ArrayList<>(JmaArchiveInventory.read(root.resolve("baseline.csv"))); inventory.addAll(JmaArchiveInventory.read(root.resolve("boundary.csv")));
        Instant now=Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS);
        // Validate all captured bytes/provenance BEFORE constructing a writer or creating lake objects.
        for(var item:request.path("jma_captures")) {
            int year=item.path("year").asInt(); var capture=metadata(root.resolve("inputs/h"+year+"-capture.json"));
            var entry=inventory.stream().filter(e->e.year()==year && e.segment().equals("full-year")).findFirst().orElseThrow();
            require(digest(capture).equals(text(item,"capture_sha256")) && text(item,"sha256").equals(text(capture,"sha256"))
                    && entry.sourceUrl().toString().equals(text(capture,"final_url")) && capture.path("http_status").asInt()==200
                    && capture.path("inventory_metadata_drift").isBoolean() && !capture.path("inventory_metadata_drift").asBoolean()
                    && !Instant.parse(text(capture,"retrieved_at_utc")).isAfter(now),"DAT03_JMA_PROVENANCE_CHANGED");
            raw(root,"h"+year+".zip",text(capture,"sha256"),capture.path("bytes").asLong());
        }
        var u=metadata(root.resolve("inputs/usgs-capture.json")); require(digest(u).equals(text(request,"usgs_capture_sha256")),"DAT03_USGS_CAPTURE_CHANGED");
        validateUsgs(u,now); raw(root,"usgs-december2023.geojson",text(u,"sha256"),u.path("bytes").asLong());
        var spark=GoldCommitJob.configured(System.getenv()).master("local[1]").appName("dat03-bounded-real-source-e2e")
                .config("spark.ui.enabled","false").config("spark.sql.shuffle.partitions","1").config("spark.sql.adaptive.enabled","false")
                .config("spark.sql.codegen.wholeStage","false").getOrCreate(); spark.sparkContext().setLogLevel("ERROR");
        try(var bronze=MinioBronzeObjectStore.fromEnvironment();var silver=MinioSilverObjectStore.fromEnvironment()) {
            var writer=new GoldIcebergWriter(spark,namespace,silver,lease);
            require(writer.baselines().values().stream().allMatch(id->id==0),"DAT03_FRESH_NAMESPACE_REQUIRED");
            var sql=new TrinoSqlClient(URI.create("http://"+System.getenv("TRINO_HOST")+":"+System.getenv("TRINO_INTERNAL_PORT")));
            var adapter=new RealEtlAdapter(spark,bronze,silver,inventory,writer,new GoldPublisher(sql,silver,lease),lease);
            var pins=new TreeMap<Integer,JsonNode>();
            for(var item:request.path("jma_captures")) {
                int year=item.path("year").asInt(); var entry=inventory.stream().filter(e->e.year()==year && e.segment().equals("full-year")).findFirst().orElseThrow();
                var capture=metadata(root.resolve("inputs/h"+year+"-capture.json"));
                require(digest(capture).equals(text(item,"capture_sha256")) && entry.sourceUrl().toString().equals(text(capture,"final_url"))
                        && capture.path("http_status").asInt()==200 && text(capture,"sha256").equals(text(item,"sha256")),"DAT03_JMA_PROVENANCE_CHANGED");
                byte[] bytes=raw(root,"h"+year+".zip",text(capture,"sha256"),capture.path("bytes").asLong());
                var retrieved=Instant.parse(text(capture,"retrieved_at_utc")); require(!retrieved.isAfter(now),"DAT03_INPUT_AFTER_CUTOFF");
                lease.run(); var written=new JmaBronzeWriter(bronze).write(new JmaBronzeWriteRequest(entry,bytes,text(capture,"sha256"),bytes.length,text(capture,"catalog_release"),
                        new JmaHttpMetadata(200,"application/zip",(long)bytes.length,capture.path("etag").isNull()?null:capture.path("etag").asText(),
                            text(capture,"last_modified_utc"),entry.sourceUrl()),run+"-h"+year,1,LocalDate.ofInstant(now,ZoneOffset.UTC),retrieved,true,run+"-h"+year,90000,null));
                require(written.ready(),"DAT03_JMA_BRONZE_REJECTED");
                var pin=JSON.createObjectNode().put("source_system","JMA_BULLETIN").put("manifest_uri",bronze.uriForKey(written.manifestKey()))
                        .put("manifest_sha256",sha(bronze.read(written.manifestKey()))).put("sha256",written.sha256()).put("year",year)
                        .put("segment","full-year").put("catalog_release",text(capture,"catalog_release"));
                require(pins.put(year,pin)==null,"DAT03_DUPLICATE_YEAR");
            }
            require(pins.keySet().equals(Set.of(2000,2011,2016,2023,2024)),"DAT03_EXACT_YEAR_SET_REQUIRED");
            Instant from=Instant.parse("2023-11-30T23:59:44Z"), to=Instant.parse("2024-01-01T00:00:16Z");
            var query=new UsgsRequest(URI.create(text(u,"url")),from,to,20000,1);
            var response=new UsgsHttpResponse("dat03-fixed-public-capture",query,200,HttpHeaders.of(Map.of("content-type",List.of(text(u,"content_type"))),(x,y)->true),
                    raw(root,"usgs-december2023.geojson",text(u,"sha256"),u.path("bytes").asLong()),1,Duration.ZERO);
            lease.run(); var usgs=new UsgsBronzeWriter(bronze).write(new UsgsBronzeWriteRequest(response,run+"-usgs",1,LocalDate.ofInstant(now,ZoneOffset.UTC),
                    Instant.parse(text(u,"retrieved_at_utc")),true,run+"-usgs",90000)); require(usgs.ready(),"DAT03_USGS_BRONZE_REJECTED");
            var usgsPin=JSON.createObjectNode().put("source_system","USGS").put("manifest_uri",bronze.uriForKey(usgs.manifestKey()))
                    .put("manifest_sha256",sha(bronze.read(usgs.manifestKey()))).put("sha256",usgs.sha256()).put("window_start_utc",from.toString()).put("window_end_utc",to.toString());
            var report=JSON.createObjectNode().put("version",VERSION).put("namespace",namespace).put("run_id",run)
                    .put("research_ready",false).put("full_research_catalog",false).put("synthetic_data_used",false).put("runner_sha256",text(request,"runner_sha256"));
            report.set("jma_bronze_inputs",JSON.valueToTree(pins)); report.set("usgs_bronze_input",usgsPin); var cases=report.putArray("cases");
            JsonNode parent=null; var retained=new TreeMap<String,String>();
            for(String month:MONTHS) {
                List<JsonNode> input=switch(month) {case "2000-01"->List.of(pins.get(2000));case "2011-04"->List.of(pins.get(2011));
                    case "2016-04"->List.of(pins.get(2016)); default->List.of(pins.get(2023),pins.get(2024),usgsPin);};
                var context=context(run+"-"+month,month,input,parent,namespace,"jar-"+text(request,"runner_sha256").substring(0,16),now,writer,silver);
                var first=execute(adapter,silver,context,root,lease); var before=writer.baselines();
                var rerun=execute(adapter,silver,context,root,lease); require(first.equals(rerun) && before.equals(writer.baselines()),"DAT03_RERUN_CHANGED_PINS");
                require(months(first.path("silver").path("affected_months")).equals(Set.of(month)),"DAT03_UNEXPECTED_AFFECTED_SCOPE");
                long snapshot=first.path("commit").path("tables").path("event_current").path("snapshot_id").asLong();
                for(var prior:retained.entrySet()) require(prior.getValue().equals(partition(spark,namespace,snapshot,prior.getKey())),"DAT03_OUTSIDE_MONTH_CHANGED");
                retained.put(month,partition(spark,namespace,snapshot,month));
                var item=cases.addObject().put("month",month).put("rerun_identical",true).put("retention_verified",true);
                item.set("first",first); item.set("rerun",rerun); parent=first;
                save(root.resolve("progress-"+month+".json"),item);
                System.out.println("event=dat03_e2e_month_passed month="+month+" trino_checks="+first.path("publication").path("checks").size());
            }
            report.set("final_snapshots",JSON.valueToTree(writer.baselines())); report.put("status","PASSED").put("completed_at_utc",Instant.now().toString());
            save(root.resolve("report.json"),report);
        } finally {spark.stop();}
    }
    static String partition(SparkSession spark,String namespace,long snapshot,String month) throws Exception {
        return MainshockWindowJob.fingerprint(spark.read().option("snapshot-id",Long.toString(snapshot)).table(namespace+".event_current")
                .filter(date_format(col("event_time_utc"),"yyyy-MM").equalTo(month)));
    }
}
