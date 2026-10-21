package ie212.earthquake.spark.usgs;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import static org.apache.spark.sql.functions.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.*;
import ie212.earthquake.spark.orchestration.*;
import ie212.earthquake.spark.gold.*;
import ie212.earthquake.spark.jma.*;
import ie212.earthquake.spark.silver.*;
import java.io.IOException;
import java.net.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** QA-04 fixed opt-in fault seams. No delete/reset, no fault flags in production requests. */
public final class FailureRecoveryAcceptanceJob {
    private FailureRecoveryAcceptanceJob() { }
    public static void main(String[] args) throws Exception {
        require(args.length==1,"QA04_EXACT_REQUEST_REQUIRED");var request=JSON.readTree(Files.readAllBytes(Path.of(args[0])));
        Path root=Path.of(text(request,"report_path")).getParent();
        try(var qa=new PipelineQaSupport(request,root)) {run(qa,request,root);}
    }
    private static void run(PipelineQaSupport qa,JsonNode request,Path root) throws Exception {
        var report=JSON.createObjectNode().put("task","QA-04").put("schema_version","qa04-storage-acceptance-v1")
            .put("namespace",qa.namespace).put("run_id",text(request,"run_id")).put("synthetic_fixture",true)
            .put("backend","MinIO/REST/Iceberg/Trino");var cases=report.putArray("cases");
        report.put("matrix_sha256",text(request,"matrix_sha256"));
        String run=text(request,"run_id");var payload=qa.payload();var features=(ArrayNode)payload.path("features");
        var august=features.get(0).deepCopy();((ObjectNode)august).put("id","fx-qa04-outside");
        ((ObjectNode)august.path("properties")).put("time",Instant.parse("2023-08-15T00:00:00Z").toEpochMilli());
        features.add(august);((ObjectNode)payload.path("metadata")).put("count",2);
        var u=qa.usgs(run+"-u","2023-07-31T23:59:44Z","2023-10-01T00:00:16Z",payload);
        var entry=qa.entry(2023,"full-year");byte[] raw=qa.archive(entry,List.of(qa.jmaRecord("2023090112345678")));
        var j=qa.jma(run+"-j",entry,raw,"release-v1");
        var parent=qa.execute(qa.context(run+"-baseline",List.of("2023-08"),List.of(u,j),null));
        report.set("baseline",parent);String outside=partition(qa,parent);
        require(!outside.equals("[]"),"QA04_POSITIVE_OUTSIDE_BASELINE_REQUIRED");
        report.put("outside_month","2023-08").put("outside_before_sha256",sha(outside.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        for(String id:List.of("fetch_timeout","archive_invalid","silver_failure","gold_commit_partial",
                "gold_commit_receipt_lost","trino_verify_failure","publication_metadata_failure")) {
            List<JsonNode> pins=List.of();String target="2023-09";
            if(id.equals("archive_invalid")) {
                target="1997-09";var old=qa.entry(1997,"jan-sep");
                byte[] a=qa.archive(old,List.of(qa.jmaRecord("1997090112345678")));
                var rejected=new JmaBronzeWriter(qa.bronze).write(new JmaBronzeWriteRequest(old,a,"0".repeat(64),a.length,"release-v1",
                    new JmaHttpMetadata(200,"application/zip",(long)a.length,null,null,old.sourceUrl()),run+"-bad-archive",1,
                    LocalDate.ofInstant(qa.now,ZoneOffset.UTC),qa.now,true,run,60000,null));
                require(!rejected.ready(),"QA04_INVALID_ARCHIVE_ACCEPTED");
                var support=qa.entry(1997,"oct-dec");
                pins=List.of(qa.jma(run+"-bad-archive",old,a,"release-v1",2),
                    qa.jma(run+"-199710",support,qa.archive(support,List.of(qa.jmaRecord("1997100112345678"))),"release-v1"));
            }
            var fetch=id.equals("fetch_timeout")?timeoutGate(payload):null;
            var context=qa.context(run+"-"+id,List.of(target),pins,parent);String op=text(context,"operation_id");
            var before=qa.writer.baselines();var fault=JSON.createObjectNode().put("case_id",id).put("operation_id",op);
            fault.set("context",context);fault.set("baseline_snapshots",JSON.valueToTree(before));
            fault.put("reason_code",Map.of("fetch_timeout","FETCH_TIMEOUT","archive_invalid","JMA_ARCHIVE_CHECKSUM_INVALID","silver_failure","SILVER_QUALITY_FAILED","gold_commit_partial","GOLD_COMMIT_PARTIAL","gold_commit_receipt_lost","GOLD_RECEIPT_LOST","trino_verify_failure","TRINO_VERIFY_FAILED","publication_metadata_failure","PUBLICATION_READBACK_FAILED").get(id));
            fault.put("fault_stage",switch(id){case "fetch_timeout"->"FETCH";case "archive_invalid"->"ARCHIVE";case "silver_failure"->"SILVER";case "trino_verify_failure"->"TRINO";case "publication_metadata_failure"->"PUBLICATION";default->"GOLD";});
            if(fetch!=null)fault.set("fetch",fetch);
            if(id.equals("archive_invalid"))fault.put("rejected_attempt",1).put("verified_attempt",2).put("rejected_before_ready",true);
            if(id.equals("silver_failure")) {
                boolean blocked=false;
                try {new SilverBundlePublisher(qa.silver).publish(new SilverWriteRequest(op,List.of(),List.of(),qa.now,false),JSON.createObjectNode().put("quality_passed",false).put("reconciliation_balanced",true));}
                catch(IOException e) {require("BUNDLE_QUALITY_GATE_BLOCKED".equals(e.getMessage()),"QA04_WRONG_SILVER_BLOCKER");blocked=true;}
                require(blocked,"QA04_SILVER_QUALITY_ACCEPTED");
            }
            if(id.startsWith("gold_") || id.startsWith("trino_") || id.startsWith("publication_")) {
                var fired=new AtomicBoolean();var active=new AtomicBoolean();
                Runnable injected=()->{
                    qa.lease.run();
                    if(id.equals("gold_commit_partial") && active.get() && !fired.get()) try {
                        if(!qa.writer.baselines().equals(before)) {fired.set(true);throw new IllegalStateException("QA04_INJECTED_PARTIAL");}
                    } catch(Exception e) {throw new IllegalStateException("QA04_INJECTED_PARTIAL",e);}
                };
                var store=new FaultStore(qa.silver,"gold-commits/"+op+"/commit.json",id.equals("gold_commit_receipt_lost"),fired);
                var sql=new TrinoSqlClient(URI.create("http://"+System.getenv("TRINO_HOST")+":"+System.getenv("TRINO_INTERNAL_PORT")));
                GoldPublisher.Sql failing=query->{
                    boolean selected=id.equals("trino_verify_failure") || (id.equals("publication_metadata_failure") && query.startsWith("INSERT INTO "));
                    if(selected && fired.compareAndSet(false,true)) throw new IOException("QA04_INJECTED_TRINO_OR_PUBLICATION_FAILURE");
                    return sql.execute(query);
                };
                var adapter=new RealEtlAdapter(qa.spark,qa.bronze,store,qa.inventory,
                    new GoldIcebergWriter(qa.spark,qa.namespace,store,injected),new GoldPublisher(failing,store,qa.lease),qa.lease);
                ObjectNode prior=null;Path work=root.resolve(op);Files.createDirectories(work);String failedPhase=null;
                try {
                    for(String phase:RealEtlAdapter.PHASES) {active.set(phase.equals("gold"));failedPhase=phase;prior=adapter.execute(phase,context,prior,work);}
                } catch(Exception e) {require(fired.get(),"QA04_UNEXPECTED_RUNTIME_FAILURE");}
                require(fired.get() && !qa.silver.exists("real-etl/"+op+"/publish-receipt.json")
                    && !qa.silver.exists("gold-verification/"+op+"/publication.json"),"QA04_FAULT_WAS_PUBLISHED");
                fault.put("failed_phase",failedPhase);fault.set("partial_snapshots",JSON.valueToTree(qa.writer.baselines()));
            } else require(before.equals(qa.writer.baselines()),"QA04_UPSTREAM_FAILURE_CHANGED_GOLD");
            var sqlGate=new TrinoSqlClient(URI.create("http://"+System.getenv("TRINO_HOST")+":"+System.getenv("TRINO_INTERNAL_PORT")));
            String commitKey="gold-commits/"+op+"/commit.json";
            String publication="pub_"+sha(qa.silver.exists(commitKey)?qa.silver.read(commitKey):op.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            boolean datasetBlocked=false;
            try {new ie212.earthquake.spark.ml.DatasetManifestWriter(sqlGate,qa.lease).build(qa.namespace,publication,qa.namespace+"_dataset_gate",JSON.createObjectNode(),op,qa.now);}
            catch(IOException error){require("DS_GOLD_NOT_PUBLISHED".equals(error.getMessage()),"QA04_UNEXPECTED_DATASET_GATE");datasetBlocked=true;}
            require(datasetBlocked && !qa.spark.catalog().tableExists(qa.namespace+"_dataset_gate.dataset_manifest"),"QA04_UNPUBLISHED_GOLD_BUILT_DATASET");
            fault.put("dataset_gate_reason","DS_GOLD_NOT_PUBLISHED").put("candidate_publication_id",publication);
            fault.put("commit_receipt_present_at_failure",qa.silver.exists(commitKey));
            if(!fault.has("partial_snapshots"))fault.set("partial_snapshots",JSON.valueToTree(qa.writer.baselines()));
            var unpublished=sqlGate.execute("SELECT count(*) FROM "+qa.namespace+".publication_status WHERE gold_run_id='"+op+"'");
            require(unpublished.rows().size()==1 && unpublished.rows().get(0).get(0).asLong()==0,"QA04_FAULT_ALREADY_PUBLISHED");
            fault.put("publication_rows_before_recovery",0);
            fault.put("downstream_blocked",true);fault.put("published_before_recovery",false);
            PipelineQaSupport.save(root.resolve(id+"-fault.json"),fault);
            var recovered=qa.execute(context);qa.oldReadable(parent.path("commit"));
            require(outside.equals(partition(qa,recovered)),"QA04_OUTSIDE_SCOPE_CHANGED");
            if(id.startsWith("gold_") || id.startsWith("trino_") || id.startsWith("publication_")) {
                for(String table:GoldIcebergWriter.TABLES) {
                    long partial=fault.path("partial_snapshots").path(table).asLong();
                    if(partial!=before.get(table)) require(partial==recovered.path("commit").path("tables").path(table).path("snapshot_id").asLong(),"QA04_RECOVERY_REPLAYED_COMMITTED_TABLE");
                }
            }
            var result=cases.addObject().put("case_id",id).put("status","PASS").put("downstream_blocked",true)
                .put("outside_scope_unchanged",true).put("recovery_status","RECOVERED");
            result.set("fault",fault);result.set("recovery",recovered);parent=recovered;
            result.put("outside_before_sha256",sha(outside.getBytes(java.nio.charset.StandardCharsets.UTF_8)))
                .put("outside_after_sha256",sha(partition(qa,recovered).getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        }
        var sql=new TrinoSqlClient(URI.create("http://"+System.getenv("TRINO_HOST")+":"+System.getenv("TRINO_INTERNAL_PORT")));
        var goldBeforeMl=qa.writer.baselines();
        var ml=ie212.earthquake.spark.ml.Qa04MlRecoveryAcceptance.accept(qa.spark,sql,root.resolve("ml"),qa.namespace.replace(".qa04_",".qa04_ml_"),qa.lease);
        require(goldBeforeMl.equals(qa.writer.baselines()),"QA04_ML_CHANGED_GOLD");report.set("ml",ml);
        for(String id:List.of("ml_bundle_invalid","ml_import_partial"))cases.addObject().put("case_id",id).put("status","PASS").put("downstream_blocked",true).put("outside_scope_unchanged",true).put("recovery_status","RECOVERED").set("evidence",ml.path(id));
        report.put("outside_after_sha256",sha(partition(qa,parent).getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        report.set("gold_before_ml",JSON.valueToTree(goldBeforeMl));report.set("gold_after_ml",JSON.valueToTree(qa.writer.baselines()));
        report.put("status","JAVA_PASSED_LEASE_PENDING");report.put("completed_at_utc",Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS).toString());
        PipelineQaSupport.save(root.resolve("java-report.json"),report);
    }
    private static String partition(PipelineQaSupport qa,JsonNode result) {return qa.events(result.path("commit").path("tables").path("event_current").path("snapshot_id").asLong())
        .filter(date_format(col("event_time_utc"),"yyyy-MM").equalTo("2023-08")).orderBy("canonical_event_id").toJSON().collectAsList().toString();}
    static ObjectNode timeoutGate(ObjectNode payload) throws Exception {
        var server=com.sun.net.httpserver.HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
        var pool=java.util.concurrent.Executors.newCachedThreadPool();server.setExecutor(pool);
        var calls=new java.util.concurrent.atomic.AtomicInteger();byte[] body=JSON.writeValueAsBytes(payload);
        server.createContext("/",exchange->{try {if(calls.incrementAndGet()==1)Thread.sleep(2500);else {exchange.getResponseHeaders().add("Content-Type","application/geo+json");exchange.sendResponseHeaders(200,body.length);exchange.getResponseBody().write(body);}}catch(InterruptedException e){Thread.currentThread().interrupt();}finally{exchange.close();}});server.start();
        try {
            var env=new HashMap<String,String>(); env.put("USGS_API_BASE_URL","https://earthquake.usgs.gov/fdsnws/event/1");
            env.put("USGS_MIN_LATITUDE","20");env.put("USGS_MAX_LATITUDE","50");env.put("USGS_MIN_LONGITUDE","120");env.put("USGS_MAX_LONGITUDE","155");
            env.put("USGS_SEED_START_UTC","2023-01-01T00:00:00Z");env.put("PIPELINE_OVERLAP_DAYS","3");env.put("USGS_MAX_WINDOW_DAYS","31");env.put("USGS_REQUEST_LIMIT","20000");
            env.put("USGS_HTTP_TIMEOUT_MS","1000");env.put("USGS_HTTP_MAX_ATTEMPTS","1");env.put("USGS_HTTP_INITIAL_BACKOFF_MS","0");env.put("USGS_HTTP_MAX_BACKOFF_MS","0");env.put("USGS_MAX_RESPONSE_BYTES","4096");env.put("USGS_EVENT_TYPE","earthquake");
            boolean failed=false;
            var client=new UsgsHttpClient(UsgsRequestConfig.fromEnvironment(env));
            var request=new UsgsRequest(URI.create("http://127.0.0.1:"+server.getAddress().getPort()+"/"),Instant.parse("2023-07-31T23:59:44Z"),Instant.parse("2023-10-01T00:00:16Z"),20000,1);
            try {client.fetch(request);}
            catch(IOException e) {Throwable cause=e;while(cause.getCause()!=null) cause=cause.getCause();require(cause instanceof java.net.http.HttpTimeoutException,"QA04_EXPECTED_FETCH_TIMEOUT");failed=true;}
            require(failed,"QA04_FETCH_TIMEOUT_NOT_OBSERVED");
            var recovery=client.fetch(request);require(recovery.statusCode()==200 && Arrays.equals(body,recovery.body()),"QA04_FETCH_RECOVERY_CHANGED_INPUT");
            return JSON.createObjectNode().put("reason","HttpTimeoutException").put("request_count",calls.get()).put("recovery_http_status",recovery.statusCode()).put("recovery_sha256",sha(recovery.body())).put("same_request",true);
        } finally {server.stop(0);pool.shutdownNow();}
    }
    private static final class FaultStore implements SilverObjectStore {
        private final SilverObjectStore delegate;private final String key;private final boolean enabled;private final AtomicBoolean fired;
        FaultStore(SilverObjectStore delegate,String key,boolean enabled,AtomicBoolean fired) {this.delegate=delegate;this.key=key;this.enabled=enabled;this.fired=fired;}
        public void put(String key,byte[] content,String type) throws IOException {if(enabled && key.equals(this.key) && fired.compareAndSet(false,true)) throw new IOException("QA04_INJECTED_RECEIPT_WRITE_FAILURE");delegate.put(key,content,type);}
        public byte[] read(String key) throws IOException {return delegate.read(key);}public boolean exists(String key) throws IOException {return delegate.exists(key);}
        public List<String> list(String prefix) throws IOException {return delegate.list(prefix);}public String uriForKey(String key) {return delegate.uriForKey(key);}
        public void delete(String key) throws IOException {throw new IOException("QA04_DELETE_FORBIDDEN");}public void deletePrefix(String prefix) throws IOException {throw new IOException("QA04_DELETE_FORBIDDEN");}
        public void move(String a,String b) throws IOException {delegate.move(a,b);}
    }
}
