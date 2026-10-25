package ie212.earthquake.spark.usgs;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.GoldCommitJob;
import ie212.earthquake.spark.gold.*;
import ie212.earthquake.spark.jma.*;
import ie212.earthquake.spark.ml.LocalFeatureJob;
import ie212.earthquake.spark.orchestration.*;
import ie212.earthquake.spark.silver.*;
import java.net.URI;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.zip.ZipFile;

/** Explicit continuation of a pinned failure, never a fresh run or automatic retry. */
public final class ResearchBoundedContinuationJob {
    public static final String VERSION="dat03-bounded-continuation-v1";
    static final String TRANSPORT="ie212/earthquake/spark/gold/TrinoSqlClient.class";
    private ResearchBoundedContinuationJob() { }

    /** All original bytecode/resources must match, except the separately recorded transport fix. */
    static ObjectNode compatibility(Path original,Path current) throws Exception {
        var differences=JSON.createObjectNode(); int matched=0;
        try(var a=new ZipFile(original.toFile());var b=new ZipFile(current.toFile())) {
            var entries=a.entries();
            while(entries.hasMoreElements()) {
                var entry=entries.nextElement(); if(entry.isDirectory() || entry.getName().equals("META-INF/MANIFEST.MF"))continue;
                var other=b.getEntry(entry.getName()); require(other!=null,"DAT03_RECOVERY_CODE_ENTRY_MISSING");
                byte[] before,after;
                try(var in=a.getInputStream(entry)){before=in.readAllBytes();}
                try(var in=b.getInputStream(other)){after=in.readAllBytes();}
                if(Arrays.equals(before,after)){matched++;continue;}
                require(TRANSPORT.equals(entry.getName()),"DAT03_RECOVERY_ETL_CODE_CHANGED");
                differences.putObject(entry.getName()).put("original_sha256",sha(before)).put("recovery_sha256",sha(after));
            }
        }
        require(differences.size()==1,"DAT03_RECOVERY_EXACT_TRANSPORT_PATCH_REQUIRED");
        var out=JSON.createObjectNode().put("version","dat03-code-compatibility-v1").put("matched_original_entries",matched);
        out.set("allowed_differences",differences);return out;
    }
    static ObjectNode readPins(JsonNode request,Path original,BronzeObjectStore bronze,List<JmaArchiveEntry> inventory,Instant cutoff) throws Exception {
        var pins=JSON.createObjectNode();var jma=pins.putObject("jma");
        String run=text(request,"run_id"),date=LocalDate.ofInstant(cutoff,ZoneOffset.UTC).toString();
        var reader=new FullMonthBronzeReader(bronze,inventory);
        for(var item:request.path("jma_captures")) {
            int year=item.path("year").asInt();
            var capture=ResearchBoundedE2EJob.metadata(original.resolve("inputs/h"+year+"-capture.json"));
            String release=text(capture,"catalog_release");require(release.matches("[A-Za-z0-9._-]{1,128}"),"DAT03_RECOVERY_RELEASE_REQUIRED");
            String key="bronze/jma/year="+year+"/catalog_release="+release+"/ingest_date="+date+"/run_id="+run+"-h"+year+"/attempt=01/manifest.json";
            byte[] bytes=bronze.read(key); require(bytes.length<=65536,"DAT03_RECOVERY_MANIFEST_LIMIT");
            var pin=JSON.createObjectNode().put("source_system","JMA_BULLETIN").put("manifest_uri",bronze.uriForKey(key))
                    .put("manifest_sha256",sha(bytes)).put("sha256",text(item,"sha256")).put("year",year).put("segment","full-year").put("catalog_release",release);
            var verified=reader.descriptor(pin,cutoff);
            require((run+"-h"+year).equals(text(verified.manifest(),"run_id")),"DAT03_RECOVERY_BRONZE_OWNER_CHANGED");
            jma.set(Integer.toString(year),pin);
        }
        String key="bronze/usgs/ingest_date="+date+"/run_id="+run+"-usgs/attempt=01/manifest.json";
        byte[] bytes=bronze.read(key);require(bytes.length<=65536,"DAT03_RECOVERY_MANIFEST_LIMIT");
        var capture=ResearchBoundedE2EJob.metadata(original.resolve("inputs/usgs-capture.json"));
        var usgs=JSON.createObjectNode().put("source_system","USGS").put("manifest_uri",bronze.uriForKey(key))
                .put("manifest_sha256",sha(bytes)).put("sha256",text(capture,"sha256"))
                .put("window_start_utc","2023-11-30T23:59:44Z").put("window_end_utc","2024-01-01T00:00:16Z");
        var verified=reader.descriptor(usgs,cutoff);require((run+"-usgs").equals(text(verified.manifest(),"run_id")),"DAT03_RECOVERY_BRONZE_OWNER_CHANGED");
        pins.set("usgs",usgs);return pins;
    }
    @FunctionalInterface interface Phase { ObjectNode execute(String phase,JsonNode context,JsonNode prior,Path root) throws Exception; }
    /** Only verify/publish may execute for the committed operation; first four receipts are reused. */
    static ObjectNode publicationOnly(Phase adapter,SilverObjectStore silver,JsonNode context,Path root,Runnable lease) throws Exception {
        RealEtlAdapter.validateContext(context);
        String op=text(context,"operation_id");Path folder=root.resolve(op);Files.createDirectories(folder);
        ResearchBoundedE2EJob.save(folder.resolve("context.json"),context);
        var record=JSON.createObjectNode();record.set("context",context);var receipts=record.putArray("receipts");ObjectNode prior=null;
        for(String phase:RealEtlAdapter.PHASES) {
            lease.run();System.out.println("event=dat03_continuation_phase phase="+phase+" reused="+(!List.of("verify","publish").contains(phase))+" operation_id="+op);
            prior=List.of("verify","publish").contains(phase)?adapter.execute(phase,context,prior,folder)
                    :(ObjectNode)ResearchRecoveryPreflight.read(silver,"real-etl/"+op+"/"+phase+"-receipt.json");
            require(phase.equals(text(prior,"phase")) && op.equals(text(prior,"operation_id"))
                    && text(context,"context_sha256").equals(text(prior,"context_sha256")),"DAT03_RECOVERY_RECEIPT_CHANGED");
            require(prior.path("upstream_sha256").equals(receipts.isEmpty()?JSON.nullNode():JSON.valueToTree(digest(receipts.get(receipts.size()-1)))),"DAT03_RECOVERY_UPSTREAM_CHANGED");
            var artifact=ResearchRecoveryPreflight.read(silver,"real-etl/"+op+"/"+phase+".json");
            require(silver.uriForKey("real-etl/"+op+"/"+phase+".json").equals(text(prior,"artifact_uri"))
                    && digest(artifact).equals(text(prior,"artifact_sha256")),"DAT03_RECOVERY_ARTIFACT_CHANGED");
            receipts.add(prior);ResearchBoundedE2EJob.save(folder.resolve(phase+".json"),artifact);
        }
        require(prior.path("published").asBoolean(false),"DAT03_RECOVERY_NOT_PUBLISHED");
        record.put("operation_id",op);record.set("commit",ResearchRecoveryPreflight.read(silver,"gold-commits/"+op+"/commit.json"));
        record.set("publication",ResearchBoundedE2EJob.metadataLarge(folder.resolve("publish.json")));
        record.set("silver",ResearchBoundedE2EJob.metadataLarge(folder.resolve("silver.json")));
        require(record.path("silver").path("reconciliation").path("balanced").asBoolean(false)
                && "COMPLETE_MONTHS".equals(record.path("silver").path("scope_completeness").asText()),"DAT03_RECOVERY_RECONCILIATION_FAILED");
        var checks=record.path("publication").path("checks");require(checks.isObject() && checks.size()>=36,"DAT03_RECOVERY_CHECKS_MISSING");
        for(var it=checks.elements();it.hasNext();) {var check=it.next();require(check.path("actual").isIntegralNumber()
                && check.path("expected").isIntegralNumber() && check.path("actual").asLong()==check.path("expected").asLong(),"DAT03_RECOVERY_CHECK_FAILED");}
        ResearchBoundedE2EJob.save(folder.resolve("record.json"),record);return record;
    }
    /** A second, explicitly pinned boundary. Never autodetect/retry a partially published operation. */
    static ObjectNode checkpoint(JsonNode input,JsonNode request,Path root,SilverObjectStore store,GoldPublisher.Sql sql,Runnable lease) throws Exception {
        var cp=input.path("checkpoint");
        require("dat03-2023-publication-checkpoint-v1".equals(text(cp,"version"))
                && text(cp,"root").matches("/opt/pipeline/staging/dat03-continuation/[a-f0-9]{32}"),"DAT03_CHECKPOINT_EXACT_ROOT_REQUIRED");
        var fields=cp.path("files_sha256");require(fields.isObject() && fields.size()==18,"DAT03_CHECKPOINT_EXACT_FILES_REQUIRED");
        var names=new HashSet<String>();fields.fieldNames().forEachRemaining(names::add);
        var expected=new HashSet<>(List.of("continuation-request.json","compatibility.json","recovery-execution.json","live-preflight.json","source-pins.json","progress-2016-04.json"));
        for(String month:List.of("2016-04","2023-12")) {
            String op=RealEtlAdapter.operation("dat03_bounded_e2e",text(request,"run_id")+"-"+month);
            expected.add(op+"/context.json");
            for(String phase:month.equals("2016-04")?RealEtlAdapter.PHASES:RealEtlAdapter.PHASES.subList(0,4))expected.add(op+"/"+phase+".json");
        }
        require(names.equals(expected),"DAT03_CHECKPOINT_EXACT_FILES_REQUIRED");
        for(String name:names) {
            lease.run();Path path=ResearchRecoveryPreflight.safe(root.resolve("checkpoint").resolve(name));
            ResearchBoundedE2EJob.metadataLarge(path);
            require(sha(Files.readAllBytes(path)).equals(text(fields,name)),"DAT03_CHECKPOINT_FILE_CHANGED");
        }
        var failed=ResearchBoundedE2EJob.metadata(root.resolve("checkpoint/recovery-execution.json"));
        require(failed.path("exit_code").isInt() && failed.path("exit_code").asInt()!=0 && failed.path("lease_retained").asBoolean(false),"DAT03_CHECKPOINT_FAILED_ATTEMPT_REQUIRED");
        JsonNode parent=null;String cutoff=null;
        for(String month:List.of("2000-01","2011-04","2016-04")) {
            var item=ResearchBoundedE2EJob.metadataLarge(root.resolve("progress-"+month+".json"));var first=item.path("first");
            require(item.path("rerun_identical").asBoolean(false) && item.path("retention_verified").asBoolean(false)
                    && first.equals(item.path("rerun")),"DAT03_CHECKPOINT_PROGRESS_CHANGED");
            var ctx=first.path("context");String op=text(ctx,"operation_id");
            ResearchRecoveryPreflight.chain(store,ctx,6,first.path("receipts"),root,lease);
            require(first.path("commit").equals(ResearchRecoveryPreflight.read(store,"gold-commits/"+op+"/commit.json"))
                    && first.path("publication").equals(ResearchRecoveryPreflight.read(store,"gold-verification/"+op+"/publication.json")),"DAT03_CHECKPOINT_PUBLICATION_CHANGED");
            String date=text(ctx.path("resolver_request"),"source_cutoff_utc");if(cutoff==null)cutoff=date;
            require(cutoff.equals(date),"DAT03_CHECKPOINT_CUTOFF_CHANGED");parent=first;
        }
        String op=RealEtlAdapter.operation("dat03_bounded_e2e",text(request,"run_id")+"-2023-12");
        var ctx=ResearchBoundedE2EJob.metadata(root.resolve(op+"/context.json"));var resolver=ctx.path("resolver_request");
        require(op.equals(text(ctx,"operation_id")) && (text(request,"run_id")+"-2023-12").equals(text(ctx,"run_id"))
                && "dat03_bounded_e2e".equals(text(ctx,"dag_id")) && "2023-12-01T00:00:00Z".equals(text(ctx,"window_start_utc"))
                && "2024-01-01T00:00:00Z".equals(text(ctx,"window_end_utc"))
                && cutoff.equals(text(resolver,"source_cutoff_utc")) && cutoff.equals(text(resolver,"processed_at_utc"))
                && text(request,"namespace").equals(text(resolver,"namespace"))
                && ("jar-"+text(request,"runner_sha256").substring(0,16)).equals(text(resolver,"code_version")),"DAT03_CHECKPOINT_CONTEXT_CHANGED");
        ResearchRecoveryPreflight.chain(store,ctx,4,null,root,lease);
        for(String name:List.of("verify.json","verify-receipt.json","publish.json","publish-receipt.json"))
            require(!store.exists("real-etl/"+op+"/"+name),"DAT03_CHECKPOINT_UNKNOWN_LATER_PHASE");
        require(!store.exists("gold-verification/"+op+"/publication.json"),"DAT03_CHECKPOINT_ALREADY_PUBLISHED");
        var commit=ResearchRecoveryPreflight.read(store,"gold-commits/"+op+"/commit.json");
        String parentOp=text(parent,"operation_id"),parentKey="gold-commits/"+parentOp+"/commit.json";
        var baseline=JSON.createObjectNode().put("run_id",parentOp).put("registry_sha256",sha(store.read(FullMonthStateResolver.root(parentOp)+"/registry.json")))
                .put("commit_key",parentKey).put("commit_sha256",sha(store.read(parentKey)));
        require(commit.equals(ResearchBoundedE2EJob.metadataLarge(root.resolve(op+"/gold.json")))
                && op.equals(text(commit,"gold_run_id")) && text(request,"namespace").equals(text(commit,"namespace"))
                && resolver.path("baseline").equals(baseline)
                && snapshots(resolver.path("baseline_snapshots")).equals(parentSnapshots(parent))
                && "orc-06-v2".equals(text(resolver,"resolver_version"))
                && ResearchBoundedE2EJob.VERSION.equals(text(resolver,"config_version"))
                && months(resolver.path("target_months")).equals(Set.of("2023-12"))
                && snapshots(commit.path("scope").path("baseline_snapshots")).equals(snapshots(resolver.path("baseline_snapshots")))
                && months(commit.path("scope").path("affected_months")).equals(Set.of("2023-12")),"DAT03_CHECKPOINT_COMMIT_CHANGED");
        var plan=new GoldVerificationPlan(commit);var checks=JSON.createObjectNode();var pins=JSON.createObjectNode();
        for(String table:GoldIcebergWriter.TABLES) {
            pins.put(table,plan.snapshot(table));
            checked(sql,lease,checks,"current:"+table,"SELECT snapshot_id FROM "+plan.metadata(table,"refs")+" WHERE name='main'",plan.snapshot(table));
            checked(sql,lease,checks,"identity:"+table,"SELECT count(*) FROM "+plan.metadata(table,"snapshots")
                    +" WHERE snapshot_id="+plan.snapshot(table)+" AND summary['gold.operation_id']="+GoldVerificationPlan.quote(op)
                    +" AND summary['gold.identity_sha256']="+GoldVerificationPlan.quote(text(commit,"identity_sha256")),1);
        }
        checked(sql,lease,checks,"publication_absent","SELECT count(*) FROM "+text(request,"namespace")
                +".publication_status WHERE gold_run_id="+GoldVerificationPlan.quote(op),0);
        var out=JSON.createObjectNode().put("status","READY_FOR_PUBLICATION_RECOVERY").put("month","2023-12")
                .put("source_cutoff_utc",cutoff).put("research_ready",false);out.set("snapshots",pins);out.set("checks",checks);return out;
    }
    static void checked(GoldPublisher.Sql sql,Runnable lease,ObjectNode checks,String name,String query,long expected) throws Exception {
        lease.run();var result=sql.execute(query);require(result.rows().size()==1 && result.rows().get(0).size()==1
                && result.rows().get(0).get(0).isIntegralNumber() && result.rows().get(0).get(0).canConvertToLong(),"DAT03_CHECKPOINT_SQL_SCALAR_REQUIRED");
        long actual=result.rows().get(0).get(0).asLong();require(actual==expected,"DAT03_CHECKPOINT_SQL_MISMATCH");
        checks.putObject(name).put("sql",query).put("actual",actual).put("expected",expected);
    }
    public static void main(String[] args) throws Exception {
        require(args.length==1,"DAT03_CONTINUATION_REQUEST_REQUIRED");Path path=ResearchRecoveryPreflight.safe(Path.of(args[0]));Path root=path.getParent();
        var input=ResearchBoundedE2EJob.metadata(path);require(VERSION.equals(text(input,"version"))
                && LocalFeatureJob.codeSha().equals(text(input,"recovery_runner_sha256")),"DAT03_CONTINUATION_RUNNER_CHANGED");
        var preflight=ResearchBoundedE2EJob.metadata(root.resolve("preflight-request.json"));
        require(sha(Files.readAllBytes(root.resolve("preflight-request.json"))).equals(text(input,"preflight_request_sha256")),"DAT03_CONTINUATION_PREFLIGHT_CHANGED");
        Path original=ResearchRecoveryPreflight.safe(Path.of(text(preflight,"original_root")));
        require(original.toString().matches("/opt/pipeline/staging/dat03-e2e/[a-f0-9]{32}")
                && root.toString().matches("/opt/pipeline/staging/dat03-continuation/[a-f0-9]{32}"),"DAT03_CONTINUATION_EXACT_ROOT_REQUIRED");
        var request=ResearchRecoveryPreflight.validateFiles(preflight,original);
        require(sha(Files.readAllBytes(root.resolve("runner.jar"))).equals(text(request,"runner_sha256")),"DAT03_CONTINUATION_ORIGINAL_RUNNER_CHANGED");
        var compatible=compatibility(root.resolve("runner.jar"),root.resolve("recovery.jar"));
        require(compatible.equals(ResearchBoundedE2EJob.metadata(root.resolve("compatibility.json")))
                && sha(Files.readAllBytes(root.resolve("compatibility.json"))).equals(text(input,"compatibility_sha256")),"DAT03_CONTINUATION_COMPATIBILITY_CHANGED");
        Path guard=Path.of(Objects.requireNonNull(System.getenv("SOURCE_GUARD_ROOT")),"owner.json");
        Runnable lease=()->{try{require(ResearchBoundedE2EJob.metadata(guard).equals(request.path("lease_owner")),"DAT03_CONTINUATION_LEASE_NOT_HELD");}
            catch(Exception ex){throw new IllegalStateException("DAT03_CONTINUATION_LEASE_NOT_HELD");}};
        String ns=text(request,"namespace"),run=text(request,"run_id");
        var inventory=new ArrayList<>(JmaArchiveInventory.read(root.resolve("baseline.csv")));inventory.addAll(JmaArchiveInventory.read(root.resolve("boundary.csv")));
        var sql=new TrinoSqlClient(URI.create("http://"+System.getenv("TRINO_HOST")+":"+System.getenv("TRINO_INTERNAL_PORT")));
        try(var bronze=MinioBronzeObjectStore.fromEnvironment();var silver=MinioSilverObjectStore.fromEnvironment()) {
            boolean checkpoint=input.has("checkpoint");
            var plan=checkpoint?checkpoint(input,request,root,silver,sql,lease):ResearchRecoveryPreflight.inspect(request,original,silver,sql,lease);
            ResearchBoundedE2EJob.save(root.resolve("live-preflight.json"),plan);
            Instant cutoff=Instant.parse(text(plan,"source_cutoff_utc"));var pins=readPins(request,original,bronze,inventory,cutoff);
            String lastOp=RealEtlAdapter.operation("dat03_bounded_e2e",run+"-2023-12");
            if(!checkpoint)for(String key:List.of("real-etl/"+lastOp+"/context.json",FullMonthStateResolver.root(lastOp)+"/identity.sha256","gold-commits/"+lastOp+"/identity.json"))
                require(!silver.exists(key),"DAT03_CONTINUATION_LAST_CASE_ALREADY_STARTED");
            if(checkpoint) require(pins.equals(ResearchBoundedE2EJob.metadata(root.resolve("checkpoint/source-pins.json"))),"DAT03_CHECKPOINT_SOURCE_CHANGED");
            ResearchBoundedE2EJob.save(root.resolve("source-pins.json"),pins);
            var spark=GoldCommitJob.configured(System.getenv()).master("local[1]").appName("dat03-bounded-continuation")
                    .config("spark.ui.enabled","false").config("spark.sql.shuffle.partitions","1").config("spark.sql.adaptive.enabled","false")
                    .config("spark.sql.codegen.wholeStage","false").getOrCreate();spark.sparkContext().setLogLevel("ERROR");
            try {
                var writer=new GoldIcebergWriter(spark,ns,silver,lease);require(writer.baselines().equals(snapshots(plan.path("snapshots"))),"DAT03_CONTINUATION_STALE_BASELINE");
                var adapter=new RealEtlAdapter(spark,bronze,silver,inventory,writer,new GoldPublisher(sql,silver,lease),lease);
                var report=JSON.createObjectNode().put("version",ResearchBoundedE2EJob.VERSION).put("namespace",ns).put("run_id",run)
                        .put("research_ready",false).put("full_research_catalog",false).put("synthetic_data_used",false).put("runner_sha256",text(request,"runner_sha256"));
                report.set("jma_bronze_inputs",pins.path("jma"));report.set("usgs_bronze_input",pins.path("usgs"));
                report.putObject("recovery").put("version",VERSION).put("runner_sha256",text(input,"recovery_runner_sha256"))
                        .put("compatibility_sha256",text(input,"compatibility_sha256")).put("preflight_request_sha256",text(input,"preflight_request_sha256"));
                var cases=report.putArray("cases");JsonNode parent=null;
                var completed=checkpoint?List.of("2000-01","2011-04","2016-04"):ResearchRecoveryPreflight.COMPLETED;
                for(String month:completed) {var item=ResearchBoundedE2EJob.metadataLarge(root.resolve("progress-"+month+".json"));cases.add(item);parent=item.path("first");}
                var retained=new TreeMap<String,String>();long previous=parent.path("commit").path("tables").path("event_current").path("snapshot_id").asLong();
                for(String month:completed)retained.put(month,ResearchBoundedE2EJob.partition(spark,ns,previous,month));
                for(String month:checkpoint?List.of("2023-12"):List.of("2016-04","2023-12")) {
                    boolean publicationOnly=checkpoint || month.equals("2016-04");
                    JsonNode context=checkpoint?ResearchBoundedE2EJob.metadata(root.resolve(lastOp+"/context.json")):month.equals("2016-04")?ResearchBoundedE2EJob.metadata(original.resolve(text(plan,"operation_id")+"/context.json"))
                            :ResearchBoundedE2EJob.context(run+"-"+month,month,List.of(pins.path("jma").path("2023"),pins.path("jma").path("2024"),pins.path("usgs")),parent,ns,
                                    "jar-"+text(request,"runner_sha256").substring(0,16),cutoff,writer,silver);
                    if(checkpoint)require(snapshots(context.path("resolver_request").path("baseline_snapshots")).equals(parentSnapshots(parent))
                            && context.path("resolver_request").path("bronze_inputs").equals(JSON.valueToTree(List.of(pins.path("jma").path("2023"),pins.path("jma").path("2024"),pins.path("usgs")))),"DAT03_CHECKPOINT_BASELINE_CHANGED");
                    var first=publicationOnly?publicationOnly(adapter::execute,silver,context,root,lease):ResearchBoundedE2EJob.execute(adapter,silver,context,root,lease);
                    var before=writer.baselines();
                    var rerun=publicationOnly?publicationOnly(adapter::execute,silver,context,root,lease):ResearchBoundedE2EJob.execute(adapter,silver,context,root,lease);
                    require(first.equals(rerun) && before.equals(writer.baselines()),"DAT03_CONTINUATION_RERUN_CHANGED");
                    require(months(first.path("silver").path("affected_months")).equals(Set.of(month)),"DAT03_CONTINUATION_AFFECTED_SCOPE_CHANGED");
                    long snapshot=first.path("commit").path("tables").path("event_current").path("snapshot_id").asLong();
                    for(var old:retained.entrySet())require(old.getValue().equals(ResearchBoundedE2EJob.partition(spark,ns,snapshot,old.getKey())),"DAT03_CONTINUATION_OUTSIDE_MONTH_CHANGED");
                    retained.put(month,ResearchBoundedE2EJob.partition(spark,ns,snapshot,month));
                    var item=cases.addObject().put("month",month).put("rerun_identical",true).put("retention_verified",true);item.set("first",first);item.set("rerun",rerun);
                    ResearchBoundedE2EJob.save(root.resolve("progress-"+month+".json"),item);parent=first;
                    System.out.println("event=dat03_continuation_month_passed month="+month+" checks="+first.path("publication").path("checks").size());
                }
                lease.run();report.set("final_snapshots",JSON.valueToTree(writer.baselines()));report.put("status","PASSED").put("completed_at_utc",Instant.now().toString());
                ResearchBoundedE2EJob.save(root.resolve("report.json"),report);
            } finally {spark.stop();}
        }
    }
    static Map<String,Long> parentSnapshots(JsonNode parent) {
        var out=new TreeMap<String,Long>();for(String table:GoldIcebergWriter.TABLES)out.put(table,parent.path("commit").path("tables").path(table).path("snapshot_id").asLong());return out;
    }
}
