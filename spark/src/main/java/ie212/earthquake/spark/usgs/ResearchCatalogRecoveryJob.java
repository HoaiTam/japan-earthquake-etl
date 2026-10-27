package ie212.earthquake.spark.usgs;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import static ie212.earthquake.spark.usgs.ResearchCatalogState.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.gold.*;
import ie212.earthquake.spark.jma.*;
import ie212.earthquake.spark.ml.LocalFeatureJob;
import ie212.earthquake.spark.orchestration.*;
import ie212.earthquake.spark.silver.*;
import java.net.URI;
import java.nio.file.*;
import java.util.*;
import java.util.jar.JarFile;

/** Publication-only recovery. No Spark session, downloader, resolver.resolve or Gold writer. */
public final class ResearchCatalogRecoveryJob {
    public static final String RECOVERY_VERSION="dat03-catalog-publication-recovery-v2";
    public static final String ENTRY="ie212/earthquake/spark/usgs/ResearchCatalogRecoveryJob.class";
    public static final String TRANSPORT="ie212/earthquake/spark/gold/TrinoSqlClient.class";
    private ResearchCatalogRecoveryJob() { }

    /** Every original entry must be byte-identical, including the SQL client. */
    static void compatible(Path original,Path recovery) throws Exception {
        compatible(original,recovery,null);
    }
    /** V2 permits only the explicitly pinned read-poll transport; all ETL/publisher bytes stay frozen. */
    static void compatible(Path original,Path recovery,String sqlProfile) throws Exception {
        require(sqlProfile==null || TrinoSqlClient.RESEARCH_READ_PROFILE.equals(sqlProfile),"RECOVERY_SQL_PROFILE_INVALID");
        try(var a=new JarFile(original.toFile());var b=new JarFile(recovery.toFile())) {
            var names=new HashSet<String>();var seen=new HashSet<String>();long bytes=0;
            for(var it=a.entries();it.hasMoreElements();) {
                var item=it.nextElement();require(names.add(item.getName()),"RECOVERY_DUPLICATE_JAR_ENTRY");
                if(item.isDirectory())continue;
                var copy=b.getJarEntry(item.getName());require(copy!=null && item.getSize()<=16*1024*1024 && copy.getSize()<=16*1024*1024,"RECOVERY_ORIGINAL_CODE_MISSING");
                byte[] before=a.getInputStream(item).readAllBytes(),after=b.getInputStream(copy).readAllBytes();bytes+=before.length+after.length;
                require(bytes<=256L*1024*1024 && (Arrays.equals(before,after)
                        || (sqlProfile!=null && TRANSPORT.equals(item.getName()))),"RECOVERY_DATA_PLANE_CHANGED");
            }
            for(var it=b.entries();it.hasMoreElements();) {
                var item=it.nextElement();require(seen.add(item.getName()),"RECOVERY_DUPLICATE_JAR_ENTRY");
                if(!names.contains(item.getName()))require(ENTRY.equals(item.getName()) && item.getSize()>0 && item.getSize()<=1024*1024,"RECOVERY_UNREVIEWED_CODE_ADDED");
            }
            require(!names.contains(ENTRY) && seen.contains(ENTRY),"RECOVERY_ENTRY_REQUIRED");
        }
    }
    static List<String> resolvedFailures(JsonNode request) throws Exception {
        String primary=text(request,"failed_session");require(primary.matches("[a-f0-9]{32}"),"RECOVERY_SESSION_ID_REQUIRED");
        var result=new ArrayList<String>();result.add(primary);var related=request.path("related_failed_sessions");
        require(related.isArray() && related.size()<=2,"RECOVERY_RELATED_FAILURES_INVALID");
        for(var id:related) {
            require(id.isTextual() && id.asText().matches("[a-f0-9]{32}") && !result.contains(id.asText()),"RECOVERY_RELATED_FAILURES_INVALID");
            result.add(id.asText());
        }
        return List.copyOf(result);
    }
    static JsonNode stored(SilverObjectStore store,String key) throws Exception {
        byte[] bytes=store.read(key);require(bytes.length>0 && bytes.length<=4*1024*1024,"RECOVERY_ARTIFACT_BOUND");return JSON.readTree(bytes);
    }
    static void unchanged(Path root,JsonNode pins) throws Exception {
        require(pins.isObject() && pins.size()>=10 && pins.size()<=20,"RECOVERY_FAILURE_PINS_REQUIRED");
        for(var it=pins.fields();it.hasNext();) {
            var pin=it.next();String name=pin.getKey();
            require(!name.startsWith("/") && !name.contains("..") && name.matches("[a-zA-Z0-9_./-]+")
                    && pin.getValue().isTextual() && pin.getValue().asText().matches("[a-f0-9]{64}"),"RECOVERY_PIN_INVALID");
            Path file=safe(root.resolve(name));require(Files.isRegularFile(file) && Files.size(file)<=16*1024*1024
                    && sha(Files.readAllBytes(file)).equals(pin.getValue().asText()),"RECOVERY_FAILED_EVIDENCE_CHANGED");
        }
    }
    static void scalar(GoldPublisher.Sql sql,Runnable lease,ObjectNode checks,String name,String query,long expected) throws Exception {
        lease.run();var result=sql.execute(query);
        require(result.rows().size()==1 && result.rows().get(0).size()==1 && result.rows().get(0).get(0).isIntegralNumber()
                && result.rows().get(0).get(0).canConvertToLong() && result.rows().get(0).get(0).asLong()==expected,"RECOVERY_SQL_MISMATCH");
        checks.putObject(name).put("sql",query).put("expected",expected).put("actual",expected);
    }
    static ObjectNode receipt(SilverObjectStore store,JsonNode context,String phase,JsonNode previous,JsonNode artifact,Runnable lease) throws Exception {
        String op=text(context,"operation_id"),root="real-etl/"+op;lease.run();
        var result=JSON.createObjectNode().put("contract_version",RealEtlAdapter.PROTOCOL).put("mode","real")
                .put("operation_id",op).put("context_sha256",text(context,"context_sha256")).put("phase",phase).put("status","ok");
        result.put("upstream_sha256",digest(previous)).put("artifact_uri",store.uriForKey(root+"/"+phase+".json")).put("artifact_sha256",digest(artifact));
        var ids=result.putObject("snapshots");for(String table:GoldIcebergWriter.TABLES)ids.set(table,artifact.path("snapshot_bundle").path(table).path("snapshot_id"));
        if(phase.equals("publish"))result.put("published",true).put("publication_id",text(artifact,"publication_id")).put("publication_status","Published");
        for(var item:Map.of(root+"/"+phase+".json",artifact,root+"/"+phase+"-receipt.json",result).entrySet()) {
            byte[] bytes=JSON.writeValueAsBytes(canonical(item.getValue()));lease.run();
            if(store.exists(item.getKey()))require(Arrays.equals(bytes,store.read(item.getKey())),"RECOVERY_IMMUTABLE_CONFLICT");
            else store.put(item.getKey(),bytes,"application/json");
            require(Arrays.equals(bytes,store.read(item.getKey())),"RECOVERY_RECEIPT_READBACK_FAILED");
        }
        return result;
    }
    public static void main(String[] args) throws Exception {
        require(args.length==2 && Set.of("preflight","run").contains(args[1]),"RECOVERY_EXPLICIT_MODE_REQUIRED");
        Path input=safe(Path.of(args[0])),attempt=input.getParent();JsonNode request=read(input,65536);
        require(attempt.toString().matches("/opt/pipeline/staging/dat03-catalog-recovery/[a-f0-9]{32}")
                && RECOVERY_VERSION.equals(text(request,"version")),"RECOVERY_EXACT_ATTEMPT_REQUIRED");
        Path root=safe(Path.of(text(request,"catalog_root"))),control=safe(Path.of(text(request,"continuation_root")));
        require(root.toString().matches("/opt/pipeline/staging/dat03-catalog/[a-f0-9]{32}")
                && control.toString().matches("/opt/pipeline/staging/dat03-catalog-continuation/[a-f0-9]{32}"),"RECOVERY_EXACT_CATALOG_REQUIRED");
        String failed=text(request,"failed_session"),sid=text(request,"recovery_session");
        require(failed.matches("[a-f0-9]{32}") && sid.matches("[a-f0-9]{32}") && !failed.equals(sid),"RECOVERY_SESSION_ID_REQUIRED");
        JsonNode r=read(root.resolve("request.json"),65536);ResearchCatalogState.validate(r);
        require(sha(Files.readAllBytes(root.resolve("request.json"))).equals(text(request,"catalog_request_sha256")),"RECOVERY_CATALOG_CHANGED");
        JsonNode continuation=read(control.resolve("continuation-request.json"),65536);
        require(sha(Files.readAllBytes(control.resolve("continuation-request.json"))).equals(text(request,"continuation_request_sha256"))
                && text(continuation,"catalog_root").equals(root.toString())
                && text(continuation,"catalog_request_sha256").equals(text(request,"catalog_request_sha256"))
                && sha(Files.readAllBytes(control.resolve("runner.jar"))).equals(text(request,"base_runner_sha256"))
                && text(continuation,"runner_sha256").equals(text(request,"base_runner_sha256"))
                && LocalFeatureJob.codeSha().equals(text(request,"runner_sha256")),"RECOVERY_RUNNER_CHANGED");
        require(ResearchCatalogCompatibility.verify(root.resolve("runner.jar"),control.resolve("runner.jar"))
                .equals(read(control.resolve("compatibility.json"),65536)),"RECOVERY_BASE_COMPATIBILITY_CHANGED");
        require(TrinoSqlClient.RESEARCH_READ_PROFILE.equals(text(request,"sql_read_profile")),"RECOVERY_SQL_PROFILE_INVALID");
        try(var jar=new JarFile(attempt.resolve("runner.jar").toFile())) {
            require(sha(jar.getInputStream(jar.getJarEntry(TRANSPORT)).readAllBytes()).equals(text(request,"transport_sha256")),"RECOVERY_TRANSPORT_PIN_CHANGED");
        }
        compatible(control.resolve("runner.jar"),attempt.resolve("runner.jar"),text(request,"sql_read_profile"));unchanged(root,request.path("failed_files"));
        var resolved=resolvedFailures(request);
        JsonNode execution=read(root.resolve("sessions/"+failed+"/execution.json"),65536);
        require(execution.path("exit_code").isInt() && execution.path("exit_code").asInt()!=0
                && execution.path("timed_out").isBoolean() && !execution.path("timed_out").asBoolean()
                && !Files.exists(root.resolve("sessions/"+failed+"/result.json")) && !Files.exists(root.resolve("sessions/"+failed+"/acceptance.json")),"RECOVERY_KNOWN_TERMINAL_FAILURE_REQUIRED");
        for(String id:resolved) {
            JsonNode prior=read(root.resolve("sessions/"+id+"/execution.json"),65536);
            require(prior.path("exit_code").isInt() && prior.path("exit_code").asInt()!=0
                    && prior.path("timed_out").isBoolean() && !prior.path("timed_out").asBoolean()
                    && !Files.exists(root.resolve("sessions/"+id+"/result.json")) && !Files.exists(root.resolve("sessions/"+id+"/acceptance.json")),"RECOVERY_RELATED_TERMINAL_FAILURE_REQUIRED");
            if(!id.equals(failed))require(text(read(root.resolve("sessions/"+id+"/intent.json"),65536),"failed_session").equals(failed),"RECOVERY_RELATED_SCOPE_CHANGED");
        }
        var done=completed(root,r);int initial=done.size();
        require(initial==request.path("initial_completed_months").asInt(-1) && initial>0 && initial<288,"RECOVERY_PREFIX_CHANGED");
        String first=MONTHS.get(initial);Path folder=safe(root.resolve("months/"+first));JsonNode context=read(folder.resolve("context.json"),65536);
        RealEtlAdapter.validateContext(context);var resolver=context.path("resolver_request");
        var targets=new ArrayList<String>();resolver.path("target_months").forEach(v->targets.add(v.asText()));
        require(!targets.isEmpty() && targets.size()<=12 && initial+targets.size()<=288
                && targets.equals(MONTHS.subList(initial,initial+targets.size()))
                && request.path("target_months").equals(resolver.path("target_months"))
                && text(resolver,"namespace").equals(text(r,"namespace"))
                && text(resolver,"source_cutoff_utc").equals(text(r,"source_cutoff_utc"))
                && text(resolver,"code_version").equals("jar-"+text(request,"base_runner_sha256").substring(0,16)),"RECOVERY_SCOPE_CHANGED");
        String run=text(r,"run_id"),op=text(context,"operation_id");Path owner=Path.of(Objects.requireNonNull(System.getenv("SOURCE_GUARD_ROOT")),"owner.json");
        Runnable lease=()->{try{JsonNode held=read(owner,65536);require(held.size()==2 && DAG.equals(text(held,"dag_id")) && run.equals(text(held,"run_id")),"RECOVERY_OWN_LEASE_REQUIRED");}
            catch(Exception e){throw new IllegalStateException("RECOVERY_OWN_LEASE_REQUIRED");}};lease.run();
        var inventory=new ArrayList<JmaArchiveEntry>();for(String name:List.of("baseline.csv","boundary.csv")) {
            require(sha(Files.readAllBytes(root.resolve(name))).equals(text(r,name.equals("baseline.csv")?"baseline_inventory_sha256":"boundary_inventory_sha256")),"RECOVERY_INVENTORY_CHANGED");
            inventory.addAll(JmaArchiveInventory.read(root.resolve(name)));
        }
        ResearchCatalogJob.validateSources(root,r,inventory);
        try(var bronze=MinioBronzeObjectStore.fromEnvironment();var store=MinioSilverObjectStore.fromEnvironment()) {
            require(stored(store,"real-etl/"+op+"/context.json").equals(context),"RECOVERY_STORED_CONTEXT_CHANGED");
            var record=JSON.createObjectNode();record.set("context",context);var receipts=record.putArray("receipts");JsonNode previous=null;
            for(String phase:List.of("readiness","bronze","silver","gold")) {
                JsonNode artifact=stored(store,"real-etl/"+op+"/"+phase+".json"),pin=stored(store,"real-etl/"+op+"/"+phase+"-receipt.json");
                require(artifact.equals(read(folder.resolve(phase+".json"),4*1024*1024)) && phase.equals(text(pin,"phase"))
                        && "ok".equals(text(pin,"status")) && op.equals(text(pin,"operation_id"))
                        && text(context,"context_sha256").equals(text(pin,"context_sha256"))
                        && digest(artifact).equals(text(pin,"artifact_sha256"))
                        && store.uriForKey("real-etl/"+op+"/"+phase+".json").equals(text(pin,"artifact_uri"))
                        && pin.path("upstream_sha256").equals(previous==null?JSON.nullNode():JSON.valueToTree(digest(previous))),"RECOVERY_PHASE_CHAIN_CHANGED");
                receipts.add(pin);previous=pin;
            }
            for(String name:List.of("verify.json","verify-receipt.json","publish.json","publish-receipt.json"))
                require(!store.exists("real-etl/"+op+"/"+name),"RECOVERY_LATER_PHASE_ALREADY_EXISTS");
            require(!store.exists("gold-verification/"+op+"/publication.json"),"RECOVERY_PUBLICATION_ALREADY_EXISTS");
            JsonNode commit=stored(store,"gold-commits/"+op+"/commit.json"),monthly=read(folder.resolve("silver.json"),4*1024*1024);
            require(commit.equals(read(folder.resolve("gold.json"),4*1024*1024)) && text(commit,"gold_run_id").equals(op)
                    && commit.path("namespace").equals(resolver.path("namespace")) && text(commit,"status").equals("COMMITTED")
                    && commit.path("scope").path("baseline_snapshots").equals(resolver.path("baseline_snapshots"))
                    && commit.path("scope").path("affected_months").equals(resolver.path("target_months"))
                    && monthly.path("reconciliation").path("balanced").asBoolean(false)
                    && "COMPLETE_MONTHS".equals(text(monthly,"scope_completeness")),"RECOVERY_COMMIT_CHANGED");
            var last=done.get(done.size()-1);JsonNode parent=read(root.resolve("months/"+text(last,"month")+"/record.json"),4*1024*1024);
            ResearchCatalogJob.assertRecord(parent,parent.path("context"));String parentOp=text(parent.path("context"),"operation_id");
            var baseline=JSON.createObjectNode().put("run_id",parentOp).put("registry_sha256",sha(store.read(FullMonthStateResolver.root(parentOp)+"/registry.json")))
                    .put("commit_key","gold-commits/"+parentOp+"/commit.json").put("commit_sha256",sha(store.read("gold-commits/"+parentOp+"/commit.json")));
            require(resolver.path("baseline").equals(baseline) && resolver.path("baseline_snapshots").equals(last.path("snapshots"))
                    && parent.path("publication").equals(stored(store,"gold-verification/"+parentOp+"/publication.json")),"RECOVERY_PARENT_CHANGED");
            JsonNode registry=stored(store,FullMonthStateResolver.root(op)+"/registry.json");
            var state=new FullMonthStateResolver(bronze,store,inventory,()->snapshots(commit.path("scope").path("baseline_snapshots")),lease);
            require(state.verify(op,digest(registry)).equals(monthly) && digest(resolver).equals(text(monthly,"request_sha256")),"RECOVERY_SILVER_CHANGED");
            var pins=read(root.resolve("bronze-pins.json"),65536);require(pins.size()==37 && pins.equals(registry.path("active_inputs")),"RECOVERY_BRONZE_PINS_CHANGED");
            var reader=new FullMonthBronzeReader(bronze,inventory,new FullMonthBronzeReader.JmaStructureCache());
            for(var pin:pins){lease.run();reader.descriptor(pin,utc(r,"source_cutoff_utc"));}
            var transport=TrinoSqlClient.researchCatalog(URI.create("http://"+System.getenv("TRINO_HOST")+":"+System.getenv("TRINO_INTERNAL_PORT")));
            var queryCount=new int[]{0};GoldPublisher.Sql sql=query->{lease.run();System.out.println("event=dat03_catalog_recovery_sql ordinal="+(++queryCount[0]));return transport.execute(query);};
            var plan=new GoldVerificationPlan(commit);var checks=JSON.createObjectNode();
            for(String table:GoldIcebergWriter.TABLES) {
                scalar(sql,lease,checks,"current:"+table,"SELECT snapshot_id FROM "+plan.metadata(table,"refs")+" WHERE name='main'",plan.snapshot(table));
                scalar(sql,lease,checks,"identity:"+table,"SELECT count(*) FROM "+plan.metadata(table,"snapshots")+" WHERE snapshot_id="+plan.snapshot(table)
                        +" AND summary['gold.operation_id']="+GoldVerificationPlan.quote(op)+" AND summary['gold.identity_sha256']="+GoldVerificationPlan.quote(text(commit,"identity_sha256")),1);
            }
            scalar(sql,lease,checks,"publication_absent","SELECT count(*) FROM "+text(r,"namespace")+".publication_status WHERE gold_run_id="+GoldVerificationPlan.quote(op),0);
            var preflight=JSON.createObjectNode().put("version",RECOVERY_VERSION).put("status","READY_FOR_PUBLICATION_RECOVERY").put("research_ready",false)
                    .put("original_failed_execution_sha256",sha(Files.readAllBytes(root.resolve("sessions/"+failed+"/execution.json"))))
                    .put("sql_read_profile",TrinoSqlClient.RESEARCH_READ_PROFILE);preflight.set("checks",checks);
            // Reproduce the real slow quality query read-only before opening publication recovery.
            var probe=JSON.createObjectNode();long probeStarted=System.nanoTime();
            scalar(sql,lease,probe,"primary_provenance:2",plan.factBlockerBucket("primary_provenance","2"),0);
            preflight.set("sql_read_probe",probe);preflight.put("sql_read_probe_seconds",(System.nanoTime()-probeStarted)/1_000_000_000.0);
            save(attempt.resolve(args[1].equals("preflight")?"preflight.json":"live-preflight.json"),preflight);
            if(args[1].equals("preflight"))return;
            Path session=safe(root.resolve("sessions/"+sid));require(Files.isDirectory(session),"RECOVERY_NEW_SESSION_REQUIRED");
            var publisher=GoldPublisher.researchCatalogCheckpointed(sql,store,lease);
            for(String phase:List.of("verify","publish")) {
                System.out.println("event=dat03_catalog_recovery_phase phase="+phase);lease.run();
                JsonNode artifact=phase.equals("verify")?publisher.verify(op,text(r,"namespace"),text(commit,"identity_sha256"))
                        :publisher.publish(op,text(r,"namespace"),text(commit,"identity_sha256"));
                require("PASSED".equals(text(artifact,"verify_status")),"RECOVERY_VERIFICATION_FAILED");
                previous=receipt(store,context,phase,previous,artifact,lease);receipts.add(previous);save(folder.resolve(phase+".json"),artifact);
            }
            record.set("commit",commit);record.put("commit_journal_sha256",sha(store.read("gold-commits/"+op+"/commit.json")));
            record.set("publication",read(folder.resolve("publish.json"),4*1024*1024));record.set("silver",monthly);ResearchCatalogJob.assertRecord(record,context);
            require(stored(store,"gold-verification/"+op+"/publication.json").equals(record.path("publication")),"RECOVERY_PUBLICATION_READBACK_FAILED");
            var finalPins=JSON.createObjectNode();for(String table:GoldIcebergWriter.TABLES) {
                finalPins.put(table,plan.snapshot(table));scalar(sql,lease,checks,"final:"+table,"SELECT snapshot_id FROM "+plan.metadata(table,"refs")+" WHERE name='main'",plan.snapshot(table));
            }
            unchanged(root,request.path("failed_files"));lease.run();
            for(String month:targets) {
                Path member=safe(root.resolve("months/"+month));Files.createDirectories(member);save(member.resolve("context.json"),context);save(member.resolve("record.json"),record);
                var checkpoint=JSON.createObjectNode().put("version",ResearchCatalogState.VERSION).put("month",month).put("status","PUBLISHED")
                        .put("request_identity_sha256",digest(r)).put("record_sha256",sha(Files.readAllBytes(member.resolve("record.json"))))
                        .put("previous_checkpoint_sha256",sha(Files.readAllBytes(root.resolve("months/"+MONTHS.get(done.size()-1)+"/checkpoint.json"))));
                checkpoint.set("snapshots",finalPins);save(member.resolve("checkpoint.json"),checkpoint);done.add(checkpoint);
            }
            var result=progress(r,completed(root,r));result.set("current_snapshots",finalPins);result.putNull("pending_month");
            result.put("completed_in_session",targets.size()).put("session_runner_sha256",text(request,"runner_sha256"))
                    .put("recovery_request_sha256",sha(Files.readAllBytes(input))).put("recovered_session",failed).put("executed_sql_count",queryCount[0])
                    .put("sql_read_profile",TrinoSqlClient.RESEARCH_READ_PROFILE);
            result.set("resolved_failed_sessions",JSON.valueToTree(resolved));
            save(session.resolve("result.json"),result);System.out.println("event=dat03_catalog_recovery_verified completed="+done.size());
        }
    }
}
