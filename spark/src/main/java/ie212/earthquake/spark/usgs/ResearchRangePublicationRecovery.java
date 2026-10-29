package ie212.earthquake.spark.usgs;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import static ie212.earthquake.spark.usgs.ResearchRangeState.*;
import static ie212.earthquake.spark.usgs.ResearchRangeState.months;
import static ie212.earthquake.spark.usgs.ResearchRangeState.validate;
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

/** Recover an interrupted verification, never replay ETL or discard a committed batch. */
public final class ResearchRangePublicationRecovery {
    public static final String ENTRY="ie212/earthquake/spark/usgs/ResearchRangePublicationRecovery.class";
    private ResearchRangePublicationRecovery() { }

    /** The recovery adds one class. Even the publisher/transport/manifest remain byte-identical. */
    static void compatible(Path original,Path recovery) throws Exception {
        try(var a=new JarFile(original.toFile());var b=new JarFile(recovery.toFile())) {
            var names=new HashSet<String>();long total=0;
            for(var it=a.entries();it.hasMoreElements();) {
                var x=it.nextElement();require(names.add(x.getName()),"RANGE_REPAIR_DUPLICATE_ENTRY");
                var y=b.getJarEntry(x.getName());require(y!=null && x.getSize()>=0 && x.getSize()<=16*1024*1024
                        && y.getSize()==x.getSize(),"RANGE_REPAIR_CODE_BOUND");
                total+=x.getSize();require(total<=128L*1024*1024
                        && Arrays.equals(a.getInputStream(x).readAllBytes(),b.getInputStream(y).readAllBytes()),"RANGE_REPAIR_ORIGINAL_CODE_CHANGED");
            }
            var added=new HashSet<String>();for(var it=b.entries();it.hasMoreElements();) {
                var x=it.nextElement();require(added.add(x.getName()),"RANGE_REPAIR_DUPLICATE_ENTRY");
                require(names.contains(x.getName()) || (ENTRY.equals(x.getName()) && x.getSize()>0 && x.getSize()<=1024*1024),"RANGE_REPAIR_EXTRA_CODE");
            }
            require(!names.contains(ENTRY) && added.size()==names.size()+1 && added.contains(ENTRY),"RANGE_REPAIR_ENTRY_REQUIRED");
        }
    }
    static void unchanged(Path root,JsonNode pins) throws Exception {
        require(pins.isObject() && pins.size()>=10 && pins.size()<=32,"RANGE_REPAIR_EVIDENCE_SET");
        for(var it=pins.fields();it.hasNext();) {
            var p=it.next();String n=p.getKey();require(!n.startsWith("/") && !n.contains("..") && n.matches("[a-zA-Z0-9_./-]+")
                    && p.getValue().asText().matches("[a-f0-9]{64}"),"RANGE_REPAIR_PIN_INVALID");
            var file=safe(root.resolve(n));require(Files.size(file)<=16*1024*1024
                    && sha(Files.readAllBytes(file)).equals(p.getValue().asText()),"RANGE_REPAIR_EVIDENCE_CHANGED");
        }
    }
    static List<String> targets(JsonNode r,int count,JsonNode ctx) throws Exception {
        var planned=months(r);var t=ctx.path("resolver_request").path("target_months");
        require(count>0 && count<planned.size() && t.isArray() && t.size()>0 && t.size()<=12
                && count+t.size()<=planned.size(),"RANGE_REPAIR_TARGET_BOUND");
        var result=new ArrayList<String>();t.forEach(m->result.add(m.asText()));
        require(result.equals(planned.subList(count,count+t.size())),"RANGE_REPAIR_EXACT_PENDING_PREFIX");return result;
    }
    static void storedRecord(SilverObjectStore store,JsonNode record) throws Exception {
        var ctx=record.path("context");ResearchCatalogJob.assertRecord(record,ctx);String op=text(ctx,"operation_id");
        byte[] commit=store.read("gold-commits/"+op+"/commit.json");
        require(sha(commit).equals(text(record,"commit_journal_sha256")) && JSON.readTree(commit).equals(record.path("commit"))
                && ResearchCatalogRecoveryJob.stored(store,"gold-verification/"+op+"/publication.json").equals(record.path("publication"))
                && ResearchCatalogRecoveryJob.stored(store,"real-etl/"+op+"/context.json").equals(ctx),"RANGE_REPAIR_STORED_PARENT_CHANGED");
        for(var receipt:record.path("receipts")) {
            String phase=text(receipt,"phase"),base="real-etl/"+op+"/"+phase;
            require(ResearchCatalogRecoveryJob.stored(store,base+"-receipt.json").equals(receipt)
                    && digest(ResearchCatalogRecoveryJob.stored(store,base+".json")).equals(text(receipt,"artifact_sha256")),"RANGE_REPAIR_STORED_CHAIN_CHANGED");
        }
    }
    public static void main(String[] args) throws Exception {
        require(args.length==2 && Set.of("preflight","run").contains(args[1]),"RANGE_REPAIR_MODE_REQUIRED");
        boolean run=args[1].equals("run");Path file=safe(Path.of(args[0])),attempt=file.getParent();var q=read(file,65536);
        require(attempt.toString().matches("/opt/pipeline/staging/dat03-range-repair/[a-f0-9]{32}")
                && "dat03-range-publication-repair-v1".equals(text(q,"version")),"RANGE_REPAIR_ATTEMPT_REQUIRED");
        Path root=safe(Path.of(text(q,"range_root")));require(root.toString().matches("/opt/pipeline/staging/dat03-range/[a-f0-9]{32}"),"RANGE_REPAIR_ROOT_REQUIRED");
        var r=read(root.resolve("request.json"),65536);validate(r);String failed=text(q,"failed_session"),recovery=text(q,"recovery_session");
        require(failed.matches("[a-f0-9]{32}") && recovery.matches("[a-f0-9]{32}") && !failed.equals(recovery)
                && root.getFileName().toString().equals(text(r,"run_id").substring(12))
                && sha(Files.readAllBytes(root.resolve("request.json"))).equals(text(q,"range_request_sha256"))
                && sha(Files.readAllBytes(root.resolve("runner.jar"))).equals(text(r,"runner_sha256"))
                && LocalFeatureJob.codeSha().equals(text(q,"runner_sha256")),"RANGE_REPAIR_IDENTITY_CHANGED");
        compatible(root.resolve("runner.jar"),attempt.resolve("runner.jar"));unchanged(root,q.path("failed_files"));
        Path old=root.resolve("sessions/"+failed);var execution=read(old.resolve("execution.json"),65536);
        require(execution.path("exit_code").isInt() && execution.path("exit_code").asInt()==1
                && execution.path("timed_out").isBoolean() && !execution.path("timed_out").asBoolean()
                && !Files.exists(old.resolve("result.json")) && !Files.exists(old.resolve("acceptance.json")),"RANGE_REPAIR_KNOWN_FAILURE_REQUIRED");
        require(Files.size(old.resolve("stderr.private"))<=16*1024*1024
                && Files.readString(old.resolve("stderr.private")).contains("java.io.IOException: TRINO_QUERY_FAILED"),"RANGE_REPAIR_TRINO_FAILURE_REQUIRED");
        Path owner=Path.of(Objects.requireNonNull(System.getenv("SOURCE_GUARD_ROOT")),"owner.json");
        Runnable lease=()->{try{var held=read(owner,65536);require(held.size()==2 && DAG.equals(text(held,"dag_id"))
                && text(r,"run_id").equals(text(held,"run_id")),"RANGE_REPAIR_OWN_LEASE_REQUIRED");}
            catch(Exception err){throw new IllegalStateException("RANGE_REPAIR_OWN_LEASE_REQUIRED");}};
        lease.run();ResearchRangeInputCheck.main(new String[]{root.toString()});
        var done=completed(root,r);require(q.path("initial_completed_months").isInt()
                && done.size()==q.path("initial_completed_months").asInt(),"RANGE_REPAIR_PREFIX_CHANGED");
        Path monthRoot=root.resolve("months/"+months(r).get(done.size()));var ctx=read(monthRoot.resolve("context.json"),65536);
        var selected=targets(r,done.size(),ctx);require(JSON.valueToTree(selected).equals(q.path("target_months")),"RANGE_REPAIR_TARGET_CHANGED");
        var inventory=new ArrayList<JmaArchiveEntry>();for(String name:List.of("baseline.csv","boundary.csv"))inventory.addAll(JmaArchiveInventory.read(root.resolve(name)));
        try(var bronze=MinioBronzeObjectStore.fromEnvironment();var store=MinioSilverObjectStore.fromEnvironment()) {
            var seen=new HashSet<String>();JsonNode prefixParent=null;var prefixPins=read(root.resolve("bronze-pins.json"),65536);
            Map<String,Long> prefixBaseline=new TreeMap<>();for(String t:GoldIcebergWriter.TABLES)prefixBaseline.put(t,0L);
            int prefixIndex=0,prefixEnd=0;String previousOp=null;JsonNode previousRecord=null;for(var cp:done) {
                var rec=read(root.resolve("months/"+text(cp,"month")+"/record.json"),4*1024*1024);
                String prefixOp=text(rec.path("context"),"operation_id");
                if(!prefixOp.equals(previousOp)) {
                    require(prefixIndex==prefixEnd,"RANGE_REPAIR_INCOMPLETE_PREFIX_BATCH");
                    require(seen.add(prefixOp),"RANGE_REPAIR_REPEATED_PREFIX_OPERATION");
                    var batch=new ArrayList<String>();rec.path("context").path("resolver_request").path("target_months").forEach(m->batch.add(m.asText()));
                    require(!batch.isEmpty() && prefixIndex+batch.size()<=done.size()
                            && batch.equals(months(r).subList(prefixIndex,prefixIndex+batch.size()))
                            && context(r,batch,prefixPins,prefixParent,prefixBaseline,text(r,"runner_sha256")).equals(rec.path("context")),"RANGE_REPAIR_PREFIX_CONTEXT_CHANGED");
                    storedRecord(store,rec);
                    prefixParent=JSON.createObjectNode().put("run_id",prefixOp)
                            .put("registry_sha256",sha(store.read(FullMonthStateResolver.root(prefixOp)+"/registry.json")))
                            .put("commit_key","gold-commits/"+prefixOp+"/commit.json").put("commit_sha256",sha(store.read("gold-commits/"+prefixOp+"/commit.json")));
                    prefixBaseline=new TreeMap<>();for(String t:GoldIcebergWriter.TABLES)prefixBaseline.put(t,rec.path("commit").path("tables").path(t).path("snapshot_id").asLong());
                    previousOp=prefixOp;
                    prefixEnd=prefixIndex+batch.size();previousRecord=rec;
                } else require(rec.equals(previousRecord),"RANGE_REPAIR_PREFIX_RECORD_CHANGED");
                var ids=new TreeMap<String,Long>();for(String t:GoldIcebergWriter.TABLES)ids.put(t,rec.path("commit").path("tables").path(t).path("snapshot_id").asLong());
                require(ids.equals(snapshots(cp.path("snapshots"))),"RANGE_REPAIR_PREFIX_PIN_CHANGED");
                boolean contains=false;for(var m:rec.path("context").path("resolver_request").path("target_months"))if(m.asText().equals(text(cp,"month")))contains=true;
                require(contains,"RANGE_REPAIR_PREFIX_MEMBER_CHANGED");prefixIndex++;
            }
            require(prefixIndex==prefixEnd,"RANGE_REPAIR_INCOMPLETE_PREFIX_BATCH");
            var last=done.get(done.size()-1);var prior=read(root.resolve("months/"+text(last,"month")+"/record.json"),4*1024*1024);
            String priorOp=text(prior.path("context"),"operation_id");
            var parent=JSON.createObjectNode().put("run_id",priorOp).put("registry_sha256",sha(store.read(FullMonthStateResolver.root(priorOp)+"/registry.json")))
                    .put("commit_key","gold-commits/"+priorOp+"/commit.json").put("commit_sha256",sha(store.read("gold-commits/"+priorOp+"/commit.json")));
            require(context(r,selected,read(root.resolve("bronze-pins.json"),65536),parent,snapshots(last.path("snapshots")),text(r,"runner_sha256")).equals(ctx),"RANGE_REPAIR_CONTEXT_CHANGED");
            String op=text(ctx,"operation_id"),base="real-etl/"+op+"/";
            require(ResearchCatalogRecoveryJob.stored(store,base+"context.json").equals(ctx),"RANGE_REPAIR_STORED_CONTEXT_CHANGED");
            var record=JSON.createObjectNode();record.set("context",ctx);var receipts=record.putArray("receipts");JsonNode previous=null;
            for(String phase:List.of("readiness","bronze","silver","gold")) {
                var artifact=ResearchCatalogRecoveryJob.stored(store,base+phase+".json");
                var receipt=ResearchCatalogRecoveryJob.stored(store,base+phase+"-receipt.json");
                require(artifact.equals(read(monthRoot.resolve(phase+".json"),4*1024*1024)) && phase.equals(text(receipt,"phase"))
                        && RealEtlAdapter.PROTOCOL.equals(text(receipt,"contract_version")) && "real".equals(text(receipt,"mode"))
                        && op.equals(text(receipt,"operation_id")) && "ok".equals(text(receipt,"status"))
                        && store.uriForKey(base+phase+".json").equals(text(receipt,"artifact_uri"))
                        && text(ctx,"context_sha256").equals(text(receipt,"context_sha256")) && digest(artifact).equals(text(receipt,"artifact_sha256"))
                        && receipt.path("upstream_sha256").equals(previous==null?JSON.nullNode():JSON.valueToTree(digest(previous))),"RANGE_REPAIR_PHASE_CHAIN_CHANGED");
                receipts.add(receipt);previous=receipt;
            }
            // This tool deliberately does NOT reconcile ambiguous partial publication writes.
            for(String name:List.of("verify.json","verify-receipt.json","publish.json","publish-receipt.json"))
                require(!store.exists(base+name) && !Files.exists(monthRoot.resolve(name)),"RANGE_REPAIR_LATER_PHASE_EXISTS_REVIEW_REQUIRED");
            require(!store.exists("gold-verification/"+op+"/publication.json"),"RANGE_REPAIR_ALREADY_PUBLISHED_REVIEW_REQUIRED");
            byte[] commitBytes=store.read("gold-commits/"+op+"/commit.json");var commit=JSON.readTree(commitBytes);
            var monthly=read(monthRoot.resolve("silver.json"),4*1024*1024);
            require(commit.equals(read(monthRoot.resolve("gold.json"),4*1024*1024)) && text(r,"namespace").equals(text(commit,"namespace"))
                    && op.equals(text(commit,"gold_run_id")) && monthly.path("reconciliation").path("balanced").asBoolean(false)
                    && "COMPLETE_MONTHS".equals(text(monthly,"scope_completeness"))
                    && commit.path("scope").path("affected_months").equals(JSON.valueToTree(selected))
                    && commit.path("scope").path("baseline_snapshots").equals(ctx.path("resolver_request").path("baseline_snapshots")),"RANGE_REPAIR_COMMIT_CHANGED");
            var registry=ResearchCatalogRecoveryJob.stored(store,FullMonthStateResolver.root(op)+"/registry.json");
            var state=new FullMonthStateResolver(bronze,store,inventory,()->snapshots(commit.path("scope").path("baseline_snapshots")),lease);
            require(state.verify(op,digest(registry)).equals(monthly)
                    && digest(ctx.path("resolver_request")).equals(text(monthly,"request_sha256")),"RANGE_REPAIR_SILVER_CHANGED");
            require(prefixPins.isArray() && prefixPins.size()==37 && prefixPins.equals(registry.path("active_inputs")),"RANGE_REPAIR_BRONZE_PINS_CHANGED");
            var reader=new FullMonthBronzeReader(bronze,inventory,new FullMonthBronzeReader.JmaStructureCache());
            for(var pin:prefixPins){lease.run();reader.descriptor(pin,utc(r,"source_cutoff_utc"));}
            var plan=new GoldVerificationPlan(commit);var transport=TrinoSqlClient.researchCatalog(URI.create("http://"+System.getenv("TRINO_HOST")+":"+System.getenv("TRINO_INTERNAL_PORT")));
            int[] ordinal={0};GoldPublisher.Sql sql=query->{lease.run();System.out.println("event=dat03_range_repair_sql ordinal="+(++ordinal[0]));return transport.execute(query);};
            var checks=JSON.createObjectNode();var metadata=JSON.createObjectNode();for(String t:GoldIcebergWriter.TABLES) {
                ResearchCatalogRecoveryJob.scalar(sql,lease,checks,t,"SELECT snapshot_id FROM "+plan.metadata(t,"refs")+" WHERE name='main'",plan.snapshot(t));
                ResearchCatalogRecoveryJob.scalar(sql,lease,metadata,"identity:"+t,"SELECT count(*) FROM "+plan.metadata(t,"snapshots")+" WHERE snapshot_id="+plan.snapshot(t)
                        +" AND summary['gold.operation_id']="+GoldVerificationPlan.quote(op)+" AND summary['gold.identity_sha256']="+GoldVerificationPlan.quote(text(commit,"identity_sha256")),1);
            }
            ResearchCatalogRecoveryJob.scalar(sql,lease,metadata,"publication_absent","SELECT count(*) FROM "+text(r,"namespace")+".publication_status WHERE gold_run_id="+GoldVerificationPlan.quote(op),0);
            unchanged(root,q.path("failed_files"));lease.run();
            if(!run) {
                var proof=JSON.createObjectNode().put("status","READY_FOR_PUBLICATION_RECOVERY").put("request_sha256",sha(Files.readAllBytes(file)))
                        .put("commit_journal_sha256",sha(commitBytes)).put("initial_completed_months",done.size());
                proof.set("target_months",JSON.valueToTree(selected));proof.set("six_current_pins",checks);proof.set("metadata_checks",metadata);
                save(attempt.resolve("preflight.json"),proof);return;
            }
            var before=read(attempt.resolve("preflight.json"),65536);
            require("READY_FOR_PUBLICATION_RECOVERY".equals(text(before,"status")) && sha(commitBytes).equals(text(before,"commit_journal_sha256"))
                    && sha(Files.readAllBytes(file)).equals(text(before,"request_sha256")),"RANGE_REPAIR_PREFLIGHT_CHANGED");
            var publisher=GoldPublisher.researchCatalogCheckpointed(sql,store,lease);
            for(String phase:List.of("verify","publish")) {
                var artifact=phase.equals("verify")?publisher.verify(op,text(r,"namespace"),text(commit,"identity_sha256"))
                        :publisher.publish(op,text(r,"namespace"),text(commit,"identity_sha256"));
                previous=ResearchCatalogRecoveryJob.receipt(store,ctx,phase,previous,artifact,lease);receipts.add(previous);
                save(monthRoot.resolve(phase+".json"),artifact);
            }
            record.set("commit",commit);record.put("commit_journal_sha256",sha(commitBytes));record.set("silver",read(monthRoot.resolve("silver.json"),4*1024*1024));
            record.set("publication",read(monthRoot.resolve("publish.json"),4*1024*1024));storedRecord(store,record);
            var ids=new TreeMap<String,Long>();for(String t:GoldIcebergWriter.TABLES)ids.put(t,plan.snapshot(t));
            var finalChecks=JSON.createObjectNode();for(String t:GoldIcebergWriter.TABLES)ResearchCatalogRecoveryJob.scalar(sql,lease,finalChecks,t,
                    "SELECT snapshot_id FROM "+plan.metadata(t,"refs")+" WHERE name='main'",plan.snapshot(t));
            unchanged(root,q.path("failed_files"));lease.run();
            for(String member:selected) {
                Path dir=safe(root.resolve("months/"+member));Files.createDirectories(dir);save(dir.resolve("context.json"),ctx);save(dir.resolve("record.json"),record);
                var cp=JSON.createObjectNode().put("version",ResearchRangeState.VERSION).put("month",member).put("status","PUBLISHED")
                        .put("request_identity_sha256",digest(r)).put("record_sha256",sha(Files.readAllBytes(dir.resolve("record.json"))))
                        .put("previous_checkpoint_sha256",sha(Files.readAllBytes(root.resolve("months/"+text(done.get(done.size()-1),"month")+"/checkpoint.json"))));
                cp.set("snapshots",JSON.valueToTree(ids));save(dir.resolve("checkpoint.json"),cp);done.add(cp);
            }
            unchanged(root,q.path("failed_files"));lease.run();var result=progress(r,completed(root,r));
            result.set("current_snapshots",JSON.valueToTree(ids));result.putNull("pending_month");result.put("session_runner_sha256",text(r,"runner_sha256"))
                    .put("recovery_runner_sha256",text(q,"runner_sha256")).put("recovery_request_sha256",sha(Files.readAllBytes(file)))
                    .put("recovered_session",failed).put("executed_sql_count",ordinal[0]);result.set("fresh_current_pin_checks",finalChecks);
            save(root.resolve("sessions/"+recovery+"/result.json"),result);
        }
    }
}
