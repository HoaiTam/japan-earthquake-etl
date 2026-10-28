package ie212.earthquake.spark.usgs;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import static ie212.earthquake.spark.usgs.ResearchRangeState.read;
import static ie212.earthquake.spark.usgs.ResearchRangeState.safe;
import static ie212.earthquake.spark.usgs.ResearchRangeState.save;
import com.fasterxml.jackson.databind.JsonNode;
import ie212.earthquake.spark.gold.*;
import ie212.earthquake.spark.ml.LocalFeatureJob;
import ie212.earthquake.spark.orchestration.*;
import ie212.earthquake.spark.silver.*;
import java.nio.file.*;
import java.net.URI;
import java.util.*;
import java.util.jar.JarFile;

/** Explicit publication-only repair for the first-month namespace gate failure.
 * No Spark, ETL replay, input replacement or implicit retry. Original run remains immutable. */
public final class ResearchRangeRecoveryJob {
    private ResearchRangeRecoveryJob() { }
    static void compatible(Path oldJar,Path newJar) throws Exception {
        String publisher="ie212/earthquake/spark/gold/GoldPublisher.class";
        String added="ie212/earthquake/spark/usgs/ResearchRangeRecoveryJob.class";
        try(var a=new JarFile(oldJar.toFile());var b=new JarFile(newJar.toFile())) {
            var seen=new HashSet<String>();long total=0;
            for(var it=a.entries();it.hasMoreElements();) {
                var x=it.nextElement();require(seen.add(x.getName()),"RANGE_RECOVERY_DUPLICATE_ENTRY");
                if(x.isDirectory() || x.getName().equals("META-INF/MANIFEST.MF"))continue;
                var y=b.getJarEntry(x.getName());require(y!=null && x.getSize()<=16*1024*1024 && y.getSize()<=16*1024*1024,"RANGE_RECOVERY_CODE_BOUND");
                byte[] before=a.getInputStream(x).readAllBytes(),after=b.getInputStream(y).readAllBytes();total+=before.length+after.length;
                require(total<=256L*1024*1024 && (Arrays.equals(before,after) || x.getName().equals(publisher)),"RANGE_RECOVERY_ETL_CODE_CHANGED");
            }
            var unique=new HashSet<String>();for(var it=b.entries();it.hasMoreElements();) {
                var x=it.nextElement();require(unique.add(x.getName()),"RANGE_RECOVERY_DUPLICATE_ENTRY");
                require(x.isDirectory() || seen.contains(x.getName()) || x.getName().equals(added),"RANGE_RECOVERY_UNREVIEWED_CLASS");
            }
        }
    }
    public static void main(String[] args) throws Exception {
        require(args.length==1,"RANGE_RECOVERY_REQUEST_REQUIRED");
        Path file=safe(Path.of(args[0])),attempt=file.getParent();var request=read(file,65536);
        require(attempt.toString().matches("/opt/pipeline/staging/dat03-range-recovery/[a-f0-9]{32}"),"RANGE_RECOVERY_ROOT_REQUIRED");
        Path root=safe(Path.of(text(request,"range_root")));
        require(root.toString().matches("/opt/pipeline/staging/dat03-range/[a-f0-9]{32}"),"RANGE_RECOVERY_ORIGINAL_ROOT_REQUIRED");
        var r=read(root.resolve("request.json"),65536);ResearchRangeState.validate(r);
        require(sha(Files.readAllBytes(root.resolve("request.json"))).equals(text(request,"original_request_sha256"))
                && sha(Files.readAllBytes(root.resolve("runner.jar"))).equals(text(r,"runner_sha256"))
                && LocalFeatureJob.codeSha().equals(text(request,"runner_sha256")),"RANGE_RECOVERY_PIN_CHANGED");
        compatible(root.resolve("runner.jar"),attempt.resolve("runner.jar"));
        String failed=text(request,"failed_session");require(failed.matches("[a-f0-9]{32}"),"RANGE_RECOVERY_SESSION_REQUIRED");
        Path session=root.resolve("sessions/"+failed);var execution=read(session.resolve("execution.json"),65536);
        require(execution.path("exit_code").isInt() && execution.path("exit_code").asInt()==1 && execution.path("timed_out").isBoolean()
                && !execution.path("timed_out").asBoolean() && !Files.exists(session.resolve("acceptance.json"))
                && !Files.exists(session.resolve("result.json")),"RANGE_RECOVERY_KNOWN_FAILURE_REQUIRED");
        ResearchCatalogRecoveryJob.unchanged(root,request.path("failed_files"));
        require(Files.size(session.resolve("stderr.private"))<=16*1024*1024
                && Files.readString(session.resolve("stderr.private")).contains("java.lang.IllegalArgumentException: DAT03_ISOLATED_CATALOG_REQUIRED"),
                "RANGE_RECOVERY_NAMESPACE_FAILURE_REQUIRED");
        String month=ResearchRangeState.months(r).get(0);Path folder=root.resolve("months/"+month);
        require(ResearchRangeState.completed(root,r).isEmpty(),"RANGE_RECOVERY_FIRST_MONTH_ONLY");
        var context=read(folder.resolve("context.json"),65536);RealEtlAdapter.validateContext(context);
        String op=text(context,"operation_id"),ns=text(r,"namespace");var resolver=context.path("resolver_request");
        require(resolver.path("target_months").equals(JSON.valueToTree(List.of(month))) && text(resolver,"namespace").equals(ns)
                && resolver.path("source_cutoff_utc").equals(r.path("source_cutoff_utc")) && resolver.path("baseline").isNull()
                && snapshots(resolver.path("baseline_snapshots")).values().stream().allMatch(v->v==0),"RANGE_RECOVERY_SCOPE_CHANGED");
        Path owner=Path.of(Objects.requireNonNull(System.getenv("SOURCE_GUARD_ROOT")),"owner.json");
        Runnable lease=()->{try{var held=read(owner,65536);require(held.size()==2 && ResearchRangeState.DAG.equals(text(held,"dag_id"))
                && text(r,"run_id").equals(text(held,"run_id")),"RANGE_RECOVERY_OWN_LEASE_REQUIRED");}
            catch(Exception error){throw new IllegalStateException("RANGE_RECOVERY_OWN_LEASE_REQUIRED");}};
        lease.run();ResearchRangeInputCheck.main(new String[]{root.toString()});
        try(var store=MinioSilverObjectStore.fromEnvironment()) {
            require(ResearchCatalogRecoveryJob.stored(store,"real-etl/"+op+"/context.json").equals(context),"RANGE_RECOVERY_CONTEXT_CHANGED");
            var record=JSON.createObjectNode();record.set("context",context);var receipts=record.putArray("receipts");JsonNode previous=null;
            for(String phase:List.of("readiness","bronze","silver","gold")) {
                var artifact=ResearchCatalogRecoveryJob.stored(store,"real-etl/"+op+"/"+phase+".json");
                var pin=ResearchCatalogRecoveryJob.stored(store,"real-etl/"+op+"/"+phase+"-receipt.json");
                require(artifact.equals(read(folder.resolve(phase+".json"),4*1024*1024)) && phase.equals(text(pin,"phase"))
                        && op.equals(text(pin,"operation_id")) && "ok".equals(text(pin,"status"))
                        && text(context,"context_sha256").equals(text(pin,"context_sha256")) && digest(artifact).equals(text(pin,"artifact_sha256"))
                        && pin.path("upstream_sha256").equals(previous==null?JSON.nullNode():JSON.valueToTree(digest(previous))),"RANGE_RECOVERY_PHASE_CHANGED");
                receipts.add(pin);previous=pin;
            }
            for(String n:List.of("verify.json","verify-receipt.json","publish.json","publish-receipt.json"))
                require(!store.exists("real-etl/"+op+"/"+n),"RANGE_RECOVERY_LATER_PHASE_EXISTS");
            require(!store.exists("gold-verification/"+op+"/publication.json"),"RANGE_RECOVERY_PUBLICATION_EXISTS");
            byte[] commitBytes=store.read("gold-commits/"+op+"/commit.json");var commit=JSON.readTree(commitBytes);
            require(commit.equals(read(folder.resolve("gold.json"),4*1024*1024))
                    && digest(commit).equals(text(request,"commit_identity_sha256")) && "COMMITTED".equals(text(commit,"status"))
                    && commit.path("scope").path("affected_months").equals(resolver.path("target_months")),"RANGE_RECOVERY_COMMIT_CHANGED");
            var transport=TrinoSqlClient.researchCatalog(URI.create("http://"+System.getenv("TRINO_HOST")+":"+System.getenv("TRINO_INTERNAL_PORT")));
            int[] count={0};GoldPublisher.Sql sql=q->{lease.run();System.out.println("event=dat03_range_recovery_sql ordinal="+(++count[0]));return transport.execute(q);};
            var publisher=GoldPublisher.researchCatalogCheckpointed(sql,store,lease);
            for(String phase:List.of("verify","publish")) {
                var artifact=phase.equals("verify")?publisher.verify(op,ns,text(commit,"identity_sha256")):publisher.publish(op,ns,text(commit,"identity_sha256"));
                previous=ResearchCatalogRecoveryJob.receipt(store,context,phase,previous,artifact,lease);receipts.add(previous);
                save(attempt.resolve(phase+".json"),artifact);
            }
            record.set("commit",commit);record.put("commit_journal_sha256",sha(commitBytes));
            record.set("silver",read(folder.resolve("silver.json"),4*1024*1024));record.set("publication",read(attempt.resolve("publish.json"),4*1024*1024));
            ResearchCatalogJob.assertRecord(record,context);save(attempt.resolve("record.json"),record);
            var plan=new GoldVerificationPlan(commit);var checks=JSON.createObjectNode();
            for(String t:GoldIcebergWriter.TABLES)ResearchCatalogRecoveryJob.scalar(sql,lease,checks,t,
                    "SELECT snapshot_id FROM "+plan.metadata(t,"refs")+" WHERE name='main'",plan.snapshot(t));
            ResearchCatalogRecoveryJob.unchanged(root,request.path("failed_files"));lease.run();
            var proof=JSON.createObjectNode().put("status","PUBLICATION_RECOVERED_QUARANTINED_TEST_RUN")
                    .put("request_sha256",sha(Files.readAllBytes(file))).put("record_sha256",sha(Files.readAllBytes(attempt.resolve("record.json"))))
                    .put("executed_sql_count",count[0]).put("month",month).put("research_ready",false).put("official_exported",false);
            proof.set("current_pin_checks",checks);save(attempt.resolve("result.json"),proof);
        }
    }
}
