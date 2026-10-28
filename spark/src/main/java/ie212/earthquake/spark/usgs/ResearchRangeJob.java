package ie212.earthquake.spark.usgs;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import static ie212.earthquake.spark.usgs.ResearchRangeState.*;
import static ie212.earthquake.spark.usgs.ResearchRangeState.VERSION;
import static ie212.earthquake.spark.usgs.ResearchRangeState.validate;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.GoldCommitJob;
import ie212.earthquake.spark.gold.*;
import ie212.earthquake.spark.jma.*;
import ie212.earthquake.spark.ml.LocalFeatureJob;
import ie212.earthquake.spark.orchestration.*;
import ie212.earthquake.spark.silver.*;
import java.net.URI;
import java.net.http.HttpHeaders;
import java.nio.file.*;
import java.time.*;
import java.util.*;

/** Explicit, isolated research backfill. Never changes a deployed runner, DAG or service. */
public final class ResearchRangeJob {
    private ResearchRangeJob() { }
    public static void main(String[] args) throws Exception {
        require(args.length==4,"CATALOG_REQUEST_SESSION_AND_BUDGET_REQUIRED");Path requestFile=safe(Path.of(args[0])),root=requestFile.getParent(),session=safe(Path.of(args[1]));
        require(root.toString().matches("/opt/pipeline/staging/dat03-range/[a-f0-9]{32}") && session.getParent().equals(root.resolve("sessions"))
                && session.getFileName().toString().matches("[a-f0-9]{32}") && Files.isDirectory(session),"CATALOG_EXACT_RUNTIME_ROOT_REQUIRED");
        int seconds=Integer.parseInt(args[2]);require(seconds>=1800 && seconds<=6600,"CATALOG_SESSION_BUDGET_INVALID");long deadline=System.nanoTime()+seconds*1_000_000_000L;
        int maxMonths=Integer.parseInt(args[3]);require(maxMonths>=1 && maxMonths<=12,"CATALOG_SESSION_MONTH_LIMIT_INVALID");
        var r=read(requestFile,65536);validate(r);String actualCode=LocalFeatureJob.codeSha();
        require(actualCode.equals(text(r,"runner_sha256")),"RANGE_RUNNER_CHANGED");
        var rangeMonths=ResearchRangeState.months(r);
        require(root.getFileName().toString().equals(text(r,"run_id").substring(12)),"RANGE_ROOT_IDENTITY_CHANGED");
        String run=text(r,"run_id");Path owner=Path.of(Objects.requireNonNull(System.getenv("SOURCE_GUARD_ROOT")),"owner.json");
        Runnable lease=()->{try{var held=read(owner,65536);require(held.size()==2 && DAG.equals(text(held,"dag_id")) && run.equals(text(held,"run_id")),"CATALOG_OWN_LEASE_REQUIRED");}
            catch(Exception e){throw new IllegalStateException("CATALOG_OWN_LEASE_REQUIRED");}};lease.run();
        var inventory=new ArrayList<JmaArchiveEntry>();for(String file:List.of("baseline.csv","boundary.csv")) {
            require(sha(Files.readAllBytes(safe(root.resolve(file)))).equals(text(r,file.equals("baseline.csv")?"baseline_inventory_sha256":"boundary_inventory_sha256")),"CATALOG_INVENTORY_CHANGED");
            inventory.addAll(JmaArchiveInventory.read(root.resolve(file)));
        }
        require(sha(Files.readAllBytes(safe(root.resolve("plan.json")))).equals(text(r,"plan_sha256")),"CATALOG_PLAN_CHANGED");
        ResearchCatalogJob.validateSources(root,r,inventory);var done=completed(root,r);int initialCount=done.size();String pending=null;
        var spark=GoldCommitJob.configured(System.getenv()).master("local[1]").appName("dat03-range-worker")
                .config("spark.ui.enabled","false").config("spark.sql.shuffle.partitions",Integer.toString(ResearchCatalogBatchPlan.SHUFFLE_PARTITIONS)).config("spark.sql.adaptive.enabled","false")
                .config("spark.sql.codegen.wholeStage","false").getOrCreate();spark.sparkContext().setLogLevel("ERROR");
        try(var bronze=MinioBronzeObjectStore.fromEnvironment();var silver=MinioSilverObjectStore.fromEnvironment()) {
            var writer=new GoldIcebergWriter(spark,text(r,"namespace"),silver,lease);
            var endpoint=URI.create("http://"+System.getenv("TRINO_HOST")+":"+System.getenv("TRINO_INTERNAL_PORT"));
            var sql=TrinoSqlClient.researchCatalog(endpoint);
            var publisher=GoldPublisher.researchCatalogCheckpointed(sql,silver,lease);
            var cache=new FullMonthBronzeReader.JmaStructureCache();
            var adapter=new RealEtlAdapter(spark,bronze,silver,inventory,writer,publisher,lease,cache);
            JsonNode pins=Files.exists(root.resolve("bronze-pins.json"))?read(root.resolve("bronze-pins.json"),65536):ResearchCatalogJob.ingest(root,r,inventory,bronze,lease);
            require(pins.isArray() && pins.size()==37,"CATALOG_BRONZE_PIN_SET_CHANGED");
            JsonNode parent=null;Map<String,Long> baseline=new TreeMap<>();for(String t:GoldIcebergWriter.TABLES)baseline.put(t,0L);
            if(!done.isEmpty()) {
                var last=done.get(done.size()-1);String oldMonth=text(last,"month");var old=read(root.resolve("months/"+oldMonth+"/record.json"),4*1024*1024);
                ResearchCatalogJob.assertRecord(old,old.path("context"));String op=text(old.path("context"),"operation_id");
                // Independent stored artifacts, not a local boolean/checkpoint, establish the latest parent.
                byte[] oldBytes=silver.read("gold-commits/"+op+"/commit.json");
                require(sha(oldBytes).equals(text(old,"commit_journal_sha256")) && JSON.readTree(oldBytes).equals(old.path("commit"))
                        && JSON.readTree(silver.read("gold-verification/"+op+"/publication.json")).equals(old.path("publication")),"CATALOG_STORED_PARENT_CHANGED");
                baseline=snapshots(last.path("snapshots"));parent=JSON.createObjectNode().put("run_id",op)
                        .put("registry_sha256",sha(silver.read(FullMonthStateResolver.root(op)+"/registry.json")))
                        .put("commit_key","gold-commits/"+op+"/commit.json").put("commit_sha256",sha(silver.read("gold-commits/"+op+"/commit.json")));
            }
            var batchPlan=read(root.resolve("plan.json"),4*1024*1024);
            while(done.size()<rangeMonths.size()) {
                long remaining=(deadline-System.nanoTime())/1_000_000_000;
                if(done.size()-initialCount>=maxMonths || !ResearchCatalogBatchPlan.mayStart(remaining))break;
                int index=done.size();
                var targets=ResearchCatalogBatchPlan.next(batchPlan,MONTHS.indexOf(rangeMonths.get(index)),Math.min(rangeMonths.size()-index,maxMonths-(done.size()-initialCount)));
                require(!targets.isEmpty(),"CATALOG_BATCH_EMPTY");
                String month=rangeMonths.get(index);Path folder=safe(root.resolve("months/"+month));Files.createDirectories(folder);
                var context=context(r,targets,pins,parent,baseline,actualCode);
                if(!Files.exists(folder.resolve("context.json")))require(writer.baselines().equals(baseline),"CATALOG_LIVE_BASELINE_CHANGED");
                ObjectNode record;
                try{record=ResearchCatalogJob.month(adapter,silver,context,folder,lease,deadline);}
                catch(java.io.IOException e){if("CATALOG_CHECKPOINT_BEFORE_PHASE".equals(e.getMessage())){pending=month;break;}throw e;}
                String op=text(context,"operation_id");baseline=new TreeMap<>();
                for(String t:GoldIcebergWriter.TABLES)baseline.put(t,record.path("commit").path("tables").path(t).path("snapshot_id").asLong());
                for(String member:targets) {
                    Path memberRoot=safe(root.resolve("months/"+member));Files.createDirectories(memberRoot);
                    save(memberRoot.resolve("context.json"),context);save(memberRoot.resolve("record.json"),record);
                    var checkpoint=JSON.createObjectNode().put("version",VERSION).put("month",member).put("status","PUBLISHED")
                            .put("request_identity_sha256",digest(r)).put("record_sha256",sha(Files.readAllBytes(memberRoot.resolve("record.json"))));
                    if(done.isEmpty())checkpoint.putNull("previous_checkpoint_sha256");else checkpoint.put("previous_checkpoint_sha256",sha(Files.readAllBytes(root.resolve("months/"+rangeMonths.get(done.size()-1)+"/checkpoint.json"))));
                    checkpoint.set("snapshots",JSON.valueToTree(baseline));save(memberRoot.resolve("checkpoint.json"),checkpoint);done.add(checkpoint);
                }
                parent=JSON.createObjectNode().put("run_id",op).put("registry_sha256",sha(silver.read(FullMonthStateResolver.root(op)+"/registry.json")))
                        .put("commit_key","gold-commits/"+op+"/commit.json").put("commit_sha256",sha(silver.read("gold-commits/"+op+"/commit.json")));
                System.out.println("event=dat03_catalog_month_published month="+month+" completed="+done.size()+" required="+rangeMonths.size());
            }
            lease.run();var result=progress(r,completed(root,r));result.set("current_snapshots",JSON.valueToTree(writer.baselines()));
            result.put("completed_in_session",done.size()-initialCount);
            result.put("session_runner_sha256",actualCode);
            result.put("batch_policy_version",ResearchCatalogBatchPlan.VERSION)
                    .put("profile_rows_per_batch",ResearchCatalogBatchPlan.PROFILE_ROWS)
                    .put("sql_read_profile",TrinoSqlClient.RESEARCH_READ_PROFILE);
            if(pending==null)result.putNull("pending_month");else result.put("pending_month",pending).put("status","PARTIAL_MONTH_REQUIRES_RECOVERY");
            save(session.resolve("result.json"),result);
        } finally {spark.stop();}
    }
}
