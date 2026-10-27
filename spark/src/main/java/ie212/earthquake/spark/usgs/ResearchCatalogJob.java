package ie212.earthquake.spark.usgs;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import static ie212.earthquake.spark.usgs.ResearchCatalogState.*;
import static ie212.earthquake.spark.usgs.ResearchCatalogState.VERSION;
import static ie212.earthquake.spark.usgs.ResearchCatalogState.validate;
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
public final class ResearchCatalogJob {
    private ResearchCatalogJob() { }
    static byte[] source(Path root,JsonNode pin,boolean metadata) throws Exception {
        String name=text(pin,"name"),suffix=metadata?".json":name.startsWith("h")?".zip":".geojson";
        Path file=safe(root.resolve("inputs/"+name+suffix));
        require(Files.isRegularFile(file,LinkOption.NOFOLLOW_LINKS) && Files.size(file)>0
                && Files.size(file)<=(metadata?65536:32L*1024*1024),"CATALOG_SOURCE_FILE_BOUND");
        byte[] bytes=Files.readAllBytes(file);
        require(sha(bytes).equals(text(pin,metadata?"metadata_sha256":"raw_sha256"))
                && (metadata || bytes.length==pin.path("bytes").asLong()),"CATALOG_SOURCE_BYTES_CHANGED");return bytes;
    }
    static void validateSources(Path root,JsonNode r,List<JmaArchiveEntry> inventory) throws Exception {
        byte[] summaryBytes=Files.readAllBytes(safe(root.resolve("usgs-summary.json")));
        require(sha(summaryBytes).equals(text(r,"usgs_summary_sha256")) && summaryBytes.length<=4*1024*1024,"CATALOG_USGS_SUMMARY_CHANGED");
        var summary=JSON.readTree(summaryBytes);
        for(var pin:r.path("sources")) {
            var meta=JSON.readTree(source(root,pin,true));byte[] raw=source(root,pin,false);
            require(text(pin,"raw_sha256").equals(text(meta,"sha256")) && meta.path("bytes").isIntegralNumber()
                    && meta.path("bytes").asLong()==raw.length && !utc(meta,"retrieved_at_utc").isAfter(utc(r,"source_cutoff_utc")),"CATALOG_PROVENANCE_CHANGED");
            if(text(pin,"name").startsWith("h")) {
                int year=Integer.parseInt(text(pin,"name").substring(1));
                var entry=inventory.stream().filter(e->e.year()==year && e.segment().equals("full-year")).findFirst().orElseThrow();
                require(entry.sourceUrl().toString().equals(text(meta,"final_url")) && meta.path("http_status").isInt()
                        && meta.path("http_status").asInt()==200 && meta.path("inventory_metadata_drift").isBoolean()
                        && !meta.path("inventory_metadata_drift").asBoolean(),"CATALOG_JMA_PROVENANCE_INVALID");
                String release=text(meta,"catalog_release");
                require(release.matches("[A-Za-z0-9][A-Za-z0-9_.~-]{0,255}") && !release.contains(".."),"CATALOG_JMA_RELEASE_INVALID");
            } else {
                var result=ResearchUsgsCaptureCheck.check(meta,raw);JsonNode expected=null;
                for(var row:summary.path("captures"))if(row.path("month").equals(result.path("month"))) {
                    require(expected==null,"CATALOG_USGS_SUMMARY_CHANGED");expected=row.path("check");
                }
                require(result.equals(expected),"CATALOG_USGS_SUMMARY_CHANGED");
            }
        }
    }
    static JsonNode ingest(Path root,JsonNode r,List<JmaArchiveEntry> inventory,BronzeObjectStore bronze,Runnable lease) throws Exception {
        // Provenance and raw bytes for ALL inputs pass before the first Bronze write.
        validateSources(root,r,inventory);var pins=JSON.createArrayNode();String run=text(r,"run_id");Instant cutoff=utc(r,"source_cutoff_utc");
        for(var pin:r.path("sources")) {
            lease.run();String name=text(pin,"name");var meta=JSON.readTree(source(root,pin,true));byte[] raw=source(root,pin,false);
            String manifestKey,rawSha;
            if(name.startsWith("h")) {
                int year=Integer.parseInt(name.substring(1));var entry=inventory.stream().filter(e->e.year()==year && e.segment().equals("full-year")).findFirst().orElseThrow();
                var written=new JmaBronzeWriter(bronze).write(new JmaBronzeWriteRequest(entry,raw,text(meta,"sha256"),raw.length,text(meta,"catalog_release"),
                        new JmaHttpMetadata(200,"application/zip",(long)raw.length,meta.path("etag").isNull()?null:meta.path("etag").asText(),
                            text(meta,"last_modified_utc"),entry.sourceUrl()),run+"-"+name,1,LocalDate.ofInstant(cutoff,ZoneOffset.UTC),utc(meta,"retrieved_at_utc"),true,run+"-"+name,90000,null));
                require(written.ready(),"CATALOG_JMA_BRONZE_REJECTED");manifestKey=written.manifestKey();rawSha=written.sha256();
                pins.addObject().put("source_system","JMA_BULLETIN").put("year",year).put("segment","full-year")
                        .put("catalog_release",text(meta,"catalog_release"));
            } else {
                var checked=ResearchUsgsCaptureCheck.check(meta,raw);Instant start=utc(checked,"window_start_utc"),end=utc(checked,"window_end_utc_exclusive");
                var response=new UsgsHttpResponse("dat03-pinned-research-capture",new UsgsRequest(URI.create(text(meta,"url")),start,end,20000,1),200,
                        HttpHeaders.of(Map.of("content-type",List.of(text(meta,"content_type"))),(a,b)->true),raw,1,Duration.ZERO);
                var written=new UsgsBronzeWriter(bronze).write(new UsgsBronzeWriteRequest(response,run+"-"+name,1,LocalDate.ofInstant(cutoff,ZoneOffset.UTC),
                        utc(meta,"retrieved_at_utc"),true,run+"-"+name,90000));
                require(written.ready(),"CATALOG_USGS_BRONZE_REJECTED");manifestKey=written.manifestKey();rawSha=written.sha256();
                pins.addObject().put("source_system","USGS").put("window_start_utc",start.toString()).put("window_end_utc",end.toString());
            }
            ((ObjectNode)pins.get(pins.size()-1)).put("manifest_uri",bronze.uriForKey(manifestKey))
                    .put("manifest_sha256",sha(bronze.read(manifestKey))).put("sha256",rawSha);
            System.out.println("event=dat03_catalog_bronze_verified source="+name);
        }
        save(root.resolve("bronze-pins.json"),pins);return pins;
    }
    static void assertRecord(JsonNode record,JsonNode context) throws Exception {
        require(record.path("context").equals(context),"CATALOG_RECORD_CONTEXT_CHANGED");String op=text(context,"operation_id");
        var receipts=record.path("receipts");require(receipts.isArray() && receipts.size()==RealEtlAdapter.PHASES.size(),"CATALOG_PHASE_CHAIN_INCOMPLETE");
        JsonNode previous=null;for(int i=0;i<receipts.size();i++) {
            var receipt=receipts.get(i);require(RealEtlAdapter.PHASES.get(i).equals(text(receipt,"phase"))
                    && op.equals(text(receipt,"operation_id")) && text(context,"context_sha256").equals(text(receipt,"context_sha256"))
                    && "ok".equals(text(receipt,"status")) && (previous==null?receipt.path("upstream_sha256").isNull():digest(previous).equals(text(receipt,"upstream_sha256"))),
                    "CATALOG_PHASE_CHAIN_CHANGED");previous=receipt;
        }
        JsonNode pub=record.path("publication"),commit=record.path("commit"),monthly=record.path("silver");
        require(op.equals(text(commit,"gold_run_id")) && text(context.path("resolver_request"),"namespace").equals(text(commit,"namespace"))
                && "COMMITTED".equals(text(commit,"status")) && !commit.path("published").asBoolean(true)
                && "PASSED".equals(text(pub,"verify_status")) && pub.path("published").asBoolean(false)
                && pub.path("snapshot_bundle").equals(commit.path("tables")) && pub.path("namespace").equals(commit.path("namespace"))
                && monthly.path("reconciliation").path("balanced").asBoolean(false) && "COMPLETE_MONTHS".equals(text(monthly,"scope_completeness"))
                && GoldPublisher.BUCKETED_OUTSIDE_VERSION.equals(text(pub,"outside_scope_strategy")),"CATALOG_PUBLICATION_REQUIRED");
        require(commit.path("tables").size()==6 && pub.path("checks").size()>=68,"CATALOG_PUBLICATION_CHECKS_INCOMPLETE");
        if(pub.has("verification_reuse")) {
            var reuse=pub.path("verification_reuse");
            require(GoldPublisher.VERIFIED_REUSE_VERSION.equals(text(reuse,"strategy")) && reuse.path("same_process").asBoolean(false)
                    && reuse.path("max_age_seconds").asInt()==300 && reuse.path("reused_check_count").asInt()==pub.path("checks").size()-8
                    && text(receipts.get(4),"artifact_sha256").equals(text(reuse,"source_report_sha256")),"CATALOG_VERIFICATION_REUSE_UNBOUND");
        }
        var plan=new GoldVerificationPlan(commit);
        for(var blocker:plan.blockers().entrySet()) {
            var check=pub.path("checks").path(blocker.getKey());
            if(pub.has("fact_blocker_strategy") && GoldVerificationPlan.BUCKETED_FACT_BLOCKERS.contains(blocker.getKey())) {
                require(GoldPublisher.BUCKETED_BLOCKERS_VERSION.equals(text(pub,"fact_blocker_strategy"))
                        && "EXACT_EXHAUSTIVE_BUCKET_SUM".equals(text(check,"kind")) && check.path("query_count").asInt()==16,"CATALOG_REQUIRED_BLOCKER_MISSING");
                for(char b:"0123456789abcdef".toCharArray()) {
                    var part=pub.path("checks").path(blocker.getKey()+":"+b);
                    require(plan.factBlockerBucket(blocker.getKey(),String.valueOf(b)).equals(text(part,"sql"))
                            && part.path("expected").asLong(-1)==0,"CATALOG_REQUIRED_BLOCKER_MISSING");
                }
            } else require(blocker.getValue().equals(text(check,"sql")) && check.path("expected").asLong(-1)==0,"CATALOG_REQUIRED_BLOCKER_MISSING");
        }
        for(String table:GoldIcebergWriter.TABLES) {
            var check=pub.path("checks").path("count:"+table);
            require(("SELECT count(*) FROM "+plan.ref(table)).equals(text(check,"sql"))
                    && check.path("expected").asLong(-1)==plan.count(table),"CATALOG_SNAPSHOT_COUNT_CHANGED");
        }
        for(var it=pub.path("checks").fields();it.hasNext();) {
            var check=it.next().getValue();require(check.path("actual").isIntegralNumber() && check.path("expected").isIntegralNumber()
                    && check.path("actual").asLong()==check.path("expected").asLong(),"CATALOG_SQL_CHECK_FAILED");
        }
        for(String table:List.of("event_current","event_source_bridge"))for(char b:"0123456789abcdef".toCharArray()) {
            var check=pub.path("checks").path("outside:"+table+":"+b);
            require(plan.outsideScopeBucket(table,String.valueOf(b)).equals(text(check,"sql"))
                    && check.path("expected").asLong(-1)==0,"CATALOG_ALL_BUCKETS_REQUIRED");
        }
        // Local checkpoints are canonical JSON; commit journals retain the writer's
        // original byte order. Never reserialize a local tree to invent that SHA.
        String bundleSha=text(record,"commit_journal_sha256");require(bundleSha.matches("[a-f0-9]{64}") && bundleSha.equals(text(pub,"bundle_sha256"))
                && ("pub_"+bundleSha).equals(text(pub,"publication_id")),"CATALOG_COMMIT_BYTES_CHANGED");
    }
    static ObjectNode month(RealEtlAdapter adapter,SilverObjectStore silver,JsonNode context,Path folder,Runnable lease,long deadline) throws Exception {
        Files.createDirectories(safe(folder));save(folder.resolve("context.json"),context);
        var record=JSON.createObjectNode();record.set("context",context);var receipts=record.putArray("receipts");ObjectNode previous=null;
        for(String phase:RealEtlAdapter.PHASES) {
            require(mayStartPhase((deadline-System.nanoTime())/1_000_000_000),"CATALOG_CHECKPOINT_BEFORE_PHASE");
            lease.run();System.out.println("event=dat03_catalog_phase month="+text(context,"window_start_utc").substring(0,7)+" phase="+phase);
            previous=adapter.execute(phase,context,previous,folder);receipts.add(previous);
            var artifact=JSON.readTree(silver.read(key(text(previous,"artifact_uri"),silver::uriForKey)));
            require(digest(artifact).equals(text(previous,"artifact_sha256")),"CATALOG_PHASE_ARTIFACT_CHANGED");save(folder.resolve(phase+".json"),artifact);
        }
        byte[] commitBytes=silver.read("gold-commits/"+text(context,"operation_id")+"/commit.json");
        record.set("commit",JSON.readTree(commitBytes));record.put("commit_journal_sha256",sha(commitBytes));
        record.set("publication",read(folder.resolve("publish.json"),4*1024*1024));record.set("silver",read(folder.resolve("silver.json"),4*1024*1024));
        assertRecord(record,context);save(folder.resolve("record.json"),record);return record;
    }
    public static void main(String[] args) throws Exception {
        require(args.length==4 || args.length==5,"CATALOG_REQUEST_SESSION_AND_BUDGET_REQUIRED");Path requestFile=safe(Path.of(args[0])),root=requestFile.getParent(),session=safe(Path.of(args[1]));
        require(root.toString().matches("/opt/pipeline/staging/dat03-catalog/[a-f0-9]{32}") && session.getParent().equals(root.resolve("sessions"))
                && session.getFileName().toString().matches("[a-f0-9]{32}") && Files.isDirectory(session),"CATALOG_EXACT_RUNTIME_ROOT_REQUIRED");
        int seconds=Integer.parseInt(args[2]);require(seconds>=1800 && seconds<=6600,"CATALOG_SESSION_BUDGET_INVALID");long deadline=System.nanoTime()+seconds*1_000_000_000L;
        int maxMonths=Integer.parseInt(args[3]);require(maxMonths>=1 && maxMonths<=288,"CATALOG_SESSION_MONTH_LIMIT_INVALID");
        var r=read(requestFile,65536);validate(r);String actualCode=LocalFeatureJob.codeSha();boolean batched=args.length==5;
        JsonNode continuation=null;
        if(!batched)require(actualCode.equals(text(r,"runner_sha256")),"CATALOG_RUNNER_PIN_CHANGED");
        else {
            Path file=safe(Path.of(args[4])),control=file.getParent();continuation=read(file,65536);
            var fields=new HashSet<String>();continuation.fieldNames().forEachRemaining(fields::add);
            require(control.toString().matches("/opt/pipeline/staging/dat03-catalog-continuation/[a-f0-9]{32}")
                    && fields.equals(Set.of("version","catalog_root","catalog_request_sha256","base_runner_sha256","runner_sha256","controller_sha256","support_sha256","compatibility_sha256"))
                    && ResearchCatalogCompatibility.VERSION.equals(text(continuation,"version"))
                    && root.toString().equals(text(continuation,"catalog_root")) && sha(Files.readAllBytes(requestFile)).equals(text(continuation,"catalog_request_sha256"))
                    && text(r,"runner_sha256").equals(text(continuation,"base_runner_sha256")) && actualCode.equals(text(continuation,"runner_sha256")),"CATALOG_CONTINUATION_PIN_CHANGED");
            Path original=safe(root.resolve("runner.jar")),runner=safe(control.resolve("runner.jar"));
            require(sha(Files.readAllBytes(original)).equals(text(r,"runner_sha256")) && sha(Files.readAllBytes(runner)).equals(actualCode),"CATALOG_CONTINUATION_RUNNERS_CHANGED");
            var proof=ResearchCatalogCompatibility.verify(original,runner);
            require(sha(Files.readAllBytes(safe(control.resolve("compatibility.json")))).equals(text(continuation,"compatibility_sha256"))
                    && proof.equals(read(control.resolve("compatibility.json"),65536)),"CATALOG_CONTINUATION_CODE_UNREVIEWED");
        }
        String run=text(r,"run_id");Path owner=Path.of(Objects.requireNonNull(System.getenv("SOURCE_GUARD_ROOT")),"owner.json");
        Runnable lease=()->{try{var held=read(owner,65536);require(held.size()==2 && DAG.equals(text(held,"dag_id")) && run.equals(text(held,"run_id")),"CATALOG_OWN_LEASE_REQUIRED");}
            catch(Exception e){throw new IllegalStateException("CATALOG_OWN_LEASE_REQUIRED");}};lease.run();
        var inventory=new ArrayList<JmaArchiveEntry>();for(String file:List.of("baseline.csv","boundary.csv")) {
            require(sha(Files.readAllBytes(safe(root.resolve(file)))).equals(text(r,file.equals("baseline.csv")?"baseline_inventory_sha256":"boundary_inventory_sha256")),"CATALOG_INVENTORY_CHANGED");
            inventory.addAll(JmaArchiveInventory.read(root.resolve(file)));
        }
        require(sha(Files.readAllBytes(safe(root.resolve("plan.json")))).equals(text(r,"plan_sha256")),"CATALOG_PLAN_CHANGED");
        validateSources(root,r,inventory);var done=completed(root,r);int initialCount=done.size();String pending=null;
        var spark=GoldCommitJob.configured(System.getenv()).master("local[1]").appName("dat03-catalog-rollout")
                .config("spark.ui.enabled","false").config("spark.sql.shuffle.partitions",batched?Integer.toString(ResearchCatalogBatchPlan.SHUFFLE_PARTITIONS):"1").config("spark.sql.adaptive.enabled","false")
                .config("spark.sql.codegen.wholeStage","false").getOrCreate();spark.sparkContext().setLogLevel("ERROR");
        try(var bronze=MinioBronzeObjectStore.fromEnvironment();var silver=MinioSilverObjectStore.fromEnvironment()) {
            var writer=new GoldIcebergWriter(spark,text(r,"namespace"),silver,lease);
            var endpoint=URI.create("http://"+System.getenv("TRINO_HOST")+":"+System.getenv("TRINO_INTERNAL_PORT"));
            var sql=batched?TrinoSqlClient.researchCatalog(endpoint):new TrinoSqlClient(endpoint);
            var publisher=batched?GoldPublisher.researchCatalogCheckpointed(sql,silver,lease):GoldPublisher.researchRollout(sql,silver,lease);
            var cache=batched?new FullMonthBronzeReader.JmaStructureCache():null;
            var adapter=new RealEtlAdapter(spark,bronze,silver,inventory,writer,publisher,lease,cache);
            JsonNode pins=Files.exists(root.resolve("bronze-pins.json"))?read(root.resolve("bronze-pins.json"),65536):ingest(root,r,inventory,bronze,lease);
            require(pins.isArray() && pins.size()==37,"CATALOG_BRONZE_PIN_SET_CHANGED");
            JsonNode parent=null;Map<String,Long> baseline=new TreeMap<>();for(String t:GoldIcebergWriter.TABLES)baseline.put(t,0L);
            if(!done.isEmpty()) {
                var last=done.get(done.size()-1);String oldMonth=text(last,"month");var old=read(root.resolve("months/"+oldMonth+"/record.json"),4*1024*1024);
                assertRecord(old,old.path("context"));String op=text(old.path("context"),"operation_id");
                // Independent stored artifacts, not a local boolean/checkpoint, establish the latest parent.
                byte[] oldBytes=silver.read("gold-commits/"+op+"/commit.json");
                require(sha(oldBytes).equals(text(old,"commit_journal_sha256")) && JSON.readTree(oldBytes).equals(old.path("commit"))
                        && JSON.readTree(silver.read("gold-verification/"+op+"/publication.json")).equals(old.path("publication")),"CATALOG_STORED_PARENT_CHANGED");
                baseline=snapshots(last.path("snapshots"));parent=JSON.createObjectNode().put("run_id",op)
                        .put("registry_sha256",sha(silver.read(FullMonthStateResolver.root(op)+"/registry.json")))
                        .put("commit_key","gold-commits/"+op+"/commit.json").put("commit_sha256",sha(silver.read("gold-commits/"+op+"/commit.json")));
            }
            var batchPlan=read(root.resolve("plan.json"),4*1024*1024);
            while(done.size()<MONTHS.size()) {
                long remaining=(deadline-System.nanoTime())/1_000_000_000;
                if(done.size()-initialCount>=maxMonths || (batched?!ResearchCatalogBatchPlan.mayStart(remaining):remaining<1800))break;
                int index=done.size();
                var targets=batched?ResearchCatalogBatchPlan.next(batchPlan,index,maxMonths-(done.size()-initialCount)):List.of(MONTHS.get(index));
                require(!targets.isEmpty(),"CATALOG_BATCH_EMPTY");
                String month=MONTHS.get(index);Path folder=safe(root.resolve("months/"+month));Files.createDirectories(folder);
                var context=context(r,targets,pins,parent,baseline,actualCode);
                if(!Files.exists(folder.resolve("context.json")))require(writer.baselines().equals(baseline),"CATALOG_LIVE_BASELINE_CHANGED");
                ObjectNode record;
                try{record=month(adapter,silver,context,folder,lease,deadline);}
                catch(java.io.IOException e){if("CATALOG_CHECKPOINT_BEFORE_PHASE".equals(e.getMessage())){pending=month;break;}throw e;}
                String op=text(context,"operation_id");baseline=new TreeMap<>();
                for(String t:GoldIcebergWriter.TABLES)baseline.put(t,record.path("commit").path("tables").path(t).path("snapshot_id").asLong());
                for(String member:targets) {
                    Path memberRoot=safe(root.resolve("months/"+member));Files.createDirectories(memberRoot);
                    save(memberRoot.resolve("context.json"),context);save(memberRoot.resolve("record.json"),record);
                    var checkpoint=JSON.createObjectNode().put("version",VERSION).put("month",member).put("status","PUBLISHED")
                            .put("request_identity_sha256",digest(r)).put("record_sha256",sha(Files.readAllBytes(memberRoot.resolve("record.json"))));
                    if(done.isEmpty())checkpoint.putNull("previous_checkpoint_sha256");else checkpoint.put("previous_checkpoint_sha256",sha(Files.readAllBytes(root.resolve("months/"+MONTHS.get(done.size()-1)+"/checkpoint.json"))));
                    checkpoint.set("snapshots",JSON.valueToTree(baseline));save(memberRoot.resolve("checkpoint.json"),checkpoint);done.add(checkpoint);
                }
                parent=JSON.createObjectNode().put("run_id",op).put("registry_sha256",sha(silver.read(FullMonthStateResolver.root(op)+"/registry.json")))
                        .put("commit_key","gold-commits/"+op+"/commit.json").put("commit_sha256",sha(silver.read("gold-commits/"+op+"/commit.json")));
                System.out.println("event=dat03_catalog_month_published month="+month+" completed="+done.size()+" required=288");
            }
            lease.run();var result=progress(r,completed(root,r));result.set("current_snapshots",JSON.valueToTree(writer.baselines()));
            result.put("completed_in_session",done.size()-initialCount);
            result.put("session_runner_sha256",actualCode);
            if(batched)result.put("batch_policy_version",ResearchCatalogBatchPlan.VERSION).put("profile_rows_per_batch",ResearchCatalogBatchPlan.PROFILE_ROWS)
                    .put("jma_structural_full_scans",cache.fullScans()).put("jma_structural_cache_hits",cache.hits())
                    .put("shuffle_partitions",ResearchCatalogBatchPlan.SHUFFLE_PARTITIONS).put("start_reserve_seconds",ResearchCatalogBatchPlan.START_RESERVE_SECONDS)
                    .put("continuation_sha256",sha(Files.readAllBytes(safe(Path.of(args[4])))));
            if(batched)result.put("sql_read_profile",TrinoSqlClient.RESEARCH_READ_PROFILE);
            if(pending==null)result.putNull("pending_month");else result.put("pending_month",pending).put("status","PARTIAL_MONTH_REQUIRES_RECOVERY");
            save(session.resolve("result.json"),result);
        } finally {spark.stop();}
    }
}
