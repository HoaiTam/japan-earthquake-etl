package ie212.earthquake.spark.usgs;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import static ie212.earthquake.spark.usgs.ResearchCatalogState.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.GoldCommitJob;
import ie212.earthquake.spark.gold.*;
import ie212.earthquake.spark.jma.*;
import ie212.earthquake.spark.ml.*;
import ie212.earthquake.spark.silver.MinioSilverObjectStore;
import java.net.URI;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;

/** Isolated checkpointed MLI-02 adapter execution after ALL 288 real catalog months. Not a scheduled DagRun. */
public final class ResearchDatasetBuildJob {
    public static final String VERSION="dat03-research-dataset-build-v1";
    static final List<String> STATUSES=List.of("VERIFIED_PUBLISHED","SNAPSHOT_PINNED","AUDIT_PASSED","MC_RESOLVED","MAINSHOCKS_SELECTED",
            "WINDOWS_BUILT","FEATURES_BUILT","FEATURE_SNAPSHOT_VALIDATED","DATASET_EXPORTED","DATASET_EXPORTED");
    private ResearchDatasetBuildJob() { }
    static ObjectNode config(String split,Instant cutoff,String code,JsonNode scaling) throws Exception {
        FeatureScalingConfig.fromJson(scaling);
        var selection=switch(split) {case "REPRODUCTION"->MainshockSelectionConfig.reproductionBaseline();
            case "EXTENSION"->MainshockSelectionConfig.extensionBaseline();default->throw new IllegalArgumentException("DS_INVALID_PERIOD");};
        var window=WindowModelConfig.expandedV1();
        var out=JSON.createObjectNode().put("dataset_split",split).put("period_start_utc",selection.periodStartUtc().toString())
                .put("period_end_utc_exclusive",selection.periodEndUtcExclusive().toString()).put("observation_cutoff_utc",cutoff.toString())
                .put("dataset_config_version",VERSION).put("audit_rule_version","gold-audit-v1").put("mc_method_version","maximum-curvature-v1")
                .put("window_model_version",window.windowModelVersion()).put("feature_version",scaling.path("feature_version").asText())
                .put("scaling_config_version",scaling.path("scaling_config_version").asText()).put("code_version","artifact_"+code);
        var f=out.putObject("filter_config").put("is_natural_earthquake",true).put("is_in_study_area",true)
                .put("canonical_source_system","JMA_BULLETIN").put("catalog_era","UNIFIED").put("window_coverage_policy",WindowCoveragePolicy.STRICT);
        f.putObject("completeness_config").put("bin_width",0.1).put("sensitivity_step",0.2).put("min_events",50).put("region_version","overall-full-split-v1");
        f.set("window_config",window.toJsonNode());f.set("mainshock_selection_config",selection.toJsonNode());f.set("feature_scaling_config",scaling.deepCopy());
        f.putObject("collection_export").put("version",ResearchCollectionExportBundle.VERSION).set("layout_limits",ResearchCollectionPlan.Limits.defaults().json());
        return out;
    }
    static JsonNode completeCatalog(Path root,JsonNode request) throws Exception {
        ResearchCatalogState.validate(request);var months=completed(root,request);
        require(months.size()==288,"RESEARCH_FULL_288_MONTH_CATALOG_REQUIRED");
        for(var month:months) {
            var record=read(root.resolve("months/"+text(month,"month")+"/record.json"),4*1024*1024);ResearchCatalogJob.assertRecord(record,record.path("context"));
            boolean covered=false;for(var target:record.path("context").path("resolver_request").path("target_months"))covered|=target.equals(month.path("month"));
            require(covered,"RESEARCH_CATALOG_MONTH_NOT_COVERED");
        }
        var last=read(root.resolve("months/2023-12/record.json"),4*1024*1024);ResearchCatalogJob.assertRecord(last,last.path("context"));return last;
    }
    public static void main(String[] args) throws Exception {
        require(args.length==2,"RESEARCH_BUILD_REQUEST_AND_BUDGET_REQUIRED");Path input=safe(Path.of(args[0])),root=input.getParent();
        require(input.toString().matches("/opt/pipeline/staging/dat03-ml-export/[a-f0-9]{32}/request.json"),"RESEARCH_BUILD_EXACT_ROOT_REQUIRED");
        int seconds=Integer.parseInt(args[1]);require(seconds>=3600 && seconds<=6600,"RESEARCH_BUILD_BUDGET_REQUIRED");long deadline=System.nanoTime()+seconds*1_000_000_000L;
        var r=read(input,65536);var fields=new HashSet<String>();r.fieldNames().forEachRemaining(fields::add);
        require(fields.equals(Set.of("version","catalog_root","catalog_request_sha256","runner_sha256","scaling_sha256","dataset_split","observation_cutoff_utc"))
                && VERSION.equals(text(r,"version")) && LocalFeatureJob.codeSha().equals(text(r,"runner_sha256")),"RESEARCH_BUILD_EXACT_REQUEST_REQUIRED");
        Path catalog=safe(Path.of(text(r,"catalog_root")));
        require(catalog.toString().matches("/opt/pipeline/staging/dat03-catalog/[a-f0-9]{32}")
                && sha(Files.readAllBytes(catalog.resolve("request.json"))).equals(text(r,"catalog_request_sha256")),"RESEARCH_BUILD_CATALOG_PIN_CHANGED");
        var source=read(catalog.resolve("request.json"),65536);var last=completeCatalog(catalog,source);
        byte[] scaleBytes=Files.readAllBytes(safe(root.resolve("feature-scaling.json")));
        require(scaleBytes.length<=65536 && sha(scaleBytes).equals(text(r,"scaling_sha256")),"RESEARCH_BUILD_SCALING_PIN_CHANGED");
        Instant cutoff=utc(r,"observation_cutoff_utc");require(!cutoff.isAfter(Instant.now()),"RESEARCH_BUILD_FUTURE_CUTOFF_FORBIDDEN");
        String run="dat03-collection-"+root.getFileName(),dag="mli_02_build_dataset",ns="iceberg.dat03_ml_"+catalog.getFileName();
        Path owner=Path.of(Objects.requireNonNull(System.getenv("SOURCE_GUARD_ROOT")),"owner.json");
        Runnable lease=()->{try{var held=read(owner,65536);require(held.size()==2 && dag.equals(text(held,"dag_id")) && run.equals(text(held,"run_id")),"RESEARCH_BUILD_OWN_LEASE_REQUIRED");}
            catch(Exception e){throw new IllegalStateException("RESEARCH_BUILD_OWN_LEASE_REQUIRED");}};lease.run();
        var spark=GoldCommitJob.configured(System.getenv()).master("local[1]").appName("dat03-research-dataset-build")
                .config("spark.ui.enabled","false").config("spark.sql.shuffle.partitions","16").config("spark.sql.adaptive.enabled","false").getOrCreate();spark.sparkContext().setLogLevel("ERROR");
        try(var store=MinioSilverObjectStore.fromEnvironment()) {
            String op=text(last.path("context"),"operation_id");
            require(sha(store.read("gold-commits/"+op+"/commit.json")).equals(text(last,"commit_journal_sha256"))
                    && JSON.readTree(store.read("gold-verification/"+op+"/publication.json")).equals(last.path("publication")),"RESEARCH_BUILD_PUBLISHED_PARENT_CHANGED");
            var sql=new TrinoSqlClient(URI.create("http://"+System.getenv("TRINO_HOST")+":"+System.getenv("TRINO_INTERNAL_PORT")));
            var pub=last.path("publication");var reference=JSON.createObjectNode().put("gold_publication_id",text(pub,"publication_id"))
                    .put("gold_snapshot_id",last.path("commit").path("tables").path("event_current").path("snapshot_id").asLong())
                    .put("physical_gold_table_name",last.path("commit").path("tables").path("event_current").path("table").asText())
                    .put("gold_table_name",text(source,"namespace")+".earthquake_event_current").put("publication_bundle_sha256",text(pub,"bundle_sha256"))
                    .put("verify_status","PASSED").put("published",true);
            var request=JSON.createObjectNode().put("contract_version",DatasetBuildJob.COLLECTION_VERSION).put("dag_id",dag).put("run_id",run)
                    .put("build_run_id",run).put("ml_namespace",ns).put("run_root",root.toString()).put("attempt",1)
                    .put("export_root",root.resolve("export").toString()).put("registry_root",root.resolve("registry").toString());
            request.set("config",config(text(r,"dataset_split"),cutoff,text(r,"runner_sha256"),JSON.readTree(scaleBytes)));request.set("publication_reference",reference);
            request.put("step","plan");var plan=DatasetBuildJob.execute(request,sql,store,spark,lease);request.set("plan",plan);save(root.resolve("plan.json"),plan);
            var receipts=safe(root.resolve("receipts"));Files.createDirectories(receipts);var chain=safe(root.resolve("receipt-chain"));Files.createDirectories(chain);
            var reports=safe(root.resolve("sessions"));Files.createDirectories(reports);
            try(var files=Files.list(reports)) {for(var file:files.toList()) {
                var old=read(file,65536);int n=old.path("completed_steps").asInt(-1);
                require(n>=0 && n<=10 && text(r,"runner_sha256").equals(text(old,"runner_sha256"))
                        && sha(Files.readAllBytes(input)).equals(text(old,"request_sha256")),"RESEARCH_BUILD_PREVIOUS_SESSION_CHANGED");
                if(n>0)require(sha(Files.readAllBytes(chain.resolve(DatasetBuildJob.STEPS.get(n-1)+".json"))).equals(text(old,"last_chain_sha256")),"RESEARCH_BUILD_PREVIOUS_CHAIN_CHANGED");
            }}
            JsonNode previous=null;int count=0;String previousChain=null;
            for(String step:DatasetBuildJob.STEPS) {
                Path receipt=receipts.resolve(step+".json");
                if(Files.exists(receipt)) {
                    var existing=read(receipt,65536);
                    require(step.equals(text(existing,"step")) && run.equals(text(existing,"run_id")) && digest(plan).equals(text(existing,"plan_sha256"))
                            && STATUSES.get(count).equals(text(existing,"status")) && plan.path("dataset_id").equals(existing.path("dataset_id"))
                            && DatasetBuildJob.COLLECTION_VERSION.equals(text(existing,"contract_version")),"RESEARCH_BUILD_RECEIPT_CHANGED");
                    var bound=read(chain.resolve(step+".json"),65536);
                    require(sha(Files.readAllBytes(receipt)).equals(text(bound,"receipt_sha256")) && digest(plan).equals(text(bound,"plan_sha256"))
                            && Objects.equals(previousChain,bound.path("previous_chain_sha256").isNull()?null:text(bound,"previous_chain_sha256")),"RESEARCH_BUILD_CHAIN_CHANGED");
                    previousChain=sha(Files.readAllBytes(chain.resolve(step+".json")));previous=existing;count++;continue;
                }
                require(DatasetBuildJob.STEPS.subList(count+1,DatasetBuildJob.STEPS.size()).stream().noneMatch(s->Files.exists(receipts.resolve(s+".json"))),"RESEARCH_BUILD_NONCONTIGUOUS_RECEIPTS");
                if((deadline-System.nanoTime())/1_000_000_000<3600)break;
                request.put("step",step);if(previous==null)request.putNull("previous_receipt_sha256");
                else request.put("previous_receipt_sha256",sha(Files.readAllBytes(receipts.resolve(DatasetBuildJob.STEPS.get(count-1)+".json"))));
                lease.run();System.out.println("event=dat03_dataset_phase step="+step+" split="+text(r,"dataset_split"));
                previous=DatasetBuildJob.execute(request,sql,store,spark,lease);
                require(STATUSES.get(count).equals(text(previous,"status")),"RESEARCH_BUILD_STEP_NOT_ACCEPTED");save(receipt,previous);
                var bound=JSON.createObjectNode().put("receipt_sha256",sha(Files.readAllBytes(receipt))).put("plan_sha256",digest(plan));
                if(previousChain==null)bound.putNull("previous_chain_sha256");else bound.put("previous_chain_sha256",previousChain);
                save(chain.resolve(step+".json"),bound);previousChain=sha(Files.readAllBytes(chain.resolve(step+".json")));count++;
            }
            var report=JSON.createObjectNode().put("version",VERSION).put("status",count==10?"DATASET_EXPORTED":"CHECKPOINTED")
                    .put("dataset_split",text(r,"dataset_split")).put("dataset_id",plan.path("dataset_id").asText()).put("completed_steps",count)
                    .put("required_steps",10).put("catalog_request_sha256",text(r,"catalog_request_sha256")).put("catalog_completed_months",288)
                    .put("runner_sha256",text(r,"runner_sha256")).put("synthetic_component_qa",false).put("scientific_approval",false);
            report.put("request_sha256",sha(Files.readAllBytes(input)));if(previousChain==null)report.putNull("last_chain_sha256");else report.put("last_chain_sha256",previousChain);
            report.set("gold_publication",reference);report.set("last_receipt",previous);
            if(count==10) {
                // A completed local prefix alone cannot authorize handoff/release: independently re-resolve EXPORTED/pins/bytes/registry now.
                request.put("step","mark_dataset_exported").put("previous_receipt_sha256",sha(Files.readAllBytes(receipts.resolve("export_parquet.json"))));
                report.set("final_readback",DatasetBuildJob.execute(request,sql,store,spark,lease));
            }
            save(reports.resolve(UUID.randomUUID()+".json"),report);
            System.out.println("event=dat03_dataset_checkpoint completed="+count+" required=10");
        } finally {spark.stop();}
    }
}
