package ie212.earthquake.spark.ml;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import static org.apache.spark.sql.functions.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import ie212.earthquake.spark.usgs.PipelineQaSupport;
import ie212.earthquake.spark.gold.*;
import java.net.URI;
import java.nio.file.*;
import java.time.*;
import java.util.*;

/** Synthetic full-period temporal QA, real Bronze/ETL/Iceberg/Trino/export. Never research approval. */
public final class FeatureExportAcceptanceJob {
    private FeatureExportAcceptanceJob() { }
    static String row(PipelineQaSupport qa, String stamp, int magnitude) throws Exception {
        var raw = new StringBuilder(qa.jmaRecord(stamp)); raw.replace(44,49,"08000"); raw.replace(52,54,String.format(Locale.ROOT,"%02d",magnitude));
        return raw.toString();
    }
    static ObjectNode config(Instant cutoff, String code) {
        var c = GoldAuditAcceptanceJob.datasetConfig("REPRODUCTION",cutoff,code); var f = (ObjectNode)c.path("filter_config");
        f.put("coverage_scope","synthetic-full-period-component-qa").put("synthetic_component_qa",true);
        var scaling = JSON.createObjectNode().put("feature_version","relative-4d-v1").put("scaling_config_version","mld05-qa-global-v1").put("mode","GLOBAL")
            .put("space_scale_km",50.).put("depth_scale_km",30.).put("time_scale_hours",720.).put("max_candidates_per_window",10000)
            .put("coordinate_tolerance_km",1e-8).put("time_tolerance_hours",1e-12);
        f.set("feature_scaling_config",scaling); f.set("window_config",WindowModelConfig.expandedV1().toJsonNode());
        f.set("mainshock_selection_config",MainshockSelectionConfig.reproductionBaseline().toJsonNode());
        c.put("window_model_version",WindowModelConfig.expandedV1().windowModelVersion()).put("scaling_config_version","mld05-qa-global-v1")
            .put("dataset_config_version","mld05-synthetic-qa-v1"); return c;
    }
    public static void main(String[] args) throws Exception {
        require(args.length==1,"MLD05_QA_REQUEST_REQUIRED"); var request=JSON.readTree(Files.readAllBytes(Path.of(args[0])));
        String ns=text(request,"namespace"), run=text(request,"run_id");
        require(ns.matches("[a-z][a-z0-9_]*\\.mld05_qa_[a-f0-9]{32}") && run.matches("mld05-qa-[a-f0-9]{32}"),"MLD05_QA_ISOLATION_REQUIRED");
        Path root=Path.of(args[0]).toAbsolutePath().getParent(); String ml=ns.replace(".mld05_qa_",".mld05_ml_qa_");
        try(var qa=new PipelineQaSupport(request,root)) {
            var sql=new TrinoSqlClient(URI.create("http://"+System.getenv("TRINO_HOST")+":"+System.getenv("TRINO_INTERNAL_PORT")));
            if(Files.isRegularFile(root.resolve("history-complete.json"))) {
                var checkpoint=(ObjectNode)JSON.readTree(Files.readAllBytes(root.resolve("history-complete.json")));
                require(ns.equals(checkpoint.path("namespace").asText()) && run.equals(checkpoint.path("run_id").asText())
                    && checkpoint.path("publications").size()==10,"MLD05_CHECKPOINT_IDENTITY_CONFLICT");
                var last=checkpoint.path("publications").get(9).path("commit").path("tables");
                for(var pin:qa.writer.baselines().entrySet()) require(pin.getValue()==last.path(pin.getKey()).path("snapshot_id").asLong(),"MLD05_CHECKPOINT_GOLD_CHANGED");
                checkpoint.put("resumed_from_checkpoint",true);
                finish(qa,sql,root,checkpoint,checkpoint.path("source_manifest")); return;
            }
            require(!qa.spark.catalog().databaseExists(ns) && !qa.spark.catalog().databaseExists(ml),"MLD05_FRESH_NAMESPACE_REQUIRED");
            var pins=new ArrayList<JsonNode>();
            for(int year=1999;year<=2018;year++) {
                var entry=qa.entry(year,"full-year"); var records=new ArrayList<String>();
                for(int month=1;month<=12;month++) {
                    boolean shock=(year==2006 || year==2010) && month==7;
                    records.add(row(qa,String.format(Locale.ROOT,"%04d%02d1512000000",year,month),shock?62:40));
                    if(shock) records.add(row(qa,String.format(Locale.ROOT,"%04d%02d1513000000",year,month),42));
                }
                pins.add(qa.jma(run+"-"+year,entry,qa.archive(entry,records),"synthetic-v1"));
            }
            var report=JSON.createObjectNode().put("task","MLD-05").put("status","RUNNING").put("synthetic_component_qa",true)
                .put("research_ready",false).put("scientific_approval",false).put("namespace",ns).put("ml_namespace",ml).put("run_id",run);
            var publications=report.putArray("publications"); ObjectNode parent=null; JsonNode rejectedManifest=null;
            var months=new ArrayList<String>(); for(YearMonth m=YearMonth.of(2000,1);m.isBefore(YearMonth.of(2018,10));m=m.plusMonths(1)) months.add(m.toString());
            for(int offset=0;offset<months.size();offset+=24) {
                var record=qa.execute(qa.context(run+"-batch-"+offset,months.subList(offset,Math.min(months.size(),offset+24)),offset==0?pins:List.of(),parent));
                publications.add(record); parent=record;
                if(offset==0) {
                    rejectedManifest=new DatasetManifestWriter(sql,qa.lease).build(ns,record.path("publication").path("publication_id").asText(),ml,
                        config(Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS),"qa-mld05-v1"),run,Instant.now());
                    boolean blocked=false;
                    try {FeatureExportJob.export(qa.spark,sql,qa.silver,ml,rejectedManifest.path("dataset_id").asText(),run,root.resolve("blocked-export"),root.resolve("blocked-registry"),qa.lease);}
                    catch(java.io.IOException error) {require(error.getMessage().startsWith("DS_EXPORT_COVERAGE_INCOMPLETE"),"MLD05_WRONG_COVERAGE_REJECTION");blocked=true;}
                    require(blocked && !Files.exists(root.resolve("blocked-export")),"MLD05_PARTIAL_DATASET_PROMOTED");
                    report.set("partial_dataset",rejectedManifest); report.put("partial_coverage_rejected",true);
                }
                System.out.println("event=mld05_qa_history months="+Math.min(months.size(),offset+24)+" publication="+record.path("publication").path("publication_id").asText());
            }
            var manifest=new DatasetManifestWriter(sql,qa.lease).build(ns,parent.path("publication").path("publication_id").asText(),ml,
                config(Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS),"qa-mld05-v1"),run,Instant.now());
            report.set("source_manifest",manifest); PipelineQaSupport.save(root.resolve("history-complete.json"),report);
            finish(qa,sql,root,report,manifest);
        }
    }
    private static void finish(PipelineQaSupport qa,GoldPublisher.Sql sql,Path root,ObjectNode report,JsonNode manifest) throws Exception {
            String run=text(report,"run_id"), ml=text(report,"ml_namespace");
            var beforeGold=qa.writer.baselines();
            var first=FeatureExportJob.export(qa.spark,sql,qa.silver,ml,manifest.path("dataset_id").asText(),run,root.resolve("ml-export"),root.resolve("registry"),qa.lease);
            var second=FeatureExportJob.export(qa.spark,sql,qa.silver,ml,manifest.path("dataset_id").asText(),run+"-rerun",root.resolve("ml-export"),root.resolve("registry"),qa.lease);
            for(String key:List.of("registry_snapshot_id","candidate_snapshot_id","mainshock_snapshot_id","dataset_manifest_sha256","bundle_manifest_sha256","checksums_sha256"))
                require(first.path(key).equals(second.path(key)),"MLD05_RERUN_CHANGED_PINS_OR_BYTES");
            require(second.path("reused").asBoolean() && beforeGold.equals(qa.writer.baselines()),"MLD05_RERUN_OR_GOLD_CHANGED");
            var resolverRequest=first.deepCopy().put("algorithm_name","WINDOW").put("model_config_version","1").put("experiment_run_id","exp_mld05_component");
            var context=TrustedDatasetResolver.resolve(resolverRequest,root.resolve("registry"),null);
            require(context.candidates().size()==first.path("feature_row_count").asLong(),"MLD05_IMPORT_HANDOFF_MISMATCH");
            report.set("first",first);report.set("rerun",second);report.set("gold_before_export",JSON.valueToTree(beforeGold));report.set("gold_after_export",JSON.valueToTree(qa.writer.baselines()));
            require("BUILDING".equals(FeatureExportJob.manifest(sql,ml,report.path("partial_dataset").path("dataset_id").asText()).path("dataset_status").asText()),"MLD05_PARTIAL_PARENT_CHANGED");
            report.put("trusted_resolver_verified",true).put("required_month_count",225).put("status","PASS")
                .put("completed_at_utc",Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS).toString());
            PipelineQaSupport.save(root.resolve("runtime-report.json"),report);
    }
}
