package ie212.earthquake.spark.ml;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import static org.apache.spark.sql.functions.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.gold.*;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.spark.sql.SparkSession;

/** Fixed QA-04 only. Synthetic EXPORTED fixture, real ML writes/Trino; no production fault flags. */
public final class Qa04MlRecoveryAcceptance {
    private Qa04MlRecoveryAcceptance() { }
    public static ObjectNode accept(SparkSession spark,GoldPublisher.Sql sql,Path root,String ns,Runnable lease) throws Exception {
        require(ns.matches("[a-z][a-z0-9_]*\\.qa04_ml_[a-f0-9]{32}"),"QA04_ISOLATED_ML_NAMESPACE_REQUIRED");
        require(!spark.catalog().databaseExists(ns),"QA04_FRESH_ML_NAMESPACE_REQUIRED");
        lease.run();Files.createDirectories(root);
        spark.sql("CREATE NAMESPACE "+ns);
        spark.sql("CREATE TABLE "+ns+".candidate_event USING iceberg AS SELECT 'ds_fixture' dataset_id,'main' mainshock_event_id,'main' candidate_event_id,'MAINSHOCK' role UNION ALL SELECT 'ds_fixture','main','post','POST'");
        spark.sql("CREATE TABLE "+ns+".mainshock_event USING iceberg AS SELECT 'ds_fixture' dataset_id,'main' mainshock_event_id");
        spark.sql("CREATE TABLE "+ns+".dataset_registry USING iceberg AS SELECT 'ds_fixture' dataset_id,'EXPORTED' dataset_status");
        var inputPins=JSON.createObjectNode();
        for(String table:List.of("candidate_event","mainshock_event","dataset_registry")) {
            long pin=Spark3Util.loadIcebergTable(spark,ns+"."+table).currentSnapshot().snapshotId();
            require(scalar(sql,"SELECT count(*) FROM "+ns+"."+table+" FOR VERSION AS OF "+pin)==(table.equals("candidate_event")?2:1),"QA04_ML_INPUT_READBACK_FAILED");
            inputPins.put(table,pin);
        }
        Path registry=root.resolve("registry");Files.createDirectory(registry);
        var export=JSON.createObjectNode().put("dataset_id","ds_fixture").put("synthetic_component_qa",true);
        export.set("snapshots",inputPins);save(registry.resolve("export.json"),export);
        var manifest=JSON.createObjectNode().put("dataset_id","ds_fixture").put("dataset_status","EXPORTED")
            .put("export_manifest_ref","export.json").put("export_manifest_sha256",sha(Files.readAllBytes(registry.resolve("export.json"))))
            .put("registry_snapshot_id",inputPins.path("dataset_registry").asText())
            .put("candidate_snapshot_id",inputPins.path("candidate_event").asText()).put("mainshock_snapshot_id",inputPins.path("mainshock_event").asText());
        var candidates=manifest.putArray("candidates");
        for(var row:spark.read().format("iceberg").option("snapshot-id",inputPins.path("candidate_event").asText()).load(ns+".candidate_event").collectAsList())
            candidates.addObject().put("mainshock_event_id",(String)row.getAs("mainshock_event_id")).put("candidate_event_id",(String)row.getAs("candidate_event_id")).put("role",(String)row.getAs("role"));
        save(registry.resolve("dataset.json"),manifest);
        var report=JSON.createObjectNode().put("namespace",ns).put("synthetic_component_qa",true).put("scientific_approval",false);
        report.set("input_snapshots",inputPins);
        Path outsideBundle=bundle(root,"exp_outside");var outsideContext=context(registry,"exp_outside");
        var baseline=attempt(spark,sql,ns,root.resolve("outside-journal"),outsideBundle,outsideContext,lease,false);
        report.set("baseline",baseline);String outside=outside(spark,ns);
        require(!outside.isBlank(),"QA04_ML_POSITIVE_RETENTION_REQUIRED");
        var invalid=report.putObject("ml_bundle_invalid");var rejects=invalid.putArray("rejections");
        var before=states(spark,ns);
        for(String kind:List.of("checksum","schema")) {
            String id="exp_bad_"+kind;Path bad=bundle(root,id);Path journal=root.resolve(id+"-journal");
            if(kind.equals("checksum")) {
                var marker=(ObjectNode)JSON.readTree(Files.readAllBytes(bad.resolve("_SUCCESS.json")));
                ((ObjectNode)marker.path("checksums")).put("memberships.parquet","0".repeat(64));replace(bad.resolve("_SUCCESS.json"),marker);
            } else {
                corruptSchema(bad);
            }
            String reason=kind.equals("checksum")?"IMP_CHECKSUM_MISMATCH":"IMP_SCHEMA_MISMATCH";
            boolean rejected=false;
            try {attempt(spark,sql,ns,journal,bad,context(registry,id),lease,false);}
            catch(Exception error) {require(reason.equals(error.getMessage()),"QA04_ML_UNEXPECTED_REJECTION");rejected=true;}
            require(rejected && before.equals(states(spark,ns)),"QA04_ML_REJECT_CHANGED_SNAPSHOTS");
            var canonical=JSON.readTree(Files.readAllBytes(journal.resolve("experiment-run-"+id+".json")));
            require("REJECTED".equals(canonical.path("experiment_status").asText()),"QA04_ML_REJECTION_NOT_DURABLE");
            require(scalar(sql,"SELECT count(*) FROM "+ns+".experiment_run WHERE experiment_run_id='"+id+"' AND experiment_status='CANDIDATE'")==0,"QA04_ML_INVALID_PUBLISHED");
            var rejection=rejects.addObject().put("experiment_run_id",id).put("reason",reason).put("candidate_rows",0);
            rejection.set("journal",canonical);rejection.set("snapshots_before",before);rejection.set("snapshots_after",states(spark,ns));
        }
        var recovered=attempt(spark,sql,ns,root.resolve("recovered-journal"),bundle(root,"exp_recovered"),context(registry,"exp_recovered"),lease,false);
        invalid.set("recovery",recovered);
        require(outside.equals(outside(spark,ns)),"QA04_ML_INVALID_RECOVERY_CHANGED_OUTSIDE");
        var partial=report.putObject("ml_import_partial");Path partialJournal=root.resolve("partial-journal");
        Path valid=bundle(root,"exp_partial");var ctx=context(registry,"exp_partial");boolean stopped=false;
        try {attempt(spark,sql,ns,partialJournal,valid,ctx,lease,true);}
        catch(IllegalStateException error) {require("IMP_COMMIT_OUTCOME_UNKNOWN".equals(error.getMessage()),"QA04_ML_WRONG_PARTIAL_FAILURE");stopped=true;}
        require(stopped,"QA04_ML_PARTIAL_NOT_INJECTED");
        long member=Spark3Util.loadIcebergTable(spark,ns+".sequence_membership").currentSnapshot().snapshotId();
        require(scalar(sql,"SELECT count(*) FROM "+ns+".experiment_run WHERE experiment_run_id='exp_partial' AND experiment_status='CANDIDATE'")==0,"QA04_ML_PARTIAL_PUBLISHED");
        require(scalar(sql,"SELECT count(*) FROM "+ns+".experiment_run WHERE experiment_run_id='exp_partial' AND experiment_status='IMPORT_VALIDATING'")==1,"QA04_ML_PARTIAL_STATE_LOST");
        var fault=JSON.readTree(Files.readAllBytes(partialJournal.resolve("experiment-run-exp_partial.json")));
        partial.set("fault_journal",fault);partial.put("membership_snapshot_before_recovery",member).put("candidate_rows_before_recovery",0);
        var resumed=attempt(spark,sql,ns,partialJournal,valid,ctx,lease,false);
        require(resumed.path("membership_snapshot_id").asLong()==member,"QA04_ML_RECOVERY_REAPPENDED");
        require(spark.table(ns+".sequence_membership.snapshots").filter(col("summary").getItem("ml.operation_id").equalTo("exp_partial")).count()==1,"QA04_ML_DUPLICATE_COMMIT");
        partial.set("recovery",resumed);partial.set("candidate_journal",JSON.readTree(Files.readAllBytes(partialJournal.resolve("experiment-run-exp_partial.json"))));
        require(outside.equals(outside(spark,ns)),"QA04_ML_PARTIAL_CHANGED_OUTSIDE");
        new MlResultSnapshotReader(spark,ns).serving(baseline.path("experiment_snapshot_id").asLong(),baseline.path("membership_snapshot_id").asLong(),baseline.path("summary_snapshot_id").asLong(),"exp_outside");
        report.put("outside_experiment_run_id","exp_outside").put("outside_before_sha256",sha(outside.getBytes(java.nio.charset.StandardCharsets.UTF_8)))
            .put("outside_after_sha256",sha(outside(spark,ns).getBytes(java.nio.charset.StandardCharsets.UTF_8))).put("old_snapshots_readable",true);
        report.set("final_snapshots",states(spark,ns));save(root.resolve("ml-evidence.json"),report);return report;
    }
    private static ResultBundleValidator.Context context(Path registry,String id) throws Exception {
        var request=JSON.createObjectNode().put("dataset_id","ds_fixture").put("experiment_run_id",id).put("algorithm_name","WINDOW").put("model_config_version","1")
            .put("dataset_manifest_ref","dataset.json").put("dataset_manifest_sha256",sha(Files.readAllBytes(registry.resolve("dataset.json"))));
        return TrustedDatasetResolver.resolve(request,registry,null);
    }
    private static ObjectNode attempt(SparkSession spark,GoldPublisher.Sql sql,String ns,Path journal,Path bundle,ResultBundleValidator.Context context,Runnable lease,boolean fail) throws Exception {
        SparkMlIcebergWriter writer=fail?new SparkMlIcebergWriter(spark,ns,lease,journal,table->{if(table.equals("sequence_membership"))throw new IllegalStateException("IMP_COMMIT_OUTCOME_UNKNOWN");})
            :new SparkMlIcebergWriter(spark,ns,lease,journal);
        var result=MlResultImportJob.importBundle(bundle,journal,context,writer,(commit,ctx,fp)->new TrinoMlVerifier(sql,ns,lease).verify(commit,ctx,fp),ns);
        long pin=result.path("experiment_snapshot_id").asLong();
        require(scalar(sql,"SELECT count(*) FROM "+ns+".experiment_run FOR VERSION AS OF "+pin+" WHERE experiment_run_id='"+context.runId()+"' AND experiment_status='CANDIDATE' AND membership_snapshot_id="+result.path("membership_snapshot_id").asLong()+" AND summary_snapshot_id="+result.path("summary_snapshot_id").asLong())==1,"QA04_ML_TRINO_PUBLICATION_INVALID");
        require(new MlResultSnapshotReader(spark,ns).serving(pin,result.path("membership_snapshot_id").asLong(),result.path("summary_snapshot_id").asLong(),context.runId()).count()==1,"QA04_ML_SERVING_FAILED");
        return result;
    }
    private static ObjectNode states(SparkSession spark,String ns) throws Exception {
        var result=JSON.createObjectNode();for(String table:List.of("sequence_membership","sequence_summary","experiment_run"))
            result.put(table,Spark3Util.loadIcebergTable(spark,ns+"."+table).currentSnapshot().snapshotId());return result;
    }
    private static String outside(SparkSession spark,String ns) {
        var result=new TreeMap<String,List<String>>();
        for(String table:List.of("sequence_membership","sequence_summary","experiment_run")) {
            var rows=spark.table(ns+"."+table).filter(col("experiment_run_id").equalTo("exp_outside")).toJSON().collectAsList();
            requireNonempty(rows);result.put(table,canonicalRows(rows));
        }
        return result.toString();
    }
    private static void requireNonempty(List<String> rows) {if(rows.isEmpty())throw new IllegalStateException("QA04_ML_POSITIVE_RETENTION_REQUIRED");}
    static List<String> canonicalRows(List<String> rows) {
        var sorted=new ArrayList<>(rows);Collections.sort(sorted);return List.copyOf(sorted);
    }
    static void corruptSchema(Path bundle) throws Exception {
        var marker=(ObjectNode)JSON.readTree(Files.readAllBytes(bundle.resolve("_SUCCESS.json")));
        marker.put("schema_version","invalid-qa04-version");replace(bundle.resolve("_SUCCESS.json"),marker);
    }
    private static Path bundle(Path root,String id) throws Exception {
        return bundle(root,id,Path.of("/opt/pipeline/ml-result-fixtures/success"));
    }
    static Path bundle(Path root,String id,Path source) throws Exception {
        require(id.matches("exp_[a-z0-9_]+"),"QA04_SAFE_FIXTURE_ID_REQUIRED");
        Path target=root.resolve(id);Files.createDirectory(target);
        try(var files=Files.list(source)) {for(Path file:files.toList()) {require(Files.isRegularFile(file)&&!Files.isSymbolicLink(file),"QA04_FIXTURE_FILE_REQUIRED");Files.copy(file,target.resolve(file.getFileName()));}}
        var decoded=MlResultBundleReader.read(source);
        writeRows(target.resolve("memberships.parquet"),"memberships.parquet",decoded.memberships(),id);
        writeRows(target.resolve("sequence_summary.parquet"),"sequence_summary.parquet",decoded.summaries(),id);
        for(String name:List.of("experiment_config.json","experiment_metrics.json","_SUCCESS.json")) {
            var doc=(ObjectNode)JSON.readTree(Files.readAllBytes(target.resolve(name)));doc.put("experiment_run_id",id);
            if(name.equals("_SUCCESS.json"))doc.put("artifact_uri","s3://fixture/results/"+id+"/");replace(target.resolve(name),doc);
        }
        rehash(target);return target;
    }
    private static void rehash(Path folder) throws Exception {
        var marker=(ObjectNode)JSON.readTree(Files.readAllBytes(folder.resolve("_SUCCESS.json")));
        var checks=(ObjectNode)marker.path("checksums");for(var names=checks.fieldNames();names.hasNext();) {String name=names.next();checks.put(name,sha(Files.readAllBytes(folder.resolve(name))));}
        replace(folder.resolve("_SUCCESS.json"),marker);
    }
    private static void writeRows(Path path,String artifact,List<JsonNode> rows,String id) throws Exception {
        var schema=MlResultBundleReader.schema(artifact);var factory=new SimpleGroupFactory(schema);
        var conf=new Configuration(false);conf.set("fs.file.impl","org.apache.hadoop.fs.RawLocalFileSystem");conf.setBoolean("fs.file.impl.disable.cache",true);
        try(var writer=ExampleParquetWriter.builder(new org.apache.hadoop.fs.Path(path.toUri())).withType(schema)
            .withWriteMode(org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE).withConf(conf).build()) {
            for(JsonNode original:rows) {
                var row=(ObjectNode)original.deepCopy();row.put("experiment_run_id",id);Group group=factory.newGroup();
                for(JsonNode field:ResultBundleContract.contract().path("schemas").path(artifact).path("fields")) {
                    String key=field.path("name").asText();JsonNode value=row.get(key);if(value==null||value.isNull())continue;
                    switch(field.path("type").asText()) {
                        case "string","json_string" -> group.add(key,value.asText());case "timestamp_utc" -> group.add(key,Instant.parse(value.asText()).toEpochMilli());
                        case "boolean" -> group.add(key,value.asBoolean());case "integer" -> group.add(key,value.asInt());case "long" -> group.add(key,value.asLong());case "double" -> group.add(key,value.asDouble());
                        case "array<string>" -> {Group list=group.addGroup(key);for(JsonNode item:value)list.addGroup("list").add("element",item.asText());}
                        default -> throw new IllegalStateException("QA04_FIXTURE_TYPE_UNSUPPORTED");
                    }
                }
                writer.write(group);
            }
        }
    }
    private static long scalar(GoldPublisher.Sql sql,String query) throws Exception {var r=sql.execute(query);require(r.rows().size()==1&&r.rows().get(0).size()==1,"QA04_ML_SQL_SHAPE");return r.rows().get(0).get(0).asLong();}
    private static void save(Path p,JsonNode doc) throws Exception {Files.write(p,JSON.writerWithDefaultPrettyPrinter().writeValueAsBytes(doc),StandardOpenOption.CREATE_NEW);}
    private static void replace(Path p,JsonNode doc) throws Exception {Files.write(p,JSON.writerWithDefaultPrettyPrinter().writeValueAsBytes(doc),StandardOpenOption.TRUNCATE_EXISTING);}
}
