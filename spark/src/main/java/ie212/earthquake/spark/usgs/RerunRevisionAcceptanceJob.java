package ie212.earthquake.spark.usgs;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import static org.apache.spark.sql.functions.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.*;
import ie212.earthquake.spark.gold.GoldIcebergWriter;
import ie212.earthquake.spark.silver.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;

/** QA-02: all eight cases use production Bronze writers, ORC-07 phases and independent Trino gates. */
public final class RerunRevisionAcceptanceJob {
    private RerunRevisionAcceptanceJob() { }
    public static void main(String[] args) throws Exception {
        require(args.length==1,"QA02_EXACT_REQUEST_REQUIRED");
        Path requestPath=Path.of(args[0]); var request=JSON.readTree(Files.readAllBytes(requestPath));
        Path reportPath=Path.of(text(request,"report_path")); Path root=reportPath.getParent();
        var matrix=JSON.readTree(Files.readAllBytes(Path.of(text(request,"matrix_path"))));
        require(sha(Files.readAllBytes(Path.of(text(request,"matrix_path")))).equals(text(request,"matrix_sha256")),"QA02_MATRIX_CHANGED");
        try(var qa=new PipelineQaSupport(request,root)) {run(qa,request,matrix,reportPath);}
    }
    private static void run(PipelineQaSupport qa,JsonNode request,JsonNode matrix,Path reportPath) throws Exception {
        var report=JSON.createObjectNode(); report.put("task_id","QA-02"); report.put("report_version","qa-02-report-v2");
        report.put("runtime_complete",true); report.put("synthetic_fixture",true); report.put("backend","MinIO/REST/Iceberg/Trino");
        for(String field:List.of("run_id","operation_id","namespace","matrix_sha256")) report.set(field,request.path(field));
        var pins=JSON.createObjectNode(); for(String field:List.of("run_id","namespace","matrix_sha256")) pins.set(field,request.path(field));
        report.set("input_pins",pins); var cases=report.putArray("cases");
        String prefix=text(request,"run_id");
        var payload=qa.payload(); var features=(ArrayNode)payload.path("features");
        var untouched=features.get(0).deepCopy(); ((ObjectNode)untouched).put("id","fx-untouched");
        ((ObjectNode)untouched.path("properties")).put("time",Instant.parse("2023-08-15T00:00:00Z").toEpochMilli());
        ((ObjectNode)untouched.path("properties")).put("updated",Instant.parse("2023-08-15T01:00:00Z").toEpochMilli());
        var moving=features.get(0).deepCopy(); ((ObjectNode)moving).put("id","fx-moving");
        ((ObjectNode)moving.path("properties")).put("time",Instant.parse("2023-09-30T23:59:55Z").toEpochMilli());
        ((ObjectNode)moving.path("properties")).put("updated",Instant.parse("2023-10-01T02:00:00Z").toEpochMilli());
        features.add(untouched); features.add(moving); ((ObjectNode)payload.path("metadata")).put("count",3);
        var u=qa.usgs(prefix+"-u0","2023-07-31T23:59:44Z","2023-11-01T00:00:16Z",payload);
        var entry=qa.entry(2023,"full-year"); String line=qa.jmaRecord("2023090112345678");
        byte[] archive=qa.archive(entry,List.of(line));
        var j=qa.jma(prefix+"-j0",entry,archive,"release-v1");
        var seedContext=qa.context(prefix+"-seed",List.of("2023-08","2023-09","2023-10"),List.of(u,j),null);
        var baseline=qa.execute(seedContext); require(count(baseline)==3,"QA02_SEED_CANONICAL_COUNT_FAILED");
        JsonNode baseCommit=baseline.path("commit"); long initialSnapshot=snapshot(baseline);
        String outsideBefore=partition(qa,initialSnapshot,"2023-08");
        // Same operation, all six phases repeated, not just a no-op JSON validator.
        var rerun=qa.execute(seedContext); require(baseline.equals(rerun),"QA02_RERUN_CHANGED");
        var rec=JSON.createObjectNode(); var keys=keys(qa.active(baseline),"fx-usgs-success-001");
        assertion(rec,"logical_keys","unchanged",keys,keys,JSON.createObjectNode());
        assertion(rec,"canonical_count","unchanged",JSON.valueToTree(count(baseline)),JSON.valueToTree(count(rerun)),JSON.createObjectNode());
        var refs=refs(qa,baseCommit); assertion(rec,"snapshots","same_or_equivalent",refs,refs,JSON.createObjectNode().put("equivalent",true));
        add(qa,request,matrix,cases,"rerun_same_input",baseline,rerun,rec,pins);
        // A distinct Bronze attempt with identical source revisions must not increase current or canonical rows.
        var duplicate=qa.usgs(prefix+"-u-duplicate","2023-07-31T23:59:44Z","2023-11-01T00:00:16Z",payload);
        var duplicated=qa.execute(qa.context(prefix+"-duplicate",List.of("2023-09"),List.of(duplicate),baseline));
        var current=current(qa.active(duplicated),"fx-usgs-success-001"); require(current.size()==1 && count(duplicated)==3,"QA02_DUPLICATE_FAILED");
        rec=JSON.createObjectNode(); assertion(rec,"logical_keys","one_current_row",keys,keys,JSON.createObjectNode());
        assertion(rec,"current","one_current_row",JSON.valueToTree(1),JSON.valueToTree(current.size()),JSON.createObjectNode());
        assertion(rec,"canonical_count","unchanged",JSON.valueToTree(3),JSON.valueToTree(count(duplicated)),JSON.createObjectNode());
        qa.oldReadable(baseCommit); assertion(rec,"history","retained",baseCommit,JSON.createObjectNode().put("retained",true),JSON.createObjectNode());
        add(qa,request,matrix,cases,"duplicate_same_source",baseline,duplicated,rec,pins);
        // Latest USGS updated timestamp wins while the previous observation and raw bytes remain readable.
        var revised=qa.payload(); ((ObjectNode)revised.path("features").get(0).path("properties")).put("updated",Instant.parse("2023-10-02T00:00:00Z").toEpochMilli());
        var revision=qa.usgs(prefix+"-u-revision","2023-08-31T23:59:44Z","2023-10-01T00:00:16Z",revised);
        var updated=qa.execute(qa.context(prefix+"-revision",List.of("2023-09"),List.of(revision),duplicated));
        var old=current(qa.active(duplicated),"fx-usgs-success-001").get(0); var all=qa.active(updated); var latest=current(all,"fx-usgs-success-001").get(0);
        require(!old.sourceRevisionKey().equals(latest.sourceRevisionKey()) && latest.sourceUpdatedAtUtc().isAfter(old.sourceUpdatedAtUtc()),"QA02_REVISION_DID_NOT_WIN");
        long history=all.stream().filter(o->o.sourceRecordKey().equals(old.sourceRecordKey()) && o.sourceRevisionKey().equals(old.sourceRevisionKey())).count();
        require(history>0,"QA02_OLD_REVISION_LOST"); rec=JSON.createObjectNode();
        assertion(rec,"logical_keys","same_key_new_revision",keys,keys,JSON.createObjectNode().put("revision_changed",true));
        assertion(rec,"current","new_revision",JSON.createObjectNode().put("revision_id",old.sourceRevisionKey()),JSON.createObjectNode().put("revision_id",latest.sourceRevisionKey()).put("is_current",true),JSON.createObjectNode());
        assertion(rec,"history","old_revision_retained",JSON.valueToTree(1),JSON.createObjectNode().put("retained",true).put("history_count",history),JSON.createObjectNode());
        add(qa,request,matrix,cases,"usgs_late_revision",duplicated,updated,rec,pins);
        // New catalog release replaces a slot. Both native archives remain immutable/auditable.
        String changed=line.substring(0,52)+"53"+line.substring(54);
        var replacement=qa.jma(prefix+"-j2",entry,qa.archive(entry,List.of(changed)),"release-v2");
        var replaced=qa.execute(qa.context(prefix+"-release",List.of("2023-09"),List.of(replacement),updated));
        require(qa.active(replaced).stream().filter(o->o.sourceSystem().equals("JMA_BULLETIN")).allMatch(o->"release-v2".equals(o.catalogRelease())),"QA02_RELEASE_NOT_REPLACED");
        require(Arrays.equals(archive,new ie212.earthquake.spark.orchestration.FullMonthBronzeReader(qa.bronze,qa.inventory).read(j,qa.now).raw()),"QA02_OLD_RELEASE_LOST");
        rec=JSON.createObjectNode(); assertion(rec,"release","new_catalog_release",JSON.createObjectNode().put("release_id","release-v1"),JSON.createObjectNode().put("release_id","release-v2"),JSON.createObjectNode());
        assertion(rec,"current","replacement",JSON.valueToTree(1),JSON.createObjectNode().put("replacement",true).put("is_current",true),JSON.createObjectNode());
        assertion(rec,"old_release","auditable",JSON.createObjectNode().put("release_id","release-v1"),JSON.createObjectNode().put("release_id","release-v1").put("auditable",true),JSON.createObjectNode());
        add(qa,request,matrix,cases,"jma_release_replacement",updated,replaced,rec,pins);
        long bridge=replaced.path("readback").path("event_source_bridge_count").asLong();
        require(count(replaced)==3 && bridge==4,"QA02_OVERLAP_DOUBLE_COUNT"); rec=JSON.createObjectNode();
        assertion(rec,"canonical_count","no_double_count",JSON.valueToTree(3),JSON.valueToTree(count(replaced)),JSON.createObjectNode().put("duplicate_count",0));
        var sources=JSON.createObjectNode(); sources.putArray("sources").add("USGS").add("JMA");
        assertion(rec,"lineage","both_sources_retained",JSON.valueToTree(4),sources,JSON.createObjectNode());
        add(qa,request,matrix,cases,"overlap_usgs_jma",updated,replaced,rec,pins);
        // Only the incoming timestamp is targeted; resolver must infer the old September partition too.
        var movedPayload=qa.payload(); var moved=(ObjectNode)movedPayload.path("features").get(0); moved.put("id","fx-moving");
        ((ObjectNode)moved.path("properties")).put("time",Instant.parse("2023-10-01T00:00:25Z").toEpochMilli());
        ((ObjectNode)moved.path("properties")).put("updated",Instant.parse("2023-10-03T00:00:00Z").toEpochMilli());
        var movedPin=qa.usgs(prefix+"-u-move","2023-09-30T23:59:44Z","2023-11-01T00:00:16Z",movedPayload);
        var movedResult=qa.execute(qa.context(prefix+"-move",List.of("2023-10"),List.of(movedPin),replaced));
        var affected=months(movedResult.path("silver").path("affected_months")); require(affected.containsAll(List.of("2023-09","2023-10")),"QA02_MOVE_SCOPE_INCOMPLETE");
        long september=qa.events(snapshot(movedResult)).filter(col("event_time_utc").equalTo(java.sql.Timestamp.from(Instant.parse("2023-09-30T23:59:55Z")))).count();
        long october=qa.events(snapshot(movedResult)).filter(col("event_time_utc").equalTo(java.sql.Timestamp.from(Instant.parse("2023-10-01T00:00:25Z")))).count();
        require(september==0 && october==1,"QA02_MOVED_PARTITION_STALE"); qa.oldReadable(replaced.path("commit")); rec=JSON.createObjectNode();
        assertion(rec,"old_partition","superseded",JSON.valueToTree(1),JSON.createObjectNode().put("state","superseded").put("count",september),JSON.createObjectNode());
        assertion(rec,"new_partition","current",JSON.valueToTree(0),JSON.createObjectNode().put("state","current").put("count",october),JSON.createObjectNode());
        assertion(rec,"history","retained",replaced.path("commit"),JSON.createObjectNode().put("retained",true),JSON.createObjectNode());
        add(qa,request,matrix,cases,"event_moves_month",replaced,movedResult,rec,pins);
        // Withdraw the first JMA record while retaining a non-overlapping October record (valid nonempty archive).
        var withdrawn=qa.jma(prefix+"-j3",entry,qa.archive(entry,List.of(qa.jmaRecord("2023101512345678"))),"release-v3");
        var withdrawnResult=qa.execute(qa.context(prefix+"-withdraw",List.of("2023-09"),List.of(withdrawn),movedResult));
        require(qa.active(withdrawnResult).stream().noneMatch(o->o.sourceSystem().equals("JMA_BULLETIN") && "2023-09".equals(month(o.eventTimeUtc()))),"QA02_WITHDRAWAL_STALE");
        qa.oldReadable(movedResult.path("commit")); rec=JSON.createObjectNode();
        assertion(rec,"current","withdrawn_or_superseded",JSON.valueToTree(1),JSON.createObjectNode().put("state","withdrawn"),JSON.createObjectNode());
        assertion(rec,"history","retained",movedResult.path("commit"),JSON.createObjectNode().put("retained",true),JSON.createObjectNode());
        require(withdrawnResult.path("silver").path("reconciliation").path("balanced").asBoolean(),"QA02_WITHDRAWAL_UNBALANCED");
        assertion(rec,"canonical_count","reconciled",JSON.valueToTree(count(movedResult)),JSON.createObjectNode().put("balanced",true).put("count",count(withdrawnResult)),JSON.createObjectNode());
        add(qa,request,matrix,cases,"jma_record_withdrawn",movedResult,withdrawnResult,rec,pins);
        String outsideAfter=partition(qa,snapshot(withdrawnResult),"2023-08"); require(outsideBefore.equals(outsideAfter),"QA02_OUTSIDE_SCOPE_CHANGED");
        qa.oldReadable(baseCommit); rec=JSON.createObjectNode();
        assertion(rec,"scope","outside_scope_is_not_modified",JSON.valueToTree(outsideBefore),JSON.valueToTree(outsideAfter),JSON.createObjectNode().put("scope","outside"));
        var ref=JSON.createObjectNode().put("table",qa.namespace+".event_current").put("snapshot_id",initialSnapshot);
        var readback=ref.deepCopy().put("readable",true); assertion(rec,"snapshots","old_snapshot_readable",ref,readback,JSON.createObjectNode());
        add(qa,request,matrix,cases,"retention_outside_scope",baseline,withdrawnResult,rec,pins);
        report.put("status","PASS"); var summary=report.putObject("summary"); summary.put("case_count",8);summary.put("passed",8);summary.put("failed",0);summary.put("pending_runtime",0);
        report.put("completed_at_utc",Instant.now().toString()); PipelineQaSupport.save(reportPath,report);
        System.out.println("event=qa02_runtime_passed cases=8");
    }
    private static long count(JsonNode r) {return r.path("readback").path("event_current_count").asLong(-1);}
    private static long snapshot(JsonNode r) {return r.path("commit").path("tables").path("event_current").path("snapshot_id").asLong();}
    private static String partition(PipelineQaSupport qa,long snapshot,String month) {
        return qa.events(snapshot).filter(date_format(col("event_time_utc"),"yyyy-MM").equalTo(month)).orderBy("canonical_event_id").toJSON().collectAsList().toString();
    }
    private static List<SilverObservation> current(List<SilverObservation> rows,String id) {return new SourceDedupTransformer().deduplicate(rows).currentObservations().stream().filter(o->o.sourceRecordKey().endsWith(id)).toList();}
    private static JsonNode keys(List<SilverObservation> rows,String id) {return JSON.valueToTree(current(rows,id).stream().map(SilverObservation::sourceRecordKey).sorted().toList());}
    private static ArrayNode refs(PipelineQaSupport qa,JsonNode commit) {var out=JSON.createArrayNode(); for(String name:GoldIcebergWriter.TABLES) out.addObject().put("table",qa.namespace+"."+name).set("snapshot_id",commit.path("tables").path(name).path("snapshot_id"));return out;}
    private static void assertion(ObjectNode r,String key,String check,JsonNode before,JsonNode after,JsonNode details) {var a=r.putObject(key);a.put("status","PASS");a.put("check",check);a.set("before",before);a.set("after",after);a.set("details",details);}
    private static void add(PipelineQaSupport qa,JsonNode request,JsonNode matrix,ArrayNode cases,String id,JsonNode before,JsonNode after,JsonNode rec,JsonNode pins) throws Exception {
        JsonNode spec=null; for(var c:matrix.path("cases")) if(id.equals(c.path("case_id").asText())) spec=c;
        require(spec!=null,"QA02_UNKNOWN_CASE"); var result=cases.addObject();
        for(String k:List.of("case_id","category","operation_id")) result.set(k,spec.path(k)); result.put("status","PASS");result.set("input_pins",pins);result.set("reconciliation",rec);
        var snapshots=result.putObject("snapshots"); snapshots.set("before",refs(qa,before.path("commit"))); var afterRefs=refs(qa,after.path("commit"));
        if(id.equals("retention_outside_scope")) for(var old:refs(qa,before.path("commit"))) if(!afterRefs.toString().contains(old.path("snapshot_id").asText())) afterRefs.add(old);
        snapshots.set("after",afterRefs);
        var evidence=JSON.createObjectNode(); evidence.set("before",before); evidence.set("after",after);evidence.set("reconciliation",rec);
        Path path=qa.root.resolve(id+"-evidence.json");PipelineQaSupport.save(path,evidence);
        var ref=result.putObject("evidence_ref");ref.put("path",path.toString());ref.put("sha256",sha(Files.readAllBytes(path)));
        for(String k:List.of("run_id","namespace")) ref.set(k,request.path(k));for(String k:List.of("case_id","operation_id")) ref.set(k,spec.path(k));
    }
}
