package ie212.earthquake.spark.usgs;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import static org.apache.spark.sql.functions.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.*;
import ie212.earthquake.spark.orchestration.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;

/** QA-03 storage evidence v1: original public captures plus explicitly synthetic release fault. */
public final class HistoricalBackfillAcceptanceJob {
    private HistoricalBackfillAcceptanceJob() { }
    public static void main(String[] args) throws Exception {
        require(args.length==1,"QA03_EXACT_REQUEST_REQUIRED");
        var request=JSON.readTree(Files.readAllBytes(Path.of(args[0]))); Path root=Path.of(text(request,"report_path")).getParent();
        try(var qa=new PipelineQaSupport(request,root)) {run(qa,request,root);}
    }
    private static void run(PipelineQaSupport qa,JsonNode request,Path root) throws Exception {
        Path captures=Path.of("/opt/pipeline/qa-history-captures");
        var capturePins=JSON.readTree(Files.readAllBytes(captures.resolve("capture-pins.json")));
        var pins=new TreeMap<String,JsonNode>();
        for(String name:List.of("h199701.zip","h199710.zip","h2000.zip","h2023.zip")) {
            byte[] raw=Files.readAllBytes(captures.resolve(name)); require(raw.length<32L*1024*1024 && sha(raw).equals(capturePins.path(name).asText()),"QA03_PUBLIC_CAPTURE_PIN_CHANGED");
            var entry=qa.inventory.stream().filter(e->e.archiveName().equals(name)).findFirst().orElseThrow();
            pins.put(name,qa.jma(text(request,"run_id")+"-"+name.replace(".zip",""),entry,raw,"release-v1"));
        }
        byte[] usgsRaw=Files.readAllBytes(captures.resolve("usgs-2023-09.geojson"));
        require(sha(usgsRaw).equals(capturePins.path("usgs-2023-09.geojson").asText()),"QA03_USGS_CAPTURE_PIN_CHANGED");
        var captured=(ObjectNode)JSON.readTree(usgsRaw);
        require(FullMonthAcceptanceJob.query(java.net.URI.create(captured.path("metadata").path("url").asText()))
            .equals(FullMonthAcceptanceJob.query(java.net.URI.create(FullMonthAcceptanceJob.QUERY))),"QA03_USGS_CAPTURE_SCOPE_CHANGED");
        var usgs=qa.usgsBytes(text(request,"run_id")+"-public-usgs","2023-08-31T23:59:44Z","2023-10-01T00:00:16Z",usgsRaw);
        var report=JSON.createObjectNode(); report.put("task","QA-03"); report.put("schema_version","qa03-storage-acceptance-v1");
        report.put("namespace",qa.namespace);report.put("run_id",text(request,"run_id"));report.put("backend","MinIO/REST/Iceberg/Trino");
        report.put("matrix_sha256",text(request,"matrix_sha256"));
        report.put("capture_pins_sha256",sha(Files.readAllBytes(captures.resolve("capture-pins.json"))));
        report.put("original_public_captures",true);report.put("controlled_release_is_synthetic",true);report.put("research_ready",false);
        report.put("source_cutoff_utc",qa.now.toString());report.set("capture_sha256",capturePins);
        report.set("bronze_inputs",JSON.valueToTree(pins));report.set("usgs_bronze_input",usgs);
        report.put("inventory_sha256",sha(Files.readAllBytes(Path.of("/opt/pipeline/config/jma/hypocenter_archives_v1.csv"))));
        require(text(report,"inventory_sha256").equals(text(request,"inventory_sha256"))
            && text(report,"capture_pins_sha256").equals(text(request,"capture_pins_sha256")),"QA03_INPUT_PINS_CHANGED");
        var records=report.putArray("publications"); var checks=report.putObject("checks");
        var evidence=report.putObject("case_evidence");
        // Retain a positive month in a DIFFERENT release slot. A new 1997 release
        // legitimately changes revision lineage for every registered month in that slot.
        var august=qa.execute(qa.context(text(request,"run_id")+"-retained",List.of("2000-02"),List.of(pins.get("h2000.zip")),null));
        records.add(august); JsonNode parent=august;
        long augustPin=august.path("commit").path("tables").path("event_current").path("snapshot_id").asLong();
        String retained=partition(qa,augustPin,"2000-02");require(!retained.equals("[]"),"QA03_POSITIVE_RETENTION_BASELINE_REQUIRED");
        ObjectNode resumeContext=null; JsonNode resumeSilver=null; JsonNode resumed=null;
        for(String month:List.of("1997-09","1997-10","2000-01","2023-09")) {
            List<JsonNode> input=switch(month) {
                case "1997-09" -> List.of(pins.get("h199701.zip"),pins.get("h199710.zip"));
                case "1997-10" -> List.of(pins.get("h199710.zip"));
                case "2000-01" -> List.of(pins.get("h2000.zip"));
                default -> List.of(pins.get("h2023.zip"),usgs);
            };
            var context=qa.context(text(request,"run_id")+"-"+month,List.of(month),input,parent);
            if(month.equals("1997-09")) {
                // Controlled stop after durable Silver, before any Gold commit; preserve all evidence.
                JsonNode prior=null; var before=qa.writer.baselines(); Path work=root.resolve(text(context,"operation_id"));Files.createDirectories(work);
                var completed=JSON.createArrayNode();
                for(String phase:List.of("readiness","bronze","silver")) {
                    prior=qa.adapter.execute(phase,context,prior,work);completed.add(prior);
                    PipelineQaSupport.save(work.resolve(phase+".json"),qa.artifact(prior));
                }
                require(before.equals(qa.writer.baselines()),"QA03_SILVER_STOP_CHANGED_GOLD");
                boolean failed=false;
                try { failAfterSilver(); }
                catch(java.io.IOException error) {require("QA_STOP_AFTER_DURABLE_SILVER".equals(error.getMessage()),"QA03_UNEXPECTED_STOP");failed=true;}
                require(failed,"QA03_ERROR_NOT_INJECTED");
                var stop=JSON.createObjectNode().put("reason","QA_STOP_AFTER_DURABLE_SILVER").put("downstream_blocked",true)
                    .put("exception_class","java.io.IOException").put("process_restart",false);
                stop.set("context",context);stop.set("completed_receipts",completed);stop.set("gold_baselines",JSON.valueToTree(before));
                PipelineQaSupport.save(root.resolve("resume-stop.json"),stop);evidence.set("resume_after_failure",stop);
                resumeContext=context;resumeSilver=prior;
            }
            var output=qa.execute(context); require(output.path("silver").path("reconciliation").path("balanced").asBoolean(),"QA03_COUNTS_UNBALANCED");
            records.add(output);parent=output;
            if(month.equals("1997-09")) resumed=output;
            qa.oldReadable(august.path("commit"));
            require(retained.equals(partition(qa,output.path("commit").path("tables").path("event_current").path("snapshot_id").asLong(),"2000-02")),"QA03_RETENTION_CHANGED");
        }
        require(resumeSilver!=null && resumeContext!=null && resumed!=null,"QA03_RESUME_MISSING");
        require(resumeSilver.equals(resumed.path("receipts").get(2)),"QA03_RESUME_REPROCESSED_SILVER");
        var reused=qa.adapter.execute("bronze",resumeContext,resumed.path("receipts").get(0),root.resolve(text(resumeContext,"operation_id")));
        require(reused.equals(resumed.path("receipts").get(1)),"QA03_REUSE_CHANGED_VERIFIED_ARCHIVE");
        evidence.set("reuse_verified_archive",JSON.createObjectNode().put("operation_id",text(resumed,"operation_id"))
            .put("download_count",0).set("reused_receipt",reused));
        checks.put("resume_after_failure",true);checks.put("reuse_verified_archive",true);checks.put("retention_outside_scope",true);
        report.put("download_count_during_execution",0); // No HTTP download client exists in this fixed storage runner.
        // Mutate exactly one September native record for a controlled new release, not an alleged JMA correction.
        var nativeEntry=qa.entry(1997,"jan-sep"); byte[] original=Files.readAllBytes(captures.resolve("h199701.zip"));
        List<String> lines=new ArrayList<>(); boolean changed=false;
        try(var zip=new java.util.zip.ZipInputStream(new java.io.ByteArrayInputStream(original))) {
            require(zip.getNextEntry()!=null,"QA03_NATIVE_MEMBER_MISSING");
            var reader=new java.io.BufferedReader(new java.io.InputStreamReader(zip,java.nio.charset.StandardCharsets.ISO_8859_1));String line;
            while((line=reader.readLine())!=null) {
                Instant time=ie212.earthquake.spark.silver.JmaFixedWidthParser.originTimeOfVerifiedRecord(line.getBytes(java.nio.charset.StandardCharsets.ISO_8859_1));
                if(!changed && time!=null && month(time).equals("1997-09")) {line=line.substring(0,52)+(line.substring(52,54).equals("52")?"53":"52")+line.substring(54);changed=true;}
                lines.add(line);
            }
        }
        require(changed,"QA03_CHANGED_RELEASE_RECORD_MISSING"); byte[] modified=qa.archive(nativeEntry,lines);
        var newPin=qa.jma(text(request,"run_id")+"-changed",nativeEntry,modified,"release-v2");
        var changedResult=qa.execute(qa.context(text(request,"run_id")+"-changed-release",List.of("1997-09"),List.of(newPin),parent));
        qa.oldReadable(parent.path("commit"));require(!parent.path("commit").path("tables").path("event_current").path("snapshot_id").equals(changedResult.path("commit").path("tables").path("event_current").path("snapshot_id")),"QA03_RELEASE_DID_NOT_COMMIT");
        require(months(changedResult.path("silver").path("affected_months")).equals(Set.of("1997-09")),"QA03_RELEASE_SCOPE_EXPANDED");
        require(retained.equals(partition(qa,changedResult.path("commit").path("tables").path("event_current").path("snapshot_id").asLong(),"2000-02")),"QA03_CHANGED_RELEASE_RETENTION_FAILED");
        records.add(changedResult);checks.put("changed_release",true);
        var release=JSON.createObjectNode().put("before_operation_id",text(parent,"operation_id"))
            .put("after_operation_id",text(changedResult,"operation_id")).put("synthetic",true).put("old_snapshot_readback",true);
        release.set("old_pin",pins.get("h199701.zip"));release.set("new_pin",newPin);evidence.set("changed_release",release);
        var conflictPin=qa.jma(text(request,"run_id")+"-conflict",nativeEntry,original,"release-v2");
        var conflict=qa.context(text(request,"run_id")+"-same-release-different-bytes",List.of("1997-09"),List.of(conflictPin),changedResult);
        var before=qa.writer.baselines(); boolean rejected=false;
        try {new FullMonthStateResolver(qa.bronze,qa.silver,qa.inventory,qa.writer::baselines,qa.lease).resolve(qa.spark,conflict.path("resolver_request"));}
        catch(java.io.IOException e) {require("JMA_RELEASE_IDENTITY_OR_ORDER_CONFLICT".equals(e.getMessage()),"QA03_UNEXPECTED_RELEASE_REJECTION");rejected=true;}
        require(rejected && before.equals(qa.writer.baselines()),"QA03_SAME_RELEASE_BYTES_ACCEPTED");checks.put("same_release_different_bytes",true);
        var collision=JSON.createObjectNode().put("reason","JMA_RELEASE_IDENTITY_OR_ORDER_CONFLICT").put("downstream_blocked",true);
        collision.set("context",conflict);collision.set("baselines_before",JSON.valueToTree(before));
        collision.set("baselines_after",JSON.valueToTree(qa.writer.baselines()));evidence.set("same_release_different_bytes",collision);
        var oversized=qa.context(text(request,"run_id")+"-resource-guard",List.of("1997-09"),List.of(),changedResult);
        var bad=(ObjectNode)oversized.path("resolver_request");var months=bad.putArray("target_months");
        for(int i=0;i<25;i++) months.add(YearMonth.of(1997,1).plusMonths(i).toString()); boolean guarded=false;
        try {validate(bad);} catch(java.io.IOException e) {require("BOUNDED_EXACT_MONTHS_REQUIRED".equals(e.getMessage()),"QA03_UNEXPECTED_RESOURCE_REJECTION");guarded=true;}
        require(guarded && before.equals(qa.writer.baselines()),"QA03_RESOURCE_GUARD_FAILED");
        // Recovery reads the already published last operation; no catalog-wide download/reprocess.
        var recovered=qa.execute((ObjectNode)changedResult.path("context"));require(changedResult.equals(recovered),"QA03_RESOURCE_RECOVERY_CHANGED");
        var guard=JSON.createObjectNode().put("reason","BOUNDED_EXACT_MONTHS_REQUIRED").put("recovery_operation_id",text(recovered,"operation_id"));
        guard.set("resolver_request",bad);guard.set("baselines_before",JSON.valueToTree(before));
        guard.set("baselines_after",JSON.valueToTree(qa.writer.baselines()));evidence.set("resource_guard",guard);
        var retention=JSON.createObjectNode().put("month","2000-02").put("before_sha256",sha(retained.getBytes(java.nio.charset.StandardCharsets.UTF_8)))
            .put("after_sha256",sha(partition(qa,changedResult.path("commit").path("tables").path("event_current").path("snapshot_id").asLong(),"2000-02").getBytes(java.nio.charset.StandardCharsets.UTF_8)))
            .put("baseline_count",august.path("commit").path("tables").path("event_current").path("count").asLong()).put("old_snapshot_readback",true);
        retention.put("before_operation_id",text(august,"operation_id"));retention.put("after_operation_id",text(changedResult,"operation_id"));
        evidence.set("retention_outside_scope",retention);
        checks.put("resource_guard",true);report.put("measured_unit","one_closed_month_per_request");
        report.put("status","PASS");report.put("completed_at_utc",Instant.now().toString());
        report.set("summary",JSON.createObjectNode().put("case_count",6).put("passed",6).put("failed",0).put("pending_runtime",0));
        PipelineQaSupport.save(Path.of(text(request,"report_path")),report);
    }
    static void failAfterSilver() throws java.io.IOException {throw new java.io.IOException("QA_STOP_AFTER_DURABLE_SILVER");}
    private static String partition(PipelineQaSupport qa,long pin,String month) {
        return qa.events(pin).filter(date_format(col("event_time_utc"),"yyyy-MM").equalTo(month)).orderBy("canonical_event_id").toJSON().collectAsList().toString();
    }
}
