package ie212.earthquake.spark.usgs;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import static org.junit.jupiter.api.Assertions.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.gold.*;
import ie212.earthquake.spark.orchestration.*;
import ie212.earthquake.spark.silver.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ResearchRecoveryPreflightTest {
    @TempDir Path root;
    final String run="dat03-"+"a".repeat(32), ns="iceberg.dat03_e2e_"+"a".repeat(32);
    final Map<String,byte[]> objects=new HashMap<>();
    final AtomicInteger queries=new AtomicInteger();
    ObjectNode request;
    SilverObjectStore readonly=new SilverObjectStore() {
        public byte[] read(String key) {return Objects.requireNonNull(objects.get(key),key);}
        public boolean exists(String key) {return objects.containsKey(key);}
        public String uriForKey(String key) {return "s3://test/silver/"+key;}
        public void put(String k,byte[] b,String t) {throw new AssertionError("write forbidden");}
        public List<String> list(String p) {throw new AssertionError("list forbidden");}
        public void delete(String k) {throw new AssertionError("delete forbidden");}
        public void deletePrefix(String p) {throw new AssertionError("delete forbidden");}
        public void move(String a,String b) {throw new AssertionError("move forbidden");}
    };
    void object(String key,JsonNode value) throws Exception {objects.put(key,JSON.writeValueAsBytes(value));}
    void local(Path path,JsonNode value) throws Exception {Files.createDirectories(path.getParent());Files.write(path,JSON.writeValueAsBytes(value));}
    void fixture() throws Exception {
        root=root.toRealPath(); // macOS /var is a system symlink; test the canonical private temp root.
        request=JSON.createObjectNode().put("version",ResearchBoundedE2EJob.VERSION).put("namespace",ns).put("run_id",run)
                .put("runner_sha256","a".repeat(64)).put("baseline_inventory_sha256","b".repeat(64))
                .put("boundary_inventory_sha256","b".repeat(64)).put("usgs_capture_sha256","b".repeat(64));
        request.putObject("lease_owner").put("dag_id","dat03_bounded_e2e").put("run_id",run);
        var captures=request.putArray("jma_captures");
        for(int y:List.of(2000,2011,2016,2023,2024)) captures.addObject().put("year",y).put("sha256","b".repeat(64)).put("capture_sha256","b".repeat(64));
        var baseline=JSON.createObjectNode();for(String t:GoldIcebergWriter.TABLES)baseline.put(t,0L);
        ObjectNode parent=null; int index=0;
        for(String month:List.of("2000-01","2011-04","2016-04")) {
            String op=RealEtlAdapter.operation("dat03_bounded_e2e",run+"-"+month);++index;
            var r=JSON.createObjectNode().put("resolver_version","orc-06-v2").put("run_id",op).put("dag_id","dat03_bounded_e2e")
                    .put("namespace",ns).put("config_version",ResearchBoundedE2EJob.VERSION).put("code_version","jar-"+"a".repeat(16))
                    .put("source_cutoff_utc","2026-10-24T00:00:00Z").put("processed_at_utc","2026-10-24T00:00:00Z");
            r.set("target_months",JSON.valueToTree(List.of(month)));r.set("baseline_snapshots",baseline.deepCopy());
            if(parent==null)r.putNull("baseline");else r.set("baseline",parent);
            r.putArray("bronze_inputs").addObject().put("source_system","JMA_BULLETIN").put("year",Integer.parseInt(month.substring(0,4)))
                    .put("segment","full-year").put("sha256","b".repeat(64)).put("catalog_release","fixture-v1");
            var c=JSON.createObjectNode().put("contract_version","orc-07-v1").put("dag_id","dat03_bounded_e2e").put("run_id",run+"-"+month)
                    .put("operation_id",op).put("mode","real").put("trigger","manual")
                    .put("window_start_utc",start(month).toString()).put("window_end_utc",end(month).toString());
            c.putNull("source_summary_sha256");c.set("resolver_request",r);c.put("context_sha256",digest(c));
            local(root.resolve(op+"/context.json"),c);object("real-etl/"+op+"/context.json",c);
            local(root.resolve("inputs/h"+month.substring(0,4)+"-capture.json"),JSON.createObjectNode().put("sha256","b".repeat(64)).put("catalog_release","fixture-v1"));
            var commit=JSON.createObjectNode().put("namespace",ns).put("gold_run_id",op).put("status","COMMITTED")
                    .put("published",false).put("identity_sha256","b".repeat(64));
            commit.putObject("scope").set("baseline_snapshots",baseline.deepCopy());commit.withObject("scope").set("affected_months",JSON.valueToTree(List.of(month)));
            var tables=commit.putObject("tables");
            for(String t:GoldIcebergWriter.TABLES) {tables.putObject(t).put("snapshot_id",index*100L).put("count",1L).put("table",ns+"."+t);baseline.put(t,index*100L);}
            object("gold-commits/"+op+"/commit.json",commit);
            var record=JSON.createObjectNode();record.set("context",c);record.set("commit",commit);var receipts=record.putArray("receipts");
            JsonNode previous=null;
            for(String phase:RealEtlAdapter.PHASES.subList(0,index==3?4:6)) {
                JsonNode artifact="gold".equals(phase)?commit:JSON.createObjectNode().put("fixture",phase);
                var receipt=JSON.createObjectNode().put("contract_version","orc-07-v1").put("mode","real").put("status","ok")
                        .put("phase",phase).put("operation_id",op).put("context_sha256",text(c,"context_sha256"))
                        .put("artifact_uri",readonly.uriForKey("real-etl/"+op+"/"+phase+".json")).put("artifact_sha256",digest(artifact));
                if(previous==null)receipt.putNull("upstream_sha256");else receipt.put("upstream_sha256",digest(previous));
                if(List.of("gold","verify","publish").contains(phase))receipt.set("snapshots",baseline.deepCopy());
                receipts.add(receipt);previous=receipt;
                object("real-etl/"+op+"/"+phase+"-receipt.json",receipt);object("real-etl/"+op+"/"+phase+".json",artifact);
                local(root.resolve(op+"/"+phase+".json"),artifact);
            }
            if(index<3) {
                var progress=JSON.createObjectNode().put("month",month).put("rerun_identical",true).put("retention_verified",true);
                progress.set("first",record);progress.set("rerun",record);local(root.resolve("progress-"+month+".json"),progress);
            }
            object(FullMonthStateResolver.root(op)+"/registry.json",JSON.createObjectNode().put("fixture",month));
            parent=JSON.createObjectNode().put("run_id",op).put("registry_sha256",sha(readonly.read(FullMonthStateResolver.root(op)+"/registry.json")))
                    .put("commit_key","gold-commits/"+op+"/commit.json").put("commit_sha256",sha(readonly.read("gold-commits/"+op+"/commit.json")));
        }
    }
    GoldPublisher.Sql sql(long current) {return q->{queries.incrementAndGet();assertTrue(q.startsWith("SELECT "));
        return new GoldPublisher.Result(List.of(),List.of(JSON.valueToTree(List.of(q.contains("$refs")?current:q.contains("publication_status")?0L:1L))));};}
    @Test void thirteenMetadataQueriesProducePlanWithoutWritesOrAcceptance() throws Exception {
        fixture();var out=ResearchRecoveryPreflight.inspect(request,root,readonly,sql(300),()->{});
        assertEquals(13,queries.get());assertEquals("READY_FOR_PUBLICATION_RECOVERY",out.path("status").asText());
        for(String flag:List.of("recovered","acceptance_passed","research_ready","can_release_lease"))assertFalse(out.path(flag).asBoolean(true));
        assertEquals(List.of("verify","publish"),JSON.convertValue(out.path("next_phases"),List.class));
        assertFalse(Files.exists(root.resolve("plan.json")));assertFalse(Files.exists(root.resolve("acceptance.json")));
    }
    @Test void changedLiveSnapshotFailsClosed() throws Exception {
        fixture();assertEquals("DAT03_RECOVERY_LIVE_PIN_CHANGED",assertThrows(Exception.class,
                ()->ResearchRecoveryPreflight.inspect(request,root,readonly,sql(301),()->{})).getMessage());
    }
    @Test void foreignLeasePreventsAllQueries() throws Exception {
        fixture();assertThrows(IllegalStateException.class,()->ResearchRecoveryPreflight.inspect(request,root,readonly,sql(300),()->{throw new IllegalStateException("foreign lease");}));
        assertEquals(0,queries.get());
    }
    @Test void changedReceiptChainIsRejectedBeforeSql() throws Exception {
        fixture();String op=RealEtlAdapter.operation("dat03_bounded_e2e",run+"-2016-04");
        var receipt=(ObjectNode)JSON.readTree(readonly.read("real-etl/"+op+"/gold-receipt.json"));receipt.put("upstream_sha256","e".repeat(64));
        object("real-etl/"+op+"/gold-receipt.json",receipt);
        assertThrows(Exception.class,()->ResearchRecoveryPreflight.inspect(request,root,readonly,sql(300),()->{}));assertEquals(0,queries.get());
    }
    @Test void unknownLaterPhaseOrPublicationCannotBeOverwritten() throws Exception {
        fixture();String op=RealEtlAdapter.operation("dat03_bounded_e2e",run+"-2016-04");
        object("real-etl/"+op+"/verify.json",JSON.createObjectNode().put("unknown",true));
        assertEquals("DAT03_RECOVERY_UNKNOWN_LATER_PHASE",assertThrows(Exception.class,
                ()->ResearchRecoveryPreflight.inspect(request,root,readonly,sql(300),()->{})).getMessage());assertEquals(0,queries.get());
    }
    @Test void changedContextOrOriginalCodeVersionCannotBeRepinned() throws Exception {
        fixture();String op=RealEtlAdapter.operation("dat03_bounded_e2e",run+"-2016-04");
        var context=(ObjectNode)JSON.readTree(Files.readAllBytes(root.resolve(op+"/context.json")));
        ((ObjectNode)context.path("resolver_request")).put("code_version","jar-new-recovery-code");
        local(root.resolve(op+"/context.json"),context);
        assertThrows(Exception.class,()->ResearchRecoveryPreflight.inspect(request,root,readonly,sql(300),()->{}));assertEquals(0,queries.get());
    }
}
