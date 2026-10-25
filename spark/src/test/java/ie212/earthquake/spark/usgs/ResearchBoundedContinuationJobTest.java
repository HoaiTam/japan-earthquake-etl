package ie212.earthquake.spark.usgs;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import static org.junit.jupiter.api.Assertions.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.silver.*;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.zip.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ResearchBoundedContinuationJobTest {
    @TempDir Path root;
    void jar(Path file,Map<String,String> entries) throws Exception {
        try(var out=new ZipOutputStream(Files.newOutputStream(file))) {
            for(var e:entries.entrySet()){out.putNextEntry(new ZipEntry(e.getKey()));out.write(e.getValue().getBytes(java.nio.charset.StandardCharsets.UTF_8));out.closeEntry();}
        }
    }
    @Test void unchangedCoreAndResourceWithOnlyTransportFixAreCompatible() throws Exception {
        Path a=root.resolve("old.jar"),b=root.resolve("new.jar");
        jar(a,Map.of("engine.class","same",ResearchBoundedContinuationJob.TRANSPORT,"old","schema.json","same"));
        jar(b,Map.of("engine.class","same",ResearchBoundedContinuationJob.TRANSPORT,"new","schema.json","same","Recovery.class","added"));
        var proof=ResearchBoundedContinuationJob.compatibility(a,b);assertEquals(2,proof.path("matched_original_entries").asInt());assertEquals(1,proof.path("allowed_differences").size());
    }
    @Test void changedCoreResourceOrMissingEntryCannotReuseOriginalContext() throws Exception {
        Path a=root.resolve("old.jar"),b=root.resolve("new.jar");jar(a,Map.of("engine.class","same",ResearchBoundedContinuationJob.TRANSPORT,"old"));
        jar(b,Map.of("engine.class","changed",ResearchBoundedContinuationJob.TRANSPORT,"new"));
        assertEquals("DAT03_RECOVERY_ETL_CODE_CHANGED",assertThrows(Exception.class,()->ResearchBoundedContinuationJob.compatibility(a,b)).getMessage());
        jar(b,Map.of(ResearchBoundedContinuationJob.TRANSPORT,"new"));
        assertEquals("DAT03_RECOVERY_CODE_ENTRY_MISSING",assertThrows(Exception.class,()->ResearchBoundedContinuationJob.compatibility(a,b)).getMessage());
    }
    ResearchRecoveryPreflightTest fixture() throws Exception {
        var f=new ResearchRecoveryPreflightTest();f.root=root;f.fixture();root=f.root;
        String op=ie212.earthquake.spark.orchestration.RealEtlAdapter.operation("dat03_bounded_e2e",f.run+"-2016-04");
        var silver=JSON.createObjectNode().put("scope_completeness","COMPLETE_MONTHS");silver.putObject("reconciliation").put("balanced",true);
        f.object("real-etl/"+op+"/silver.json",silver);
        var receipt=(ObjectNode)JSON.readTree(f.objects.get("real-etl/"+op+"/silver-receipt.json"));receipt.put("artifact_sha256",digest(silver));
        f.object("real-etl/"+op+"/silver-receipt.json",receipt);
        var gold=(ObjectNode)JSON.readTree(f.objects.get("real-etl/"+op+"/gold-receipt.json"));gold.put("upstream_sha256",digest(receipt));f.object("real-etl/"+op+"/gold-receipt.json",gold);
        return f;
    }
    JsonNode context(ResearchRecoveryPreflightTest f) throws Exception {
        String op=ie212.earthquake.spark.orchestration.RealEtlAdapter.operation("dat03_bounded_e2e",f.run+"-2016-04");
        return JSON.readTree(Files.readAllBytes(root.resolve(op+"/context.json")));
    }
    @Test void resumesOnlyVerifyPublishAndRerunIsIdentical() throws Exception {
        var f=fixture();var context=context(f);String op=text(context,"operation_id");var called=new ArrayList<String>();
        // In-memory fake server: the helper itself must not execute the four committed phases.
        var store=MinioSilverObjectStore.inMemory("test","silver");for(var e:f.objects.entrySet())store.put(e.getKey(),e.getValue(),"application/json");
        var unchanged=new HashMap<String,byte[]>();for(String phase:List.of("readiness","bronze","silver","gold"))
            unchanged.put(phase,store.read("real-etl/"+op+"/"+phase+"-receipt.json"));
        ResearchBoundedContinuationJob.Phase adapter=(phase,ctx,upstream,work)->{
            called.add(phase);assertTrue(List.of("verify","publish").contains(phase));
            var artifact=JSON.createObjectNode().put("verify_status","PASSED").put("published",phase.equals("publish"));
            var checks=artifact.putObject("checks");for(int i=0;i<36;i++)checks.putObject("fixture_"+i).put("actual",0).put("expected",0);
            String key="real-etl/"+op+"/"+phase+".json";store.put(key,JSON.writeValueAsBytes(artifact),"application/json");
            var receipt=JSON.createObjectNode().put("phase",phase).put("operation_id",op).put("context_sha256",text(ctx,"context_sha256"))
                    .put("upstream_sha256",digest(upstream)).put("artifact_uri",store.uriForKey(key)).put("artifact_sha256",digest(artifact));
            if(phase.equals("publish"))receipt.put("published",true);
            store.put("real-etl/"+op+"/"+phase+"-receipt.json",JSON.writeValueAsBytes(receipt),"application/json");return receipt;
        };
        var first=ResearchBoundedContinuationJob.publicationOnly(adapter,store,context,root.resolve("attempt"),()->{});
        var rerun=ResearchBoundedContinuationJob.publicationOnly(adapter,store,context,root.resolve("attempt"),()->{});
        assertEquals(first,rerun);assertEquals(List.of("verify","publish","verify","publish"),called);
        for(var e:unchanged.entrySet())assertArrayEquals(e.getValue(),store.read("real-etl/"+op+"/"+e.getKey()+"-receipt.json"));
    }
    @Test void unknownVerifyOutcomeDoesNotPublishOrProduceCompletedRecord() throws Exception {
        var f=fixture();var ctx=context(f);var called=new ArrayList<String>();
        assertThrows(IOException.class,()->ResearchBoundedContinuationJob.publicationOnly((phase,c,u,p)->{
            called.add(phase);throw new IOException("TRINO_QUERY_OUTCOME_UNKNOWN");},f.readonly,ctx,root.resolve("attempt"),()->{}));
        assertEquals(List.of("verify"),called);assertFalse(Files.exists(root.resolve("attempt/"+text(ctx,"operation_id")+"/record.json")));
    }
    @Test void changedUpstreamOrForeignLeaseCannotReachPublication() throws Exception {
        var f=fixture();var ctx=context(f);String op=text(ctx,"operation_id");
        var changed=(ObjectNode)JSON.readTree(f.objects.get("real-etl/"+op+"/gold-receipt.json"));changed.put("upstream_sha256","e".repeat(64));
        f.object("real-etl/"+op+"/gold-receipt.json",changed);
        assertThrows(IOException.class,()->ResearchBoundedContinuationJob.publicationOnly((a,b,c,d)->{throw new AssertionError("must not call");},f.readonly,ctx,root.resolve("attempt"),()->{}));
        assertThrows(IllegalStateException.class,()->ResearchBoundedContinuationJob.publicationOnly((a,b,c,d)->{throw new AssertionError("must not call");},f.readonly,ctx,root.resolve("foreign"),()->{throw new IllegalStateException("foreign lease");}));
    }
    @Test void checkpointRequiresExactVersionRootAndFileInventoryBeforeAnySql() throws Exception {
        var f=fixture();var config=JSON.createObjectNode();var cp=config.putObject("checkpoint");
        cp.put("version","dat03-2023-publication-checkpoint-v1").put("root","/opt/pipeline/staging/dat03-continuation/"+"a".repeat(32));
        cp.putObject("files_sha256").put("../outside","a".repeat(64));
        assertEquals("DAT03_CHECKPOINT_EXACT_FILES_REQUIRED",assertThrows(Exception.class,()->ResearchBoundedContinuationJob.checkpoint(config,
                JSON.createObjectNode(),root,f.readonly,q->{throw new AssertionError("SQL forbidden");},()->{})).getMessage());
        cp.put("root","/opt/pipeline/staging/dat03-continuation/../elsewhere");
        assertEquals("DAT03_CHECKPOINT_EXACT_ROOT_REQUIRED",assertThrows(Exception.class,()->ResearchBoundedContinuationJob.checkpoint(config,
                JSON.createObjectNode(),root,f.readonly,q->{throw new AssertionError("SQL forbidden");},()->{})).getMessage());
    }
    @Test void checkpointMetadataSqlRequiresExactIntegralScalarAndLease() throws Exception {
        var checks=JSON.createObjectNode();
        ResearchBoundedContinuationJob.checked(q->new ie212.earthquake.spark.gold.GoldPublisher.Result(List.of(),List.of(JSON.readTree("[42]"))),()->{},checks,"pin","SELECT 42",42);
        assertEquals(42,checks.path("pin").path("actual").asLong());
        for(String row:List.of("[41]","[\"42\"]","[null]","[42,42]","[9223372036854775808]"))
            assertThrows(Exception.class,()->ResearchBoundedContinuationJob.checked(q->new ie212.earthquake.spark.gold.GoldPublisher.Result(List.of(),List.of(JSON.readTree(row))),()->{},checks,"pin","SELECT 42",42));
        assertThrows(IllegalStateException.class,()->ResearchBoundedContinuationJob.checked(q->{throw new AssertionError("SQL forbidden");},()->{throw new IllegalStateException("foreign lease");},checks,"pin","SELECT 42",42));
    }
}
