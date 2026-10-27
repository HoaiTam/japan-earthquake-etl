package ie212.earthquake.spark.usgs;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import static org.junit.jupiter.api.Assertions.*;
import ie212.earthquake.spark.gold.*;
import ie212.earthquake.spark.silver.*;
import java.nio.file.*;
import java.util.*;
import java.util.zip.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ResearchCatalogRecoveryJobTest {
    @TempDir Path root;
    void jar(Path path,Map<String,String> entries) throws Exception {
        try(var out=new ZipOutputStream(Files.newOutputStream(path))) {
            for(var e:entries.entrySet()){out.putNextEntry(new ZipEntry(e.getKey()));out.write(e.getValue().getBytes());out.closeEntry();}
        }
    }
    @Test void onlyOneRecoveryClassMayBeAddedAndAllOriginalBytesMustMatch() throws Exception {
        Path old=root.resolve("old.jar"),next=root.resolve("new.jar");jar(old,Map.of("engine.class","same","client.class","same"));
        jar(next,Map.of("engine.class","same","client.class","same",ResearchCatalogRecoveryJob.ENTRY,"new"));
        ResearchCatalogRecoveryJob.compatible(old,next);
        jar(next,Map.of("engine.class","same","client.class","changed",ResearchCatalogRecoveryJob.ENTRY,"new"));
        assertThrows(Exception.class,()->ResearchCatalogRecoveryJob.compatible(old,next));
        jar(next,Map.of("engine.class","same","client.class","same",ResearchCatalogRecoveryJob.ENTRY,"new","extra.class","new"));
        assertThrows(Exception.class,()->ResearchCatalogRecoveryJob.compatible(old,next));
    }
    @Test void metadataScalarCannotAcceptBooleanWrongCountOrForeignLease() throws Exception {
        for(String value:List.of("true","2","null","\"1\"")) {
            var row=JSON.readTree("["+value+"]");var checks=JSON.createObjectNode();
            assertThrows(Exception.class,()->ResearchCatalogRecoveryJob.scalar(q->new GoldPublisher.Result(List.of(),List.of(row)),()->{},checks,"pin","SELECT metadata",1));
            assertTrue(checks.isEmpty());
        }
        assertThrows(IllegalStateException.class,()->ResearchCatalogRecoveryJob.scalar(q->{throw new AssertionError("must not query");},
                ()->{throw new IllegalStateException("foreign");},JSON.createObjectNode(),"pin","SELECT metadata",1));
    }
    @Test void explicitV2ProfileChangesOnlyTransportAndListsDistinctFailedSessions() throws Exception {
        Path old=root.resolve("base.jar"),next=root.resolve("profile.jar");
        jar(old,Map.of(ResearchCatalogRecoveryJob.TRANSPORT,"old","publisher.class","same"));
        jar(next,Map.of(ResearchCatalogRecoveryJob.TRANSPORT,"new","publisher.class","same",ResearchCatalogRecoveryJob.ENTRY,"recovery"));
        assertThrows(Exception.class,()->ResearchCatalogRecoveryJob.compatible(old,next));
        ResearchCatalogRecoveryJob.compatible(old,next,TrinoSqlClient.RESEARCH_READ_PROFILE);
        assertThrows(Exception.class,()->ResearchCatalogRecoveryJob.compatible(old,next,"unbounded"));
        jar(next,Map.of(ResearchCatalogRecoveryJob.TRANSPORT,"new","publisher.class","changed",ResearchCatalogRecoveryJob.ENTRY,"recovery"));
        assertThrows(Exception.class,()->ResearchCatalogRecoveryJob.compatible(old,next,TrinoSqlClient.RESEARCH_READ_PROFILE));
        var request=JSON.createObjectNode().put("failed_session","a".repeat(32));request.putArray("related_failed_sessions").add("b".repeat(32));
        assertEquals(List.of("a".repeat(32),"b".repeat(32)),ResearchCatalogRecoveryJob.resolvedFailures(request));
        request.withArray("related_failed_sessions").add("a".repeat(32));
        assertThrows(Exception.class,()->ResearchCatalogRecoveryJob.resolvedFailures(request));
    }
    @Test void receiptsMatchRealAdapterProtocolAndAreImmutable() throws Exception {
        var store=MinioSilverObjectStore.inMemory("test","silver");var ctx=JSON.createObjectNode().put("operation_id","operation").put("context_sha256","a".repeat(64));
        var prior=JSON.createObjectNode().put("phase","gold");var artifact=JSON.createObjectNode().put("verify_status","PASSED").put("published",false);
        var bundle=artifact.putObject("snapshot_bundle");for(String t:GoldIcebergWriter.TABLES)bundle.putObject(t).put("snapshot_id",9007199254740997L);
        var first=ResearchCatalogRecoveryJob.receipt(store,ctx,"verify",prior,artifact,()->{});
        assertEquals(digest(prior),first.path("upstream_sha256").asText());assertEquals(digest(artifact),first.path("artifact_sha256").asText());
        assertEquals(9007199254740997L,first.path("snapshots").path("event_current").asLong());
        assertEquals(first,ResearchCatalogRecoveryJob.receipt(store,ctx,"verify",prior,artifact,()->{}));
        artifact.put("changed",true);assertThrows(Exception.class,()->ResearchCatalogRecoveryJob.receipt(store,ctx,"verify",prior,artifact,()->{}));
    }
}
