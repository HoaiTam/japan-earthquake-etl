package ie212.earthquake.spark.usgs;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import static ie212.earthquake.spark.usgs.ResearchCatalogState.*;
import static ie212.earthquake.spark.usgs.ResearchCatalogState.VERSION;
import static ie212.earthquake.spark.usgs.ResearchCatalogState.validate;
import static org.junit.jupiter.api.Assertions.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.gold.GoldIcebergWriter;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

class ResearchCatalogStateTest {
    @TempDir Path root;
    @BeforeEach void realTemporaryRoot() throws Exception {root=root.toRealPath();}
    static ObjectNode request() {
        var r=JSON.createObjectNode().put("version",VERSION).put("run_id","dat03-catalog-"+"a".repeat(32))
                .put("namespace","iceberg.dat03_catalog_"+"a".repeat(32)).put("source_cutoff_utc","2026-10-24T00:00:00Z");
        for(String k:List.of("runner_sha256","controller_sha256","support_sha256","baseline_inventory_sha256","boundary_inventory_sha256","plan_sha256","usgs_summary_sha256"))r.put(k,"b".repeat(64));
        var sources=r.putArray("sources");for(int y=2000;y<=2024;y++)source(sources.addObject(),"h"+y);
        for(int m=1;m<=12;m++)source(sources.addObject(),String.format(Locale.ROOT,"usgs-2023-%02d",m));
        var capacity=r.putArray("trino_capacity");for(String t:List.of("event_current","event_source_bridge"))capacity.addObject().put("table",t).put("request_sha256","c".repeat(64)).put("receipt_sha256","d".repeat(64));
        return r;
    }
    static void source(ObjectNode n,String name) {n.put("name",name).put("raw_sha256","c".repeat(64)).put("metadata_sha256","d".repeat(64)).put("bytes",10);}
    static Map<String,Long> pins(long n) {var p=new TreeMap<String,Long>();GoldIcebergWriter.TABLES.forEach(t->p.put(t,n));return p;}
    @Test void exact288MonthsAnd22563SplitHaveNoOverlapOrGap() {
        assertEquals(288,MONTHS.size());assertEquals("2000-01",MONTHS.get(0));assertEquals("2018-09",MONTHS.get(224));
        assertEquals("2018-10",MONTHS.get(225));assertEquals("2023-12",MONTHS.get(287));assertEquals(288,new HashSet<>(MONTHS).size());
    }
    @Test void rejectsUnknownFieldsWrongNamespaceMissingBoundaryDuplicateSourceAndLossySize() throws Exception {
        validate(request());var unknown=request().put("research_ready",true);assertThrows(Exception.class,()->validate(unknown));
        var shared=request().put("namespace","iceberg.gold");assertThrows(Exception.class,()->validate(shared));
        for(String name:List.of("h2023","h1999")) {var r=request();((ObjectNode)r.path("sources").get(24)).put("name",name);assertThrows(Exception.class,()->validate(r));}
        var loss=request();((ObjectNode)loss.path("sources").get(0)).put("bytes",10.0);assertThrows(Exception.class,()->validate(loss));
    }
    @Test void bothCapacityTablesAndTypedPinsAreRequired() {
        var r=request();((ObjectNode)r.path("trino_capacity").get(1)).put("table","event_current");assertThrows(Exception.class,()->validate(r));
        var bad=request().put("runner_sha256",true);assertThrows(Exception.class,()->validate(bad));
    }
    @Test void contextBootstrapsExactSourcesOnceThenUsesPinnedParentWithoutLatest() throws Exception {
        var r=request();var inputs=JSON.createArrayNode();inputs.addObject().put("manifest_uri","s3://bucket/pinned");
        var first=context(r,"2000-01",inputs,null,pins(0));
        assertEquals(inputs,first.path("resolver_request").path("bronze_inputs"));
        assertEquals("orc-06-v2",first.path("resolver_request").path("resolver_version").asText());
        var parent=JSON.createObjectNode().put("run_id",first.path("operation_id").asText()).put("registry_sha256","c".repeat(64))
                .put("commit_key","gold-commits/"+first.path("operation_id").asText()+"/commit.json").put("commit_sha256","d".repeat(64));
        var next=context(r,"2000-02",inputs,parent,pins(9007199254740993L));
        assertTrue(next.path("resolver_request").path("bronze_inputs").isEmpty());
        assertEquals(9007199254740993L,next.path("resolver_request").path("baseline_snapshots").path("event_current").asLong());
        assertEquals(first.path("resolver_request").path("source_cutoff_utc"),next.path("resolver_request").path("source_cutoff_utc"));
        assertThrows(Exception.class,()->context(r,"2024-01",inputs,parent,pins(1)));
    }
    @Test void checkpointsAreImmutableSymlinkSafeAndRequireAContiguousChain() throws Exception {
        var r=request();Files.createDirectories(root.resolve("months/2000-01"));Path file=root.resolve("months/2000-01/record.json");save(file,JSON.createObjectNode().put("fixture",true));
        var c=JSON.createObjectNode().put("version",VERSION).put("month","2000-01").put("status","PUBLISHED").put("request_identity_sha256",digest(r))
                .put("record_sha256",sha(Files.readAllBytes(file)));c.putNull("previous_checkpoint_sha256");c.set("snapshots",JSON.valueToTree(pins(1)));
        save(root.resolve("months/2000-01/checkpoint.json"),c);assertEquals(1,completed(root,r).size());
        assertThrows(Exception.class,()->save(file,JSON.createObjectNode().put("changed",true)));
        Files.createDirectories(root.resolve("months/2000-03"));save(root.resolve("months/2000-03/checkpoint.json"),c);
        assertThrows(Exception.class,()->completed(root,r));
        Path link=root.resolve("link.json");Files.createSymbolicLink(link,file);assertThrows(Exception.class,()->read(link,65536));
    }
    @Test void sourceAndCheckpointIdentityCannotChangeOnResume() throws Exception {
        var a=request();var b=request().put("source_cutoff_utc","2026-10-23T00:00:00Z");assertNotEquals(digest(a),digest(b));
        var inputs=JSON.createArrayNode();inputs.addObject().put("manifest_uri","exact");
        assertNotEquals(context(a,"2000-01",inputs,null,pins(0)),context(b,"2000-01",inputs,null,pins(0)));
    }
    @Test void phaseBudgetAndProgressCannotClaimOfficialExportOrResearchReadiness() throws Exception {
        assertFalse(mayStartPhase(1199));assertTrue(mayStartPhase(1200));
        var p=progress(request(),List.of());assertEquals("CHECKPOINTED",p.path("status").asText());
        assertFalse(p.path("full_research_catalog").asBoolean());assertFalse(p.path("official_exported").asBoolean());assertFalse(p.path("research_ready").asBoolean());
    }
}
