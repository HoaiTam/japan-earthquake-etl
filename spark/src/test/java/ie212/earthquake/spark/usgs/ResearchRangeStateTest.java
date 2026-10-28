package ie212.earthquake.spark.usgs;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import static org.junit.jupiter.api.Assertions.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ResearchRangeStateTest {
    @TempDir Path temp;
    static ObjectNode request(String owner) {
        var r=ResearchCatalogStateTest.request();r.remove("trino_capacity");
        r.put("version",ResearchRangeState.VERSION).put("run_id","dat03-range-"+"a".repeat(32))
            .put("namespace","iceberg.dat03_range_"+"a".repeat(32)).put("owner",owner)
            .put("input_manifest_sha256","b".repeat(64)).put("catalog_controller_sha256","b".repeat(64));
        r.put("start_month",owner.equals("ThanhTris")?"2013-08":owner.equals("rosy179")?"2018-10":"2008-06")
            .put("end_month_exclusive",owner.equals("ThanhTris")?"2018-10":owner.equals("rosy179")?"2024-01":"2013-08");
        return r;
    }
    @Test void ownersHaveDisjointFrozenIntervals() throws Exception {
        var seen=new HashSet<String>();
        for(String owner:List.of("HoaiTam","ThanhTris","rosy179")) {
            var r=request(owner);ResearchRangeState.validate(r);
            var months=ResearchRangeState.months(r);
            assertEquals(owner.equals("rosy179")?63:62,months.size());
            for(String month:months)assertTrue(seen.add(month));
        }
        assertEquals(187,seen.size());
    }
    @Test void cannotUseOwnerCatalogNamespaceOrChangeAssignment() {
        for(var r:List.of(request("ThanhTris").put("start_month","2000-01"),request("rosy179").put("end_month_exclusive","2024-02"),
                request("ThanhTris").put("namespace","iceberg.gold"),request("unknown"),request("rosy179").put("research_ready",true)))
            assertThrows(Exception.class,()->ResearchRangeState.validate(r));
        assertThrows(Exception.class,()->ResearchCatalogState.validate(request("ThanhTris")));
        assertThrows(Exception.class,()->ResearchRangeState.validate(ResearchCatalogStateTest.request()));
    }
    @Test void exactAssignedMonthsOnlyAndVersionedOwnLeaseContext() throws Exception {
        var r=request("ThanhTris");var pins=JSON.createArrayNode();pins.addObject().put("manifest_uri","s3://bucket/pinned");
        var ctx=ResearchRangeState.context(r,"2013-08",pins,null,ResearchCatalogStateTest.pins(0));
        assertEquals("dat03_range_worker",ctx.path("dag_id").asText());
        assertEquals("2013-08-01T00:00:00Z",ctx.path("window_start_utc").asText());
        assertEquals(pins,ctx.path("resolver_request").path("bronze_inputs"));
        for(String month:List.of("2013-07","2018-10","2000-01"))
            assertThrows(Exception.class,()->ResearchRangeState.context(r,month,pins,null,ResearchCatalogStateTest.pins(0)));
        assertThrows(Exception.class,()->ResearchRangeState.context(r,List.of("2018-09","2018-10"),pins,null,ResearchCatalogStateTest.pins(0),"b".repeat(64)));
    }
    @Test void checkpointStartsAtAssignedMonthAndRejectsGapsTampering() throws Exception {
        var root=temp.toRealPath();var r=request("rosy179");var folder=root.resolve("months/2018-10");Files.createDirectories(folder);
        ResearchRangeState.save(folder.resolve("record.json"),JSON.createObjectNode().put("fixture",true));
        var p=JSON.createObjectNode().put("version",ResearchRangeState.VERSION).put("month","2018-10").put("status","PUBLISHED")
            .put("request_identity_sha256",digest(r)).put("record_sha256",sha(Files.readAllBytes(folder.resolve("record.json"))));
        p.putNull("previous_checkpoint_sha256");p.set("snapshots",JSON.valueToTree(ResearchCatalogStateTest.pins(1)));
        ResearchRangeState.save(folder.resolve("checkpoint.json"),p);
        assertEquals(1,ResearchRangeState.completed(root,r).size());
        Files.createDirectories(root.resolve("months/2018-12"));ResearchRangeState.save(root.resolve("months/2018-12/checkpoint.json"),p);
        assertThrows(Exception.class,()->ResearchRangeState.completed(root,r));
        assertThrows(Exception.class,()->ResearchRangeState.save(folder.resolve("record.json"),JSON.createObjectNode()));
    }
    @Test void rangeCompletionNeverClaimsFullCatalogOrOfficialExport() throws Exception {
        var r=request("rosy179");var p=ResearchRangeState.progress(r,Collections.nCopies(63,JSON.createObjectNode()));
        assertEquals("RANGE_PUBLISHED",p.path("status").asText());assertEquals(63,p.path("required_months").asInt());
        for(String flag:List.of("full_research_catalog","official_exported","research_ready"))assertFalse(p.path(flag).asBoolean());
    }
    @Test void missingUsgsAndBoundaryAreRejected() {
        for(int index:List.of(24,25,36)) {
            var r=request("rosy179");((com.fasterxml.jackson.databind.node.ArrayNode)r.path("sources")).remove(index);
            assertThrows(Exception.class,()->ResearchRangeState.validate(r));
        }
    }
}
