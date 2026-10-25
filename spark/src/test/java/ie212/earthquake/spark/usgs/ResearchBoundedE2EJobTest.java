package ie212.earthquake.spark.usgs;

import static org.junit.jupiter.api.Assertions.*;
import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import java.nio.file.*;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ResearchBoundedE2EJobTest {
    @TempDir Path root;
    static com.fasterxml.jackson.databind.node.ObjectNode request() {
        var r=JSON.createObjectNode().put("version",ResearchBoundedE2EJob.VERSION).put("run_id","dat03-"+"a".repeat(32))
                .put("namespace","iceberg.dat03_e2e_"+"a".repeat(32));
        r.putObject("lease_owner").put("dag_id","dat03_bounded_e2e").put("run_id",r.path("run_id").asText());
        for(String k:java.util.List.of("runner_sha256","baseline_inventory_sha256","boundary_inventory_sha256","usgs_capture_sha256")) r.put(k,"b".repeat(64));
        var a=r.putArray("jma_captures"); for(int year:new int[]{2000,2011,2016,2023,2024})
            a.addObject().put("year",year).put("sha256","c".repeat(64)).put("capture_sha256","d".repeat(64));
        return r;
    }
    @Test void exactYearSetNamespaceOwnerAndTypedRequestBeforeAnyWrites() throws Exception {
        ResearchBoundedE2EJob.validateRequest(request());
        var r=request(); r.put("extra",true); assertThrows(Exception.class,()->ResearchBoundedE2EJob.validateRequest(r));
        var duplicate=request(); ((com.fasterxml.jackson.databind.node.ObjectNode)duplicate.path("jma_captures").get(4)).put("year",2023);
        assertThrows(Exception.class,()->ResearchBoundedE2EJob.validateRequest(duplicate));
        var ns=request(); ns.put("namespace","iceberg.gold"); assertThrows(Exception.class,()->ResearchBoundedE2EJob.validateRequest(ns));
        var year=request(); ((com.fasterxml.jackson.databind.node.ObjectNode)year.path("jma_captures").get(0)).put("year",2000.0);
        assertThrows(Exception.class,()->ResearchBoundedE2EJob.validateRequest(year));
    }
    @Test void rawRequiresExactBoundedRegularFileAndExternalSha() throws Exception {
        Files.createDirectory(root.resolve("inputs")); Files.writeString(root.resolve("inputs/h2000.zip"),"zip");
        String hash=sha("zip".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        assertEquals(3,ResearchBoundedE2EJob.raw(root,"h2000.zip",hash,3).length);
        assertThrows(Exception.class,()->ResearchBoundedE2EJob.raw(root,"../h2000.zip",hash,3));
        assertThrows(Exception.class,()->ResearchBoundedE2EJob.raw(root,"h2000.zip","b".repeat(64),3));
        assertThrows(Exception.class,()->ResearchBoundedE2EJob.raw(root,"h2000.zip",hash,33L*1024*1024));
    }
    @Test void officialUSGSQueryRequiresWholeMonthHaloNoMagnitudeFilterAndHttpProvenance() throws Exception {
        String url="https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&eventtype=earthquake&starttime=2023-11-30T23%3A59%3A44Z"
                +"&endtime=2024-01-01T00%3A00%3A15.999Z&minlatitude=20.0&maxlatitude=50.0&minlongitude=120.0&maxlongitude=155.0&orderby=time-asc&limit=20000&offset=1";
        var c=JSON.createObjectNode().put("url",url).put("http_status",200).put("content_type","application/json; charset=utf-8")
                .put("retrieved_at_utc","2026-10-24T00:00:00Z"); Instant now=Instant.parse("2026-10-24T01:00:00Z");
        ResearchBoundedE2EJob.validateUsgs(c,now);
        c.put("url",url+"&minmagnitude=3"); assertThrows(Exception.class,()->ResearchBoundedE2EJob.validateUsgs(c,now));
        c.put("url",url.replace("earthquake.usgs.gov","example.org")); assertThrows(Exception.class,()->ResearchBoundedE2EJob.validateUsgs(c,now));
        c.put("url",url); c.put("http_status",201); assertThrows(Exception.class,()->ResearchBoundedE2EJob.validateUsgs(c,now));
    }
}
