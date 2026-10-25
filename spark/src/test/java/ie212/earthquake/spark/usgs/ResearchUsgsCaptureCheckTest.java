package ie212.earthquake.spark.usgs;

import static org.junit.jupiter.api.Assertions.*;
import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;

class ResearchUsgsCaptureCheckTest {
    String url(String month) {
        Instant start=YearMonth.parse(month).atDay(1).atStartOfDay(ZoneOffset.UTC).toInstant().minusSeconds(16);
        if(start.isBefore(Instant.parse("2023-01-01T00:00:00Z"))) start=Instant.parse("2023-01-01T00:00:00Z");
        Instant end=YearMonth.parse(month).plusMonths(1).atDay(1).atStartOfDay(ZoneOffset.UTC).toInstant().plusSeconds(16).minusMillis(1);
        var query=new LinkedHashMap<String,String>();
        query.put("format","geojson");query.put("eventtype","earthquake");query.put("starttime",start.toString());query.put("endtime",end.toString());
        query.put("minlatitude","20.0");query.put("maxlatitude","50.0");query.put("minlongitude","120.0");query.put("maxlongitude","155.0");
        query.put("orderby","time-asc");query.put("limit","20000");query.put("offset","1");
        return "https://earthquake.usgs.gov/fdsnws/event/1/query?"+String.join("&",query.entrySet().stream()
                .map(e->e.getKey()+"="+URLEncoder.encode(e.getValue(),StandardCharsets.UTF_8)).toList());
    }
    ObjectNode payload(String month,int count) {
        var p=JSON.createObjectNode().put("type","FeatureCollection");
        p.putObject("metadata").put("url",url(month)).put("status",200).put("count",count);
        var features=p.putArray("features");for(int i=0;i<count;i++)features.addObject().put("type","Feature");return p;
    }
    ObjectNode capture(String month,byte[] raw) throws Exception {
        return JSON.createObjectNode().put("month",month).put("url",url(month)).put("http_status",200)
                .put("content_type","application/json; charset=utf-8").put("sha256",sha(raw)).put("bytes",raw.length)
                .put("retrieved_at_utc","2026-10-24T12:00:00Z");
    }
    @Test void allTwelveMonthsRequireOriginalUnfilteredResponseAndDoNotClaimCoverage() throws Exception {
        for(int n=1;n<=12;n++) {
            String month=String.format(Locale.ROOT,"2023-%02d",n); byte[] raw=JSON.writeValueAsBytes(payload(month,1));
            var result=ResearchUsgsCaptureCheck.check(capture(month,raw),raw);
            assertEquals(1,result.path("feature_count").asInt()); assertFalse(result.path("bronze_ready").asBoolean(true));
            assertFalse(result.path("coverage_verified").asBoolean(true));assertFalse(result.path("research_ready").asBoolean(true));
        }
    }
    @Test void januaryClampsSeedButDecemberRetainsNextYearHalo() throws Exception {
        byte[] jan=JSON.writeValueAsBytes(payload("2023-01",0));
        assertEquals("2023-01-01T00:00:00Z",ResearchUsgsCaptureCheck.check(capture("2023-01",jan),jan).path("window_start_utc").asText());
        byte[] dec=JSON.writeValueAsBytes(payload("2023-12",0));
        assertEquals("2024-01-01T00:00:16Z",ResearchUsgsCaptureCheck.check(capture("2023-12",dec),dec).path("window_end_utc_exclusive").asText());
    }
    @Test void emptyValidResponseIsNotMissingSource() throws Exception {
        byte[] raw=JSON.writeValueAsBytes(payload("2023-02",0));
        assertEquals(0,ResearchUsgsCaptureCheck.check(capture("2023-02",raw),raw).path("feature_count").asInt());
    }
    @Test void payloadTamperAndWrongHttpTypeOrFutureRetrievalAreRejected() throws Exception {
        byte[] raw=JSON.writeValueAsBytes(payload("2023-02",1));
        for(String field:List.of("sha256","http_status","bytes","content_type","retrieved_at_utc")) {
            var c=capture("2023-02",raw);
            switch(field) {case "sha256"->c.put(field,"0".repeat(64));case "http_status"->c.put(field,true);
                case "bytes"->c.put(field,raw.length+1);case "content_type"->c.put(field,"text/html");
                default->c.put(field,"2999-01-01T00:00:00Z");}
            assertThrows(Exception.class,()->ResearchUsgsCaptureCheck.check(c,raw),field);
        }
    }
    @Test void filteredPaginatedDuplicateOrForeignQueryCannotBeCompletenessInput() throws Exception {
        byte[] raw=JSON.writeValueAsBytes(payload("2023-02",0));
        for(String url:List.of(url("2023-02")+"&minmagnitude=3",url("2023-02").replace("offset=1","offset=2"),
                url("2023-02")+"&limit=20000",url("2023-02").replace("earthquake.usgs.gov","example.invalid"),
                url("2023-02").replace("20.0","21.0"))) {
            var c=capture("2023-02",raw).put("url",url);
            assertThrows(Exception.class,()->ResearchUsgsCaptureCheck.check(c,raw));
        }
    }
    @Test void responseCountStatusAndEchoedQueryAreCheckedIndependentlyOfCaptureMetadata() throws Exception {
        for(String field:List.of("count","status","url")) {
            var p=payload("2023-02",1);var meta=(ObjectNode)p.path("metadata");
            if(field.equals("count"))meta.put(field,2);else if(field.equals("status"))meta.put(field,500);else meta.put(field,url("2023-03"));
            byte[] raw=JSON.writeValueAsBytes(p);
            assertThrows(Exception.class,()->ResearchUsgsCaptureCheck.check(capture("2023-02",raw),raw));
        }
    }
    @Test void saturatedEnvelopeIsRejectedNotSilentlyUsedAsFullMonth() throws Exception {
        byte[] raw=JSON.writeValueAsBytes(payload("2023-02",20000));
        assertThrows(Exception.class,()->ResearchUsgsCaptureCheck.check(capture("2023-02",raw),raw));
    }
    @Test void catalogMayOmitCountButExplicitNullWrongCountOrPagingIsNeverAccepted() throws Exception {
        var p=payload("2023-02",1);var meta=(ObjectNode)p.path("metadata");meta.remove("count");meta.put("limit",20000).put("offset",1);
        byte[] raw=JSON.writeValueAsBytes(p);var result=ResearchUsgsCaptureCheck.check(capture("2023-02",raw),raw);
        assertEquals(1,result.path("feature_count").asInt());assertFalse(result.path("response_count_field_present").asBoolean(true));
        meta.putNull("count");byte[] nullCount=JSON.writeValueAsBytes(p);
        assertThrows(Exception.class,()->ResearchUsgsCaptureCheck.check(capture("2023-02",nullCount),nullCount));
        meta.remove("count");meta.put("offset",2);byte[] nextPage=JSON.writeValueAsBytes(p);
        assertThrows(Exception.class,()->ResearchUsgsCaptureCheck.check(capture("2023-02",nextPage),nextPage));
    }
    @Test void oversizedIntegersCannotWrapIntoValidHttpByteCountOrPagingValues() throws Exception {
        for(String field:List.of("http_status","bytes","status","count","limit","offset")) {
            var p=payload("2023-02",1);var meta=(ObjectNode)p.path("metadata");meta.put("limit",20000).put("offset",1);
            if(!Set.of("http_status","bytes").contains(field)) {
                long expected=switch(field) {case "status"->200;case "limit"->20000;default->1;};
                meta.put(field,java.math.BigInteger.ONE.shiftLeft(64).add(java.math.BigInteger.valueOf(expected)));
            }
            byte[] raw=JSON.writeValueAsBytes(p);var c=capture("2023-02",raw);
            if(Set.of("http_status","bytes").contains(field))
                c.put(field,java.math.BigInteger.ONE.shiftLeft(64).add(java.math.BigInteger.valueOf(field.equals("bytes")?raw.length:200)));
            assertThrows(Exception.class,()->ResearchUsgsCaptureCheck.check(c,raw),field);
        }
    }
}
