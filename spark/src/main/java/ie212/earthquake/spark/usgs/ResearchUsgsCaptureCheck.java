package ie212.earthquake.spark.usgs;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.net.URI;
import java.nio.file.*;
import java.time.*;
import java.util.*;

/** Bounded original-response validation only; no Bronze, Spark, quality or coverage publication. */
public final class ResearchUsgsCaptureCheck {
    public static final String VERSION="dat03-usgs-capture-check-v1";
    private ResearchUsgsCaptureCheck() { }
    // Capture checking deliberately runs without Spark/Parquet dependencies.
    private static String rawSha(byte[] raw) throws Exception {
        return HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(raw));
    }
    public static ObjectNode check(JsonNode capture,byte[] raw) throws Exception {
        String month=text(capture,"month");
        require(month.matches("2023-(0[1-9]|1[0-2])"),"RESEARCH_USGS_MONTH_REQUIRED");
        var start=YearMonth.parse(month).atDay(1).atStartOfDay(ZoneOffset.UTC).toInstant().minusSeconds(16);
        if(start.isBefore(Instant.parse("2023-01-01T00:00:00Z"))) start=Instant.parse("2023-01-01T00:00:00Z");
        var end=YearMonth.parse(month).plusMonths(1).atDay(1).atStartOfDay(ZoneOffset.UTC).toInstant().plusSeconds(16);
        var expected=new TreeMap<>(Map.ofEntries(Map.entry("format","geojson"),Map.entry("eventtype","earthquake"),
                Map.entry("starttime",start.toString()),Map.entry("endtime",end.minusMillis(1).toString()),
                Map.entry("minlatitude","20.0"),Map.entry("maxlatitude","50.0"),Map.entry("minlongitude","120.0"),
                Map.entry("maxlongitude","155.0"),Map.entry("orderby","time-asc"),Map.entry("limit","20000"),Map.entry("offset","1")));
        var uri=URI.create(text(capture,"url"));
        require("https".equals(uri.getScheme()) && "earthquake.usgs.gov".equals(uri.getHost()) && uri.getPort()==-1
                && uri.getUserInfo()==null && uri.getFragment()==null && "/fdsnws/event/1/query".equals(uri.getPath())
                && FullMonthAcceptanceJob.query(uri).equals(expected),"RESEARCH_USGS_EXACT_QUERY_REQUIRED");
        require(capture.path("http_status").isIntegralNumber() && capture.path("http_status").canConvertToInt() && capture.path("http_status").asInt()==200
                && Set.of("application/json","application/geo+json").contains(text(capture,"content_type").split(";")[0].trim()),
                "RESEARCH_USGS_HTTP_REQUIRED");
        require(capture.path("bytes").isIntegralNumber() && capture.path("bytes").canConvertToLong() && capture.path("bytes").asLong()==raw.length && raw.length>0
                && raw.length<=16*1024*1024 && rawSha(raw).equals(text(capture,"sha256")),"RESEARCH_USGS_RAW_PIN_CHANGED");
        Instant retrieved=utc(capture,"retrieved_at_utc");
        require(!end.isAfter(retrieved) && !retrieved.isAfter(Instant.now()),"RESEARCH_USGS_RETRIEVAL_INVALID");
        var checked=new UsgsGeoJsonValidator().validate(raw);
        require(checked.valid() && checked.featureCount()<20000,"RESEARCH_USGS_INVALID_OR_SATURATED");
        var payload=JSON.readTree(raw); var meta=payload.path("metadata");
        // FDSN catalog queries may omit metadata.count (unlike summary feeds).
        // The actual validated features-array length is always authoritative.
        require((!meta.has("count") || (meta.path("count").isIntegralNumber() && meta.path("count").canConvertToLong() && meta.path("count").asLong()==checked.featureCount()))
                && meta.path("status").isIntegralNumber() && meta.path("status").canConvertToInt() && meta.path("status").asInt()==200,
                "RESEARCH_USGS_RESPONSE_COUNT_MISMATCH");
        if(meta.has("limit") || meta.has("offset")) require(meta.path("limit").isIntegralNumber() && meta.path("limit").canConvertToInt() && meta.path("limit").asInt()==20000
                && meta.path("offset").isIntegralNumber() && meta.path("offset").canConvertToInt() && meta.path("offset").asInt()==1,"RESEARCH_USGS_RESPONSE_PAGING_MISMATCH");
        var origin=URI.create(text(meta,"url"));
        require(uri.getScheme().equals(origin.getScheme()) && uri.getHost().equals(origin.getHost()) && origin.getPort()==-1
                && origin.getUserInfo()==null && origin.getFragment()==null && uri.getPath().equals(origin.getPath())
                && FullMonthAcceptanceJob.query(origin).equals(expected),"RESEARCH_USGS_RESPONSE_QUERY_MISMATCH");
        return JSON.createObjectNode().put("version",VERSION).put("month",month).put("status","CAPTURE_STRUCTURE_VERIFIED")
                .put("raw_sha256",rawSha(raw)).put("feature_count",checked.featureCount())
                .put("response_count_field_present",meta.has("count"))
                .put("window_start_utc",start.toString()).put("window_end_utc_exclusive",end.toString())
                .put("bronze_ready",false).put("coverage_verified",false).put("research_ready",false);
    }
    public static void main(String[] args) throws Exception {
        require(args.length==2,"RESEARCH_USGS_CAPTURE_AND_RAW_REQUIRED");
        Path meta=Path.of(args[0]).toAbsolutePath(), raw=Path.of(args[1]).toAbsolutePath();
        for(Path p:List.of(meta,raw)) for(Path q=p;q!=null;q=q.getParent())
            require(!Files.isSymbolicLink(q),"RESEARCH_USGS_SYMLINK_REJECTED");
        require(Files.isRegularFile(meta,LinkOption.NOFOLLOW_LINKS) && Files.size(meta)<=65536
                && Files.isRegularFile(raw,LinkOption.NOFOLLOW_LINKS) && Files.size(raw)<=16*1024*1024,"RESEARCH_USGS_FILE_BOUND");
        System.out.println(JSON.writeValueAsString(check(JSON.readTree(Files.readAllBytes(meta)),Files.readAllBytes(raw))));
    }
}
