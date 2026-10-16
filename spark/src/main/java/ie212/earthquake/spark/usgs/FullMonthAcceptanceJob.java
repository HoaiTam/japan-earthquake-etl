package ie212.earthquake.spark.usgs;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import ie212.earthquake.spark.gold.*;
import ie212.earthquake.spark.jma.*;
import ie212.earthquake.spark.orchestration.*;
import ie212.earthquake.spark.silver.*;
import java.net.*;
import java.net.http.HttpHeaders;
import java.nio.file.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import org.apache.spark.sql.SparkSession;

/** Opt-in bounded acceptance on actual public captures. File-backed QA, NOT MinIO/Gold publication. */
public final class FullMonthAcceptanceJob {
    public static final String QUERY="https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&eventtype=earthquake"
            +"&starttime=2023-08-31T23%3A59%3A44Z&endtime=2023-10-01T00%3A00%3A15.999Z"
            +"&minlatitude=20.0&maxlatitude=50.0&minlongitude=120.0&maxlongitude=155.0&orderby=time-asc&limit=20000&offset=1";
    private FullMonthAcceptanceJob() { }
    public static void main(String[] args) throws Exception {
        require(args.length==3,"USGS_CAPTURE_JMA_ARCHIVE_AND_SCOPED_ROOT_REQUIRED");
        Path usgsPath=Path.of(args[0]), jmaPath=Path.of(args[1]), root=Path.of(args[2]).toAbsolutePath();
        require(Files.size(usgsPath)<32L*1024*1024 && Files.size(jmaPath)<32L*1024*1024,"BOUNDED_PUBLIC_CAPTURES_REQUIRED");
        Files.createDirectories(root);
        byte[] usgsRaw=Files.readAllBytes(usgsPath), jmaRaw=Files.readAllBytes(jmaPath);
        var usgs=JSON.readTree(usgsRaw); URI origin=URI.create(usgs.path("metadata").path("url").asText());
        require("earthquake.usgs.gov".equals(origin.getHost()),"PUBLIC_USGS_CAPTURE_REQUIRED");
        require(query(origin).equals(query(URI.create(QUERY))),"PUBLIC_CAPTURE_QUERY_SCOPE_MISMATCH");
        Instant processed=Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS);
        String id="orc06-"+sha(usgsRaw).substring(0,12)+"-"+sha(jmaRaw).substring(0,12);
        Path requestPath=root.resolve("request.json");
        var bronze=new FileBronzeObjectStore(root.resolve("bronze")); var silver=new FileSilverObjectStore(root.resolve("silver"));
        var inventory=JmaArchiveInventory.read(Path.of("config/jma/hypocenter_archives_v1.csv"));
        if(!Files.exists(requestPath)) {
            var request=JSON.createObjectNode(); request.put("resolver_version",VERSION); request.put("run_id",id);
            request.put("dag_id","orc_06_bounded_qa"); request.put("namespace","iceberg.gold_qa_orc06_"+sha(usgsRaw).substring(0,8));
            request.put("config_version","orc-06-v1"); request.put("code_version","orc-06-v1");
            request.put("processed_at_utc",processed.toString()); request.put("source_cutoff_utc",processed.toString());
            request.putArray("target_months").add("2023-09"); request.putNull("baseline"); var snapshots=request.putObject("baseline_snapshots");
            GoldIcebergWriter.TABLES.forEach(t->snapshots.put(t,0)); var inputs=request.putArray("bronze_inputs");
            var response=new UsgsHttpResponse("actual-public-capture",new UsgsRequest(URI.create(QUERY),Instant.parse("2023-08-31T23:59:44Z"),
                    Instant.parse("2023-10-01T00:00:16Z"),20000,1),200,
                    HttpHeaders.of(Map.of("content-type",List.of("application/geo+json")),(a,b)->true),usgsRaw,1,Duration.ZERO);
            var u=new UsgsBronzeWriter(bronze).write(new UsgsBronzeWriteRequest(response,id+"-usgs",1,
                    LocalDate.ofInstant(processed,ZoneOffset.UTC),processed,true,id,60000)); require(u.ready(),"USGS_CAPTURE_INVALID");
            var up=inputs.addObject(); up.put("source_system","USGS"); up.put("manifest_uri",bronze.uriForKey(u.manifestKey()));
            up.put("manifest_sha256",sha(bronze.read(u.manifestKey()))); up.put("sha256",sha(usgsRaw));
            up.put("window_start_utc","2023-08-31T23:59:44Z"); up.put("window_end_utc","2023-10-01T00:00:16Z");
            var entry=inventory.stream().filter(e->e.year()==2023).findFirst().orElseThrow();
            String release="retrieved-"+DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC).format(processed)+"-"+sha(jmaRaw).substring(0,12);
            var j=new JmaBronzeWriter(bronze).write(new JmaBronzeWriteRequest(entry,jmaRaw,sha(jmaRaw),jmaRaw.length,release,
                    new JmaHttpMetadata(200,"application/zip",(long)jmaRaw.length,null,null,entry.sourceUrl()),id+"-jma",1,
                    LocalDate.ofInstant(processed,ZoneOffset.UTC),processed,true,id,60000,jmaPath.toUri().toString())); require(j.ready(),"JMA_CAPTURE_INVALID");
            var jp=inputs.addObject(); jp.put("source_system","JMA_BULLETIN"); jp.put("manifest_uri",bronze.uriForKey(j.manifestKey()));
            jp.put("manifest_sha256",sha(bronze.read(j.manifestKey()))); jp.put("sha256",sha(jmaRaw));
            jp.put("year",2023); jp.put("segment","full-year"); jp.put("catalog_release",release);
            Files.write(requestPath,JSON.writeValueAsBytes(request),StandardOpenOption.CREATE_NEW);
        }
        var request=JSON.readTree(Files.readAllBytes(requestPath)); require(id.equals(request.path("run_id").asText()),"QA_CAPTURE_ROOT_CONFLICT");
        var spark=SparkSession.builder().master("local[1]").appName("ORC-06-public-month-acceptance")
                .config("spark.ui.enabled","false").config("spark.sql.session.timeZone","UTC").config("spark.sql.shuffle.partitions","1").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        try {
            var resolver=new FullMonthStateResolver(bronze,silver,inventory,()->snapshots(request.path("baseline_snapshots")),()->{});
            var first=resolver.resolve(spark,request); var rerun=resolver.resolve(spark,request);
            require(first.equals(rerun),"REAL_MONTH_RERUN_CHANGED");
            var run=new GoldRunContext(id,Instant.parse("2023-09-01T00:00:00Z"),Instant.parse("2023-10-01T00:00:00Z"),
                    LocalDate.ofInstant(utc(request,"processed_at_utc"),ZoneOffset.UTC),true,"orc-06-v1",List.of(first.path("silver_bundle_uri").asText()));
            var transformed=GoldInputReader.read(spark,silver,id,first.path("silver_bundle_sha256").asText(),run,utc(request,"processed_at_utc"),root.resolve("gold-readback"));
            require(transformed.canonicalEventCount()==first.path("reconciliation").path("canonical").asLong()
                    && transformed.bridgeRowCount()==first.path("reconciliation").path("current").asLong(),"REAL_GOLD_HANDOFF_COUNT_MISMATCH");
            var evidence=JSON.createObjectNode(); evidence.put("task","ORC-06"); evidence.put("backend","file-backed-public-capture-QA");
            evidence.put("rerun_identical",true); evidence.put("gold_handoff_verified",true); evidence.put("gold_published",false);
            evidence.put("usgs_capture_sha256",sha(usgsRaw)); evidence.put("jma_archive_sha256",sha(jmaRaw)); evidence.set("receipt",first);
            Files.write(root.resolve("acceptance.json"),JSON.writerWithDefaultPrettyPrinter().writeValueAsBytes(evidence));
            System.out.println(JSON.writeValueAsString(evidence));
        } finally { spark.stop(); }
    }
    private static Map<String,String> query(URI uri) throws Exception {
        var out=new TreeMap<String,String>();
        for(String token:uri.getRawQuery().split("&")) {
            var pair=token.split("=",2); require(pair.length==2,"EXACT_CAPTURE_QUERY_REQUIRED");
            String k=URLDecoder.decode(pair[0],java.nio.charset.StandardCharsets.UTF_8);
            require(out.putIfAbsent(k,URLDecoder.decode(pair[1],java.nio.charset.StandardCharsets.UTF_8))==null,"DUPLICATE_CAPTURE_QUERY_PARAMETER");
        }
        return out;
    }
}
