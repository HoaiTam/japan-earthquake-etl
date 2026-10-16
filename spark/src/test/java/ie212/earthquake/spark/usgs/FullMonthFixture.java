package ie212.earthquake.spark.usgs;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import com.fasterxml.jackson.databind.node.*;
import ie212.earthquake.spark.jma.*;
import java.net.*;
import java.net.http.HttpHeaders;
import java.nio.file.*;
import java.time.*;
import java.util.*;

/** Synthetic producer fixtures using the real immutable Bronze writers, never source network. */
public final class FullMonthFixture {
    public static final Instant RETRIEVED=Instant.parse("2026-10-13T00:00:00Z");
    public static final JmaArchiveEntry ENTRY=new JmaArchiveEntry("1.0",2023,"full-year",
            "2023-01-01T00:00:00+09:00","2024-01-01T00:00:00+09:00","h2023.zip","hypo.dat",
            URI.create("https://example.invalid/h2023.zip"),null,null,null,"application/zip","jma-hypocenter-96-byte-v1","UNIFIED");
    private FullMonthFixture() { }
    public static Path repo() {
        var root=Path.of(System.getProperty("user.dir")).toAbsolutePath();
        while(root!=null && !Files.exists(root.resolve("tests/fixtures/cases.json"))) root=root.getParent();
        return Objects.requireNonNull(root);
    }
    public static ObjectNode payload(String fixture) throws Exception {
        return (ObjectNode)JSON.readTree(Files.readAllBytes(repo().resolve("tests/fixtures/usgs/"+fixture+".geojson")));
    }
    public static ObjectNode usgs(BronzeObjectStore store,String run,String from,String to,ObjectNode payload) throws Exception {
        Instant start=Instant.parse(from), end=Instant.parse(to);
        String url="https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&eventtype=earthquake"
                +"&starttime="+URLEncoder.encode(from,java.nio.charset.StandardCharsets.UTF_8)
                +"&endtime="+URLEncoder.encode(end.minusMillis(1).toString(),java.nio.charset.StandardCharsets.UTF_8)
                +"&minlatitude=20.0&maxlatitude=50.0&minlongitude=120.0&maxlongitude=155.0&orderby=time-asc&limit=20000&offset=1";
        var req=new UsgsRequest(URI.create(url),start,end,20000,1);
        var response=new UsgsHttpResponse("synthetic-orc06",req,200,
                HttpHeaders.of(Map.of("content-type",List.of("application/geo+json")),(a,b)->true),JSON.writeValueAsBytes(payload),1,Duration.ZERO);
        var written=new UsgsBronzeWriter(store).write(new UsgsBronzeWriteRequest(response,run,1,LocalDate.of(2026,10,13),RETRIEVED,true,run,60000));
        if(!written.ready()) throw new IllegalStateException("FIXTURE_NOT_READY");
        var pin=JSON.createObjectNode(); pin.put("source_system","USGS"); pin.put("manifest_uri",store.uriForKey(written.manifestKey()));
        pin.put("manifest_sha256",sha(store.read(written.manifestKey()))); pin.put("sha256",written.sha256());
        pin.put("window_start_utc",from); pin.put("window_end_utc",to); return pin;
    }
    public static ObjectNode jma(BronzeObjectStore store,String fixture) throws Exception {
        byte[] raw=Files.readAllBytes(repo().resolve("tests/fixtures/jma/archives/"+fixture+".zip"));
        return jmaBytes(store,raw,"jma-"+fixture,"release-v1");
    }
    public static byte[] jmaRecordArchive(String utcJstDigits) throws Exception {
        String line=Files.readAllLines(repo().resolve("tests/fixtures/jma/fixed-width/success.hyp")).get(0);
        line=line.substring(0,1)+utcJstDigits+line.substring(17);
        var bytes=new java.io.ByteArrayOutputStream();
        try(var zip=new java.util.zip.ZipOutputStream(bytes)) {
            var entry=new java.util.zip.ZipEntry("hypo.dat"); entry.setTime(0); zip.putNextEntry(entry);
            zip.write((line+"\n").getBytes(java.nio.charset.StandardCharsets.ISO_8859_1)); zip.closeEntry();
        }
        return bytes.toByteArray();
    }
    public static ObjectNode jmaBytes(BronzeObjectStore store,byte[] raw,String run,String release) throws Exception {
        var written=new JmaBronzeWriter(store).write(new JmaBronzeWriteRequest(ENTRY,raw,sha(raw),raw.length,release,
                new JmaHttpMetadata(200,"application/zip",(long)raw.length,null,null,ENTRY.sourceUrl()),run,1,
                LocalDate.of(2026,10,13),RETRIEVED,true,run,60000,null));
        if(!written.ready()) throw new IllegalStateException("FIXTURE_NOT_READY");
        var pin=JSON.createObjectNode(); pin.put("source_system","JMA_BULLETIN"); pin.put("manifest_uri",store.uriForKey(written.manifestKey()));
        pin.put("manifest_sha256",sha(store.read(written.manifestKey()))); pin.put("sha256",sha(raw));
        pin.put("year",2023); pin.put("segment","full-year"); pin.put("catalog_release",release); return pin;
    }
}
