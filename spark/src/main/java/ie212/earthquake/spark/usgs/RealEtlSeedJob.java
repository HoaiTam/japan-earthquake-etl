package ie212.earthquake.spark.usgs;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import ie212.earthquake.spark.jma.*;
import ie212.earthquake.spark.gold.GoldIcebergWriter;
import java.net.URI;
import java.net.http.HttpHeaders;
import java.nio.file.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;

/** Bounded ORC-07 acceptance seed: original public bytes -> actual MinIO Bronze writers. */
public final class RealEtlSeedJob {
    private RealEtlSeedJob() { }
    public static void main(String[] args) throws Exception {
        require(args.length==5,"EXACT_SEED_ARGUMENTS_REQUIRED");
        String run=args[3],namespace=args[4];
        require(run.matches("orc07-seed-[a-f0-9]{32}") && namespace.matches("[a-z][a-z0-9_]*\\.gold_qa_[a-f0-9]{32}"),"ISOLATED_SEED_REQUIRED");
        var owner=JSON.readTree(Files.readAllBytes(Path.of(Objects.requireNonNull(System.getenv("SOURCE_GUARD_ROOT")),"owner.json")));
        require(run.equals(owner.path("run_id").asText()) && "orc07_acceptance".equals(owner.path("dag_id").asText()),"EXACT_SEED_LEASE_REQUIRED");
        require(Files.size(Path.of(args[0]))<32L*1024*1024 && Files.size(Path.of(args[1]))<32L*1024*1024,"BOUNDED_PUBLIC_CAPTURES_REQUIRED");
        var raw=Files.readAllBytes(Path.of(args[0])); var jmaRaw=Files.readAllBytes(Path.of(args[1]));
        URI origin=URI.create(JSON.readTree(raw).path("metadata").path("url").asText());
        require("earthquake.usgs.gov".equals(origin.getHost())
                && FullMonthAcceptanceJob.query(origin).equals(FullMonthAcceptanceJob.query(URI.create(FullMonthAcceptanceJob.QUERY))),"PUBLIC_USGS_CAPTURE_REQUIRED");
        var now=Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS); var date=LocalDate.ofInstant(now,ZoneOffset.UTC);
        var request=JSON.createObjectNode(); request.put("resolver_version",VERSION); request.put("run_id",run); request.put("dag_id","orc07_acceptance");
        request.put("namespace",namespace); request.put("config_version","orc-06-v1"); request.put("code_version","orc-07-v1");
        request.put("processed_at_utc",now.toString()); request.put("source_cutoff_utc",now.toString());
        request.putArray("target_months").add("2023-09"); request.putNull("baseline");
        var bases=request.putObject("baseline_snapshots"); GoldIcebergWriter.TABLES.forEach(t->bases.put(t,0)); var pins=request.putArray("bronze_inputs");
        try(var store=MinioBronzeObjectStore.fromEnvironment()) {
            var response=new UsgsHttpResponse("public-month-capture",new UsgsRequest(URI.create(FullMonthAcceptanceJob.QUERY),
                    Instant.parse("2023-08-31T23:59:44Z"),Instant.parse("2023-10-01T00:00:16Z"),20000,1),200,
                    HttpHeaders.of(Map.of("content-type",List.of("application/geo+json")),(a,b)->true),raw,1,Duration.ZERO);
            var u=new UsgsBronzeWriter(store).write(new UsgsBronzeWriteRequest(response,run+"-usgs",1,date,now,true,run,60000));
            require(u.ready(),"USGS_SEED_NOT_READY");
            var up=pins.addObject(); up.put("source_system","USGS"); up.put("manifest_uri",store.uriForKey(u.manifestKey()));
            up.put("manifest_sha256",sha(store.read(u.manifestKey()))); up.put("sha256",sha(raw));
            up.put("window_start_utc","2023-08-31T23:59:44Z"); up.put("window_end_utc","2023-10-01T00:00:16Z");
            var entries=JmaArchiveInventory.read(Path.of(System.getenv().getOrDefault("JMA_INVENTORY_PATH","/opt/pipeline/config/jma/hypocenter_archives_v1.csv")));
            var entry=entries.stream().filter(e->e.year()==2023 && "full-year".equals(e.segment())).findFirst().orElseThrow();
            var release="retrieved-"+DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC).format(now)+"-"+sha(jmaRaw).substring(0,12);
            var j=new JmaBronzeWriter(store).write(new JmaBronzeWriteRequest(entry,jmaRaw,sha(jmaRaw),jmaRaw.length,release,
                    new JmaHttpMetadata(200,"application/zip",(long)jmaRaw.length,null,null,entry.sourceUrl()),run+"-jma",1,date,now,true,run,60000,Path.of(args[1]).toUri().toString()));
            require(j.ready(),"JMA_SEED_NOT_READY"); var jp=pins.addObject(); jp.put("source_system","JMA_BULLETIN");
            jp.put("manifest_uri",store.uriForKey(j.manifestKey())); jp.put("manifest_sha256",sha(store.read(j.manifestKey()))); jp.put("sha256",sha(jmaRaw));
            jp.put("year",2023); jp.put("segment","full-year"); jp.put("catalog_release",release);
        }
        Files.write(Path.of(args[2]),JSON.writeValueAsBytes(request),StandardOpenOption.CREATE_NEW);
        System.out.println("event=orc07_seed_passed");
    }
}
