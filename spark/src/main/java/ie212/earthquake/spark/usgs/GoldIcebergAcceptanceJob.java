package ie212.earthquake.spark.usgs;

import com.fasterxml.jackson.databind.*;
import ie212.earthquake.spark.GoldCommitJob;
import ie212.earthquake.spark.gold.*;
import ie212.earthquake.spark.silver.*;
import java.net.*;
import java.net.http.HttpHeaders;
import java.nio.file.*;
import java.time.*;
import java.util.*;

/** Opt-in live storage smoke from a pinned public capture; restricted to a fresh QA namespace. */
public final class GoldIcebergAcceptanceJob {
    private static final ObjectMapper JSON = new ObjectMapper();
    private GoldIcebergAcceptanceJob() { }
    public static void main(String[] args) throws Exception {
        if (args.length != 2) throw new IllegalArgumentException("QA_REQUEST_AND_CAPTURE_REQUIRED");
        var request = JSON.readTree(Files.readAllBytes(Path.of(args[0])));
        String runId = request.path("run_id").asText(); SilverBundlePublisher.root(runId);
        String namespace = request.path("namespace").asText();
        if (!namespace.matches("[a-z][a-z0-9_]*\\.gold_qa_[a-z0-9_]+")) throw new IllegalArgumentException("QA_NAMESPACE_REQUIRED");
        Path ownerPath = Path.of(System.getenv("SOURCE_GUARD_ROOT"), "owner.json");
        Runnable lease = () -> { try { var owner = JSON.readTree(Files.readAllBytes(ownerPath));
            if (!owner.path("run_id").asText().equals(runId) || !owner.path("dag_id").asText().equals("gold_storage_qa"))
                throw new IllegalStateException();
        } catch (Exception error) { throw new IllegalStateException("QA_SHARED_LEASE_NOT_HELD"); } };
        lease.run(); byte[] raw = Files.readAllBytes(Path.of(args[1]));
        if (!SilverBundlePublisher.sha(raw).equals(request.path("capture_sha256").asText())) throw new IllegalArgumentException("CAPTURE_PIN_MISMATCH");
        Instant processed = Instant.parse(request.path("processed_at_utc").asText());
        Instant start = Instant.parse("2023-01-01T00:00:00Z"), end = Instant.parse("2023-01-04T00:00:00Z");
        var rawJson = JSON.readTree(raw);
        String sourceUrl = rawJson.path("metadata").path("url").asText();
        if (!"earthquake.usgs.gov".equals(URI.create(sourceUrl).getHost())) throw new IllegalArgumentException("PUBLIC_USGS_CAPTURE_REQUIRED");
        UsgsBronzeWriteResult bronzeResult;
        String manifestUri, rawUri, manifestId;
        try (var bronze = MinioBronzeObjectStore.fromEnvironment()) {
            var response = new UsgsHttpResponse("captured-raw", new UsgsRequest(URI.create(sourceUrl),start,end,20000,1),
                200, HttpHeaders.of(Map.of(), (a,b)->true), raw, 1, Duration.ZERO);
            bronzeResult = new UsgsBronzeWriter(bronze).write(new UsgsBronzeWriteRequest(response, runId+"-bronze",1,
                LocalDate.ofInstant(processed,ZoneOffset.UTC), processed, true, "gold-qa-capture-"+runId,60000));
            if (!bronzeResult.ready()) throw new IllegalStateException("CAPTURE_INVALID");
            var manifest = JSON.readTree(bronze.read(bronzeResult.manifestKey()));
            manifestId = manifest.path("manifest_id").asText(); rawUri = manifest.path("raw_object_uri").asText();
            manifestUri = bronze.uriForKey(bronzeResult.manifestKey());
        }
        var parsed = new UsgsGeoJsonParser().parse(raw, new UsgsParseContext(manifestId, rawUri,
                SilverBundlePublisher.sha(raw), runId+"-bronze", processed));
        var integrated = new SilverMultiSourceIntegrationRunner().run(runId+"-silver", parsed.observations(), parsed.rejects(),
            SilverLinkConfig.defaultConfig(), null, false, processed, Map.of(manifestId, runId+"-bronze"));
        if (!integrated.isPublishable()) throw new IllegalStateException("QA_SILVER_GATE_BLOCKED");
        var spark = GoldCommitJob.configured(System.getenv()).getOrCreate(); spark.sparkContext().setLogLevel("ERROR");
        try (var store = MinioSilverObjectStore.fromEnvironment()) {
            var context = JSON.createObjectNode(); context.put("quality_passed",true); context.put("reconciliation_balanced",true);
            context.put("capture_sha256",SilverBundlePublisher.sha(raw)); context.put("bronze_manifest_uri",manifestUri);
            context.put("source_mode","pinned-public-capture"); context.put("coverage","USGS-2023-01-01..03-only");
            var publisher = new SilverBundlePublisher(store);
            publisher.publish(new SilverWriteRequest(runId+"-silver", integrated.allObservations(), integrated.qualityResult().rejectedRecords(),
                integrated.sourceLinks(),integrated.canonicalMemberships(), processed,false),context);
            publisher.verify(runId+"-silver");
            String silverUri = store.uriForKey(SilverBundlePublisher.root(runId+"-silver")+"/manifest.json");
            String silverSha = SilverBundlePublisher.sha(store.read(SilverBundlePublisher.root(runId+"-silver")+"/manifest.json"));
            var run = new GoldRunContext(runId,start,end,LocalDate.ofInstant(processed,ZoneOffset.UTC),true,
                    request.path("config_version").asText(),List.of(silverUri));
            var gold = GoldInputReader.read(spark,store,runId+"-silver",silverSha,run,processed,
                    Path.of(args[0]).toAbsolutePath().getParent().resolve("readback"));
            var writer = new GoldIcebergWriter(spark,namespace,store,lease);
            Map<String,Long> baseline = new TreeMap<>(); GoldIcebergWriter.TABLES.forEach(name->baseline.put(name,0L));
            var scope = new GoldCommitScope(run,silverSha,request.path("code_version").asText(),List.of("2023-01"),baseline);
            var first = writer.commit(gold,scope); var rerun = writer.commit(gold,scope);
            if (!first.equals(rerun)) throw new IllegalStateException("QA_RERUN_CHANGED");
            var report = JSON.createObjectNode(); report.put("task","GLD-03"); report.put("source_mode","pinned-public-capture");
            report.put("coverage","16 USGS events; no claim of JMA/full-month production coverage");
            report.put("java_version",System.getProperty("java.version")); report.put("spark_version",spark.version());
            report.put("capture_sha256",SilverBundlePublisher.sha(raw)); report.put("silver_bundle_sha256",silverSha);
            report.put("rerun_unchanged",true); report.set("commit",first);
            Files.write(Path.of(args[0]).toAbsolutePath().getParent().resolve("gld-03-report.json"),JSON.writeValueAsBytes(report));
            System.out.println(JSON.writeValueAsString(report));
        } finally { spark.stop(); }
    }
}
