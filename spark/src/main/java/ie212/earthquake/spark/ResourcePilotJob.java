package ie212.earthquake.spark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import ie212.earthquake.spark.bronze.BronzeReuseVerifier;
import ie212.earthquake.spark.jma.JmaBronzeWriter;
import ie212.earthquake.spark.silver.*;
import ie212.earthquake.spark.usgs.MinioBronzeObjectStore;
import java.io.*;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.zip.ZipInputStream;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.sum;

/** Bounded read-only sizing probe, NOT a Silver writer or an ETL acceptance job. */
public final class ResourcePilotJob {
    private static final ObjectMapper JSON = new ObjectMapper();
    public static final int JMA_LINES = 10000;
    private ResourcePilotJob() { }

    public static void main(String[] args) throws Exception {
        if (args.length != 1 || Files.size(Path.of(args[0])) > 65536) {
            throw new IllegalArgumentException("EXACT_PILOT_REQUEST_REQUIRED");
        }
        long started = System.nanoTime();
        JsonNode request = JSON.readTree(Files.readAllBytes(Path.of(args[0])));
        var observations = new ArrayList<SilverObservation>();
        var counts = JSON.createArrayNode();
        try (var store = MinioBronzeObjectStore.fromEnvironment()) {
            var sources = new java.util.HashSet<String>();
            if (request.path("bronze_inputs").size() != 2
                    || !request.path("observability_version").asText().equals("orc-04-v1")) {
                throw new IOException("TWO_PINNED_SOURCES_REQUIRED");
            }
            for (JsonNode pin : request.path("bronze_inputs")) {
                sources.add(pin.path("source_system").asText());
                String uri = pin.path("manifest_uri").asText();
                if (!store.uriForKey(key(uri)).equals(uri)) { throw new IOException("PILOT_BUCKET_MISMATCH"); }
                byte[] bytes = store.read(key(uri));
                if (bytes.length > 65536 || !JmaBronzeWriter.sha256(bytes)
                        .equals(pin.path("manifest_sha256").asText())) { throw new IOException("PILOT_PIN_MISMATCH"); }
                JsonNode manifest = JSON.readTree(bytes);
                if (manifest.path("content_length_bytes").asLong(Long.MAX_VALUE) > 32L * 1024 * 1024
                        || ("USGS".equals(pin.path("source_system").asText())
                        && manifest.path("record_count_estimate").asLong(Long.MAX_VALUE) > 10000)) {
                    throw new IOException("PILOT_RAW_SIZE_LIMIT");
                }
            }
            if (!sources.equals(java.util.Set.of("USGS", "JMA_BULLETIN"))) {
                throw new IOException("TWO_PINNED_SOURCES_REQUIRED");
            }
            // Fresh raw/manifest checksums, full ZIP CRC/structure and full input count.
            JsonNode verified = BronzeReuseVerifier.verify(request, store);
            for (JsonNode pin : verified.path("observability").path("bronze_inputs")) {
                byte[] manifestBytes = store.read(key(pin.path("manifest_uri").asText()));
                if (!JmaBronzeWriter.sha256(manifestBytes).equals(pin.path("manifest_sha256").asText())) {
                    throw new IOException("PILOT_INPUT_CHANGED");
                }
                JsonNode manifest = JSON.readTree(manifestBytes);
                if (manifest.path("content_length_bytes").asLong() > 32L * 1024 * 1024) {
                    throw new IOException("PILOT_RAW_SIZE_LIMIT");
                }
                String rawUri = manifest.path("raw_object_uri").asText();
                if (!rawUri.equals(pin.path("raw_object_uri").asText())) {
                    throw new IOException("PILOT_INPUT_CHANGED");
                }
                byte[] raw = store.read(key(rawUri));
                if (!JmaBronzeWriter.sha256(raw).equals(pin.path("sha256").asText())) {
                    throw new IOException("PILOT_INPUT_CHANGED");
                }
                String id = manifest.path("manifest_id").asText();
                String run = manifest.path("run_id").asText();
                String sha = pin.path("sha256").asText();
                int parsed, rejected, selected;
                if ("USGS".equals(pin.path("source_system").asText())) {
                    var result = new UsgsGeoJsonParser().parse(raw,
                            new UsgsParseContext(id, rawUri, sha, run, Instant.now()));
                    observations.addAll(result.observations());
                    parsed = result.parsedCount(); rejected = result.rejects().size(); selected = parsed;
                } else {
                    String member = manifest.path("provenance").path("member_name").asText();
                    try (var zip = new ZipInputStream(new ByteArrayInputStream(raw))) {
                        var entry = zip.getNextEntry();
                        if (entry == null || !member.equals(entry.getName())) {
                            throw new IOException("PILOT_MEMBER_CHANGED");
                        }
                        byte[] sample = firstLines(zip, JMA_LINES);
                        selected = (int) new String(sample, java.nio.charset.StandardCharsets.US_ASCII)
                                .chars().filter(value -> value == '\n').count();
                        var result = new JmaFixedWidthParser().parse(sample,
                                new JmaParseContext(id, rawUri, sha, run, pin.path("catalog_release").asText(),
                                        null, null, member, Instant.now()));
                        observations.addAll(result.observations());
                        parsed = result.parsedCount(); rejected = result.rejects().size();
                    }
                }
                var row = counts.addObject();
                row.put("source_system", pin.path("source_system").asText());
                row.put("full_bronze_input", pin.path("record_count_estimate").asLong());
                row.put("selected_records", selected); row.put("parser_considered", parsed);
                row.put("parser_rejects", rejected); row.put("parser_ignored", selected - parsed);
                row.put("observations", parsed - rejected);
            }
        }
        SparkSession spark = SparkSession.builder().appName("orc-05-resource-pilot")
                .config("spark.sql.session.timeZone", "UTC").getOrCreate();
        try {
            var data = SilverSchemas.toObservationDataset(spark, observations);
            // Force repartition + aggregate on actual standardized observation rows.
            var groups = data.repartition(4).groupBy("source_system", "event_year_utc", "event_month_utc").count();
            long grouped = ((Number) groups.agg(sum("count")).first().get(0)).longValue();
            if (observations.isEmpty() || grouped != observations.size()) {
                throw new IOException("PILOT_COUNT_MISMATCH");
            }
            var result = JSON.createObjectNode();
            result.put("task", "ORC-05"); result.put("profile_version", "orc-05-local-v1");
            result.put("application_id", spark.sparkContext().applicationId());
            result.put("master", spark.sparkContext().master()); result.set("sources", counts);
            result.put("shuffled_observations", grouped); result.put("lake_writes", false);
            result.put("published", false); result.put("driver_heap_max_bytes", Runtime.getRuntime().maxMemory());
            long peak = ManagementFactory.getMemoryPoolMXBeans().stream().filter(pool ->
                    pool.getType() == java.lang.management.MemoryType.HEAP)
                    .mapToLong(pool -> pool.getPeakUsage().getUsed()).sum();
            result.put("heap_pool_peak_sum_bytes", peak); // Upper bound, not simultaneous heap/RSS peak.
            result.put("duration_seconds", (System.nanoTime() - started) / 1e9);
            System.out.println(JSON.writeValueAsString(result));
        } finally { spark.stop(); }
    }

    static byte[] firstLines(InputStream input, int limit) throws IOException {
        if (limit < 1 || limit > JMA_LINES) { throw new IOException("PILOT_LINE_LIMIT"); }
        var output = new ByteArrayOutputStream();
        int value, lines = 0, width = 0;
        while (lines < limit && (value = input.read()) != -1) {
            output.write(value);
            if (value == '\n') { lines++; width = 0; }
            else if (++width > 97) { throw new IOException("PILOT_RECORD_WIDTH"); }
        }
        if (width > 0) { output.write('\n'); }
        return output.toByteArray();
    }

    private static String key(String uri) { return URI.create(uri).getPath().substring(1); }
}
