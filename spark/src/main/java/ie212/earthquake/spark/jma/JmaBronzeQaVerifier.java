package ie212.earthquake.spark.jma;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.usgs.BronzeObjectStore;
import ie212.earthquake.spark.usgs.MinioBronzeObjectStore;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Read-only, exact-key acceptance gate. Payloads stay in Java, never in QA stdout. */
public final class JmaBronzeQaVerifier {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Set<String> SCOPE = Set.of("1997/jan-sep", "1997/oct-dec", "2000/full-year", "2023/full-year");
    private static final List<String> FLAGS = List.of("object_write_completed", "raw_readback_verified",
            "checksum_verified", "source_structure_valid", "manifest_consistent");

    private JmaBronzeQaVerifier() { }

    public static void main(String[] args) {
        try {
            Map<String, String> options = new LinkedHashMap<>();
            for (int i = 0; i < args.length; i += 2) {
                if (i + 1 >= args.length || options.put(args[i], args[i + 1]) != null) {
                    throw new IOException("invalid arguments");
                }
            }
            require(options.keySet().equals(Set.of("--summary", "--catalog", "--inventory", "--report"))
                    || options.keySet().equals(Set.of("--summary", "--catalog", "--inventory", "--report", "--baseline")));
            byte[] inventory = Files.readAllBytes(Path.of(options.get("--inventory")));
            try (var store = MinioBronzeObjectStore.fromEnvironment()) {
                ObjectNode report = verify(read(options.get("--summary")), read(options.get("--catalog")),
                        JmaBronzeWriter.sha256(inventory), JmaArchiveInventory.read(Path.of(options.get("--inventory"))),
                        store, options.containsKey("--baseline") ? read(options.get("--baseline")) : null);
                report.put("verified_at_utc", Instant.now().toString());
                report.put("java_version", Runtime.version().toString());
                Path output = Path.of(options.get("--report"));
                Files.createDirectories(output.toAbsolutePath().getParent());
                // Reports are new evidence, not a mutable "latest" gate left over from an earlier run.
                Files.write(output, JSON.writerWithDefaultPrettyPrinter().writeValueAsBytes(report),
                        java.nio.file.StandardOpenOption.CREATE_NEW, java.nio.file.StandardOpenOption.WRITE);
                System.out.println("event=jma_qa_verified archives=4 rerun=" + (options.containsKey("--baseline")));
            }
        } catch (Exception exception) {
            System.err.println("event=jma_qa_failed reason=READBACK_OR_IDENTITY_MISMATCH");
            System.exit(2);
        }
    }

    static ObjectNode verify(JsonNode summary, JsonNode catalog, String inventorySha,
            List<JmaArchiveEntry> inventory, BronzeObjectStore store, JsonNode baseline) throws IOException {
        require(summary.path("preview").isBoolean() && !summary.path("preview").asBoolean()
                && ready(summary) && number(summary, "planned_archives") == 4 && number(summary, "ready_archives") == 4);
        JsonNode context = summary.path("run_context");
        String run = text(context, "run_id");
        require(run.matches("[A-Za-z0-9][A-Za-z0-9._~-]{0,179}") && !run.contains("..")
                && text(context, "inventory_sha256").equals(inventorySha)
                && context.path("is_backfill").isBoolean() && context.path("is_backfill").asBoolean());
        Map<String, JsonNode> selected = segments(summary);
        require(selected.keySet().equals(SCOPE));
        Map<String, JsonNode> previous = baseline == null ? Map.of() : baselineEntries(baseline, inventorySha, run);
        ObjectNode report = JSON.createObjectNode();
        report.put("qa_version", "jma-05-v1"); report.put("qa_status", "VERIFIED");
        report.put("run_id", run); report.put("inventory_sha256", inventorySha);
        report.put("config_version", text(context, "config_version"));
        report.put("rerun_verified", baseline != null);
        if (baseline != null) { report.put("baseline_run_id", text(baseline, "run_id")); }
        report.put("count_semantics", "structural_records_before_silver");
        var samples = report.putArray("dat01_readback");
        require(catalog.path("samples").isArray() && catalog.path("samples").size() == 2);
        JsonNode usgs = sample(catalog, "USGS"), jma = sample(catalog, "JMA_BULLETIN");
        require("BRONZE_READY".equals(text(usgs, "sample_state")) && "STAGED_SOURCE".equals(text(jma, "sample_state")));
        byte[] usgsRaw = readUri(store, text(usgs, "raw_object_uri"));
        JsonNode usgsManifest = JSON.readTree(readUri(store, text(usgs, "manifest_uri")));
        checksum(usgsRaw, usgs);
        require("BronzeReady".equals(text(usgsManifest, "bronze_status"))
                && text(usgsManifest, "source_system").equals("USGS")
                && text(usgsManifest, "raw_object_uri").equals(text(usgs, "raw_object_uri"))
                && text(usgsManifest, "sha256").equals(text(usgs, "sha256"))
                && number(usgsManifest, "content_length_bytes") == usgsRaw.length
                && number(usgsManifest, "record_count_estimate") == number(usgs, "expected_record_count"));
        JsonNode features = JSON.readTree(usgsRaw).path("features");
        require(features.isArray() && features.size() == number(usgs, "expected_record_count"));
        for (String flag : FLAGS) { require(usgsManifest.path("validation").path(flag).isBoolean()
                && usgsManifest.path("validation").path(flag).asBoolean()); }
        samples.add(sampleEvidence(usgs));
        byte[] staged = readUri(store, text(jma, "staged_object_uri"));
        checksum(staged, jma);
        var stagedStructure = new JmaArchiveValidator().validate(staged, text(jma, "member_name"));
        require(stagedStructure.valid() && stagedStructure.recordCount() == number(jma, "expected_record_count"));
        samples.add(sampleEvidence(jma));
        var archives = report.putArray("archives");
        for (String identity : SCOPE.stream().sorted().toList()) {
            JsonNode result = selected.get(identity);
            JmaArchiveEntry entry = inventory.stream().filter(e -> identity.equals(e.year() + "/" + e.segment()))
                    .findFirst().orElseThrow(() -> new IOException("missing inventory entry"));
            require(ready(result) && "BronzeReady".equals(text(result, "bronze_status"))
                    && text(result, "run_id").equals(run) && number(result, "attempt") > 0);
            String sha = text(result, "sha256"), release = text(result, "catalog_release");
            require(sha.matches("[0-9a-f]{64}") && release.matches("[A-Za-z0-9][A-Za-z0-9._~-]{0,179}")
                    && !release.contains("..") && release.endsWith("-sha256-" + sha.substring(0, 12)));
            String rawKey = text(result, "raw_object_key"), manifestKey = text(result, "manifest_key");
            require(rawKey.startsWith("bronze/jma/year=" + entry.year() + "/catalog_release=" + release + "/")
                    && rawKey.matches("bronze/jma/year=[0-9]{4}/catalog_release=[A-Za-z0-9._~-]+/ingest_date=[0-9]{4}-[0-9]{2}-[0-9]{2}/run_id=[A-Za-z0-9._~-]+/attempt=[0-9]{2,}/archive[.]zip")
                    && rawKey.endsWith("/archive.zip") && !rawKey.contains("..")
                    && manifestKey.equals(rawKey.substring(0, rawKey.length() - "archive.zip".length()) + "manifest.json")
                    && store.uriForKey(rawKey).equals(text(result, "raw_object_uri"))
                    && store.uriForKey(manifestKey).equals(text(result, "manifest_uri")));
            byte[] manifestBytes = store.read(manifestKey), raw = store.read(rawKey);
            JsonNode manifest = JSON.readTree(manifestBytes);
            checksum(raw, manifest);
            var structure = new JmaArchiveValidator().validate(raw, entry.memberName());
            require(structure.valid() && structure.recordCount() == number(manifest, "record_count_estimate")
                    && structure.recordCount() == number(result, "record_count_estimate")
                    && sha.equals(text(manifest, "sha256")) && release.equals(text(manifest, "catalog_release"))
                    && "1.0".equals(text(manifest, "manifest_version"))
                    && "BronzeReady".equals(text(manifest, "bronze_status"))
                    && "JMA_BULLETIN".equals(text(manifest, "source_system"))
                    && rawKey.equals(text(manifest, "raw_object_key"))
                    && store.uriForKey(rawKey).equals(text(manifest, "raw_object_uri")));
            JsonNode interval = manifest.path("data_interval"), provenance = manifest.path("provenance");
            String publicationRun = text(manifest, "run_id");
            long publicationAttempt = number(manifest, "attempt");
            String ingestDate = LocalDate.parse(text(manifest, "ingest_date_utc")).toString();
            require(publicationRun.matches("[A-Za-z0-9][A-Za-z0-9._~-]{0,199}") && !publicationRun.contains("..")
                    && publicationAttempt > 0
                    && rawKey.equals("bronze/jma/year=" + entry.year() + "/catalog_release=" + release
                        + "/ingest_date=" + ingestDate + "/run_id=" + publicationRun + "/attempt="
                        + String.format(java.util.Locale.ROOT, "%02d", publicationAttempt) + "/archive.zip"));
            require(number(interval, "year") == entry.year() && text(interval, "segment").equals(entry.segment())
                    && text(interval, "native_start").equals(entry.nativeStartJst())
                    && text(interval, "native_end").equals(entry.nativeEndJst())
                    && "Asia/Tokyo".equals(text(interval, "native_timezone"))
                    && "[start,end)".equals(text(interval, "interval_semantics"))
                    && "GET".equals(text(manifest.path("request"), "method"))
                    && text(manifest.path("request"), "url").equals(entry.sourceUrl().toString())
                    && text(provenance, "inventory_version").equals(entry.inventoryVersion())
                    && text(provenance, "member_name").equals(entry.memberName())
                    && text(provenance, "archive_name").equals(entry.archiveName())
                    && text(provenance, "record_format").equals(entry.recordFormat())
                    && text(provenance, "catalog_era").equals(entry.catalogEra())
                    && number(provenance, "record_length_bytes") == 96);
            JsonNode response = manifest.path("response");
            long httpStatus = number(response, "http_status");
            require((httpStatus == 200 || httpStatus == 206)
                    && "application/zip".equalsIgnoreCase(text(response, "content_type").split(";", 2)[0].trim())
                    && (response.path("content_length_header").isNull() || number(response, "content_length_header") == raw.length));
            for (String flag : FLAGS) { require(manifest.path("validation").path(flag).isBoolean()
                    && manifest.path("validation").path(flag).asBoolean()); }
            if (entry.year() == 2023) {
                require(sha.equals(text(jma, "sha256")) && raw.length == number(jma, "content_length_bytes")
                        && structure.recordCount() == number(jma, "expected_record_count")
                        && release.equals(text(jma, "catalog_release")));
            }
            ObjectNode evidence = archives.addObject();
            evidence.put("year", entry.year()); evidence.put("segment", entry.segment());
            evidence.put("catalog_era", entry.catalogEra()); evidence.put("source_url", entry.sourceUrl().toString());
            evidence.put("native_start_jst", entry.nativeStartJst()); evidence.put("native_end_jst", entry.nativeEndJst());
            for (String key : List.of("raw_object_key", "manifest_key", "raw_object_uri", "manifest_uri", "sha256", "catalog_release")) {
                evidence.put(key, text(result, key));
            }
            evidence.put("publication_run_id", text(manifest, "run_id"));
            evidence.put("publication_attempt", number(manifest, "attempt"));
            evidence.put("http_status", httpStatus);
            evidence.put("retrieved_at_utc", Instant.parse(text(manifest, "retrieved_at_utc")).toString());
            evidence.put("content_length_bytes", raw.length); evidence.put("record_count_estimate", structure.recordCount());
            evidence.put("manifest_sha256", JmaBronzeWriter.sha256(manifestBytes)); evidence.put("readback_verified", true);
            evidence.put("download_reused", result.path("download_reused").asBoolean());
            evidence.put("publication_reused", result.path("publication_reused").asBoolean());
            if (baseline != null) {
                JsonNode before = previous.get(identity);
                for (String key : List.of("manifest_sha256", "raw_object_uri", "manifest_uri", "sha256", "catalog_release",
                        "content_length_bytes", "record_count_estimate", "publication_run_id", "publication_attempt")) {
                    JsonNode current = evidence.path(key), old = before.path(key);
                    require(current.isIntegralNumber() && old.isIntegralNumber()
                            ? current.asLong() == old.asLong() : current.equals(old));
                }
                require(result.path("download_reused").isBoolean() && result.path("download_reused").asBoolean()
                        && result.path("publication_reused").isBoolean() && result.path("publication_reused").asBoolean());
            }
        }
        return report;
    }

    private static Map<String, JsonNode> segments(JsonNode summary) throws IOException {
        Map<String, JsonNode> results = new LinkedHashMap<>();
        require(summary.path("years").isArray() && summary.path("years").size() == 3);
        for (JsonNode year : summary.path("years")) {
            require("BronzeReady".equals(text(year, "status")) && year.path("segments").isArray());
            for (JsonNode result : year.path("segments")) {
                require(number(year, "year") == number(result, "year"));
                require(results.put(number(result, "year") + "/" + text(result, "segment"), result) == null);
            }
        }
        return results;
    }

    private static Map<String, JsonNode> baselineEntries(JsonNode baseline, String sha, String run) throws IOException {
        require("VERIFIED".equals(text(baseline, "qa_status")) && "jma-05-v1".equals(text(baseline, "qa_version"))
                && sha.equals(text(baseline, "inventory_sha256")) && !run.equals(text(baseline, "run_id"))
                && baseline.path("archives").isArray());
        Map<String, JsonNode> result = new LinkedHashMap<>();
        for (JsonNode entry : baseline.path("archives")) {
            require(result.put(number(entry, "year") + "/" + text(entry, "segment"), entry) == null);
        }
        require(result.keySet().equals(SCOPE)); return result;
    }

    private static ObjectNode sampleEvidence(JsonNode sample) throws IOException {
        ObjectNode node = JSON.createObjectNode(); node.put("sample_id", text(sample, "sample_id"));
        node.put("sample_state", text(sample, "sample_state")); node.put("sha256", text(sample, "sha256"));
        node.put("content_length_bytes", number(sample, "content_length_bytes"));
        node.put("record_count", number(sample, "expected_record_count")); node.put("readback_verified", true); return node;
    }

    private static JsonNode sample(JsonNode catalog, String source) throws IOException {
        JsonNode found = null;
        for (JsonNode node : catalog.path("samples")) {
            if (source.equals(node.path("source_system").asText())) { require(found == null); found = node; }
        }
        require(found != null); return found;
    }

    private static byte[] readUri(BronzeObjectStore store, String value) throws IOException {
        URI uri = URI.create(value);
        require("s3".equals(uri.getScheme()) && uri.getHost() != null && uri.getUserInfo() == null
                && uri.getQuery() == null && uri.getFragment() == null && uri.getPath().startsWith("/bronze/"));
        String key = uri.getPath().substring(1);
        require(store.uriForKey(key).equals(value)); return store.read(key);
    }

    private static void checksum(byte[] raw, JsonNode metadata) throws IOException {
        require(raw.length == number(metadata, "content_length_bytes")
                && JmaBronzeWriter.sha256(raw).equals(text(metadata, "sha256")));
    }
    private static boolean ready(JsonNode node) { return "BronzeReady".equals(node.path("status").asText())
            && node.path("verified").isBoolean() && node.path("verified").asBoolean(); }
    private static long number(JsonNode node, String key) throws IOException {
        require(node.path(key).isIntegralNumber() && node.path(key).canConvertToLong() && node.path(key).asLong() >= 0);
        return node.path(key).asLong();
    }
    private static String text(JsonNode node, String key) throws IOException {
        require(node.path(key).isTextual() && !node.path(key).asText().isBlank()); return node.path(key).asText();
    }
    private static JsonNode read(String path) throws IOException { return JSON.readTree(Files.readAllBytes(Path.of(path))); }
    private static void require(boolean condition) throws IOException {
        if (!condition) { throw new IOException("JMA QA identity/readback mismatch"); }
    }
}
