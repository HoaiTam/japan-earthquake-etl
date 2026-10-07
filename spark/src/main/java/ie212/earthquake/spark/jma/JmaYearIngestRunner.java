package ie212.earthquake.spark.jma;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import ie212.earthquake.spark.usgs.BronzeObjectStore;
import ie212.earthquake.spark.usgs.MinioBronzeObjectStore;
import java.io.IOException;
import java.net.http.HttpClient;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** One exact year/segment per process. Airflow bounds concurrency; bytes never enter stdout. */
public final class JmaYearIngestRunner {
    private static final ObjectMapper JSON = new ObjectMapper();
    @FunctionalInterface interface StoreFactory { BronzeObjectStore create(); }
    private final Map<String, String> environment;
    private final JmaArchiveTransport transport;
    private final StoreFactory storeFactory;
    private final Clock clock;

    JmaYearIngestRunner(Map<String, String> environment, JmaArchiveTransport transport,
            StoreFactory storeFactory, Clock clock) {
        this.environment = Map.copyOf(environment);
        this.transport = Objects.requireNonNull(transport);
        this.storeFactory = Objects.requireNonNull(storeFactory);
        this.clock = Objects.requireNonNull(clock);
    }

    public static void main(String[] args) {
        try {
            if (args.length != 2 || !"--context-file".equals(args[0])) {
                throw new IllegalArgumentException("--context-file is required");
            }
            Map<String, String> env = System.getenv();
            int timeout = integer(env, "JMA_HTTP_TIMEOUT_MS", 60000, 1000, 300000);
            int maxBytes = integer(env, "JMA_MAX_ARCHIVE_BYTES", 134217728, 1024, 134217728);
            HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(timeout))
                    .followRedirects(HttpClient.Redirect.NORMAL).build();
            JmaYearIngestRunner runner = new JmaYearIngestRunner(env,
                    JmaArchiveDownloader.httpTransport(client, Duration.ofMillis(timeout), maxBytes),
                    () -> MinioBronzeObjectStore.fromEnvironment(env), Clock.systemUTC());
            System.out.println(JSON.writeValueAsString(runner.run(Path.of(args[1]))));
        } catch (Exception exception) {
            // Do not echo arbitrary URI, credentials, context or SDK exception messages.
            System.err.println("event=jma_runner_failed reason=INVALID_CONTEXT_OR_RUNTIME");
            System.exit(2);
        }
    }

    Map<String, Object> run(Path contextFile) throws IOException {
        JsonNode input = JSON.readTree(Files.readAllBytes(contextFile));
        JsonNode context = input.path("run_context");
        if (!"ingest".equals(text(input, "phase")) || !context.path("is_backfill").asBoolean()) {
            throw new IllegalArgumentException("an explicit backfill context is required");
        }
        String runId = text(context, "run_id");
        String runPath = text(context, "run_id_path");
        if (!runPath.matches("[A-Za-z0-9][A-Za-z0-9._~-]{0,179}") || runPath.contains("..")) {
            throw new IllegalArgumentException("unsafe run_id_path");
        }
        String configVersion = text(context, "config_version");
        LocalDate processingDate = LocalDate.parse(text(context, "processing_date"));
        Instant windowStart = Instant.parse(text(context, "window_start_utc"));
        Instant windowEnd = Instant.parse(text(context, "window_end_utc"));
        int attempt = input.path("attempt").asInt(0);
        if (attempt < 1 || !windowStart.isBefore(windowEnd) || !input.path("force_download").isBoolean()) {
            throw new IllegalArgumentException("invalid attempt/window/force_download");
        }
        Path inventory = Path.of(environment.getOrDefault("JMA_INVENTORY_PATH",
                "/opt/pipeline/config/jma/hypocenter_archives_v1.csv"));
        String inventorySha = JmaBronzeWriter.sha256(Files.readAllBytes(inventory));
        if (!inventorySha.equals(text(context, "inventory_sha256"))) {
            throw new IOException("inventory changed after preview");
        }
        JsonNode selection = input.path("archive");
        int year = selection.path("year").asInt(0);
        String segment = text(selection, "segment");
        var selected = JmaArchiveInventory.read(inventory).stream()
                .filter(entry -> entry.year() == year && entry.segment().equals(segment)).toList();
        if (selected.size() != 1) { throw new IOException("archive is not uniquely pinned by inventory"); }
        JmaArchiveEntry entry = selected.get(0);
        if (Instant.parse(entry.nativeStartJst()).isBefore(windowStart)
                || Instant.parse(entry.nativeEndJst()).isAfter(windowEnd)) {
            throw new IOException("selected archive escapes resolved run window");
        }
        Path staging = Path.of(environment.getOrDefault("JMA_STAGING_ROOT", "/opt/pipeline/staging/jma"))
                .toAbsolutePath().normalize();
        Path entryRoot = staging.resolve("downloads/year=" + year + "/segment=" + segment);
        Files.createDirectories(entryRoot);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("run_id", runId);
        result.put("year", year);
        result.put("segment", segment);
        result.put("attempt", attempt);
        result.put("config_version", configVersion);
        result.put("processing_date", processingDate.toString());
        result.put("status", "FAILED");
        result.put("verified", false);
        // Shared LocalExecutor volume: never mutate downloader state concurrently for the same segment.
        try (FileChannel channel = FileChannel.open(entryRoot.resolve("ingest.lock"),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE); var lock = channel.lock()) {
            int maxBytes = integer(environment, "JMA_MAX_ARCHIVE_BYTES", 134217728, 1024, 134217728);
            JmaDownloadResult download = new JmaArchiveDownloader(transport, staging.resolve("downloads"), clock, maxBytes)
                    .downloadOne(entry, input.path("force_download").asBoolean());
            result.put("download_status", download.status());
            if (!download.succeeded()) {
                result.put("reason", "DOWNLOAD_FAILED");
                return result;
            }
            result.put("sha256", download.sha256());
            result.put("catalog_release", download.catalogRelease());
            result.put("download_reused", download.idempotentReuse());
            Path pointer = entryRoot.resolve("publication-" + download.sha256() + ".json");
            BronzeObjectStore store = storeFactory.create();
            try {
                if (Files.exists(pointer)) {
                    JsonNode publication = JSON.readTree(Files.readAllBytes(pointer));
                    verify(store, entry, publication, download.sha256());
                    result.put("manifest_key", text(publication, "manifest_key"));
                    result.put("raw_object_key", text(publication, "raw_object_key"));
                    result.put("record_count_estimate", publication.path("record_count_estimate").asLong());
                    result.put("catalog_release", text(publication, "catalog_release"));
                    result.put("publication_reused", true);
                } else {
                    Path archivePath = Path.of(download.archivePath());
                    if (Files.size(archivePath) > maxBytes) { throw new IOException("ARCHIVE_SIZE_LIMIT"); }
                    String publicationRun = runPath + "-" + year + "-" + segment;
                    String logicalKey = "JMA_BULLETIN|" + year + "|" + segment + "|" + download.catalogRelease() + "|backfill";
                    JmaBronzeWriteResult written = new JmaBronzeWriter(store).write(new JmaBronzeWriteRequest(
                            entry, Files.readAllBytes(archivePath), download.sha256(), download.contentLengthBytes(),
                            download.catalogRelease(), download.http(), publicationRun, attempt,
                            LocalDate.now(clock),
                            download.retrievedAtUtc(), true, logicalKey,
                            integer(environment, "JMA_HTTP_TIMEOUT_MS", 60000, 1000, 300000), archivePath.toUri().toString()));
                    result.put("manifest_key", written.manifestKey());
                    result.put("raw_object_key", written.rawObjectKey());
                    if (!written.ready()) {
                        result.put("bronze_status", "Rejected");
                        result.put("reason", written.rejectionReason());
                        return result;
                    }
                    var publication = JSON.createObjectNode();
                    publication.put("manifest_key", written.manifestKey());
                    publication.put("manifest_sha256", JmaBronzeWriter.sha256(store.read(written.manifestKey())));
                    publication.put("raw_object_key", written.rawObjectKey());
                    publication.put("sha256", written.sha256());
                    publication.put("catalog_release", download.catalogRelease());
                    publication.put("record_count_estimate", written.recordCountEstimate());
                    verify(store, entry, publication, download.sha256());
                    writeJson(pointer, publication);
                    result.put("record_count_estimate", written.recordCountEstimate());
                    result.put("publication_reused", written.idempotentReuse());
                }
                result.put("manifest_uri", store.uriForKey((String) result.get("manifest_key")));
                result.put("raw_object_uri", store.uriForKey((String) result.get("raw_object_key")));
                result.put("status", "BronzeReady");
                result.put("bronze_status", "BronzeReady");
                result.put("verified", true);
            } finally {
                if (store instanceof AutoCloseable closeable) {
                    try { closeable.close(); }
                    catch (Exception exception) { throw new IOException("store close failed", exception); }
                }
            }
        } catch (IOException | RuntimeException exception) {
            result.put("status", "FAILED");
            result.put("verified", false);
            result.remove("bronze_status");
            result.put("reason", "PUBLICATION_OR_STATE_FAILED");
        }
        return result;
    }

    private static void verify(BronzeObjectStore store, JmaArchiveEntry entry, JsonNode publication,
            String expectedSha) throws IOException {
        byte[] manifestBytes = store.read(text(publication, "manifest_key"));
        if (!JmaBronzeWriter.sha256(manifestBytes).equals(text(publication, "manifest_sha256"))) {
            throw new IOException("manifest readback differs");
        }
        JsonNode manifest = JSON.readTree(manifestBytes);
        String rawKey = text(publication, "raw_object_key");
        if (!"BronzeReady".equals(manifest.path("bronze_status").asText())
                || !"JMA_BULLETIN".equals(manifest.path("source_system").asText())
                || !expectedSha.equals(manifest.path("sha256").asText())
                || !expectedSha.equals(publication.path("sha256").asText())
                || !rawKey.equals(manifest.path("raw_object_key").asText())
                || !store.uriForKey(rawKey).equals(manifest.path("raw_object_uri").asText())
                || !text(publication, "catalog_release").equals(manifest.path("catalog_release").asText())
                || manifest.path("data_interval").path("year").asInt() != entry.year()
                || !entry.segment().equals(manifest.path("data_interval").path("segment").asText())
                || !entry.nativeStartJst().equals(manifest.path("data_interval").path("native_start").asText())
                || !entry.nativeEndJst().equals(manifest.path("data_interval").path("native_end").asText())
                || !entry.sourceUrl().toString().equals(manifest.path("request").path("url").asText())
                || !entry.inventoryVersion().equals(manifest.path("provenance").path("inventory_version").asText())) {
            throw new IOException("publication identity differs");
        }
        for (String flag : new String[] {"object_write_completed", "raw_readback_verified", "checksum_verified",
                "source_structure_valid", "manifest_consistent"}) {
            if (!manifest.path("validation").path(flag).asBoolean()) { throw new IOException("validation flag failed"); }
        }
        byte[] bytes = store.read(rawKey);
        JmaArchiveValidationResult structure = new JmaArchiveValidator().validate(bytes, entry.memberName());
        if (!expectedSha.equals(JmaBronzeWriter.sha256(bytes))
                || bytes.length != manifest.path("content_length_bytes").asLong(-1)
                || !structure.valid() || structure.recordCount() != manifest.path("record_count_estimate").asLong(-1)
                || structure.recordCount() != publication.path("record_count_estimate").asLong(-1)) {
            throw new IOException("raw readback differs");
        }
    }

    private static void writeJson(Path target, JsonNode value) throws IOException {
        Path temporary = Files.createTempFile(target.getParent(), ".publication-", ".tmp");
        try {
            Files.write(temporary, JSON.writeValueAsBytes(value));
            try {
                Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (java.nio.file.AtomicMoveNotSupportedException exception) {
                Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING);
            }
        } finally { Files.deleteIfExists(temporary); }
    }

    static int integer(Map<String, String> env, String key, int defaultValue, int min, int max) {
        int value = Integer.parseInt(env.getOrDefault(key, Integer.toString(defaultValue)));
        if (value < min || value > max) { throw new IllegalArgumentException(key + " outside allowed range"); }
        return value;
    }

    private static String text(JsonNode node, String key) {
        String value = node.path(key).asText(null);
        if (value == null || value.isBlank()) { throw new IllegalArgumentException(key + " is required"); }
        return value;
    }
}
