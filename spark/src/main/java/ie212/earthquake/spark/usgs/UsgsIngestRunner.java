package ie212.earthquake.spark.usgs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.http.HttpHeaders;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Executable bridge between the Airflow phase protocol and the Java USGS
 * client/Bronze writer. Raw bytes stay in staging or Bronze and never enter
 * stdout.
 */
public final class UsgsIngestRunner {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Set<String> PHASES = Set.of("fetch", "validate", "upload", "verify");
    private static final Pattern SAFE_RUN_ID = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._~-]*");
    private static final Set<String> SAFE_RESPONSE_HEADERS = Set.of(
            "content-type", "content-length", "etag", "last-modified", "date");
    private static final String FETCH_BODY_FILE = "fetch-response.geojson";
    private static final String FETCH_STATE_FILE = "fetch-state.json";
    private static final String UPLOAD_STATE_FILE = "upload-state.json";

    @FunctionalInterface
    interface Fetcher {
        UsgsHttpResponse fetch(UsgsRequest request) throws IOException;
    }

    @FunctionalInterface
    interface FetcherFactory {
        Fetcher create(UsgsRequestConfig config);
    }

    @FunctionalInterface
    interface StoreFactory {
        BronzeObjectStore create(Map<String, String> environment) throws IOException;
    }

    private final Map<String, String> environment;
    private final FetcherFactory fetcherFactory;
    private final StoreFactory storeFactory;
    private final Clock clock;

    UsgsIngestRunner(
            Map<String, String> environment,
            FetcherFactory fetcherFactory,
            StoreFactory storeFactory,
            Clock clock) {
        this.environment = Map.copyOf(Objects.requireNonNull(environment, "environment"));
        this.fetcherFactory = Objects.requireNonNull(fetcherFactory, "fetcherFactory");
        this.storeFactory = Objects.requireNonNull(storeFactory, "storeFactory");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public static void main(String[] args) {
        int status = executeCli(args, System.getenv(), System.out, System.err);
        if (status != 0) {
            System.exit(status);
        }
    }

    static int executeCli(
            String[] args,
            Map<String, String> environment,
            PrintStream stdout,
            PrintStream stderr) {
        String phase = "unknown";
        try {
            CliArguments cli = CliArguments.parse(args);
            phase = cli.phase();
            UsgsIngestRunner runner = new UsgsIngestRunner(
                    environment,
                    config -> new UsgsHttpClient(config)::fetch,
                    MinioBronzeObjectStore::fromEnvironment,
                    Clock.systemUTC());
            Map<String, Object> result = runner.runPhase(cli.phase(), cli.contextFile());
            stdout.println(JSON.writeValueAsString(result));
            return 0;
        } catch (Exception exception) {
            stderr.printf(
                    Locale.ROOT,
                    "event=usgs_runner_failed phase=%s error=%s%n",
                    safeLogValue(phase),
                    safeLogValue(exception.getMessage()));
            return 2;
        }
    }

    Map<String, Object> runPhase(String phase, Path contextFile) throws IOException {
        if (!PHASES.contains(phase)) {
            throw new IllegalArgumentException("unsupported USGS runner phase: " + phase);
        }
        JsonNode root = JSON.readTree(Files.readAllBytes(contextFile));
        if (!phase.equals(requiredText(root, "phase"))) {
            throw new IllegalArgumentException("context phase does not match --phase");
        }
        RunnerContext context = RunnerContext.from(root.path("run_context"));
        Path runDirectory = resolveRunDirectory(context);
        Files.createDirectories(runDirectory);

        return switch (phase) {
            case "fetch" -> fetch(context, runDirectory);
            case "validate" -> validate(context, runDirectory);
            case "upload" -> upload(context, runDirectory);
            case "verify" -> verify(context, runDirectory);
            default -> throw new AssertionError("phase allow-list and switch are inconsistent");
        };
    }

    private Map<String, Object> fetch(RunnerContext context, Path runDirectory)
            throws IOException {
        Path bodyPath = runDirectory.resolve(FETCH_BODY_FILE);
        Path statePath = runDirectory.resolve(FETCH_STATE_FILE);
        if (Files.exists(bodyPath) || Files.exists(statePath)) {
            FetchedArtifact existing = loadFetchedArtifact(context, runDirectory);
            return fetchSummary(context, bodyPath, existing, true);
        }

        UsgsRequestConfig config = UsgsRequestConfig.fromEnvironment(environment);
        UsgsRequestPlan plan = new UsgsRequestBuilder(config).buildResolvedPlan(
                context.targetWindowStartUtc(),
                context.targetWindowEndUtc(),
                context.windowStartUtc(),
                context.windowEndUtc());
        if (plan.requests().size() != 1) {
            throw new IllegalArgumentException(
                    "USGS live runner requires one resolved request; keep the Airflow window within USGS_MAX_WINDOW_DAYS");
        }

        UsgsHttpResponse response = fetcherFactory.create(config).fetch(plan.requests().get(0));
        Instant retrievedAt = clock.instant();
        byte[] body = response.body();
        String sha256 = UsgsBronzeWriter.sha256(body);
        writeImmutable(bodyPath, body);
        byte[] state = buildFetchState(context, response, retrievedAt, body.length, sha256);
        writeImmutable(statePath, state);
        FetchedArtifact fetched = loadFetchedArtifact(context, runDirectory);
        return fetchSummary(context, bodyPath, fetched, false);
    }

    private Map<String, Object> validate(RunnerContext context, Path runDirectory)
            throws IOException {
        FetchedArtifact fetched = loadFetchedArtifact(context, runDirectory);
        UsgsGeoJsonValidationResult validation = new UsgsGeoJsonValidator()
                .validate(fetched.response().body());
        Map<String, Object> summary = baseSummary("validate", context);
        summary.put("sha256", fetched.sha256());
        summary.put("record_count_estimate", validation.featureCount());
        summary.put("valid", validation.valid());
        if (validation.valid()) {
            return summary;
        }

        BronzeObjectStore objectStore = storeFactory.create(environment);
        try {
            UsgsBronzeWriteResult rejected = new UsgsBronzeWriter(objectStore)
                    .write(writeRequest(context, fetched));
            summary.put("bronze_status", rejected.bronzeStatus());
            summary.put("quarantine_uri", objectStore.uriForKey(rejected.manifestKey()));
            return summary;
        } finally {
            closeObjectStore(objectStore);
        }
    }

    private Map<String, Object> upload(RunnerContext context, Path runDirectory)
            throws IOException {
        FetchedArtifact fetched = loadFetchedArtifact(context, runDirectory);
        BronzeObjectStore objectStore = storeFactory.create(environment);
        try {
            UsgsBronzeWriteResult result = new UsgsBronzeWriter(objectStore)
                    .write(writeRequest(context, fetched));
            writeUploadState(runDirectory, result);

            Map<String, Object> summary = baseSummary("upload", context);
            summary.put("bronze_status", result.bronzeStatus());
            summary.put("raw_object_uri", objectStore.uriForKey(result.rawObjectKey()));
            summary.put("manifest_uri", objectStore.uriForKey(result.manifestKey()));
            summary.put("sha256", result.sha256());
            summary.put("record_count_estimate", result.recordCountEstimate());
            summary.put("idempotent_reuse", result.idempotentReuse());
            if (!result.ready()) {
                summary.put("valid", false);
                summary.put("quarantine_uri", objectStore.uriForKey(result.manifestKey()));
            }
            return summary;
        } finally {
            closeObjectStore(objectStore);
        }
    }

    private Map<String, Object> verify(RunnerContext context, Path runDirectory)
            throws IOException {
        JsonNode uploadState = JSON.readTree(Files.readAllBytes(runDirectory.resolve(UPLOAD_STATE_FILE)));
        String bronzeStatus = requiredText(uploadState, "bronze_status");
        String rawKey = requiredText(uploadState, "raw_object_key");
        String manifestKey = requiredText(uploadState, "manifest_key");
        String expectedSha256 = requiredText(uploadState, "sha256");
        int expectedCount = uploadState.path("record_count_estimate").asInt(-1);
        if (!"BronzeReady".equals(bronzeStatus) || expectedCount < 0) {
            throw new IOException("Bronze upload state is not ready for verification");
        }

        BronzeObjectStore objectStore = storeFactory.create(environment);
        try {
            byte[] manifestBytes = objectStore.read(manifestKey);
            JsonNode manifest = JSON.readTree(manifestBytes);
            byte[] raw = objectStore.read(rawKey);
            boolean verified = "BronzeReady".equals(manifest.path("bronze_status").asText())
                    && rawKey.equals(manifest.path("raw_object_key").asText())
                    && objectStore.uriForKey(rawKey).equals(manifest.path("raw_object_uri").asText())
                    && expectedSha256.equals(manifest.path("sha256").asText())
                    && context.runIdPath().equals(manifest.path("run_id").asText())
                    && expectedCount == manifest.path("record_count_estimate").asInt(-1)
                    && raw.length == manifest.path("content_length_bytes").asInt(-1)
                    && expectedSha256.equals(UsgsBronzeWriter.sha256(raw))
                    && manifest.path("validation").path("raw_readback_verified").asBoolean(false)
                    && manifest.path("validation").path("checksum_verified").asBoolean(false)
                    && manifest.path("validation").path("manifest_consistent").asBoolean(false);
            if (!verified) {
                throw new IOException("Bronze manifest/raw verification failed");
            }

            Map<String, Object> summary = baseSummary("verify", context);
            summary.put("bronze_status", "BronzeReady");
            summary.put("verified", true);
            summary.put("raw_object_uri", objectStore.uriForKey(rawKey));
            summary.put("manifest_uri", objectStore.uriForKey(manifestKey));
            summary.put("sha256", expectedSha256);
            summary.put("record_count_estimate", expectedCount);
            return summary;
        } finally {
            closeObjectStore(objectStore);
        }
    }

    private UsgsBronzeWriteRequest writeRequest(
            RunnerContext context,
            FetchedArtifact fetched) {
        UsgsRequestConfig config = UsgsRequestConfig.fromEnvironment(environment);
        return new UsgsBronzeWriteRequest(
                fetched.response(),
                context.runIdPath(),
                1,
                context.processingDate(),
                fetched.retrievedAtUtc(),
                context.backfill(),
                context.logicalRunKey(),
                Math.toIntExact(config.requestTimeout().toMillis()));
    }

    private FetchedArtifact loadFetchedArtifact(
            RunnerContext context,
            Path runDirectory)
            throws IOException {
        Path bodyPath = runDirectory.resolve(FETCH_BODY_FILE);
        Path statePath = runDirectory.resolve(FETCH_STATE_FILE);
        if (!Files.isRegularFile(bodyPath) || !Files.isRegularFile(statePath)) {
            throw new IOException("fetch artifact is incomplete for run " + context.runIdPath());
        }
        JsonNode state = JSON.readTree(Files.readAllBytes(statePath));
        if (!context.logicalRunKey().equals(requiredText(state, "logical_run_key"))) {
            throw new IOException("fetch artifact belongs to a different logical run");
        }
        byte[] body = Files.readAllBytes(bodyPath);
        String sha256 = requiredText(state, "sha256");
        int expectedLength = state.path("body_length_bytes").asInt(-1);
        if (body.length != expectedLength || !sha256.equals(UsgsBronzeWriter.sha256(body))) {
            throw new IOException("staged USGS body checksum or length mismatch");
        }

        UsgsRequest request = new UsgsRequest(
                URI.create(requiredText(state.path("request"), "uri")),
                Instant.parse(requiredText(state.path("request"), "window_start_utc")),
                Instant.parse(requiredText(state.path("request"), "window_end_utc")),
                state.path("request").path("limit").asInt(-1),
                state.path("request").path("offset").asInt(-1));
        if (!request.windowStartUtc().equals(context.windowStartUtc())
                || !request.windowEndExclusiveUtc().equals(context.windowEndUtc())) {
            throw new IOException("staged request window differs from Airflow run context");
        }

        Map<String, List<String>> headers = readHeaders(state.path("headers"));
        UsgsHttpResponse response = new UsgsHttpResponse(
                "staged-" + context.runIdPath(),
                request,
                state.path("status_code").asInt(-1),
                HttpHeaders.of(headers, (name, value) -> true),
                body,
                state.path("attempts").asInt(1),
                Duration.ofMillis(state.path("elapsed_ms").asLong(0L)));
        return new FetchedArtifact(
                response,
                Instant.parse(requiredText(state, "retrieved_at_utc")),
                sha256);
    }

    private byte[] buildFetchState(
            RunnerContext context,
            UsgsHttpResponse response,
            Instant retrievedAt,
            int bodyLength,
            String sha256)
            throws IOException {
        ObjectNode state = JSON.createObjectNode();
        state.put("schema_version", "1.0");
        state.put("logical_run_key", context.logicalRunKey());
        state.put("retrieved_at_utc", retrievedAt.toString());
        state.put("status_code", response.statusCode());
        state.put("attempts", response.attempts());
        state.put("elapsed_ms", response.elapsed().toMillis());
        state.put("body_length_bytes", bodyLength);
        state.put("sha256", sha256);
        ObjectNode request = state.putObject("request");
        request.put("uri", response.request().uri().toString());
        request.put("window_start_utc", response.request().windowStartUtc().toString());
        request.put("window_end_utc", response.request().windowEndExclusiveUtc().toString());
        request.put("limit", response.request().limit());
        request.put("offset", response.request().offset());
        ObjectNode headers = state.putObject("headers");
        response.headers().forEach((name, values) -> {
            if (name != null && SAFE_RESPONSE_HEADERS.contains(name.toLowerCase(Locale.ROOT))) {
                ArrayNode array = headers.putArray(name.toLowerCase(Locale.ROOT));
                values.forEach(array::add);
            }
        });
        return JSON.writeValueAsBytes(state);
    }

    private void writeUploadState(Path runDirectory, UsgsBronzeWriteResult result)
            throws IOException {
        ObjectNode state = JSON.createObjectNode();
        state.put("schema_version", "1.0");
        state.put("bronze_status", result.bronzeStatus());
        state.put("raw_object_key", result.rawObjectKey());
        state.put("manifest_key", result.manifestKey());
        state.put("sha256", result.sha256());
        if (result.recordCountEstimate() == null) {
            state.putNull("record_count_estimate");
        } else {
            state.put("record_count_estimate", result.recordCountEstimate());
        }
        writeImmutable(runDirectory.resolve(UPLOAD_STATE_FILE), JSON.writeValueAsBytes(state));
    }

    private Map<String, Object> fetchSummary(
            RunnerContext context,
            Path bodyPath,
            FetchedArtifact fetched,
            boolean reused) {
        Map<String, Object> summary = baseSummary("fetch", context);
        summary.put("artifact_uri", bodyPath.toUri().toString());
        summary.put("sha256", fetched.sha256());
        summary.put("idempotent_reuse", reused);
        return summary;
    }

    private static Map<String, Object> baseSummary(String phase, RunnerContext context) {
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("phase", phase);
        summary.put("status", "ok");
        summary.put("run_id", context.runId());
        return summary;
    }

    private Path resolveRunDirectory(RunnerContext context) {
        String rootValue = environment.getOrDefault(
                "USGS_STAGING_ROOT", "/opt/pipeline/staging/usgs");
        Path root = Path.of(rootValue).toAbsolutePath().normalize();
        Path runDirectory = root.resolve(context.runIdPath()).normalize();
        if (!runDirectory.startsWith(root)) {
            throw new IllegalArgumentException("run directory escapes USGS_STAGING_ROOT");
        }
        return runDirectory;
    }

    private static void writeImmutable(Path target, byte[] payload) throws IOException {
        if (Files.exists(target)) {
            requireSameBytes(target, payload);
            return;
        }
        Files.createDirectories(Objects.requireNonNull(target.getParent(), "target parent"));
        Path temporary = Files.createTempFile(target.getParent(), "." + target.getFileName(), ".tmp");
        try {
            Files.write(
                    temporary,
                    payload,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE);
            try {
                Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE);
            } catch (FileAlreadyExistsException exception) {
                requireSameBytes(target, payload);
            } catch (java.nio.file.AtomicMoveNotSupportedException exception) {
                try {
                    Files.move(temporary, target);
                } catch (FileAlreadyExistsException race) {
                    requireSameBytes(target, payload);
                }
            }
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    private static void requireSameBytes(Path target, byte[] expected) throws IOException {
        if (!java.util.Arrays.equals(Files.readAllBytes(target), expected)) {
            throw new IOException("immutable runner artifact differs: " + target.getFileName());
        }
    }

    private static Map<String, List<String>> readHeaders(JsonNode node) {
        Map<String, List<String>> headers = new LinkedHashMap<>();
        node.properties().forEach(entry -> {
            if (entry.getValue().isArray()) {
                headers.put(
                        entry.getKey(),
                        java.util.stream.StreamSupport.stream(
                                        entry.getValue().spliterator(), false)
                                .map(JsonNode::asText)
                                .toList());
            }
        });
        return headers;
    }

    private static void closeObjectStore(BronzeObjectStore objectStore) throws IOException {
        if (objectStore instanceof AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (IOException exception) {
                throw exception;
            } catch (Exception exception) {
                throw new IOException("Bronze object store close failed", exception);
            }
        }
    }

    private static String requiredText(JsonNode node, String field) {
        String value = node.path(field).asText(null);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " is required in runner context/state");
        }
        return value;
    }

    private static String safeLogValue(String value) {
        if (value == null || value.isBlank()) {
            return "unspecified";
        }
        return value.replaceAll("[\\r\\n\\t]", " ").substring(0, Math.min(value.length(), 500));
    }

    private record FetchedArtifact(
            UsgsHttpResponse response,
            Instant retrievedAtUtc,
            String sha256) {
    }

    private record CliArguments(String phase, Path contextFile) {
        private static CliArguments parse(String[] args) {
            String phase = null;
            Path contextFile = null;
            for (int index = 0; index < args.length; index++) {
                switch (args[index]) {
                    case "--phase" -> {
                        if (++index >= args.length) {
                            throw new IllegalArgumentException("--phase requires a value");
                        }
                        phase = args[index];
                    }
                    case "--context-file" -> {
                        if (++index >= args.length) {
                            throw new IllegalArgumentException("--context-file requires a value");
                        }
                        contextFile = Path.of(args[index]);
                    }
                    default -> throw new IllegalArgumentException("unknown runner argument");
                }
            }
            if (phase == null || contextFile == null) {
                throw new IllegalArgumentException("--phase and --context-file are required");
            }
            if (!PHASES.contains(phase)) {
                throw new IllegalArgumentException("unsupported USGS runner phase: " + phase);
            }
            if (!Files.isRegularFile(contextFile)) {
                throw new IllegalArgumentException("context file does not exist or is not a file");
            }
            return new CliArguments(phase, contextFile);
        }
    }

    private record RunnerContext(
            String runId,
            String runIdPath,
            Instant windowStartUtc,
            Instant windowEndUtc,
            Instant targetWindowStartUtc,
            Instant targetWindowEndUtc,
            LocalDate processingDate,
            boolean backfill,
            String logicalRunKey) {

        private static RunnerContext from(JsonNode node) {
            String runId = requiredText(node, "run_id");
            String runIdPath = requiredText(node, "run_id_path");
            if (!SAFE_RUN_ID.matcher(runIdPath).matches()) {
                throw new IllegalArgumentException("run_id_path must be URL-safe");
            }
            Instant windowStart = Instant.parse(requiredText(node, "window_start_utc"));
            Instant windowEnd = Instant.parse(requiredText(node, "window_end_utc"));
            Instant targetStart = Instant.parse(requiredText(node, "target_window_start_utc"));
            Instant targetEnd = Instant.parse(requiredText(node, "target_window_end_utc"));
            if (!windowStart.isBefore(windowEnd)
                    || !targetStart.isBefore(targetEnd)
                    || windowStart.isAfter(targetStart)
                    || !windowEnd.equals(targetEnd)) {
                throw new IllegalArgumentException("runner context windows are inconsistent");
            }
            return new RunnerContext(
                    runId,
                    runIdPath,
                    windowStart,
                    windowEnd,
                    targetStart,
                    targetEnd,
                    LocalDate.parse(requiredText(node, "processing_date")),
                    node.path("is_backfill").asBoolean(false),
                    requiredText(node, "logical_run_key"));
        }
    }
}
