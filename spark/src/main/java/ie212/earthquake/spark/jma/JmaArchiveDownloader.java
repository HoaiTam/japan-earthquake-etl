package ie212.earthquake.spark.jma;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.time.Clock;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.nio.ByteBuffer;
import java.util.concurrent.Flow;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.io.ByteArrayOutputStream;

/**
 * Downloads JMA archives with bounded concurrency and release-aware reruns.
 * JMA-03 owns ZIP/member validation; this class only preserves bytes and
 * download metadata for that writer.
 */
public final class JmaArchiveDownloader {
    private static final ObjectMapper JSON = new ObjectMapper();

    private final JmaArchiveTransport transport;
    private final Path root;
    private final Clock clock;
    private final long maxArchiveBytes;

    public JmaArchiveDownloader(JmaArchiveTransport transport, Path root, Clock clock) {
        this(transport, root, clock, 128L * 1024 * 1024);
    }

    public JmaArchiveDownloader(JmaArchiveTransport transport, Path root, Clock clock, long maxArchiveBytes) {
        this.transport = Objects.requireNonNull(transport, "transport");
        this.root = Objects.requireNonNull(root, "root").toAbsolutePath().normalize();
        this.clock = Objects.requireNonNull(clock, "clock");
        if (maxArchiveBytes < 1 || maxArchiveBytes > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("maxArchiveBytes must fit a positive byte array");
        }
        this.maxArchiveBytes = maxArchiveBytes;
    }

    public List<JmaDownloadResult> downloadInventory(Path inventoryCsv, int maxConcurrency) throws IOException {
        return downloadAll(JmaArchiveInventory.read(inventoryCsv), maxConcurrency);
    }

    public List<JmaDownloadResult> downloadAll(List<JmaArchiveEntry> entries, int maxConcurrency) {
        Objects.requireNonNull(entries, "entries");
        if (maxConcurrency < 1) {
            throw new IllegalArgumentException("maxConcurrency must be positive");
        }
        ExecutorService executor = Executors.newFixedThreadPool(maxConcurrency);
        try {
            List<Future<JmaDownloadResult>> futures = new ArrayList<>();
            for (JmaArchiveEntry entry : entries) {
                futures.add(executor.submit(() -> downloadOne(entry)));
            }
            List<JmaDownloadResult> results = new ArrayList<>();
            for (int index = 0; index < futures.size(); index++) {
                try {
                    results.add(futures.get(index).get());
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    results.add(failure(entries.get(index), exception));
                } catch (ExecutionException exception) {
                    results.add(failure(entries.get(index), exception.getCause()));
                }
            }
            return List.copyOf(results);
        } finally {
            executor.shutdownNow();
        }
    }

    public JmaDownloadResult downloadOne(JmaArchiveEntry entry) {
        return downloadOne(entry, false);
    }

    /** HEAD and checksum-validated local cache only. Null means ingest is needed; never GET here. */
    public JmaDownloadResult probe(JmaArchiveEntry entry) throws IOException {
        try {
            JmaHttpMetadata head = transport.head(entry.sourceUrl());
            validateHead(head);
            return cached(entry, head);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IOException("JMA probe interrupted", exception);
        }
    }

    private void validateHead(JmaHttpMetadata head) throws IOException {
        if (head.statusCode() < 200 || head.statusCode() >= 300) {
            throw new IOException("JMA preflight failed");
        }
        if (head.contentLengthBytes() != null && head.contentLengthBytes() > maxArchiveBytes) {
            throw new IOException("ARCHIVE_SIZE_LIMIT");
        }
    }

    private JmaDownloadResult cached(JmaArchiveEntry entry, JmaHttpMetadata head) throws IOException {
        Path statePath = entryRoot(entry).resolve("state.json");
        JsonNode previous = readJsonIfPresent(statePath);
        if (previous != null && previous.hasNonNull("http_status") && previous.hasNonNull("content_type")
                && entry.sourceUrl().toString().equals(previous.path("source_url").asText())
                && (head.etag() != null || head.lastModified() != null) && samePreflight(previous, head)) {
            Path archive = resolveArchive(previous, entryRoot(entry));
            if (archive != null && Files.isRegularFile(archive)
                    && Files.size(archive) == previous.path("content_length_bytes").asLong(-1L)
                    && sha256(archive).equalsIgnoreCase(previous.path("sha256").asText())) {
                return result(entry, "REUSED", archive, statePath, previous.path("catalog_release").asText(null),
                        previous.path("sha256").asText(null), Files.size(archive), false, true, null,
                        httpFromState(previous), Instant.parse(previous.path("retrieved_at_utc").asText()));
            }
        }
        return null;
    }

    public JmaDownloadResult downloadOne(JmaArchiveEntry entry, boolean forceDownload) {
        Objects.requireNonNull(entry, "entry");
        Path entryRoot = entryRoot(entry);
        Path statePath = entryRoot.resolve("state.json");
        try {
            Files.createDirectories(entryRoot);
            JmaHttpMetadata head = transport.head(entry.sourceUrl());
            validateHead(head);
            JsonNode previous = readJsonIfPresent(statePath);
            JmaDownloadResult reuse = forceDownload ? null : cached(entry, head);
            if (reuse != null) { return reuse; }

            Path part = entryRoot.resolve("archive.zip.part");
            Path partialState = entryRoot.resolve("partial-state.json");
            JsonNode partial = readJsonIfPresent(partialState);
            // Only resume bytes tied to the current source validators, never an untracked/stale prefix.
            if (Files.exists(part) && (forceDownload || partial == null || !samePreflight(partial, head)
                    || !entry.sourceUrl().toString().equals(partial.path("source_url").asText())
                    || (head.etag() == null && head.lastModified() == null))) {
                Files.delete(part);
            }
            ObjectNode partialNode = JSON.createObjectNode();
            partialNode.put("source_url", entry.sourceUrl().toString());
            partialNode.put("content_length_bytes", head.contentLengthBytes());
            putNullable(partialNode, "etag", head.etag());
            putNullable(partialNode, "last_modified", head.lastModified());
            writeJson(partialState, partialNode);
            long offset = Files.exists(part) ? Files.size(part) : 0L;
            if (offset >= maxArchiveBytes || (head.contentLengthBytes() != null && offset >= head.contentLengthBytes())) {
                Files.deleteIfExists(part);
                offset = 0;
            }
            JmaHttpPayload payload = transport.get(entry.sourceUrl(), offset);
            byte[] received = payload.body();
            if (payload.statusCode() < 200 || payload.statusCode() >= 300) {
                throw new IOException("JMA download returned HTTP " + payload.statusCode());
            }
            if (payload.contentLengthBytes() != null && payload.contentLengthBytes() != received.length) {
                throw new IOException("response length does not match Content-Length");
            }
            if (offset > 0 && payload.statusCode() == 206) {
                String range = "bytes " + offset + "-" + (offset + received.length - 1)
                        + "/" + head.contentLengthBytes();
                if (!range.equals(payload.contentRange())
                        || (payload.etag() != null && !Objects.equals(head.etag(), payload.etag()))
                        || (payload.lastModified() != null && !Objects.equals(head.lastModified(), payload.lastModified()))) {
                    Files.deleteIfExists(part);
                    throw new IOException("INVALID_RESUME_RANGE");
                }
                if (offset + received.length > maxArchiveBytes) {
                    throw new IOException("ARCHIVE_SIZE_LIMIT");
                }
                Files.write(part, received, java.nio.file.StandardOpenOption.CREATE,
                        java.nio.file.StandardOpenOption.APPEND);
            } else {
                if (payload.statusCode() == 206) {
                    throw new IOException("UNEXPECTED_PARTIAL_RESPONSE");
                }
                if (received.length > maxArchiveBytes) {
                    throw new IOException("ARCHIVE_SIZE_LIMIT");
                }
                Files.write(part, received, java.nio.file.StandardOpenOption.CREATE,
                        java.nio.file.StandardOpenOption.TRUNCATE_EXISTING);
            }
            long size = Files.size(part);
            if (payload.statusCode() == 206 && (head.contentLengthBytes() == null || head.contentLengthBytes() != size)) {
                throw new IOException("resumed archive length differs from HEAD total");
            }
            if (payload.contentLengthBytes() != null
                    && payload.statusCode() != 206
                    && payload.contentLengthBytes() != size) {
                throw new IOException("downloaded length does not match Content-Length");
            }
            String sha256 = sha256(part);
            String previousSha = previous == null ? null : previous.path("sha256").asText(null);
            Path archive;
            String release;
            if (previousSha != null && previousSha.equals(sha256)) {
                archive = resolveArchive(previous, entryRoot);
                if (archive == null) {
                    throw new IOException("existing state has no archive path");
                }
                if (!Files.isRegularFile(archive) || Files.size(archive) != size || !sha256(archive).equals(sha256)) {
                    moveAtomically(part, archive);
                } else {
                    Files.deleteIfExists(part);
                }
                release = previous.path("catalog_release").asText(null);
            } else {
                release = catalogRelease(payload.lastModified(), sha256);
                Path releaseRoot = entryRoot.resolve("catalog_release=" + release);
                Files.createDirectories(releaseRoot);
                archive = releaseRoot.resolve("archive.zip");
                moveAtomically(part, archive);
            }
            Instant retrievedAt = Instant.now(clock);
            JmaHttpMetadata http = new JmaHttpMetadata(payload.statusCode(), payload.contentType(),
                    payload.statusCode() == 206 ? head.contentLengthBytes() : payload.contentLengthBytes(),
                    payload.etag(), payload.lastModified(), payload.finalUri());
            writeState(statePath, entry, head, http, archive, release, sha256, size, retrievedAt);
            Files.deleteIfExists(partialState);
            return result(entry, "DOWNLOADED", archive, statePath, release, sha256, size,
                    previous != null, previousSha != null && previousSha.equals(sha256), null, http, retrievedAt);
        } catch (Exception exception) {
            if (exception instanceof InterruptedException) { Thread.currentThread().interrupt(); }
            return failure(entry, exception);
        }
    }

    private void writeState(
            Path statePath,
            JmaArchiveEntry entry,
            JmaHttpMetadata head,
            JmaHttpMetadata http,
            Path archive,
            String release,
            String sha256,
            long size, Instant retrievedAt)
            throws IOException {
        ObjectNode node = JSON.createObjectNode();
        node.put("year", entry.year());
        node.put("segment", entry.segment());
        node.put("source_url", entry.sourceUrl().toString());
        node.put("final_url", http.finalUri().toString());
        node.put("http_status", http.statusCode());
        node.put("content_type", http.contentType());
        node.put("http_content_length_bytes", http.contentLengthBytes());
        putNullable(node, "http_etag", http.etag());
        putNullable(node, "http_last_modified", http.lastModified());
        node.put("archive_path", root.relativize(archive).toString().replace('\\', '/'));
        node.put("catalog_release", release);
        node.put("sha256", sha256);
        node.put("content_length_bytes", size);
        putNullable(node, "etag", head.etag());
        putNullable(node, "last_modified", head.lastModified());
        node.put("retrieved_at_utc", DateTimeFormatter.ISO_INSTANT.format(retrievedAt));
        writeJson(statePath, node);
    }

    private static JmaHttpMetadata httpFromState(JsonNode node) {
        return new JmaHttpMetadata(node.path("http_status").asInt(), node.path("content_type").asText(null),
                node.path("http_content_length_bytes").isNull() ? null : node.path("http_content_length_bytes").asLong(),
                node.path("http_etag").asText(null), node.path("http_last_modified").asText(null),
                URI.create(node.path("final_url").asText()));
    }

    private static void writeJson(Path target, JsonNode node) throws IOException {
        Path temporary = Files.createTempFile(target.getParent(), ".state-", ".tmp");
        try {
            Files.writeString(temporary, JSON.writerWithDefaultPrettyPrinter().writeValueAsString(node) + "\n");
            moveAtomically(temporary, target);
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    private boolean samePreflight(JsonNode previous, JmaHttpMetadata current) {
        return sameNullable(previous, "etag", current.etag())
                && sameNullable(previous, "last_modified", current.lastModified())
                && sameNullableLong(previous, "content_length_bytes", current.contentLengthBytes());
    }

    private static boolean sameNullable(JsonNode previous, String field, String current) {
        String old = previous.path(field).isMissingNode() || previous.path(field).isNull()
                ? null : previous.path(field).asText();
        return Objects.equals(old, current);
    }

    private static boolean sameNullableLong(JsonNode previous, String field, Long current) {
        if (current == null || previous.path(field).isMissingNode() || previous.path(field).isNull()) {
            return current == null && (previous.path(field).isMissingNode() || previous.path(field).isNull());
        }
        return previous.path(field).asLong(-1L) == current;
    }

    private JsonNode readJsonIfPresent(Path path) throws IOException {
        return Files.isRegularFile(path) ? JSON.readTree(Files.readAllBytes(path)) : null;
    }

    private Path resolveArchive(JsonNode state, Path entryRoot) {
        String value = state.path("archive_path").asText(null);
        if (value == null || value.isBlank()) {
            return null;
        }
        Path archive = root.resolve(value).normalize();
        return archive.startsWith(root) ? archive : null;
    }

    private Path entryRoot(JmaArchiveEntry entry) {
        return root.resolve("year=" + entry.year()).resolve("segment=" + safe(entry.segment()));
    }

    private static String safe(String value) {
        return value.replaceAll("[^A-Za-z0-9._~-]+", "_");
    }

    private String catalogRelease(String lastModified, String sha256) {
        String prefix = sha256.substring(0, 12);
        Instant timestamp = clock.instant();
        boolean modified = lastModified != null && !lastModified.isBlank();
        if (modified) {
            try {
                timestamp = Instant.parse(lastModified);
            } catch (java.time.format.DateTimeParseException exception) {
                timestamp = ZonedDateTime.parse(lastModified, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant();
            }
        }
        String marker = (modified ? "lm-" : "retrieved-")
                + DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(java.time.ZoneOffset.UTC).format(timestamp);
        return "jma-" + marker + "-sha256-" + prefix;
    }

    private static String sha256(Path path) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (var input = Files.newInputStream(path)) {
                byte[] buffer = new byte[8192];
                int read;
                while ((read = input.read(buffer)) >= 0) {
                    if (read > 0) {
                        digest.update(buffer, 0, read);
                    }
                }
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("JVM does not provide SHA-256", exception);
        }
    }

    private JmaDownloadResult result(
            JmaArchiveEntry entry,
            String status,
            Path archive,
            Path state,
            String release,
            String sha256,
            long size,
            boolean changed,
            boolean reused,
            String error, JmaHttpMetadata http, Instant retrievedAt) {
        return new JmaDownloadResult(entry.year(), entry.segment(), status,
                archive.toString(), state.toString(), release, sha256, size, changed, reused, error, http, retrievedAt);
    }

    private JmaDownloadResult failure(JmaArchiveEntry entry, Throwable exception) {
        return new JmaDownloadResult(entry.year(), entry.segment(), "FAILED", null,
                statePath(entry).toString(), null, null, 0L, true, false,
                exception == null ? "unknown failure" : exception.getMessage());
    }

    private Path statePath(JmaArchiveEntry entry) {
        return entryRoot(entry).resolve("state.json");
    }

    private static void putNullable(ObjectNode node, String field, String value) {
        if (value == null) {
            node.putNull(field);
        } else {
            node.put(field, value);
        }
    }

    private static void moveAtomically(Path source, Path target) throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException exception) {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    /** Production transport using JDK HttpClient and optional HTTP range resume. */
    public static JmaArchiveTransport httpTransport(HttpClient client) {
        return httpTransport(client, Duration.ofSeconds(60), 128L * 1024 * 1024);
    }

    public static JmaArchiveTransport httpTransport(HttpClient client, Duration timeout, long maxBytes) {
        Objects.requireNonNull(client, "client");
        return new JmaArchiveTransport() {
            @Override
            public JmaHttpMetadata head(URI uri) throws IOException, InterruptedException {
                HttpRequest request = HttpRequest.newBuilder(uri).method("HEAD", HttpRequest.BodyPublishers.noBody())
                        .timeout(timeout)
                        .header("Accept", "application/zip")
                        .build();
                HttpResponse<Void> response = client.send(request, HttpResponse.BodyHandlers.discarding());
                return new JmaHttpMetadata(response.statusCode(), response.headers().firstValue("content-type").orElse(null),
                        headerLength(response),
                        response.headers().firstValue("etag").orElse(null),
                        response.headers().firstValue("last-modified").orElse(null), response.uri());
            }

            @Override
            public JmaHttpPayload get(URI uri, long rangeStart) throws IOException, InterruptedException {
                HttpRequest.Builder builder = HttpRequest.newBuilder(uri).GET().timeout(timeout).header("Accept", "application/zip");
                if (rangeStart > 0) {
                    builder.header("Range", "bytes=" + rangeStart + "-");
                }
                HttpResponse<byte[]> response = client.send(builder.build(), info -> new LimitedBody(maxBytes));
                return new JmaHttpPayload(response.statusCode(), response.headers().firstValue("content-type").orElse(null),
                        headerLength(response),
                        response.headers().firstValue("etag").orElse(null),
                        response.headers().firstValue("last-modified").orElse(null), response.uri(), response.body(),
                        response.headers().firstValue("content-range").orElse(null));
            }
        };
    }

    private static final class LimitedBody implements HttpResponse.BodySubscriber<byte[]> {
        private final long limit;
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        private final CompletableFuture<byte[]> result = new CompletableFuture<>();
        private Flow.Subscription subscription;

        LimitedBody(long limit) { this.limit = limit; }
        @Override public CompletionStage<byte[]> getBody() { return result; }
        @Override public void onSubscribe(Flow.Subscription value) {
            subscription = value;
            value.request(1);
        }
        @Override public void onNext(List<ByteBuffer> items) {
            for (ByteBuffer item : items) {
                if ((long) bytes.size() + item.remaining() > limit) {
                    subscription.cancel();
                    result.completeExceptionally(new IOException("ARCHIVE_SIZE_LIMIT"));
                    return;
                }
                byte[] chunk = new byte[item.remaining()];
                item.get(chunk);
                bytes.writeBytes(chunk);
            }
            subscription.request(1);
        }
        @Override public void onError(Throwable error) { result.completeExceptionally(error); }
        @Override public void onComplete() { result.complete(bytes.toByteArray()); }
    }

    private static Long headerLength(HttpResponse<?> response) {
        return response.headers().firstValueAsLong("content-length").isPresent()
                ? response.headers().firstValueAsLong("content-length").getAsLong()
                : null;
    }
}
