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

    public JmaArchiveDownloader(JmaArchiveTransport transport, Path root, Clock clock) {
        this.transport = Objects.requireNonNull(transport, "transport");
        this.root = Objects.requireNonNull(root, "root").toAbsolutePath().normalize();
        this.clock = Objects.requireNonNull(clock, "clock");
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
        Objects.requireNonNull(entry, "entry");
        Path entryRoot = entryRoot(entry);
        Path statePath = entryRoot.resolve("state.json");
        try {
            Files.createDirectories(entryRoot);
            JmaHttpMetadata head = transport.head(entry.sourceUrl());
            if (head.statusCode() < 200 || head.statusCode() >= 300) {
                throw new IOException("JMA preflight returned HTTP " + head.statusCode());
            }
            JsonNode previous = readJsonIfPresent(statePath);
            if (previous != null && samePreflight(previous, head)) {
                Path archive = resolveArchive(previous, entryRoot);
                if (archive != null && Files.isRegularFile(archive)
                        && Files.size(archive) == previous.path("content_length_bytes").asLong(-1L)
                        && sha256(archive).equalsIgnoreCase(previous.path("sha256").asText())) {
                    return result(entry, "REUSED", archive, statePath, previous.path("catalog_release").asText(null),
                            previous.path("sha256").asText(null), Files.size(archive), false, true, null);
                }
            }

            Path part = entryRoot.resolve("archive.zip.part");
            long offset = Files.exists(part) ? Files.size(part) : 0L;
            JmaHttpPayload payload = transport.get(entry.sourceUrl(), offset);
            if (payload.statusCode() < 200 || payload.statusCode() >= 300) {
                throw new IOException("JMA download returned HTTP " + payload.statusCode());
            }
            if (offset > 0 && payload.statusCode() == 206) {
                Files.write(part, payload.body(), java.nio.file.StandardOpenOption.CREATE,
                        java.nio.file.StandardOpenOption.APPEND);
            } else {
                Files.write(part, payload.body(), java.nio.file.StandardOpenOption.CREATE,
                        java.nio.file.StandardOpenOption.TRUNCATE_EXISTING);
            }
            long size = Files.size(part);
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
                Files.deleteIfExists(part);
                release = previous.path("catalog_release").asText(null);
            } else {
                release = catalogRelease(payload.lastModified(), sha256);
                Path releaseRoot = entryRoot.resolve("catalog_release=" + release);
                Files.createDirectories(releaseRoot);
                archive = releaseRoot.resolve("archive.zip");
                moveAtomically(part, archive);
            }
            writeState(statePath, entry, head, payload, archive, release, sha256, size);
            return result(entry, "DOWNLOADED", archive, statePath, release, sha256, size,
                    previous != null, previousSha != null && previousSha.equals(sha256), null);
        } catch (Exception exception) {
            return failure(entry, exception);
        }
    }

    private void writeState(
            Path statePath,
            JmaArchiveEntry entry,
            JmaHttpMetadata head,
            JmaHttpPayload payload,
            Path archive,
            String release,
            String sha256,
            long size)
            throws IOException {
        ObjectNode node = JSON.createObjectNode();
        node.put("year", entry.year());
        node.put("segment", entry.segment());
        node.put("source_url", entry.sourceUrl().toString());
        node.put("final_url", (payload.finalUri() == null ? head.finalUri() : payload.finalUri()).toString());
        node.put("archive_path", root.relativize(archive).toString().replace('\\', '/'));
        node.put("catalog_release", release);
        node.put("sha256", sha256);
        node.put("content_length_bytes", size);
        putNullable(node, "etag", payload.etag() == null ? head.etag() : payload.etag());
        putNullable(node, "last_modified", payload.lastModified() == null ? head.lastModified() : payload.lastModified());
        node.put("retrieved_at_utc", DateTimeFormatter.ISO_INSTANT.format(Instant.now(clock)));
        Path temporary = statePath.resolveSibling(".state.json.tmp");
        Files.writeString(temporary, JSON.writerWithDefaultPrettyPrinter().writeValueAsString(node) + "\n");
        moveAtomically(temporary, statePath);
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

    private static String catalogRelease(String lastModified, String sha256) {
        String prefix = sha256.substring(0, 12);
        String marker = lastModified == null || lastModified.isBlank()
                ? "retrieved-unknown"
                : "lm-" + lastModified.replaceAll("[^0-9A-Za-z]", "");
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
            String error) {
        return new JmaDownloadResult(entry.year(), entry.segment(), status,
                archive.toString(), state.toString(), release, sha256, size, changed, reused, error);
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
        Objects.requireNonNull(client, "client");
        return new JmaArchiveTransport() {
            @Override
            public JmaHttpMetadata head(URI uri) throws IOException, InterruptedException {
                HttpRequest request = HttpRequest.newBuilder(uri).method("HEAD", HttpRequest.BodyPublishers.noBody())
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
                HttpRequest.Builder builder = HttpRequest.newBuilder(uri).GET().header("Accept", "application/zip");
                if (rangeStart > 0) {
                    builder.header("Range", "bytes=" + rangeStart + "-");
                }
                HttpResponse<byte[]> response = client.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());
                return new JmaHttpPayload(response.statusCode(), response.headers().firstValue("content-type").orElse(null),
                        headerLength(response),
                        response.headers().firstValue("etag").orElse(null),
                        response.headers().firstValue("last-modified").orElse(null), response.uri(), response.body());
            }
        };
    }

    private static Long headerLength(HttpResponse<?> response) {
        return response.headers().firstValueAsLong("content-length").isPresent()
                ? response.headers().firstValueAsLong("content-length").getAsLong()
                : null;
    }
}
