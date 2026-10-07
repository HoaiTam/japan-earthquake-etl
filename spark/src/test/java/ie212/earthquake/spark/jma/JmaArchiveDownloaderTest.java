package ie212.earthquake.spark.jma;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class JmaArchiveDownloaderTest {
    private static final JmaArchiveEntry ENTRY = new JmaArchiveEntry(
            "1.0", 2023, "full-year", "2023-01-01T00:00:00+09:00", "2024-01-01T00:00:00+09:00",
            "h2023.zip", "h2023", URI.create("https://example.invalid/h2023.zip"),
            "lm-20251210T014153Z", "2025-12-10T01:41:53Z", 7L,
            "application/zip", "jma-hypocenter-96-byte-v1", "UNIFIED");

    @Test
    void productionTransportBoundsChunkedResponseAndRequestTimeout() throws Exception {
        var server = com.sun.net.httpserver.HttpServer.create(new java.net.InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/chunked", exchange -> {
            exchange.getResponseHeaders().set("Content-Type", "application/zip");
            exchange.sendResponseHeaders(200, 0); // no fixed length; guard must inspect received chunks
            try (var output = exchange.getResponseBody()) { output.write("too many bytes".getBytes()); }
        });
        server.createContext("/slow", exchange -> {
            try { Thread.sleep(500); } catch (InterruptedException exception) { Thread.currentThread().interrupt(); }
            try {
                exchange.sendResponseHeaders(200, 1);
                exchange.getResponseBody().write('x');
            } catch (java.io.IOException ignored) { /* timed-out client has closed the connection */ }
            finally { exchange.close(); }
        });
        server.start();
        try {
            String base = "http://127.0.0.1:" + server.getAddress().getPort();
            var client = java.net.http.HttpClient.newHttpClient();
            var bounded = JmaArchiveDownloader.httpTransport(client, java.time.Duration.ofSeconds(2), 4);
            assertThrows(java.io.IOException.class, () -> bounded.get(URI.create(base + "/chunked"), 0));
            var complete = JmaArchiveDownloader.httpTransport(client, java.time.Duration.ofSeconds(2), 100)
                    .get(URI.create(base + "/chunked"), 0);
            assertEquals("application/zip", complete.contentType());
            assertArrayEquals("too many bytes".getBytes(), complete.body());
            var timeout = JmaArchiveDownloader.httpTransport(client, java.time.Duration.ofMillis(100), 100);
            assertThrows(java.net.http.HttpTimeoutException.class, () -> timeout.get(URI.create(base + "/slow"), 0));
        } finally { server.stop(0); }
    }

    @Test
    void retainsActualGetMetadataAndLegacyStateRequiresFreshGet(@TempDir Path temp) throws Exception {
        FakeTransport transport = new FakeTransport("first", "2025-12-10T01:41:53Z");
        JmaArchiveDownloader downloader = new JmaArchiveDownloader(transport, temp, Clock.systemUTC());
        JmaDownloadResult first = downloader.downloadOne(ENTRY);
        assertEquals(200, first.http().statusCode());
        assertEquals("application/zip", first.http().contentType());
        assertEquals(5L, first.http().contentLengthBytes());
        var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        var oldState = (com.fasterxml.jackson.databind.node.ObjectNode) mapper.readTree(Files.readAllBytes(Path.of(first.statePath())));
        oldState.remove("http_status"); oldState.remove("content_type");
        Files.write(Path.of(first.statePath()), mapper.writeValueAsBytes(oldState));
        JmaDownloadResult migrated = downloader.downloadOne(ENTRY);
        assertEquals("DOWNLOADED", migrated.status());
        assertEquals(2, transport.getCalls.get());
        assertEquals(200, migrated.http().statusCode());
        assertEquals(first.catalogRelease(), migrated.catalogRelease());
    }

    @Test
    void resumesOnlyTrackedPrefixAndPassesFullLengthFor206(@TempDir Path temp) throws Exception {
        FakeTransport transport = new FakeTransport("first", "2025-12-10T01:41:53Z");
        Path entryRoot = temp.resolve("year=2023/segment=full-year");
        Files.createDirectories(entryRoot);
        Files.writeString(entryRoot.resolve("archive.zip.part"), "fi");
        var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        var state = mapper.createObjectNode().put("source_url", ENTRY.sourceUrl().toString())
                .put("content_length_bytes", 5).put("last_modified", transport.lastModified).putNull("etag");
        Files.write(entryRoot.resolve("partial-state.json"), mapper.writeValueAsBytes(state));
        JmaDownloadResult result = new JmaArchiveDownloader(transport, temp, Clock.systemUTC()).downloadOne(ENTRY);
        assertEquals("DOWNLOADED", result.status());
        assertEquals(206, result.http().statusCode());
        assertEquals(5L, result.http().contentLengthBytes());
        assertArrayEquals("first".getBytes(), Files.readAllBytes(Path.of(result.archivePath())));
        assertFalse(Files.exists(entryRoot.resolve("archive.zip.part")));
    }

    @Test
    void untrackedOrInvalidRangeCannotCorruptResume(@TempDir Path temp) throws Exception {
        FakeTransport transport = new FakeTransport("first", "2025-12-10T01:41:53Z");
        Path entryRoot = temp.resolve("year=2023/segment=full-year");
        Files.createDirectories(entryRoot);
        Files.writeString(entryRoot.resolve("archive.zip.part"), "untracked");
        var downloader = new JmaArchiveDownloader(transport, temp, Clock.systemUTC());
        var complete = downloader.downloadOne(ENTRY);
        assertEquals(200, complete.http().statusCode());
        Files.delete(Path.of(complete.statePath()));
        Files.writeString(entryRoot.resolve("archive.zip.part"), "fi");
        var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        Files.write(entryRoot.resolve("partial-state.json"), mapper.writeValueAsBytes(mapper.createObjectNode()
                .put("source_url", ENTRY.sourceUrl().toString()).put("content_length_bytes", 5)
                .put("last_modified", transport.lastModified).putNull("etag")));
        transport.invalidRange = true;
        assertEquals("FAILED", downloader.downloadOne(ENTRY).status());
        assertFalse(Files.exists(entryRoot.resolve("archive.zip.part")));
        assertEquals("DOWNLOADED", downloader.downloadOne(ENTRY).status());
    }

    @Test
    void sizeGuardsAndForcedRevalidationAreBounded(@TempDir Path temp) {
        FakeTransport transport = new FakeTransport("first", "2025-12-10T01:41:53Z");
        var tooSmall = new JmaArchiveDownloader(transport, temp, Clock.systemUTC(), 4);
        assertEquals("FAILED", tooSmall.downloadOne(ENTRY).status());
        assertEquals(0, transport.getCalls.get());
        var downloader = new JmaArchiveDownloader(transport, temp, Clock.systemUTC(), 20);
        var first = downloader.downloadOne(ENTRY);
        transport.body = "other"; // same validators/size: explicit full GET detects the revision
        var revision = downloader.downloadOne(ENTRY, true);
        assertNotEquals(first.sha256(), revision.sha256());
        assertEquals(2, transport.getCalls.get());
    }

    @Test
    void rerunUsesVerifiedArchiveWithoutBlindDownload(@TempDir Path temp) {
        FakeTransport transport = new FakeTransport("first", "2025-12-10T01:41:53Z");
        JmaArchiveDownloader downloader = new JmaArchiveDownloader(
                transport, temp, Clock.fixed(Instant.parse("2026-10-06T00:00:00Z"), ZoneOffset.UTC));

        JmaDownloadResult first = downloader.downloadOne(ENTRY);
        JmaDownloadResult second = downloader.downloadOne(ENTRY);

        assertEquals("DOWNLOADED", first.status());
        assertEquals("REUSED", second.status());
        assertEquals(1, transport.getCalls.get());
        assertTrue(Files.isRegularFile(Path.of(first.archivePath())));
    }

    @Test
    void changedShaCreatesNewReleaseButSameShaHeaderChangeKeepsRawArchive(@TempDir Path temp) {
        FakeTransport transport = new FakeTransport("first", "2025-12-10T01:41:53Z");
        JmaArchiveDownloader downloader = new JmaArchiveDownloader(transport, temp, Clock.systemUTC());

        JmaDownloadResult first = downloader.downloadOne(ENTRY);
        transport.lastModified = "2026-01-01T00:00:00Z";
        JmaDownloadResult sameBytes = downloader.downloadOne(ENTRY);
        transport.body = "second";
        transport.contentLength = 6L;
        JmaDownloadResult changedBytes = downloader.downloadOne(ENTRY);

        assertEquals(first.sha256(), sameBytes.sha256());
        assertEquals(first.archivePath(), sameBytes.archivePath());
        assertEquals(3, transport.getCalls.get());
        assertNotEquals(first.archivePath(), changedBytes.archivePath());
        assertNotEquals(first.sha256(), changedBytes.sha256());
    }

    @Test
    void oneArchiveFailureDoesNotHideOtherResults(@TempDir Path temp) {
        FakeTransport transport = new FakeTransport("first", "2025-12-10T01:41:53Z");
        JmaArchiveEntry failed = new JmaArchiveEntry("1.0", 2022, "full-year",
                "2022-01-01T00:00:00+09:00", "2023-01-01T00:00:00+09:00", "h2022.zip", "h2022",
                URI.create("https://example.invalid/h2022.zip"), null, null, null,
                "application/zip", "jma-hypocenter-96-byte-v1", "UNIFIED");
        transport.failUri = failed.sourceUrl();

        List<JmaDownloadResult> results = new JmaArchiveDownloader(transport, temp, Clock.systemUTC())
                .downloadAll(List.of(ENTRY, failed), 2);

        assertEquals(2, results.size());
        assertTrue(results.stream().anyMatch(JmaDownloadResult::succeeded));
        assertTrue(results.stream().anyMatch(result -> "FAILED".equals(result.status())));
    }

    private static final class FakeTransport implements JmaArchiveTransport {
        private String body;
        private String lastModified;
        private Long contentLength;
        private URI failUri;
        private boolean invalidRange;
        private final AtomicInteger getCalls = new AtomicInteger();

        private FakeTransport(String body, String lastModified) {
            this.body = body;
            this.lastModified = lastModified;
            this.contentLength = (long) body.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        }

        @Override
        public JmaHttpMetadata head(URI uri) throws java.io.IOException {
            if (uri.equals(failUri)) {
                throw new java.io.IOException("simulated failure");
            }
            return new JmaHttpMetadata(200, "application/zip", contentLength, null, lastModified, uri);
        }

        @Override
        public JmaHttpPayload get(URI uri, long rangeStart) throws java.io.IOException {
            if (uri.equals(failUri)) {
                throw new java.io.IOException("simulated failure");
            }
            getCalls.incrementAndGet();
            byte[] bytes = body.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            byte[] response = java.util.Arrays.copyOfRange(bytes, (int) rangeStart, bytes.length);
            return new JmaHttpPayload(rangeStart > 0 ? 206 : 200, "application/zip",
                    (long) response.length, null, lastModified, uri, response,
                    rangeStart > 0 ? (invalidRange ? "wrong" : "bytes " + rangeStart + "-" + (bytes.length - 1) + "/" + bytes.length) : null);
        }
    }
}
