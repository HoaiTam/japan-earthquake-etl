package ie212.earthquake.spark.jma;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
            return new JmaHttpPayload(rangeStart > 0 ? 206 : 200, "application/zip",
                    (long) bytes.length, null, lastModified, uri, bytes);
        }
    }
}
