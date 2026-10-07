package ie212.earthquake.spark.jma;

import java.net.URI;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Objects;

/** Downloaded bytes and provenance for one archive; credentials never belong here. */
public record JmaBronzeWriteRequest(
        JmaArchiveEntry entry, byte[] archive, String expectedSha256, long expectedLengthBytes,
        String catalogRelease, JmaHttpMetadata http, String runId, int attempt,
        LocalDate ingestDateUtc, Instant retrievedAtUtc, boolean backfill,
        String logicalRunKey, int requestTimeoutMs, String stagedObjectUri) {
    public JmaBronzeWriteRequest {
        Objects.requireNonNull(entry, "entry");
        Objects.requireNonNull(archive, "archive");
        Objects.requireNonNull(http, "http");
        Objects.requireNonNull(ingestDateUtc, "ingestDateUtc");
        Objects.requireNonNull(retrievedAtUtc, "retrievedAtUtc");
        archive = archive.clone();
        if (expectedSha256 == null || !expectedSha256.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("expectedSha256 must be a lowercase SHA-256");
        }
        if (expectedLengthBytes < 0 || attempt < 1 || requestTimeoutMs < 1) {
            throw new IllegalArgumentException("length must be non-negative; attempt/timeout must be positive");
        }
        requireSlug(runId, "runId");
        requireSlug(catalogRelease, "catalogRelease");
        requireSlug(entry.segment(), "segment");
        if (!JmaArchiveValidator.safeMember(entry.memberName())
                || !JmaArchiveValidator.safeMember(entry.archiveName())) {
            throw new IllegalArgumentException("inventory archive/member names must be safe");
        }
        requireHttp(entry.sourceUrl());
        requireHttp(http.finalUri());
        if (logicalRunKey == null || logicalRunKey.isBlank()
                || logicalRunKey.chars().anyMatch(Character::isISOControl)) {
            throw new IllegalArgumentException("logicalRunKey must be non-blank without control characters");
        }
        if (!"application/zip".equals(entry.mediaType())
                || !"jma-hypocenter-96-byte-v1".equals(entry.recordFormat())) {
            throw new IllegalArgumentException("unsupported JMA inventory format");
        }
        OffsetDateTime start = OffsetDateTime.parse(entry.nativeStartJst());
        OffsetDateTime end = OffsetDateTime.parse(entry.nativeEndJst());
        LocalDate expectedStart = LocalDate.of(entry.year(), 1, 1);
        LocalDate expectedEnd = expectedStart.plusYears(1);
        String expectedEra = entry.year() < 1997 ? "LEGACY" : "UNIFIED";
        if (entry.year() == 1997 && "jan-sep".equals(entry.segment())) {
            expectedEnd = LocalDate.of(1997, 10, 1);
            expectedEra = "LEGACY";
        } else if (entry.year() == 1997 && "oct-dec".equals(entry.segment())) {
            expectedStart = LocalDate.of(1997, 10, 1);
        } else if (entry.year() == 1997 || !"full-year".equals(entry.segment())) {
            throw new IllegalArgumentException("inventory segment must be full-year or one of the two 1997 segments");
        }
        if (!start.getOffset().equals(ZoneOffset.ofHours(9))
                || !end.getOffset().equals(ZoneOffset.ofHours(9))
                || !start.toLocalDate().equals(expectedStart) || !end.toLocalDate().equals(expectedEnd)
                || !start.toLocalTime().equals(LocalTime.MIDNIGHT) || !end.toLocalTime().equals(LocalTime.MIDNIGHT)
                || !expectedEra.equals(entry.catalogEra())) {
            throw new IllegalArgumentException("native interval/era must match the JMA inventory year and segment");
        }
        if (stagedObjectUri != null) {
            URI staged = URI.create(stagedObjectUri);
            if (!("file".equals(staged.getScheme()) || "s3".equals(staged.getScheme()))
                    || staged.getUserInfo() != null || staged.getQuery() != null || staged.getFragment() != null
                    || staged.getPath() == null || !staged.getPath().startsWith("/")
                    || ("s3".equals(staged.getScheme()) && staged.getHost() == null)) {
                throw new IllegalArgumentException("stagedObjectUri must be a file/S3 URI without credentials/query/fragment");
            }
        }
    }

    @Override
    public byte[] archive() {
        return archive.clone();
    }

    private static void requireSlug(String value, String field) {
        if (!JmaArchiveValidator.safeMember(value)) {
            throw new IllegalArgumentException(field + " must be a URL-safe slug without separators or '..'");
        }
    }

    private static void requireHttp(URI uri) {
        if (uri == null || !("http".equals(uri.getScheme()) || "https".equals(uri.getScheme()))
                || uri.getHost() == null || uri.getUserInfo() != null || uri.getQuery() != null
                || uri.getFragment() != null) {
            throw new IllegalArgumentException("source/final URL must be HTTP(S) without credentials/query/fragment");
        }
    }
}
