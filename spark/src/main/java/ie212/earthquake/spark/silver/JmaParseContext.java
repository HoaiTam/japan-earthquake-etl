package ie212.earthquake.spark.silver;

import java.net.URI;
import java.time.Instant;
import java.util.Objects;

/** Exact Bronze lineage and inventory metadata; never infer a release from a filename. */
public record JmaParseContext(
        String bronzeManifestId,
        String rawObjectUri,
        String rawSha256,
        String ingestRunId,
        String catalogRelease,
        Instant catalogReleaseAtUtc,
        String sourceUrl,
        String memberName,
        Instant processedAtUtc) {

    public JmaParseContext {
        requireText(bronzeManifestId, "bronzeManifestId");
        requireText(rawObjectUri, "rawObjectUri");
        requireText(ingestRunId, "ingestRunId");
        requireText(catalogRelease, "catalogRelease");
        if (rawSha256 == null || !rawSha256.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("rawSha256 must be a lowercase SHA-256 digest");
        }
        if (memberName == null || !memberName.matches("[A-Za-z0-9][A-Za-z0-9._~-]*")
                || memberName.contains("..")) {
            throw new IllegalArgumentException("memberName must be one safe inventory filename");
        }
        URI raw = URI.create(rawObjectUri);
        if (!("file".equals(raw.getScheme()) || "s3".equals(raw.getScheme()))
                || raw.getPath() == null || !raw.getPath().startsWith("/")
                || ("s3".equals(raw.getScheme()) && raw.getHost() == null)
                || raw.getUserInfo() != null || raw.getQuery() != null || raw.getFragment() != null) {
            throw new IllegalArgumentException("rawObjectUri must be a credential-free file/S3 URI");
        }
        if (sourceUrl != null) {
            URI url = URI.create(sourceUrl);
            if (!("https".equals(url.getScheme()) || "http".equals(url.getScheme()))
                    || url.getHost() == null || url.getUserInfo() != null
                    || url.getQuery() != null || url.getFragment() != null) {
                throw new IllegalArgumentException("sourceUrl must be a public HTTP(S) archive URL");
            }
        }
        Objects.requireNonNull(processedAtUtc, "processedAtUtc");
    }

    public static JmaParseContext of(ResolvedBronzeInput input, String memberName,
            String sourceUrl, Instant processedAtUtc) {
        Objects.requireNonNull(input, "input");
        if (!JmaFixedWidthParser.SOURCE_SYSTEM.equals(input.sourceSystem())) {
            throw new IllegalArgumentException("JMA parser requires JMA_BULLETIN input");
        }
        // SLV-01's staging manifest does not carry release time or inventory member/URL.
        // Pass member/URL explicitly from the matching inventory; unknown release time stays null.
        return new JmaParseContext(input.manifestId(), input.rawObjectUri(), input.sha256(),
                input.runId(), input.catalogRelease(), null, sourceUrl, memberName, processedAtUtc);
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank() || !value.equals(value.trim())
                || value.chars().anyMatch(Character::isISOControl)) {
            throw new IllegalArgumentException(field + " must be nonblank, trimmed and control-free");
        }
    }
}
