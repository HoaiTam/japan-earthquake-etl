package ie212.earthquake.spark.jma;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.Objects;

/** One controlled row from the JMA archive inventory. */
public record JmaArchiveEntry(
        String inventoryVersion,
        int year,
        String segment,
        String nativeStartJst,
        String nativeEndJst,
        String archiveName,
        String memberName,
        URI sourceUrl,
        String observedReleaseHint,
        String observedLastModifiedUtc,
        Long observedContentLengthBytes,
        String mediaType,
        String recordFormat,
        String catalogEra) {

    public JmaArchiveEntry {
        requireText(inventoryVersion, "inventoryVersion");
        if (year < 1900 || year > 2200) {
            throw new IllegalArgumentException("year is outside the supported range");
        }
        requireText(segment, "segment");
        requireText(nativeStartJst, "nativeStartJst");
        requireText(nativeEndJst, "nativeEndJst");
        requireText(archiveName, "archiveName");
        requireText(memberName, "memberName");
        Objects.requireNonNull(sourceUrl, "sourceUrl");
        requireText(mediaType, "mediaType");
        requireText(recordFormat, "recordFormat");
        requireText(catalogEra, "catalogEra");
        if (observedContentLengthBytes != null && observedContentLengthBytes < 0) {
            throw new IllegalArgumentException("observedContentLengthBytes must be non-negative");
        }
        // Inventory values are parsed here so invalid ranges fail before any download starts.
        OffsetDateTime.parse(nativeStartJst);
        OffsetDateTime.parse(nativeEndJst);
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
    }
}
