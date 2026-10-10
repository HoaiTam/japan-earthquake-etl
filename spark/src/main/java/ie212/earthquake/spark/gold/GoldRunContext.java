package ie212.earthquake.spark.gold;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

/** Shared immutable run identity. Scope is resolved by the caller, never by scanning a prefix. */
public record GoldRunContext(String runId, Instant windowStartUtc, Instant windowEndUtc,
        LocalDate processingDate, boolean isBackfill, String configVersion, List<String> inputManifestUris) {
    public GoldRunContext {
        if (runId == null || runId.isBlank() || configVersion == null || configVersion.isBlank()
                || processingDate == null || windowStartUtc == null || windowEndUtc == null
                || !windowStartUtc.isBefore(windowEndUtc) || inputManifestUris == null || inputManifestUris.isEmpty()) {
            throw new IllegalArgumentException("invalid Gold run context");
        }
        inputManifestUris = List.copyOf(inputManifestUris);
        if (inputManifestUris.stream().anyMatch(uri -> uri == null || uri.isBlank()
                || uri.contains("*") || uri.contains("?") || uri.contains("@"))) {
            throw new IllegalArgumentException("input manifests must be exact safe references");
        }
        if (inputManifestUris.stream().distinct().count() != inputManifestUris.size()) {
            throw new IllegalArgumentException("duplicate input manifest");
        }
    }
}
