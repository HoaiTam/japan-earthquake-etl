package ie212.earthquake.spark.silver;

import java.time.Instant;
import java.util.Objects;

/**
 * Contextual metadata required for parsing Bronze USGS payloads into Silver observation records.
 */
public record UsgsParseContext(
        String bronzeManifestId,
        String rawObjectUri,
        String rawSha256,
        String ingestRunId,
        Instant processedAtUtc) {

    public UsgsParseContext {
        Objects.requireNonNull(bronzeManifestId, "bronzeManifestId");
        Objects.requireNonNull(rawObjectUri, "rawObjectUri");
        Objects.requireNonNull(rawSha256, "rawSha256");
        Objects.requireNonNull(ingestRunId, "ingestRunId");
        if (processedAtUtc == null) {
            processedAtUtc = Instant.now();
        }
    }

    public static UsgsParseContext of(ResolvedBronzeInput input) {
        return new UsgsParseContext(
                input.manifestId(),
                input.rawObjectUri(),
                input.sha256(),
                input.runId(),
                Instant.now());
    }

    public static UsgsParseContext of(ResolvedBronzeInput input, Instant processedAtUtc) {
        return new UsgsParseContext(
                input.manifestId(),
                input.rawObjectUri(),
                input.sha256(),
                input.runId(),
                processedAtUtc);
    }

    public static UsgsParseContext synthetic(String runId) {
        return new UsgsParseContext(
                "manifest-" + runId,
                "s3://japan-earthquake-bronze/usgs/run_id=" + runId + "/response.geojson",
                "0000000000000000000000000000000000000000000000000000000000000000",
                runId,
                Instant.now());
    }
}
