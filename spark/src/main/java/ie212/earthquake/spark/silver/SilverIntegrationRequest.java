package ie212.earthquake.spark.silver;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/**
 * Request specification for executing the end-to-end multi-source Silver integration pipeline (SLV-09).
 */
public record SilverIntegrationRequest(
        String runId,
        byte[] usgsRawPayload,
        UsgsParseContext usgsContext,
        byte[] jmaRawPayload,
        JmaParseContext jmaContext,
        SilverLinkConfig linkConfig,
        SilverObjectStore objectStore,
        boolean persistOutput,
        Instant executionTimeUtc) implements Serializable {

    public SilverIntegrationRequest {
        Objects.requireNonNull(runId, "runId");
        if (runId.isBlank()) {
            throw new IllegalArgumentException("runId must not be blank");
        }
        if (usgsRawPayload == null && jmaRawPayload == null) {
            throw new IllegalArgumentException("At least one source payload (USGS or JMA) must be provided");
        }
        if (usgsRawPayload != null && usgsContext == null) {
            throw new IllegalArgumentException("usgsContext must be provided when usgsRawPayload is present");
        }
        if (jmaRawPayload != null && jmaContext == null) {
            throw new IllegalArgumentException("jmaContext must be provided when jmaRawPayload is present");
        }
        linkConfig = linkConfig != null ? linkConfig : SilverLinkConfig.defaultConfig();
        executionTimeUtc = executionTimeUtc != null ? executionTimeUtc : Instant.now();
    }
}
