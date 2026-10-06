package ie212.earthquake.spark.silver;

import java.time.Instant;
import java.util.List;

/** Parser output accepted by the common Silver quality boundary. */
public record SilverObservation(
        String sourceSystem,
        String sourceRecordKey,
        String sourceRevisionKey,
        Instant eventTimeUtc,
        Double latitude,
        Double longitude,
        Double depthKm,
        Double magnitude,
        String eventTypeCode,
        String catalogRelease,
        String catalogEra,
        String determiningAgencyCode,
        String determinationFlag,
        String bronzeManifestId,
        String rawObjectUri,
        String rawSha256,
        String rawRecordLocator,
        String rawRecordHash,
        String ingestRunId,
        List<String> qualityFlags) {

    public SilverObservation {
        qualityFlags = qualityFlags == null ? List.of() : List.copyOf(qualityFlags);
    }
}
