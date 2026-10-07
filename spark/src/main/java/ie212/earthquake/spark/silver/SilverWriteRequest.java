package ie212.earthquake.spark.silver;

import java.io.Serializable;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Write request for Silver Parquet persistence (CON-03 1.0, SLV-08).
 */
public record SilverWriteRequest(
        String runId,
        List<SilverObservation> observations,
        List<SilverRejectRecord> rejects,
        Instant publishedAtUtc,
        boolean overwritePartition) implements Serializable {

    public SilverWriteRequest {
        Objects.requireNonNull(runId, "runId");
        if (runId.isBlank()) {
            throw new IllegalArgumentException("runId must not be blank");
        }
        observations = observations != null ? Collections.unmodifiableList(observations) : List.of();
        rejects = rejects != null ? Collections.unmodifiableList(rejects) : List.of();
        publishedAtUtc = publishedAtUtc != null ? publishedAtUtc : Instant.now();
    }

    public static SilverWriteRequest of(String runId, List<SilverObservation> observations) {
        return new SilverWriteRequest(runId, observations, List.of(), Instant.now(), true);
    }

    public static SilverWriteRequest of(
            String runId,
            List<SilverObservation> observations,
            List<SilverRejectRecord> rejects) {
        return new SilverWriteRequest(runId, observations, rejects, Instant.now(), true);
    }
}
