package ie212.earthquake.spark.silver;

import java.io.Serializable;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Write request for Silver Parquet persistence (CON-03 1.0, SLV-08, SLV-09).
 * Encapsulates observations, rejects, cross-source links, and canonical memberships.
 */
public record SilverWriteRequest(
        String runId,
        List<SilverObservation> observations,
        List<SilverRejectRecord> rejects,
        List<SilverSourceLink> links,
        List<SilverCanonicalMembership> memberships,
        Instant publishedAtUtc,
        boolean overwritePartition) implements Serializable {

    public SilverWriteRequest {
        Objects.requireNonNull(runId, "runId");
        if (runId.isBlank()) {
            throw new IllegalArgumentException("runId must not be blank");
        }
        observations = observations != null ? List.copyOf(observations) : List.of();
        rejects = rejects != null ? List.copyOf(rejects) : List.of();
        links = links != null ? List.copyOf(links) : List.of();
        memberships = memberships != null ? List.copyOf(memberships) : List.of();
        publishedAtUtc = publishedAtUtc != null ? publishedAtUtc : Instant.now();
    }

    public SilverWriteRequest(
            String runId,
            List<SilverObservation> observations,
            List<SilverRejectRecord> rejects,
            Instant publishedAtUtc,
            boolean overwritePartition) {
        this(runId, observations, rejects, List.of(), List.of(), publishedAtUtc, overwritePartition);
    }

    public static SilverWriteRequest of(String runId, List<SilverObservation> observations) {
        return new SilverWriteRequest(runId, observations, List.of(), List.of(), List.of(), Instant.now(), true);
    }

    public static SilverWriteRequest of(
            String runId,
            List<SilverObservation> observations,
            List<SilverRejectRecord> rejects) {
        return new SilverWriteRequest(runId, observations, rejects, List.of(), List.of(), Instant.now(), true);
    }

    public static SilverWriteRequest of(
            String runId,
            List<SilverObservation> observations,
            List<SilverRejectRecord> rejects,
            List<SilverSourceLink> links,
            List<SilverCanonicalMembership> memberships) {
        return new SilverWriteRequest(runId, observations, rejects, links, memberships, Instant.now(), true);
    }
}
