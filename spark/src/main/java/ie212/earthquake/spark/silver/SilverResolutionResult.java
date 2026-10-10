package ie212.earthquake.spark.silver;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Encapsulates the complete result of Silver entity resolution (SLV-07),
 * containing generated links, canonical memberships, and reconciliation report.
 */
public record SilverResolutionResult(
        List<SilverSourceLink> sourceLinks,
        List<SilverCanonicalMembership> canonicalMemberships,
        SilverMatchReport report) implements Serializable {

    public SilverResolutionResult {
        Objects.requireNonNull(sourceLinks, "sourceLinks");
        Objects.requireNonNull(canonicalMemberships, "canonicalMemberships");
        Objects.requireNonNull(report, "report");
        sourceLinks = Collections.unmodifiableList(sourceLinks);
        canonicalMemberships = Collections.unmodifiableList(canonicalMemberships);
    }

    /**
     * Returns all canonical memberships associated with a given canonical event ID.
     */
    public List<SilverCanonicalMembership> membershipsForEvent(String canonicalEventId) {
        Objects.requireNonNull(canonicalEventId, "canonicalEventId");
        return canonicalMemberships.stream()
                .filter(membership -> canonicalEventId.equals(membership.canonicalEventId()))
                .collect(Collectors.toList());
    }

    /**
     * Returns the set of all unique canonical event IDs produced by this resolution.
     */
    public Set<String> canonicalEventIds() {
        return canonicalMemberships.stream()
                .map(SilverCanonicalMembership::canonicalEventId)
                .collect(Collectors.toSet());
    }
}
