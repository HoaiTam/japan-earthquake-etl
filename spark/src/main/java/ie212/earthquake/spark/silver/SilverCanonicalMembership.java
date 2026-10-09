package ie212.earthquake.spark.silver;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Objects;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

/**
 * Logical canonical membership record conforming to silver.canonical_membership (CON-03 8.2).
 * Maps source observations to unified, deduplicated canonical earthquake events.
 */
public record SilverCanonicalMembership(
        String canonicalEventId,
        String sourceObservationId,
        String membershipStatus,
        String sourceLinkId,
        String canonicalModelVersion,
        Instant assignedAtUtc) implements Serializable {

    public SilverCanonicalMembership {
        Objects.requireNonNull(canonicalEventId, "canonicalEventId");
        Objects.requireNonNull(sourceObservationId, "sourceObservationId");
        Objects.requireNonNull(membershipStatus, "membershipStatus");
        if (!"PRIMARY".equals(membershipStatus) && !"SUPPORTING".equals(membershipStatus)) {
            throw new IllegalArgumentException("membershipStatus must be PRIMARY or SUPPORTING, got: " + membershipStatus);
        }
        Objects.requireNonNull(canonicalModelVersion, "canonicalModelVersion");
        Objects.requireNonNull(assignedAtUtc, "assignedAtUtc");
    }

    /**
     * Converts this membership record into a Spark Row matching SilverSchemas.CANONICAL_MEMBERSHIP_SCHEMA.
     */
    public Row toRow() {
        return RowFactory.create(
                canonicalEventId,
                sourceObservationId,
                membershipStatus,
                sourceLinkId,
                canonicalModelVersion,
                Timestamp.from(assignedAtUtc));
    }
}
