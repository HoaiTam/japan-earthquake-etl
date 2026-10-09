package ie212.earthquake.spark.silver;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

/**
 * Logical source link record conforming to silver.source_link (CON-03 8.1).
 * Stores link decision, metrics, and confidence score for a candidate pair across sources.
 */
public record SilverSourceLink(
        String sourceLinkId,
        String leftObservationId,
        String rightObservationId,
        double timeDeltaSeconds,
        double distanceKm,
        Double depthDeltaKm,
        Double magnitudeDelta,
        double matchScore,
        String linkDecision,
        List<String> decisionReasonCodes,
        String matchModelVersion,
        Instant decidedAtUtc) implements Serializable {

    public SilverSourceLink {
        Objects.requireNonNull(sourceLinkId, "sourceLinkId");
        Objects.requireNonNull(leftObservationId, "leftObservationId");
        Objects.requireNonNull(rightObservationId, "rightObservationId");
        Objects.requireNonNull(linkDecision, "linkDecision");
        decisionReasonCodes = decisionReasonCodes != null
                ? Collections.unmodifiableList(decisionReasonCodes)
                : List.of();
        Objects.requireNonNull(matchModelVersion, "matchModelVersion");
        Objects.requireNonNull(decidedAtUtc, "decidedAtUtc");
    }

    /**
     * Converts this link into a Spark Row matching SilverSchemas.SOURCE_LINK_SCHEMA.
     */
    public Row toRow() {
        return RowFactory.create(
                sourceLinkId,
                leftObservationId,
                rightObservationId,
                timeDeltaSeconds,
                distanceKm,
                depthDeltaKm,
                magnitudeDelta,
                matchScore,
                linkDecision,
                decisionReasonCodes.toArray(new String[0]),
                matchModelVersion,
                Timestamp.from(decidedAtUtc));
    }
}
