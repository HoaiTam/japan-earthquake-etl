package ie212.earthquake.spark.silver;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Result of parsing a USGS GeoJSON payload, containing valid observations and rejected records.
 */
public record UsgsParseResult(
        List<SilverObservation> observations,
        List<SilverRejectRecord> rejects) {

    public UsgsParseResult {
        Objects.requireNonNull(observations, "observations");
        Objects.requireNonNull(rejects, "rejects");
        observations = Collections.unmodifiableList(observations);
        rejects = Collections.unmodifiableList(rejects);
    }

    /**
     * Total records parsed: validCount + rejectedCount.
     */
    public int parsedCount() {
        return observations.size() + rejects.size();
    }

    /**
     * Total valid observations successfully normalized.
     */
    public int validCount() {
        return observations.size();
    }

    /**
     * Total records rejected due to validation or schema errors.
     */
    public int rejectedCount() {
        return rejects.size();
    }
}
