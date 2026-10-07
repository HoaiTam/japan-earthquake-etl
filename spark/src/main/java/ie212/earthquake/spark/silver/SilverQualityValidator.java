package ie212.earthquake.spark.silver;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Deterministic, source-neutral Silver validation and reject accounting. */
public final class SilverQualityValidator {
    private static final List<String> SOURCE_SYSTEMS = List.of("USGS", "JMA_BULLETIN");
    private static final List<String> EVENT_TYPES = List.of("EARTHQUAKE", "ARTIFICIAL", "ERUPTION", "OTHER", "UNKNOWN");
    private static final List<String> JMA_DETERMINATION_FLAGS = List.of("K", "S", "k", "s", "A", "a", "N", "F");

    public SilverQualityResult validate(List<SilverObservation> observations, String ingestRunId) {
        if (observations == null) {
            throw new IllegalArgumentException("observations must not be null");
        }
        List<SilverObservation> valid = new ArrayList<>();
        List<SilverRejectRecord> rejected = new ArrayList<>();
        Map<String, Long> reasonCounts = new LinkedHashMap<>();
        for (SilverObservation observation : observations) {
            List<String> reasons = reasonsFor(observation, ingestRunId);
            if (reasons.isEmpty()) {
                valid.add(observation);
            } else {
                rejected.add(new SilverRejectRecord(observation, "VALIDATE", reasons));
                for (String reason : reasons) {
                    reasonCounts.merge(reason, 1L, Long::sum);
                }
            }
        }
        // A rejected row is a publish blocker until the upstream quality policy
        // explicitly supplies a non-blocking warning classification.
        return new SilverQualityResult(ingestRunId, observations.size(), valid.size(), rejected.size(),
                !rejected.isEmpty(), valid, rejected, reasonCounts);
    }

    private List<String> reasonsFor(SilverObservation observation, String expectedRunId) {
        List<String> reasons = new ArrayList<>();
        if (observation == null) {
            reasons.add("CONTRACT_MISMATCH");
            return reasons;
        }
        if (!SOURCE_SYSTEMS.contains(observation.sourceSystem())) {
            reasons.add("CONTRACT_MISMATCH");
        }
        if (blank(observation.sourceRecordKey()) || blank(observation.sourceRevisionKey())) {
            reasons.add("MISSING_SOURCE_KEY");
        }
        if (observation.eventTimeUtc() == null) {
            reasons.add("INVALID_EVENT_TIME");
        }
        if (invalidCoordinate(observation.latitude(), -90.0, 90.0)) {
            reasons.add("INVALID_LATITUDE");
        }
        if (invalidCoordinate(observation.longitude(), -180.0, 180.0)) {
            reasons.add("INVALID_LONGITUDE");
        }
        if (nonFinite(observation.depthKm()) || nonFinite(observation.magnitude())) {
            reasons.add("NON_FINITE_NUMBER");
        }
        if (observation.eventTypeCode() == null || !EVENT_TYPES.contains(observation.eventTypeCode())) {
            reasons.add("UNSUPPORTED_RECORD_TYPE");
        }
        if (blank(observation.bronzeManifestId()) || blank(observation.rawObjectUri())
                || blank(observation.rawSha256()) || blank(observation.rawRecordLocator())
                || blank(observation.rawRecordHash()) || blank(observation.ingestRunId())) {
            reasons.add("CONTRACT_MISMATCH");
        }
        if (expectedRunId != null && !expectedRunId.equals(observation.ingestRunId())) {
            reasons.add("CONTRACT_MISMATCH");
        }
        if ("JMA_BULLETIN".equals(observation.sourceSystem())) {
            if (blank(observation.catalogRelease())
                    || observation.catalogEra() == null
                    || !("LEGACY".equals(observation.catalogEra()) || "UNIFIED".equals(observation.catalogEra()))) {
                reasons.add("CONTRACT_MISMATCH");
            }
            if (observation.determinationFlag() != null
                    && !observation.determinationFlag().isBlank()
                    && !JMA_DETERMINATION_FLAGS.contains(observation.determinationFlag())) {
                reasons.add("CONTRACT_MISMATCH");
            }
        } else if (observation.catalogRelease() != null || observation.catalogEra() != null) {
            reasons.add("CONTRACT_MISMATCH");
        }
        return List.copyOf(reasons);
    }

    private static boolean invalidCoordinate(Double value, double minimum, double maximum) {
        return value == null || !Double.isFinite(value) || value < minimum || value > maximum;
    }

    private static boolean nonFinite(Double value) {
        return value != null && !Double.isFinite(value);
    }

    private static boolean blank(String value) {
        return value == null || value.isBlank();
    }
}
