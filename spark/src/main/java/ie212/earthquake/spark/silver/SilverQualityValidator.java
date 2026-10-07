package ie212.earthquake.spark.silver;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Deterministic, source-neutral Silver validation and reject accounting. */
public final class SilverQualityValidator {
    private static final List<String> SOURCE_SYSTEMS = List.of("USGS", "JMA_BULLETIN");
    private static final List<String> EVENT_TYPES = List.of("EARTHQUAKE", "ARTIFICIAL", "ERUPTION", "OTHER", "UNKNOWN");
    private static final List<String> JMA_DETERMINATION_FLAGS = List.of("K", "S", "k", "s", "A", "a", "N", "F");
    private final Clock clock;

    public SilverQualityValidator() {
        this(Clock.systemUTC());
    }

    public SilverQualityValidator(Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public SilverQualityResult validate(List<SilverObservation> observations, String ingestRunId) {
        if (observations == null) {
            throw new IllegalArgumentException("observations must not be null");
        }
        if (blank(ingestRunId)) {
            throw new IllegalArgumentException("ingestRunId must not be blank");
        }
        Instant rejectedAtUtc = clock.instant();
        List<SilverObservation> valid = new ArrayList<>();
        List<SilverRejectRecord> rejected = new ArrayList<>();
        Map<String, Long> reasonCounts = new LinkedHashMap<>();
        for (SilverObservation observation : observations) {
            // A null reference has no raw locator to audit; fail closed rather than invent lineage.
            if (observation == null) {
                throw new IllegalArgumentException("observations must contain parsed records with lineage, not null");
            }
            List<String> reasons = reasonsFor(observation, ingestRunId);
            if (reasons.isEmpty()) {
                valid.add(observation);
            } else {
                rejected.add(new SilverRejectRecord(
                        observation.schemaVersion(), observation.sourceSystem(), observation.sourceRecordKey(),
                        observation.bronzeManifestId(), observation.rawObjectUri(), observation.rawSha256(),
                        observation.rawRecordLocator(), observation.rawRecordHash(), "VALIDATE", reasons,
                        observation.ingestRunId(), observation.parserVersion(), rejectedAtUtc));
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

    /** Includes parser rejects in the run gate; validating only observations must not hide failed raw rows. */
    public SilverQualityResult validate(UsgsParseResult parsed, String ingestRunId) {
        Objects.requireNonNull(parsed, "parsed");
        return validateParsed(parsed.observations(), parsed.rejects(), ingestRunId);
    }

    /** JMA parser rejects must also participate in the checked publication gate. */
    public SilverQualityResult validate(JmaParseResult parsed, String ingestRunId) {
        Objects.requireNonNull(parsed, "parsed");
        return validateParsed(parsed.observations(), parsed.rejects(), ingestRunId);
    }

    private SilverQualityResult validateParsed(List<SilverObservation> observations,
            List<SilverRejectRecord> parserRejects, String ingestRunId) {
        SilverQualityResult quality = validate(observations, ingestRunId);
        List<SilverRejectRecord> rejected = new ArrayList<>(parserRejects);
        Map<String, Long> counts = new LinkedHashMap<>(quality.reasonCounts());
        for (SilverRejectRecord record : parserRejects) {
            if (record == null || !ingestRunId.equals(record.ingestRunId())) {
                throw new IllegalArgumentException("parser rejects must belong to the requested ingest run");
            }
            for (String reason : record.rejectReasonCodes().stream().distinct().toList()) {
                counts.merge(reason, 1L, Long::sum);
            }
        }
        rejected.addAll(quality.rejectedRecords());
        return new SilverQualityResult(ingestRunId, observations.size() + parserRejects.size(), quality.validCount(), rejected.size(),
                !rejected.isEmpty(), quality.validObservations(), rejected, counts);
    }

    private List<String> reasonsFor(SilverObservation observation, String expectedRunId) {
        List<String> reasons = new ArrayList<>();
        if (!"1.0".equals(observation.schemaVersion()) || !SOURCE_SYSTEMS.contains(observation.sourceSystem())) {
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
        if (!expectedRunId.equals(observation.ingestRunId())) {
            reasons.add("CONTRACT_MISMATCH");
        }
        if (!List.of("VALID", "WARNING").contains(observation.qualityStatus())) {
            reasons.add("CONTRACT_MISMATCH");
        }
        if ("JMA_BULLETIN".equals(observation.sourceSystem())) {
            if (blank(observation.catalogRelease())
                    || observation.catalogEra() == null
                    || !("LEGACY".equals(observation.catalogEra()) || "UNIFIED".equals(observation.catalogEra()))) {
                reasons.add("CONTRACT_MISMATCH");
            }
            // CON-03 maps JMA determination/quality status to source_status, not a new schema field.
            if (!blank(observation.sourceStatus()) && !JMA_DETERMINATION_FLAGS.contains(observation.sourceStatus())) {
                reasons.add("CONTRACT_MISMATCH");
            }
        } else if (observation.catalogRelease() != null || observation.catalogEra() != null) {
            reasons.add("CONTRACT_MISMATCH");
        }
        if ("USGS".equals(observation.sourceSystem()) && observation.sourceUpdatedAtUtc() == null) {
            reasons.add("CONTRACT_MISMATCH");
        }
        return reasons.stream().distinct().toList();
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
