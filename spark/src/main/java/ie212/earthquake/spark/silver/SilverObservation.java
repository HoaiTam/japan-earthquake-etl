package ie212.earthquake.spark.silver;

import java.io.Serializable;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

/**
 * Logical observation record conforming to silver.source_observation (CON-03 1.0).
 * Stores one revision of an observation parsed from an upstream source system.
 */
public record SilverObservation(
        String schemaVersion,
        String sourceObservationId,
        String sourceSystem,
        String sourceRecordKey,
        String sourceRevisionKey,
        Instant sourceUpdatedAtUtc,
        String catalogRelease,
        Instant catalogReleaseAtUtc,
        boolean isCurrentSourceRevision,
        Instant eventTimeUtc,
        LocalDateTime eventTimeJst,
        LocalDate eventDateUtc,
        LocalDate eventDateJst,
        int eventYearUtc,
        int eventMonthUtc,
        double latitude,
        double longitude,
        Double depthKm,
        Double magnitude,
        String magnitudeType,
        String eventTypeCode,
        String placeName,
        Boolean tsunamiFlag,
        String alertLevel,
        Integer significance,
        String maxIntensityCode,
        String determiningAgencyCode,
        String catalogEra,
        String sourceStatus,
        String sourceUrl,
        boolean isInStudyArea,
        String qualityStatus,
        List<String> qualityFlags,
        String bronzeManifestId,
        String rawObjectUri,
        String rawSha256,
        String rawRecordLocator,
        String rawRecordHash,
        String ingestRunId,
        String parserName,
        String parserVersion,
        Instant processedAtUtc) implements Serializable {

    public SilverObservation {
        Objects.requireNonNull(schemaVersion, "schemaVersion");
        Objects.requireNonNull(sourceObservationId, "sourceObservationId");
        Objects.requireNonNull(sourceSystem, "sourceSystem");
        Objects.requireNonNull(sourceRecordKey, "sourceRecordKey");
        Objects.requireNonNull(sourceRevisionKey, "sourceRevisionKey");
        Objects.requireNonNull(eventTimeUtc, "eventTimeUtc");
        Objects.requireNonNull(eventTimeJst, "eventTimeJst");
        Objects.requireNonNull(eventDateUtc, "eventDateUtc");
        Objects.requireNonNull(eventDateJst, "eventDateJst");
        Objects.requireNonNull(eventTypeCode, "eventTypeCode");
        Objects.requireNonNull(qualityStatus, "qualityStatus");
        qualityFlags = qualityFlags != null ? Collections.unmodifiableList(qualityFlags) : List.of();
        Objects.requireNonNull(bronzeManifestId, "bronzeManifestId");
        Objects.requireNonNull(rawObjectUri, "rawObjectUri");
        Objects.requireNonNull(rawSha256, "rawSha256");
        Objects.requireNonNull(rawRecordLocator, "rawRecordLocator");
        Objects.requireNonNull(rawRecordHash, "rawRecordHash");
        Objects.requireNonNull(ingestRunId, "ingestRunId");
        Objects.requireNonNull(parserName, "parserName");
        Objects.requireNonNull(parserVersion, "parserVersion");
        Objects.requireNonNull(processedAtUtc, "processedAtUtc");
    }

    /**
     * Converts this observation into a Spark Row matching SilverSchemas.OBSERVATION_SCHEMA.
     */
    public Row toRow() {
        return RowFactory.create(
                schemaVersion,
                sourceObservationId,
                sourceSystem,
                sourceRecordKey,
                sourceRevisionKey,
                sourceUpdatedAtUtc != null ? Timestamp.from(sourceUpdatedAtUtc) : null,
                catalogRelease,
                catalogReleaseAtUtc != null ? Timestamp.from(catalogReleaseAtUtc) : null,
                isCurrentSourceRevision,
                Timestamp.from(eventTimeUtc),
                Timestamp.valueOf(eventTimeJst),
                Date.valueOf(eventDateUtc),
                Date.valueOf(eventDateJst),
                eventYearUtc,
                eventMonthUtc,
                latitude,
                longitude,
                depthKm,
                magnitude,
                magnitudeType,
                eventTypeCode,
                placeName,
                tsunamiFlag,
                alertLevel,
                significance,
                maxIntensityCode,
                determiningAgencyCode,
                catalogEra,
                sourceStatus,
                sourceUrl,
                isInStudyArea,
                qualityStatus,
                qualityFlags.toArray(new String[0]),
                bronzeManifestId,
                rawObjectUri,
                rawSha256,
                rawRecordLocator,
                rawRecordHash,
                ingestRunId,
                parserName,
                parserVersion,
                Timestamp.from(processedAtUtc));
    }
}
