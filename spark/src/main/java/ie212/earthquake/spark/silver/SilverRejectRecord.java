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
 * Logical reject record conforming to silver.reject_record (CON-03 1.0).
 * Stores reason codes and locators for raw records that failed validation/parsing.
 */
public record SilverRejectRecord(
        String schemaVersion,
        String sourceSystem,
        String sourceRecordKeyCandidate,
        String bronzeManifestId,
        String rawObjectUri,
        String rawSha256,
        String rawRecordLocator,
        String rawRecordHash,
        String rejectStage,
        List<String> rejectReasonCodes,
        String ingestRunId,
        String parserVersion,
        Instant rejectedAtUtc) implements Serializable {

    public SilverRejectRecord {
        Objects.requireNonNull(schemaVersion, "schemaVersion");
        Objects.requireNonNull(sourceSystem, "sourceSystem");
        Objects.requireNonNull(bronzeManifestId, "bronzeManifestId");
        Objects.requireNonNull(rawObjectUri, "rawObjectUri");
        Objects.requireNonNull(rawSha256, "rawSha256");
        Objects.requireNonNull(rawRecordLocator, "rawRecordLocator");
        Objects.requireNonNull(rawRecordHash, "rawRecordHash");
        Objects.requireNonNull(rejectStage, "rejectStage");
        Objects.requireNonNull(rejectReasonCodes, "rejectReasonCodes");
        rejectReasonCodes = Collections.unmodifiableList(rejectReasonCodes);
        Objects.requireNonNull(ingestRunId, "ingestRunId");
        Objects.requireNonNull(parserVersion, "parserVersion");
        Objects.requireNonNull(rejectedAtUtc, "rejectedAtUtc");
    }

    /**
     * Converts this reject record into a Spark Row matching SilverSchemas.REJECT_SCHEMA.
     */
    public Row toRow() {
        return RowFactory.create(
                schemaVersion,
                sourceSystem,
                sourceRecordKeyCandidate,
                bronzeManifestId,
                rawObjectUri,
                rawSha256,
                rawRecordLocator,
                rawRecordHash,
                rejectStage,
                rejectReasonCodes.toArray(new String[0]),
                ingestRunId,
                parserVersion,
                Timestamp.from(rejectedAtUtc));
    }
}
