package ie212.earthquake.spark.silver;

import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Objects;

/**
 * Encapsulates and validates the complete audit lineage of an observation back to its Bronze origin (SLV-04).
 */
public record SilverLineage(
        String sourceSystem,
        String bronzeManifestId,
        String rawObjectUri,
        String rawSha256,
        String rawRecordLocator,
        String rawRecordHash,
        String ingestRunId,
        String parserName,
        String parserVersion,
        Instant processedAtUtc) implements Serializable {

    public SilverLineage {
        Objects.requireNonNull(sourceSystem, "sourceSystem");
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
     * Extracts lineage from a SilverObservation.
     */
    public static SilverLineage of(SilverObservation observation) {
        Objects.requireNonNull(observation, "observation");
        return new SilverLineage(
                observation.sourceSystem(),
                observation.bronzeManifestId(),
                observation.rawObjectUri(),
                observation.rawSha256(),
                observation.rawRecordLocator(),
                observation.rawRecordHash(),
                observation.ingestRunId(),
                observation.parserName(),
                observation.parserVersion(),
                observation.processedAtUtc());
    }

    /**
     * Extracts lineage from a SilverRejectRecord.
     */
    public static SilverLineage of(SilverRejectRecord reject) {
        Objects.requireNonNull(reject, "reject");
        return new SilverLineage(
                reject.sourceSystem(),
                reject.bronzeManifestId(),
                reject.rawObjectUri(),
                reject.rawSha256(),
                reject.rawRecordLocator(),
                reject.rawRecordHash(),
                reject.ingestRunId(),
                "reject-handler",
                reject.parserVersion(),
                reject.rejectedAtUtc());
    }

    /**
     * Verifies that the raw record bytes match the recorded rawRecordHash.
     */
    public boolean verifyRawRecord(byte[] rawRecordBytes) {
        if (rawRecordBytes == null) {
            return false;
        }
        return rawRecordHash.equalsIgnoreCase(digest(rawRecordBytes));
    }

    /**
     * Verifies that the raw object bytes match the recorded rawSha256.
     */
    public boolean verifyRawObject(byte[] objectBytes) {
        if (objectBytes == null) {
            return false;
        }
        return rawSha256.equalsIgnoreCase(digest(objectBytes));
    }

    private static String digest(byte[] data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(data));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("JVM does not support SHA-256", e);
        }
    }
}
