package ie212.earthquake.spark.silver;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/**
 * Versioned, deterministic key and ID generator for Silver observations and Canonical events (SLV-04).
 * Ensures source keys and canonical IDs remain stable across reruns, revisions, and upstream changes.
 */
public final class SourceKeyGenerator {
    public static final String JMA_KEY_VERSION = "jma_k1";

    private SourceKeyGenerator() {
    }

    /**
     * Generates a USGS source record key from a feature id.
     */
    public static String usgsRecordKey(String featureId) {
        if (featureId == null || featureId.isBlank()) {
            throw new IllegalArgumentException("USGS feature id cannot be null or blank");
        }
        return featureId.trim();
    }

    /**
     * Generates a USGS source revision key combining updated timestamp and raw record hash.
     */
    public static String usgsRevisionKey(Long updatedMillis, String rawRecordHash) {
        Objects.requireNonNull(rawRecordHash, "rawRecordHash");
        return (updatedMillis != null ? updatedMillis : "null") + ":" + rawRecordHash;
    }

    /**
     * Generates a deterministic JMA source record key from official identity fields:
     * agency, origin time in JST, latitude degree/minute, and longitude degree/minute.
     * Versioned algorithm (jma_k1) produces stable keys across revisions and reruns.
     */
    public static String jmaRecordKey(String agency, String originTimeJst, String latDegMin, String lonDegMin) {
        Objects.requireNonNull(agency, "agency");
        Objects.requireNonNull(originTimeJst, "originTimeJst");
        Objects.requireNonNull(latDegMin, "latDegMin");
        Objects.requireNonNull(lonDegMin, "lonDegMin");

        String seed = agency.trim() + "|" + originTimeJst.trim() + "|" + latDegMin.trim() + "|" + lonDegMin.trim();
        return JMA_KEY_VERSION + "_" + sha256(seed).substring(0, 24);
    }

    /**
     * Generates a JMA source record key directly from a 96-byte hypocenter line.
     * Uses official hypocenter columns: agency (col 1), origin time (cols 2-17),
     * latitude (cols 22-28), and longitude (cols 33-40).
     */
    public static String jmaRecordKeyFromLine(String line) {
        if (line == null || line.length() < 40) {
            throw new IllegalArgumentException("JMA hypocenter line must be at least 40 characters to extract identity");
        }
        String agency = line.substring(0, 1).trim();
        String originJst = line.substring(1, 17).trim();
        String latDegMin = line.substring(21, 28).trim();
        String lonDegMin = line.substring(32, 40).trim();
        return jmaRecordKey(agency, originJst, latDegMin, lonDegMin);
    }

    /**
     * Generates a JMA source revision key combining catalog release and raw record hash.
     */
    public static String jmaRevisionKey(String catalogRelease, String rawRecordHash) {
        Objects.requireNonNull(rawRecordHash, "rawRecordHash");
        String release = (catalogRelease != null && !catalogRelease.isBlank()) ? catalogRelease.trim() : "unversioned";
        return release + ":" + rawRecordHash;
    }

    /**
     * Generates the unique source_observation_id according to CON-03:
     * obs_ + sha256(source_system | source_record_key | source_revision_key)
     */
    public static String observationId(String sourceSystem, String sourceRecordKey, String sourceRevisionKey) {
        Objects.requireNonNull(sourceSystem, "sourceSystem");
        Objects.requireNonNull(sourceRecordKey, "sourceRecordKey");
        Objects.requireNonNull(sourceRevisionKey, "sourceRevisionKey");
        return "obs_" + sha256(sourceSystem + "|" + sourceRecordKey + "|" + sourceRevisionKey);
    }

    /**
     * Generates an opaque, stable canonical event ID seeded from primary source identity:
     * evt_ + sha256(CANONICAL | source_system | source_record_key).
     *
     * STRICT CONTRACT: Never uses event origin time or coordinates to create canonical event ID.
     */
    public static String canonicalEventId(String sourceSystem, String sourceRecordKey) {
        Objects.requireNonNull(sourceSystem, "sourceSystem");
        Objects.requireNonNull(sourceRecordKey, "sourceRecordKey");
        return "evt_" + sha256("CANONICAL|" + sourceSystem + "|" + sourceRecordKey).substring(0, 32);
    }

    /**
     * Generates a deterministic source_link_id for a candidate pair and model version (CON-03 8.1):
     * lnk_ + sha256(matchModelVersion | min(leftId, rightId) | max(leftId, rightId)).
     */
    public static String sourceLinkId(String matchModelVersion, String leftObservationId, String rightObservationId) {
        Objects.requireNonNull(matchModelVersion, "matchModelVersion");
        Objects.requireNonNull(leftObservationId, "leftObservationId");
        Objects.requireNonNull(rightObservationId, "rightObservationId");
        String first = leftObservationId.compareTo(rightObservationId) <= 0 ? leftObservationId : rightObservationId;
        String second = leftObservationId.compareTo(rightObservationId) <= 0 ? rightObservationId : leftObservationId;
        return "lnk_" + sha256(matchModelVersion.trim() + "|" + first.trim() + "|" + second.trim()).substring(0, 32);
    }

    public static String sha256(byte[] data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(data));
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("JVM does not support SHA-256", exception);
        }
    }

    public static String sha256(String text) {
        return sha256(text.getBytes(StandardCharsets.UTF_8));
    }
}
