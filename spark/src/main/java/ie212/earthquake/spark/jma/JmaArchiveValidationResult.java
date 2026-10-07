package ie212.earthquake.spark.jma;

/** Structural validation only; no interpretation or filtering of event fields. */
public record JmaArchiveValidationResult(boolean valid, Long recordCount, String reason) {
    static JmaArchiveValidationResult rejected(String reason) {
        return new JmaArchiveValidationResult(false, null, reason);
    }
}
