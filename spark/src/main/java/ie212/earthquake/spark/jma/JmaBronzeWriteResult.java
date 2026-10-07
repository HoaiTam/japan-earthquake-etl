package ie212.earthquake.spark.jma;

/** Compact publication outcome for orchestration; never includes the payload. */
public record JmaBronzeWriteResult(String bronzeStatus, String rawObjectKey, String manifestKey,
        String sha256, Long recordCountEstimate, boolean idempotentReuse, String rejectionReason) {
    public boolean ready() {
        return "BronzeReady".equals(bronzeStatus);
    }
}
