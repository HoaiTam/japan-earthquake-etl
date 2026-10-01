package vn.edu.uit.ie212.earthquake.spark.usgs;

import java.util.Objects;

/** Outcome of a Bronze validation/write attempt. */
public record UsgsBronzeWriteResult(
        String bronzeStatus,
        String rawObjectKey,
        String manifestKey,
        String sha256,
        Integer recordCountEstimate,
        boolean idempotentReuse,
        String rejectionReason) {

    public UsgsBronzeWriteResult {
        Objects.requireNonNull(bronzeStatus, "bronzeStatus");
        Objects.requireNonNull(rawObjectKey, "rawObjectKey");
        Objects.requireNonNull(manifestKey, "manifestKey");
        Objects.requireNonNull(sha256, "sha256");
        if ("BronzeReady".equals(bronzeStatus) && rejectionReason != null) {
            throw new IllegalArgumentException("BronzeReady result must not have rejection reason");
        }
        if ("Rejected".equals(bronzeStatus)
                && (rejectionReason == null || rejectionReason.isBlank())) {
            throw new IllegalArgumentException("Rejected result must have rejection reason");
        }
    }

    public boolean ready() {
        return "BronzeReady".equals(bronzeStatus);
    }
}
