package ie212.earthquake.spark.gold;

import java.util.Map;

/** GLD-03 preparation SPI. No Iceberg writer is implemented by this interface. */
public interface GoldCommitAdapter {
    /** Read durable operation metadata before retrying. UNKNOWN must fail closed. */
    OperationInspection inspect(GoldCommitRequest request) throws Exception;

    /** Must compare baseline snapshots and hold the shared writer lease before changing any table. */
    CommitReceipt commit(GoldCommitRequest request) throws Exception;

    enum OperationState { NOT_STARTED, PARTIAL, COMMITTED, UNKNOWN }

    record OperationInspection(OperationState state, String requestSha256, Map<String, Long> committedSnapshots) {
        public OperationInspection {
            java.util.Objects.requireNonNull(state);
            committedSnapshots = Map.copyOf(committedSnapshots);
            if (!committedSnapshots.isEmpty() && (requestSha256 == null || !requestSha256.matches("[0-9a-f]{64}"))) {
                throw new IllegalArgumentException("operation identity required for committed snapshots");
            }
            if (committedSnapshots.values().stream().anyMatch(id -> id <= 0)) {
                throw new IllegalArgumentException("invalid snapshot");
            }
        }
    }

    /** A commit receipt is not publication evidence. GLD-04 must verify every table/snapshot. */
    record CommitReceipt(String requestSha256, Map<String, Long> snapshots, boolean readbackConfirmed) {
        public CommitReceipt {
            if (requestSha256 == null || !requestSha256.matches("[0-9a-f]{64}")) {
                throw new IllegalArgumentException("request SHA required");
            }
            snapshots = Map.copyOf(snapshots);
            if (snapshots.isEmpty() || snapshots.values().stream().anyMatch(id -> id <= 0) || !readbackConfirmed) {
                throw new IllegalArgumentException("complete committed readback required");
            }
        }
        public void validateAgainst(GoldCommitRequest request) {
            if (!request.identitySha256().equals(requestSha256) || !snapshots.keySet().equals(request.baselineSnapshots().keySet())) {
                throw new IllegalArgumentException("snapshot scope/operation mismatch");
            }
        }
    }
}
