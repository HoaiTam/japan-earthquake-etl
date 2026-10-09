package ie212.earthquake.spark.gold;

import static org.junit.jupiter.api.Assertions.*;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class GoldCommitInterfaceTest {
    private static final String EVENT = "iceberg.gold.event_current", BRIDGE = "iceberg.gold.event_source_bridge";
    private static GoldCommitRequest request(boolean quality, Map<String, Long> baseline) {
        return new GoldCommitRequest("op-fixture", "run-fixture", Instant.parse("2023-01-01T00:00:00Z"),
                Instant.parse("2023-02-01T00:00:00Z"), LocalDate.parse("2023-02-01"), true, "v1",
                List.of("s3://fixture/silver/manifest.json"), "iceberg", baseline,
                List.of(new GoldCommitRequest.AffectedPartition(2023, 1)), Map.of(EVENT, 0L, BRIDGE, 0L), quality);
    }
    @Test void emptyAffectedPartitionAndNewTableBaselineAreExplicit() {
        var request = request(true, Map.of(EVENT, 0L, BRIDGE, 0L));
        assertEquals(0, request.expectedRowCounts().get(EVENT));
        assertEquals(1, request.affectedPartitions().size());
        assertEquals(64, request.identitySha256().length());
    }
    @Test void qualityAndTableScopeFailClosed() {
        assertThrows(IllegalArgumentException.class, () -> request(false, Map.of(EVENT, 1L, BRIDGE, 2L)));
        assertThrows(IllegalArgumentException.class, () -> request(true, Map.of(EVENT, 1L)));
        assertThrows(IllegalArgumentException.class, () -> request(true, Map.of(EVENT, -1L, BRIDGE, 2L)));
    }
    @Test void mapOrderDoesNotChangeOperationIdentityAndScopeChangesDo() {
        assertEquals(request(true, Map.of(EVENT, 1L, BRIDGE, 2L)).identitySha256(), request(true, Map.of(BRIDGE, 2L, EVENT, 1L)).identitySha256());
        assertNotEquals(request(true, Map.of(EVENT, 1L, BRIDGE, 2L)).identitySha256(), request(true, Map.of(EVENT, 3L, BRIDGE, 2L)).identitySha256());
    }
    @Test void partialOrForeignCommitCannotProduceCompleteReceipt() {
        var request = request(true, Map.of(EVENT, 1L, BRIDGE, 2L));
        assertThrows(IllegalArgumentException.class, () -> new GoldCommitAdapter.CommitReceipt(request.identitySha256(), Map.of(EVENT, 3L), false));
        assertThrows(IllegalArgumentException.class, () -> new GoldCommitAdapter.CommitReceipt(request.identitySha256(), Map.of(EVENT, 3L), true).validateAgainst(request));
        assertThrows(IllegalArgumentException.class, () -> new GoldCommitAdapter.CommitReceipt("a".repeat(64), Map.of(EVENT, 3L, BRIDGE, 4L), true).validateAgainst(request));
    }
    @Test void mockRerunReusesExactSnapshotBundleWithoutPublication() throws Exception {
        var request = request(true, Map.of(EVENT, 1L, BRIDGE, 2L));
        class FixtureCatalog implements GoldCommitAdapter {
            int commits;
            CommitReceipt receipt;
            public OperationInspection inspect(GoldCommitRequest input) {
                return new OperationInspection(receipt == null ? OperationState.NOT_STARTED : OperationState.COMMITTED,
                        input.identitySha256(), receipt == null ? Map.of() : receipt.snapshots());
            }
            public CommitReceipt commit(GoldCommitRequest input) {
                if (receipt == null) { commits++; receipt = new CommitReceipt(input.identitySha256(), Map.of(EVENT, 3L, BRIDGE, 4L), true); }
                receipt.validateAgainst(input);
                return receipt;
            }
        }
        FixtureCatalog mock = new FixtureCatalog();
        assertEquals(GoldCommitAdapter.OperationState.NOT_STARTED, mock.inspect(request).state());
        assertEquals(mock.commit(request), mock.commit(request));
        assertEquals(1, mock.commits);
        assertEquals(GoldCommitAdapter.OperationState.COMMITTED, mock.inspect(request).state());
    }
}
