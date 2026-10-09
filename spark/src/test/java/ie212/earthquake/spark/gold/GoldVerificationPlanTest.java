package ie212.earthquake.spark.gold;

import static org.junit.jupiter.api.Assertions.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class GoldVerificationPlanTest {
    private static final Map<String, Long> SNAPSHOTS = Map.of("iceberg.gold.event_current", 101L, "iceberg.gold.event_source_bridge", 202L);
    private static final Map<String, Long> COUNTS = Map.of("iceberg.gold.event_current", 1L, "iceberg.gold.event_source_bridge", 2L);
    private static Map<String, Boolean> checks() {
        Map<String, Boolean> checks = new HashMap<>();
        GoldVerificationPlan.REQUIRED_CHECKS.forEach(key -> checks.put(key, true));
        return checks;
    }
    @Test void passedFixtureCannotPublish() {
        var report = new GoldVerificationPlan("iceberg", SNAPSHOTS, COUNTS).evaluateFixture(SNAPSHOTS, COUNTS, checks());
        assertTrue(report.fixturePassed()); assertFalse(report.canPublish()); assertEquals("MOCK_TRINO", report.engine());
    }
    @Test void eachBlockerFailsFixtureGate() {
        for (String blocker : GoldVerificationPlan.REQUIRED_CHECKS) {
            var failed = checks(); failed.put(blocker, false);
            var report = new GoldVerificationPlan("iceberg", SNAPSHOTS, COUNTS).evaluateFixture(SNAPSHOTS, COUNTS, failed);
            assertFalse(report.fixturePassed()); assertFalse(report.canPublish());
        }
    }
    @Test void staleSnapshotCountMismatchOrMissingCheckIsRejected() {
        var plan = new GoldVerificationPlan("iceberg", SNAPSHOTS, COUNTS);
        assertThrows(IllegalArgumentException.class, () -> plan.evaluateFixture(Map.of("iceberg.gold.event_current", 103L, "iceberg.gold.event_source_bridge", 202L), COUNTS, checks()));
        assertThrows(IllegalArgumentException.class, () -> plan.evaluateFixture(SNAPSHOTS, Map.of("iceberg.gold.event_current", 2L), checks()));
        var missing = checks(); missing.remove("scope_respected");
        assertThrows(IllegalArgumentException.class, () -> plan.evaluateFixture(SNAPSHOTS, COUNTS, missing));
    }
    @Test void sqlCannotInjectIdentifiersOrReadUnpinnedTable() {
        assertThrows(IllegalArgumentException.class, () -> new GoldVerificationPlan("iceberg; drop schema gold", SNAPSHOTS, COUNTS));
        assertThrows(IllegalArgumentException.class, () -> new GoldVerificationPlan("iceberg", Map.of("iceberg.gold.event_current", -1L), COUNTS));
        var plan = new GoldVerificationPlan("iceberg", SNAPSHOTS, COUNTS);
        assertThrows(IllegalArgumentException.class, () -> plan.snapshotReference("iceberg.gold.foreign"));
        assertTrue(plan.sql().get("count:iceberg.gold.event_current").contains("FOR VERSION AS OF 101"));
        assertTrue(plan.outsideScopeSql(99L, List.of(202301)).contains("FOR VERSION AS OF 99"));
        assertThrows(IllegalArgumentException.class, () -> plan.outsideScopeSql(99L, List.of(202313)));
    }
    @Test void emptyGoldIsValidOnlyWithCompletePinnedBundle() {
        var zero = Map.of("iceberg.gold.event_current", 0L, "iceberg.gold.event_source_bridge", 0L);
        assertTrue(new GoldVerificationPlan("iceberg", SNAPSHOTS, zero).evaluateFixture(SNAPSHOTS, zero, checks()).fixturePassed());
    }
}
