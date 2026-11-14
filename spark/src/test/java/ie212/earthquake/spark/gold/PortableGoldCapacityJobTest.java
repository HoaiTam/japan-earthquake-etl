package ie212.earthquake.spark.gold;

import static org.junit.jupiter.api.Assertions.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

class PortableGoldCapacityJobTest {
    private ObjectNode request() {
        return new ObjectMapper().createObjectNode().put("version", "dat03-portable-gold-capacity-v1")
                .put("source_namespace", "iceberg.dat03_catalog_" + "a".repeat(32)).put("table", "event_current")
                .put("source_publication_id", "pub_" + "b".repeat(64)).put("source_bundle_sha256", "b".repeat(64))
                .put("snapshot_id", 9007199254740993L).put("source_count", 2_000_000).put("copies", 3).put("required_rows", 4_500_000);
    }
    @Test void fullReadOnlyWorkloadIsAccepted() { assertDoesNotThrow(() -> PortableGoldCapacityJob.validate(request())); }
    @Test void insufficientWorkloadIsRejected() { var r = request().put("copies", 1); assertThrows(IllegalArgumentException.class, () -> PortableGoldCapacityJob.validate(r)); }
    @Test void fractionalSnapshotIsRejected() { var r = request().put("snapshot_id", 123.5); assertThrows(IllegalArgumentException.class, () -> PortableGoldCapacityJob.validate(r)); }
    @Test void operationalNamespaceIsRejected() { var r = request().put("source_namespace", "iceberg.operational_current_month"); assertThrows(IllegalArgumentException.class, () -> PortableGoldCapacityJob.validate(r)); }
    @Test void unboundedStressIsRejected() { var r = request().put("copies", 999); assertThrows(IllegalArgumentException.class, () -> PortableGoldCapacityJob.validate(r)); }
    @Test void multipliedWorkloadMustAlsoRespectTheRowLimit() {
        var r = request().put("source_count", 5_000_000).put("copies", 8);
        assertThrows(IllegalArgumentException.class, () -> PortableGoldCapacityJob.validate(r));
    }
    @Test void trinoDeltaEncodingUsesOnlySessionScopedReaderCompatibility() {
        assertEquals(java.util.Map.of("spark.sql.iceberg.vectorization.enabled", "false"), PortableGoldCapacityJob.readCompatibilityOptions());
    }
}
