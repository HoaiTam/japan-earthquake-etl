package ie212.earthquake.spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class SilverBronzeIntegrationJobTest {
    private static ObjectNode request() {
        var node = new ObjectMapper().createObjectNode(); node.put("integration_version", "slv09-live-v1");
        node.put("observability_version", "orc-04-v1"); node.put("run_id", "test-scope");
        node.put("processed_at_utc", "2026-10-10T00:00:00Z"); node.put("config_version", "v1");
        node.put("processing_date", "2026-10-10"); node.put("is_backfill", true);
        node.put("window_start_utc", "1999-12-31T15:00:00Z"); node.put("window_end_utc", "2024-01-01T00:00:00Z");
        node.put("jma_records_per_archive", 256);
        var pins = node.putArray("bronze_inputs"); pins.addObject().put("source_system", "USGS");
        pins.addObject().put("source_system", "JMA_BULLETIN").put("year", 2000);
        pins.addObject().put("source_system", "JMA_BULLETIN").put("year", 2023); return node;
    }
    @Test void boundedTwoYearScopeAccepted() { assertDoesNotThrow(() -> SilverBronzeIntegrationJob.validate(request())); }
    @Test void unboundedOrWrongVersionRejected() {
        var request = request(); request.put("jma_records_per_archive", 513);
        assertThrows(IOException.class, () -> SilverBronzeIntegrationJob.validate(request));
        request.put("jma_records_per_archive", 0);
        assertThrows(IOException.class, () -> SilverBronzeIntegrationJob.validate(request));
        request.put("jma_records_per_archive", 256); request.put("integration_version", "other");
        assertThrows(IOException.class, () -> SilverBronzeIntegrationJob.validate(request));
    }
    @Test void missingReproductionYearRejected() {
        var request = request(); ((ObjectNode) request.path("bronze_inputs").get(1)).put("year", 2023);
        assertThrows(IOException.class, () -> SilverBronzeIntegrationJob.validate(request));
    }
    @Test void escapingRunIdRejected() {
        var request = request(); request.put("run_id", "../escape");
        assertThrows(IllegalArgumentException.class, () -> SilverBronzeIntegrationJob.validate(request));
    }
    @Test void mismatchedProcessingDateAndDailyClaimRejected() {
        var request = request(); request.put("processing_date", "2026-10-11");
        assertThrows(IOException.class, () -> SilverBronzeIntegrationJob.validate(request));
        request.put("processing_date", "2026-10-10"); request.put("is_backfill", false);
        assertThrows(IOException.class, () -> SilverBronzeIntegrationJob.validate(request));
    }
}
