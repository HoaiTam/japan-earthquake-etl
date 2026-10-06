package ie212.earthquake.spark.silver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SilverQualityValidatorTest {
    @Test
    void validAndRejectedCountsReconcileAndRejectKeepsLineage() {
        SilverObservation valid = observation("USGS", "usgs-1", "revision-1", 10.0, 140.0, "EARTHQUAKE");
        SilverObservation invalid = new SilverObservation(
                "USGS", "", "revision-2", null, 95.0, 140.0, null, null, "EARTHQUAKE", null, null, null, null,
                "manifest-1", "s3://bucket/raw", "a".repeat(64), "feature-1", "hash-1", "run-1", List.of());

        SilverQualityResult result = new SilverQualityValidator().validate(List.of(valid, invalid), "run-1");

        assertEquals(2, result.parsedCount());
        assertEquals(1, result.validCount());
        assertEquals(1, result.rejectedCount());
        assertTrue(result.publishBlocked());
        assertTrue(result.reasonCounts().containsKey("MISSING_SOURCE_KEY"));
        assertTrue(result.reasonCounts().containsKey("INVALID_EVENT_TIME"));
        assertEquals("s3://bucket/raw", result.rejectedRecords().get(0).observation().rawObjectUri());
    }

    @Test
    void nullableMagnitudeDepthAndNegativeDepthRemainValidWithContractLineage() {
        SilverObservation observation = observation("USGS", "usgs-2", "revision-1", -10.0, 130.0, "EARTHQUAKE");
        SilverQualityResult result = new SilverQualityValidator().validate(List.of(observation), "run-1");

        assertEquals(1, result.validCount());
        assertEquals(0, result.rejectedCount());
    }

    @Test
    void jmaRequiresReleaseAndEraButKeepsAgency() {
        SilverObservation observation = new SilverObservation(
                "JMA_BULLETIN", "jma-1", "release-1:hash", Instant.parse("2023-01-01T00:00:00Z"),
                35.0, 139.0, 10.0, 4.2, "EARTHQUAKE", "release-1", "UNIFIED", "J", "K",
                "manifest-jma", "s3://bucket/archive.zip", "b".repeat(64), "line-1", "hash-1", "run-1", List.of());

        SilverQualityResult result = new SilverQualityValidator().validate(List.of(observation), "run-1");

        assertEquals(1, result.validCount());
        assertEquals(0, result.rejectedCount());
    }

    @Test
    void writesQualitySummaryWithRejectLineage(@TempDir Path temp) throws Exception {
        SilverObservation invalid = new SilverObservation(
                "USGS", "", "revision-1", null, 91.0, 10.0, null, null, "EARTHQUAKE", null, null,
                null, null, "manifest-1", "s3://bucket/raw", "a".repeat(64), "feature-1", "hash-1", "run-1", List.of());
        SilverQualityResult result = new SilverQualityValidator().validate(List.of(invalid), "run-1");
        Path summary = new SilverQualitySummaryWriter().write(temp.resolve("quality-summary.json"), result, "USGS");

        String json = Files.readString(summary);
        assertTrue(json.contains("\"publish_blocked\" : true"));
        assertTrue(json.contains("manifest-1"));
        assertTrue(json.contains("INVALID_LATITUDE"));
    }

    private static SilverObservation observation(
            String source, String key, String revision, double depth, double longitude, String eventType) {
        return new SilverObservation(
                source, key, revision, Instant.parse("2023-01-01T00:00:00Z"), 35.0, longitude, depth,
                null, eventType, null, null, null, null, "manifest-1", "s3://bucket/raw", "a".repeat(64),
                "feature-1", "hash-1", "run-1", List.of());
    }
}
