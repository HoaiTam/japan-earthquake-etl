package vn.edu.uit.ie212.earthquake.spark.usgs;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UsgsRequestBuilderTest {
    private static final Instant SEED = Instant.parse("2023-01-01T00:00:00Z");

    @Test
    void buildsDailyPlanWithThreeDayRevisionOverlap() {
        UsgsRequestBuilder builder = new UsgsRequestBuilder(UsgsRequestConfig.fromEnvironment(config()));

        UsgsRequestPlan plan = builder.buildDailyPlan(Instant.parse("2023-09-16T00:15:00Z"));

        assertEquals(Instant.parse("2023-09-15T00:00:00Z"), plan.targetStartUtc());
        assertEquals(Instant.parse("2023-09-16T00:00:00Z"), plan.targetEndExclusiveUtc());
        assertEquals(Instant.parse("2023-09-13T00:00:00Z"), plan.queryStartUtc());
        assertEquals(1, plan.requests().size());
        String query = plan.requests().get(0).uri().getRawQuery();
        assertTrue(query.contains("starttime=2023-09-13T00%3A00%3A00Z"));
        assertTrue(query.contains("endtime=2023-09-15T23%3A59%3A59.999Z"));
        assertTrue(query.contains("eventtype=earthquake"));
        assertTrue(query.contains("limit=20000"));
        assertTrue(query.contains("offset=0"));
        assertFalse(query.contains("minmagnitude"));
    }

    @Test
    void clipsRevisionOverlapAtSeed() {
        UsgsRequestBuilder builder = new UsgsRequestBuilder(UsgsRequestConfig.fromEnvironment(config()));

        UsgsRequestPlan plan = builder.buildDailyPlan(Instant.parse("2023-01-02T00:15:00Z"));

        assertEquals(SEED, plan.targetStartUtc());
        assertEquals(SEED, plan.queryStartUtc());
        assertEquals(1, plan.requests().size());
    }

    @Test
    void splitsLongBackfillWithoutGaps() {
        Map<String, String> values = config();
        values.put("USGS_MAX_WINDOW_DAYS", "2");
        UsgsRequestBuilder builder = new UsgsRequestBuilder(UsgsRequestConfig.fromEnvironment(values));

        UsgsRequestPlan plan = builder.buildBackfillPlan(
                Instant.parse("2023-09-01T00:00:00Z"),
                Instant.parse("2023-09-06T00:00:00Z"));

        assertEquals(4, plan.requests().size());
        Instant cursor = plan.queryStartUtc();
        for (UsgsRequest request : plan.requests()) {
            assertEquals(cursor, request.windowStartUtc());
            assertTrue(request.windowEndExclusiveUtc().compareTo(
                    request.windowStartUtc().plusSeconds(2 * 24 * 60 * 60)) <= 0);
            assertEquals(20000, request.limit());
            assertEquals(0, request.offset());
            cursor = request.windowEndExclusiveUtc();
        }
        assertEquals(plan.queryEndExclusiveUtc(), cursor);
    }

    @Test
    void rejectsBackfillBeforeSeed() {
        UsgsRequestBuilder builder = new UsgsRequestBuilder(UsgsRequestConfig.fromEnvironment(config()));

        assertThrows(
                IllegalArgumentException.class,
                () -> builder.buildBackfillPlan(
                        Instant.parse("2022-12-31T00:00:00Z"),
                        SEED));
    }

    @Test
    void rejectsInvalidConfigurationEarly() {
        Map<String, String> values = config();
        values.put("USGS_API_BASE_URL", "http://localhost:8080/query");

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> UsgsRequestConfig.fromEnvironment(values));

        assertTrue(exception.getMessage().contains("USGS_API_BASE_URL"));
    }

    @Test
    void rejectsInvalidBoundsAndMissingValues() {
        Map<String, String> invalidBounds = config();
        invalidBounds.put("USGS_MIN_LATITUDE", "51");
        assertThrows(IllegalArgumentException.class, () -> UsgsRequestConfig.fromEnvironment(invalidBounds));

        Map<String, String> missingLimit = config();
        missingLimit.remove("USGS_REQUEST_LIMIT");
        assertThrows(IllegalArgumentException.class, () -> UsgsRequestConfig.fromEnvironment(missingLimit));
    }

    @Test
    void rejectsNonUtcSeedAndOutOfRangeLimit() {
        Map<String, String> invalidSeed = config();
        invalidSeed.put("USGS_SEED_START_UTC", "2023-01-01T09:00:00+09:00");
        assertThrows(IllegalArgumentException.class, () -> UsgsRequestConfig.fromEnvironment(invalidSeed));

        Map<String, String> invalidLimit = config();
        invalidLimit.put("USGS_REQUEST_LIMIT", "20001");
        assertThrows(IllegalArgumentException.class, () -> UsgsRequestConfig.fromEnvironment(invalidLimit));

        Map<String, String> invalidBackoff = config();
        invalidBackoff.put("USGS_HTTP_INITIAL_BACKOFF_MS", "5000");
        invalidBackoff.put("USGS_HTTP_MAX_BACKOFF_MS", "1000");
        assertThrows(IllegalArgumentException.class, () -> UsgsRequestConfig.fromEnvironment(invalidBackoff));
    }

    private static Map<String, String> config() {
        Map<String, String> values = new HashMap<>();
        values.put("USGS_API_BASE_URL", "https://earthquake.usgs.gov/fdsnws/event/1/query");
        values.put("USGS_MIN_LATITUDE", "20.0");
        values.put("USGS_MAX_LATITUDE", "50.0");
        values.put("USGS_MIN_LONGITUDE", "120.0");
        values.put("USGS_MAX_LONGITUDE", "155.0");
        values.put("USGS_SEED_START_UTC", SEED.toString());
        values.put("PIPELINE_OVERLAP_DAYS", "3");
        values.put("USGS_MAX_WINDOW_DAYS", "3");
        values.put("USGS_REQUEST_LIMIT", "20000");
        values.put("USGS_HTTP_TIMEOUT_MS", "30000");
        values.put("USGS_HTTP_MAX_ATTEMPTS", "4");
        values.put("USGS_HTTP_INITIAL_BACKOFF_MS", "250");
        values.put("USGS_HTTP_MAX_BACKOFF_MS", "4000");
        values.put("USGS_MAX_RESPONSE_BYTES", "10485760");
        values.put("USGS_EVENT_TYPE", "earthquake");
        return values;
    }
}
