package ie212.earthquake.spark.usgs;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Builds reproducible USGS daily/backfill plans without performing network I/O. */
public final class UsgsRequestBuilder {
    private static final DateTimeFormatter UTC_FORMATTER = DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.UTC);
    private static final String FORMAT = "geojson";
    private static final String ORDER_BY = "time-asc";

    private final UsgsRequestConfig config;

    public UsgsRequestBuilder(UsgsRequestConfig config) {
        this.config = Objects.requireNonNull(config, "config");
    }

    /** Builds the target day and the revision overlap for a daily run. */
    public UsgsRequestPlan buildDailyPlan(Instant runAtUtc) {
        Objects.requireNonNull(runAtUtc, "runAtUtc");
        Instant currentDayStart = runAtUtc.truncatedTo(ChronoUnit.DAYS);
        Instant targetEnd = currentDayStart;
        Instant targetStart = targetEnd.minus(1, ChronoUnit.DAYS);
        if (targetStart.isBefore(config.seedStartUtc())) {
            targetStart = config.seedStartUtc();
        }
        if (!targetStart.isBefore(targetEnd)) {
            throw new IllegalArgumentException("daily target is before the configured USGS seed");
        }
        Instant queryStart = targetEnd.minus(config.revisionWindowDays(), ChronoUnit.DAYS);
        return buildTargetPlan(targetStart, targetEnd, queryStart);
    }

    /** Builds a backfill plan with the same revision overlap semantics as daily runs. */
    public UsgsRequestPlan buildBackfillPlan(Instant targetStartUtc, Instant targetEndExclusiveUtc) {
        requireWindow(targetStartUtc, targetEndExclusiveUtc);
        if (targetStartUtc.isBefore(config.seedStartUtc())) {
            throw new IllegalArgumentException("backfill start must not precede USGS_SEED_START_UTC");
        }
        Instant queryStart = targetStartUtc.minus(config.revisionWindowDays(), ChronoUnit.DAYS);
        return buildTargetPlan(targetStartUtc, targetEndExclusiveUtc, queryStart);
    }

    /**
     * Rebuilds a plan from an already resolved orchestration context without
     * applying the revision overlap a second time.
     */
    public UsgsRequestPlan buildResolvedPlan(
            Instant targetStartUtc,
            Instant targetEndExclusiveUtc,
            Instant queryStartUtc,
            Instant queryEndExclusiveUtc) {
        requireWindow(targetStartUtc, targetEndExclusiveUtc);
        requireWindow(queryStartUtc, queryEndExclusiveUtc);
        if (queryStartUtc.isBefore(config.seedStartUtc())) {
            throw new IllegalArgumentException("resolved query start must not precede USGS_SEED_START_UTC");
        }
        if (queryStartUtc.isAfter(targetStartUtc)
                || !queryEndExclusiveUtc.equals(targetEndExclusiveUtc)) {
            throw new IllegalArgumentException(
                    "resolved query window must cover the target and end at the target end");
        }
        return new UsgsRequestPlan(
                targetStartUtc,
                targetEndExclusiveUtc,
                queryStartUtc,
                queryEndExclusiveUtc,
                buildRequests(queryStartUtc, queryEndExclusiveUtc));
    }

    private UsgsRequestPlan buildTargetPlan(
            Instant targetStartUtc,
            Instant targetEndExclusiveUtc,
            Instant queryStart) {
        if (queryStart.isBefore(config.seedStartUtc())) {
            queryStart = config.seedStartUtc();
        }
        List<UsgsRequest> requests = buildRequests(queryStart, targetEndExclusiveUtc);
        return new UsgsRequestPlan(
                targetStartUtc,
                targetEndExclusiveUtc,
                queryStart,
                targetEndExclusiveUtc,
                requests);
    }

    private List<UsgsRequest> buildRequests(Instant queryStartUtc, Instant queryEndExclusiveUtc) {
        requireWindow(queryStartUtc, queryEndExclusiveUtc);
        List<UsgsRequest> requests = new ArrayList<>();
        Instant chunkStart = queryStartUtc;
        while (chunkStart.isBefore(queryEndExclusiveUtc)) {
            Instant chunkEnd = chunkStart.plus(config.maxWindowDays(), ChronoUnit.DAYS);
            if (chunkEnd.isAfter(queryEndExclusiveUtc)) {
                chunkEnd = queryEndExclusiveUtc;
            }
            requests.add(new UsgsRequest(
                    buildUri(chunkStart, chunkEnd),
                    chunkStart,
                    chunkEnd,
                    config.requestLimit(),
                    1));
            chunkStart = chunkEnd;
        }
        return List.copyOf(requests);
    }

    private URI buildUri(Instant startUtc, Instant endExclusiveUtc) {
        Map<String, String> parameters = new LinkedHashMap<>();
        parameters.put("format", FORMAT);
        parameters.put("eventtype", config.eventType());
        parameters.put("starttime", UTC_FORMATTER.format(startUtc));
        // USGS endtime is inclusive; subtract 1 ms to preserve the project [start,end) contract.
        parameters.put("endtime", UTC_FORMATTER.format(endExclusiveUtc.minusMillis(1)));
        parameters.put("minlatitude", Double.toString(config.minLatitude()));
        parameters.put("maxlatitude", Double.toString(config.maxLatitude()));
        parameters.put("minlongitude", Double.toString(config.minLongitude()));
        parameters.put("maxlongitude", Double.toString(config.maxLongitude()));
        parameters.put("orderby", ORDER_BY);
        parameters.put("limit", Integer.toString(config.requestLimit()));
        // USGS pagination is one-based: offset=1 is the first event.
        parameters.put("offset", "1");

        StringBuilder query = new StringBuilder();
        for (Map.Entry<String, String> entry : parameters.entrySet()) {
            if (query.length() > 0) {
                query.append('&');
            }
            query.append(encode(entry.getKey())).append('=').append(encode(entry.getValue()));
        }
        return URI.create(config.endpoint() + "?" + query);
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private static void requireWindow(Instant startUtc, Instant endExclusiveUtc) {
        Objects.requireNonNull(startUtc, "startUtc");
        Objects.requireNonNull(endExclusiveUtc, "endExclusiveUtc");
        if (!startUtc.isBefore(endExclusiveUtc)) {
            throw new IllegalArgumentException("window start must be before exclusive end");
        }
    }
}
