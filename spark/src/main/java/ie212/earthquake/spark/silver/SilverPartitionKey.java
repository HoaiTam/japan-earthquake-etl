package ie212.earthquake.spark.silver;

import java.io.Serializable;
import java.time.ZoneOffset;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Logical partition key for the Silver source_observation dataset (CON-03 1.0).
 * Partition hierarchy: event_year_utc/event_month_utc/source_system.
 */
public record SilverPartitionKey(int eventYearUtc, int eventMonthUtc, String sourceSystem)
        implements Serializable, Comparable<SilverPartitionKey> {

    private static final Pattern PARTITION_PATH_PATTERN = Pattern.compile(
            "^event_year_utc=(\\d{4})/event_month_utc=(\\d{1,2})/source_system=([A-Za-z0-9_]+)$");

    public SilverPartitionKey {
        if (eventYearUtc < 1900 || eventYearUtc > 2100) {
            throw new IllegalArgumentException("eventYearUtc out of valid range [1900, 2100]: " + eventYearUtc);
        }
        if (eventMonthUtc < 1 || eventMonthUtc > 12) {
            throw new IllegalArgumentException("eventMonthUtc out of valid range [1, 12]: " + eventMonthUtc);
        }
        Objects.requireNonNull(sourceSystem, "sourceSystem");
        if (sourceSystem.isBlank()) {
            throw new IllegalArgumentException("sourceSystem must not be blank");
        }
    }

    /**
     * Extracts and validates the partition key from an observation, ensuring
     * event_year_utc and event_month_utc strictly match event_time_utc in UTC.
     */
    public static SilverPartitionKey from(SilverObservation observation) {
        Objects.requireNonNull(observation, "observation");
        int expectedYear = observation.eventTimeUtc().atZone(ZoneOffset.UTC).getYear();
        int expectedMonth = observation.eventTimeUtc().atZone(ZoneOffset.UTC).getMonthValue();

        if (observation.eventYearUtc() != expectedYear) {
            throw new IllegalArgumentException(String.format(
                    "Observation event_year_utc (%d) does not match event_time_utc year (%d)",
                    observation.eventYearUtc(), expectedYear));
        }
        if (observation.eventMonthUtc() != expectedMonth) {
            throw new IllegalArgumentException(String.format(
                    "Observation event_month_utc (%d) does not match event_time_utc month (%d)",
                    observation.eventMonthUtc(), expectedMonth));
        }

        return new SilverPartitionKey(observation.eventYearUtc(), observation.eventMonthUtc(), observation.sourceSystem());
    }

    /**
     * Formats the standard partition path segment:
     * event_year_utc=YYYY/event_month_utc=MM/source_system=SOURCE
     */
    public String partitionPath() {
        return String.format("event_year_utc=%04d/event_month_utc=%02d/source_system=%s",
                eventYearUtc, eventMonthUtc, sourceSystem);
    }

    /**
     * Parses a partition path string back into a SilverPartitionKey.
     */
    public static SilverPartitionKey parse(String partitionPath) {
        Objects.requireNonNull(partitionPath, "partitionPath");
        Matcher matcher = PARTITION_PATH_PATTERN.matcher(partitionPath.trim());
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid Silver partition path: " + partitionPath);
        }
        int year = Integer.parseInt(matcher.group(1));
        int month = Integer.parseInt(matcher.group(2));
        String source = matcher.group(3);
        return new SilverPartitionKey(year, month, source);
    }

    @Override
    public int compareTo(SilverPartitionKey other) {
        int cmp = Integer.compare(this.eventYearUtc, other.eventYearUtc);
        if (cmp != 0) return cmp;
        cmp = Integer.compare(this.eventMonthUtc, other.eventMonthUtc);
        if (cmp != 0) return cmp;
        return this.sourceSystem.compareTo(other.sourceSystem);
    }

    @Override
    public String toString() {
        return partitionPath();
    }
}
