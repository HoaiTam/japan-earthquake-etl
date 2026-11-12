-- Trino Gold Serving View: v_event_daily
-- Daily earthquake aggregates grouped by JST date, region, magnitude band, depth band, and source coverage.
-- Source: iceberg.gold.event_current filtered to natural earthquakes in Japan study area.
-- Enforces:
-- 1. Zero double-counting from source bridge (groups canonical events only).
-- 2. Honest NULL policy: magnitude and depth averages remain NULL when count is 0.
-- 3. Invariant: sum(event_count) strictly equals canonical event count for identical filter scope.

CREATE OR REPLACE VIEW iceberg.gold.v_event_daily AS
SELECT
    e.event_date_key_jst,
    e.event_date_jst,
    CAST(SUBSTR(CAST(e.event_date_key_jst AS VARCHAR), 1, 6) AS INTEGER) AS year_month_jst,
    COALESCE(e.region_key, 'UNKNOWN') AS region_key,
    COALESCE(e.region_name, 'Unknown') AS region_name,
    COALESCE(e.region_category, 'UNKNOWN') AS region_category,
    COALESCE(e.magnitude_band_code, 'UNKNOWN') AS magnitude_band_code,
    COALESCE(e.depth_band_code, 'UNKNOWN') AS depth_band_code,
    e.source_coverage_code,
    COUNT(*) AS event_count,
    COUNT(e.magnitude) AS magnitude_non_null_count,
    SUM(e.magnitude) AS magnitude_sum,
    AVG(e.magnitude) AS magnitude_avg,
    MAX(e.magnitude) AS magnitude_max,
    COUNT(e.depth_km) AS depth_non_null_count,
    SUM(e.depth_km) AS depth_sum,
    AVG(e.depth_km) AS depth_avg,
    SUM(CASE WHEN e.is_strong_earthquake = true THEN 1 ELSE 0 END) AS strong_earthquake_count,
    SUM(CASE WHEN e.tsunami_flag = true THEN 1 ELSE 0 END) AS tsunami_flagged_count,
    SUM(CASE WHEN e.alert_level IS NOT NULL THEN 1 ELSE 0 END) AS alerted_event_count,
    SUM(CASE WHEN e.jma_max_intensity IS NOT NULL THEN 1 ELSE 0 END) AS intensity_reported_count,
    SUM(CASE WHEN e.region_category IN ('OFFSHORE', 'UNKNOWN') THEN 1 ELSE 0 END) AS offshore_or_unknown_count
FROM iceberg.gold.event_current e
WHERE e.is_earthquake = true
  AND e.is_in_study_area = true
GROUP BY
    e.event_date_key_jst,
    e.event_date_jst,
    CAST(SUBSTR(CAST(e.event_date_key_jst AS VARCHAR), 1, 6) AS INTEGER),
    COALESCE(e.region_key, 'UNKNOWN'),
    COALESCE(e.region_name, 'Unknown'),
    COALESCE(e.region_category, 'UNKNOWN'),
    COALESCE(e.magnitude_band_code, 'UNKNOWN'),
    COALESCE(e.depth_band_code, 'UNKNOWN'),
    e.source_coverage_code;
