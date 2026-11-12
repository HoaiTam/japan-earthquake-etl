-- Trino Gold Serving View: v_event_monthly
-- Monthly earthquake aggregates rolled up from daily aggregates by year-month, region, bands, and coverage.
-- Source: iceberg.gold.v_event_daily.
-- Enforces:
-- 1. Exact additive rollup: sum(event_count) monthly equals sum(event_count) daily.
-- 2. Weighted average preservation: averages are computed as sum / non_null_count, not avg of avgs.
-- 3. Honest NULL policy: averages remain NULL if non-null count is 0.

CREATE OR REPLACE VIEW iceberg.gold.v_event_monthly AS
SELECT
    d.year_month_jst,
    d.region_key,
    d.region_name,
    d.region_category,
    d.magnitude_band_code,
    d.depth_band_code,
    d.source_coverage_code,
    SUM(d.event_count) AS event_count,
    SUM(d.magnitude_non_null_count) AS magnitude_non_null_count,
    SUM(d.magnitude_sum) AS magnitude_sum,
    CASE
        WHEN SUM(d.magnitude_non_null_count) > 0
        THEN SUM(d.magnitude_sum) / CAST(SUM(d.magnitude_non_null_count) AS DOUBLE)
        ELSE NULL
    END AS magnitude_avg,
    MAX(d.magnitude_max) AS magnitude_max,
    SUM(d.depth_non_null_count) AS depth_non_null_count,
    SUM(d.depth_sum) AS depth_sum,
    CASE
        WHEN SUM(d.depth_non_null_count) > 0
        THEN SUM(d.depth_sum) / CAST(SUM(d.depth_non_null_count) AS DOUBLE)
        ELSE NULL
    END AS depth_avg,
    SUM(d.strong_earthquake_count) AS strong_earthquake_count,
    SUM(d.tsunami_flagged_count) AS tsunami_flagged_count,
    SUM(d.alerted_event_count) AS alerted_event_count,
    SUM(d.intensity_reported_count) AS intensity_reported_count,
    SUM(d.offshore_or_unknown_count) AS offshore_or_unknown_count
FROM iceberg.gold.v_event_daily d
GROUP BY
    d.year_month_jst,
    d.region_key,
    d.region_name,
    d.region_category,
    d.magnitude_band_code,
    d.depth_band_code,
    d.source_coverage_code;
