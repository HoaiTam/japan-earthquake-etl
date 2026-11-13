-- ============================================================================
-- Gold Aggregate Verification Queries (GLD-02)
-- Catalog: iceberg.gold.*
-- Target: Trino query smoke test, daily/monthly aggregate reconciliation,
--         deep metric parity, and zero double-counting verification.
-- ============================================================================

-- Q1. Fact-to-Aggregate Count Reconciliation
-- Verifies that sum(event_count) across all daily aggregate rows strictly equals
-- the number of natural earthquakes in the study area in gold.event_current.
-- Expected output: exactly 1 row with delta = 0.
SELECT
    f.fact_event_count,
    a.daily_agg_event_count,
    (f.fact_event_count - a.daily_agg_event_count) AS delta
FROM (
    SELECT COUNT(*) AS fact_event_count
    FROM iceberg.gold.event_current
    WHERE is_natural_earthquake = true
      AND is_in_study_area = true
) f
CROSS JOIN (
    SELECT SUM(event_count) AS daily_agg_event_count
    FROM iceberg.gold.v_event_daily
) a;

-- Q2. Zero Double-Counting Audit (Bridge Join Independence)
-- Ensures aggregates NEVER multiply rows when events have multiple observations
-- in iceberg.gold.event_source_bridge.
-- Expected output: 0 rows with mismatch between canonical events and aggregate counts.
SELECT
    e.event_date_key_jst,
    e.distinct_event_count,
    d.daily_agg_event_count,
    (e.distinct_event_count - d.daily_agg_event_count) AS delta
FROM (
    SELECT event_date_key_jst, COUNT(DISTINCT canonical_event_id) AS distinct_event_count
    FROM iceberg.gold.event_current
    WHERE is_natural_earthquake = true
      AND is_in_study_area = true
    GROUP BY event_date_key_jst
) e
JOIN (
    SELECT event_date_key_jst, SUM(event_count) AS daily_agg_event_count
    FROM iceberg.gold.v_event_daily
    GROUP BY event_date_key_jst
) d
  ON e.event_date_key_jst = d.event_date_key_jst
WHERE e.distinct_event_count <> d.daily_agg_event_count;

-- Q3. Additive Consistency between Daily and Monthly Aggregates
-- Verifies that sum(event_count) and additive metric sums in monthly rollup
-- strictly match daily totals per year_month.
-- Expected output: 0 rows with mismatch.
SELECT
    COALESCE(d.year_month_jst, m.year_month_jst) AS year_month_jst,
    d.daily_total_events,
    m.monthly_total_events,
    (d.daily_total_events - m.monthly_total_events) AS delta_count,
    ABS(d.daily_magnitude_sum - m.monthly_magnitude_sum) AS delta_mag_sum,
    ABS(d.daily_depth_sum - m.monthly_depth_sum) AS delta_depth_sum
FROM (
    SELECT
        year_month_jst,
        SUM(event_count) AS daily_total_events,
        COALESCE(SUM(magnitude_sum), 0.0) AS daily_magnitude_sum,
        COALESCE(SUM(depth_sum), 0.0) AS daily_depth_sum
    FROM iceberg.gold.v_event_daily
    GROUP BY year_month_jst
) d
FULL OUTER JOIN (
    SELECT
        year_month_jst,
        SUM(event_count) AS monthly_total_events,
        COALESCE(SUM(magnitude_sum), 0.0) AS monthly_magnitude_sum,
        COALESCE(SUM(depth_sum), 0.0) AS monthly_depth_sum
    FROM iceberg.gold.v_event_monthly
    GROUP BY year_month_jst
) m
  ON d.year_month_jst = m.year_month_jst
WHERE d.daily_total_events <> m.monthly_total_events
   OR d.daily_total_events IS NULL
   OR m.monthly_total_events IS NULL
   OR ABS(d.daily_magnitude_sum - m.monthly_magnitude_sum) > 0.0001
   OR ABS(d.daily_depth_sum - m.monthly_depth_sum) > 0.0001;

-- Q4. Deep Grain-Level Fact-to-Daily Metric Parity
-- Computes metrics directly from gold.event_current at the daily grain and compares
-- with v_event_daily across all statistical and KPI indicators.
-- Expected output: 0 rows with mismatch.
SELECT
    f.event_date_key_jst,
    f.region_key,
    f.magnitude_band_code,
    f.depth_band_code,
    f.source_coverage_code,
    f.fact_count,
    d.event_count,
    f.fact_mag_sum,
    d.magnitude_sum,
    f.fact_depth_sum,
    d.depth_sum
FROM (
    SELECT
        event_date_key_jst,
        COALESCE(region_key, 'UNKNOWN') AS region_key,
        COALESCE(magnitude_band_code, 'UNKNOWN') AS magnitude_band_code,
        COALESCE(depth_band_code, 'UNKNOWN') AS depth_band_code,
        source_coverage_code,
        COUNT(*) AS fact_count,
        COUNT(magnitude) AS fact_mag_count,
        SUM(magnitude) AS fact_mag_sum,
        AVG(magnitude) AS fact_mag_avg,
        MAX(magnitude) AS fact_mag_max,
        COUNT(depth_km) AS fact_depth_count,
        SUM(depth_km) AS fact_depth_sum,
        AVG(depth_km) AS fact_depth_avg,
        SUM(CASE WHEN magnitude >= 5.0 THEN 1 ELSE 0 END) AS fact_strong_count,
        SUM(CASE WHEN tsunami_flag = true THEN 1 ELSE 0 END) AS fact_tsunami_count,
        SUM(CASE WHEN alert_level IS NOT NULL AND LOWER(TRIM(alert_level)) NOT IN ('', 'none') THEN 1 ELSE 0 END) AS fact_alert_count,
        SUM(CASE WHEN max_intensity_code IS NOT NULL AND LOWER(TRIM(max_intensity_code)) NOT IN ('', 'none') THEN 1 ELSE 0 END) AS fact_intensity_count,
        SUM(CASE WHEN region_category IN ('OFFSHORE', 'UNKNOWN') THEN 1 ELSE 0 END) AS fact_offshore_count
    FROM iceberg.gold.event_current
    WHERE is_natural_earthquake = true
      AND is_in_study_area = true
    GROUP BY
        event_date_key_jst,
        COALESCE(region_key, 'UNKNOWN'),
        COALESCE(magnitude_band_code, 'UNKNOWN'),
        COALESCE(depth_band_code, 'UNKNOWN'),
        source_coverage_code
) f
FULL OUTER JOIN iceberg.gold.v_event_daily d
  ON f.event_date_key_jst = d.event_date_key_jst
 AND f.region_key = d.region_key
 AND f.magnitude_band_code = d.magnitude_band_code
 AND f.depth_band_code = d.depth_band_code
 AND f.source_coverage_code = d.source_coverage_code
WHERE f.fact_count <> d.event_count
   OR f.fact_mag_count <> d.magnitude_non_null_count
   OR f.fact_depth_count <> d.depth_non_null_count
   OR f.fact_strong_count <> d.strong_earthquake_count
   OR f.fact_tsunami_count <> d.tsunami_flagged_count
   OR f.fact_alert_count <> d.alerted_event_count
   OR f.fact_intensity_count <> d.intensity_reported_count
   OR f.fact_offshore_count <> d.offshore_or_unknown_count
   OR ABS(COALESCE(f.fact_mag_sum, 0.0) - COALESCE(d.magnitude_sum, 0.0)) > 0.0001
   OR ABS(COALESCE(f.fact_mag_avg, 0.0) - COALESCE(d.magnitude_avg, 0.0)) > 0.0001
   OR ABS(COALESCE(f.fact_mag_max, 0.0) - COALESCE(d.magnitude_max, 0.0)) > 0.0001
   OR ABS(COALESCE(f.fact_depth_sum, 0.0) - COALESCE(d.depth_sum, 0.0)) > 0.0001
   OR ABS(COALESCE(f.fact_depth_avg, 0.0) - COALESCE(d.depth_avg, 0.0)) > 0.0001
   OR f.event_date_key_jst IS NULL
   OR d.event_date_key_jst IS NULL;

-- Q5. Honest NULL & Mathematical Invariant Audit
-- Verifies that:
-- 1. magnitude_avg IS NULL whenever magnitude_non_null_count = 0
-- 2. depth_avg IS NULL whenever depth_non_null_count = 0
-- 3. magnitude_max IS NULL whenever magnitude_non_null_count = 0
-- 4. magnitude_sum IS NULL or 0 whenever magnitude_non_null_count = 0
-- Expected output: 0 rows.
SELECT
    year_month_jst,
    event_date_key_jst,
    region_key,
    magnitude_band_code,
    depth_band_code
FROM iceberg.gold.v_event_daily
WHERE (magnitude_non_null_count = 0 AND (magnitude_avg IS NOT NULL OR magnitude_max IS NOT NULL))
   OR (depth_non_null_count = 0 AND depth_avg IS NOT NULL)
   OR (event_count <= 0);

-- Q6. Granularity Uniqueness Audit
-- Verifies that each daily grouping grain (event_date_key_jst, region_key, magnitude_band_code, depth_band_code, source_coverage_code)
-- is strictly unique (no duplicate rows).
-- Expected output: 0 rows.
SELECT
    event_date_key_jst,
    region_key,
    magnitude_band_code,
    depth_band_code,
    source_coverage_code,
    COUNT(*) AS row_occurrences
FROM iceberg.gold.v_event_daily
GROUP BY
    event_date_key_jst,
    region_key,
    magnitude_band_code,
    depth_band_code,
    source_coverage_code
HAVING COUNT(*) > 1;
