-- ============================================================================
-- Gold Aggregate Verification Queries (GLD-02)
-- Catalog: iceberg.gold.*
-- Target: Trino query smoke test, daily/monthly aggregate reconciliation,
--         deep metric parity, bidirectional Honest NULL, and zero double-counting verification.
-- ============================================================================

-- Q1. Fact-to-Aggregate Count Reconciliation
-- Verifies that sum(event_count) across all daily aggregate rows strictly equals
-- the number of natural earthquakes in the study area in gold.event_current.
-- Supports valid empty datasets (coalesces to 0, returning delta = 0).
-- Expected output: exactly 1 row with delta = 0.
SELECT
    COALESCE(f.fact_event_count, 0) AS fact_event_count,
    COALESCE(a.daily_agg_event_count, 0) AS daily_agg_event_count,
    (COALESCE(f.fact_event_count, 0) - COALESCE(a.daily_agg_event_count, 0)) AS delta
FROM (
    SELECT COUNT(*) AS fact_event_count
    FROM iceberg.gold.event_current
    WHERE is_natural_earthquake = true
      AND is_in_study_area = true
) f
CROSS JOIN (
    SELECT COALESCE(SUM(event_count), 0) AS daily_agg_event_count
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

-- Q3. Additive Consistency and Full Metric Rollup Parity between Daily and Monthly
-- Verifies that monthly aggregates strictly equal the rollup of daily aggregates
-- at the full grain (year_month_jst, region_key, magnitude_band_code, depth_band_code, source_coverage_code)
-- across all 13 statistical and KPI metrics, including weighted averages, maxima, and missing/extra groups.
-- Expected output: 0 rows with mismatch.
SELECT
    COALESCE(d.year_month_jst, m.year_month_jst) AS year_month_jst,
    COALESCE(d.region_key, m.region_key) AS region_key,
    COALESCE(d.magnitude_band_code, m.magnitude_band_code) AS magnitude_band_code,
    COALESCE(d.depth_band_code, m.depth_band_code) AS depth_band_code,
    COALESCE(d.source_coverage_code, m.source_coverage_code) AS source_coverage_code,
    d.event_count AS daily_rollup_events,
    m.event_count AS monthly_events,
    d.magnitude_sum AS daily_rollup_mag_sum,
    m.magnitude_sum AS monthly_mag_sum,
    d.magnitude_avg AS daily_rollup_mag_avg,
    m.magnitude_avg AS monthly_mag_avg,
    d.magnitude_max AS daily_rollup_mag_max,
    m.magnitude_max AS monthly_mag_max,
    d.depth_sum AS daily_rollup_depth_sum,
    m.depth_sum AS monthly_depth_sum,
    d.depth_avg AS daily_rollup_depth_avg,
    m.depth_avg AS monthly_depth_avg
FROM (
    SELECT
        d_in.year_month_jst,
        d_in.region_key,
        d_in.region_name,
        d_in.region_category,
        d_in.magnitude_band_code,
        d_in.depth_band_code,
        d_in.source_coverage_code,
        SUM(d_in.event_count) AS event_count,
        SUM(d_in.magnitude_non_null_count) AS magnitude_non_null_count,
        SUM(d_in.magnitude_sum) AS magnitude_sum,
        CASE
            WHEN SUM(d_in.magnitude_non_null_count) > 0
            THEN SUM(d_in.magnitude_sum) / CAST(SUM(d_in.magnitude_non_null_count) AS DOUBLE)
            ELSE NULL
        END AS magnitude_avg,
        MAX(d_in.magnitude_max) AS magnitude_max,
        SUM(d_in.depth_non_null_count) AS depth_non_null_count,
        SUM(d_in.depth_sum) AS depth_sum,
        CASE
            WHEN SUM(d_in.depth_non_null_count) > 0
            THEN SUM(d_in.depth_sum) / CAST(SUM(d_in.depth_non_null_count) AS DOUBLE)
            ELSE NULL
        END AS depth_avg,
        SUM(d_in.strong_earthquake_count) AS strong_earthquake_count,
        SUM(d_in.tsunami_flagged_count) AS tsunami_flagged_count,
        SUM(d_in.alerted_event_count) AS alerted_event_count,
        SUM(d_in.intensity_reported_count) AS intensity_reported_count,
        SUM(d_in.offshore_or_unknown_count) AS offshore_or_unknown_count
    FROM iceberg.gold.v_event_daily d_in
    GROUP BY
        d_in.year_month_jst,
        d_in.region_key,
        d_in.region_name,
        d_in.region_category,
        d_in.magnitude_band_code,
        d_in.depth_band_code,
        d_in.source_coverage_code
) d
FULL OUTER JOIN iceberg.gold.v_event_monthly m
  ON d.year_month_jst = m.year_month_jst
 AND d.region_key = m.region_key
 AND d.magnitude_band_code = m.magnitude_band_code
 AND d.depth_band_code = m.depth_band_code
 AND d.source_coverage_code = m.source_coverage_code
WHERE d.year_month_jst IS NULL
   OR m.year_month_jst IS NULL
   OR d.region_name <> m.region_name
   OR d.region_category <> m.region_category
   OR d.event_count <> m.event_count
   OR d.magnitude_non_null_count <> m.magnitude_non_null_count
   OR d.depth_non_null_count <> m.depth_non_null_count
   OR d.strong_earthquake_count <> m.strong_earthquake_count
   OR d.tsunami_flagged_count <> m.tsunami_flagged_count
   OR d.alerted_event_count <> m.alerted_event_count
   OR d.intensity_reported_count <> m.intensity_reported_count
   OR d.offshore_or_unknown_count <> m.offshore_or_unknown_count
   OR (d.magnitude_sum IS NULL AND m.magnitude_sum IS NOT NULL)
   OR (d.magnitude_sum IS NOT NULL AND m.magnitude_sum IS NULL)
   OR ABS(d.magnitude_sum - m.magnitude_sum) > 0.0001
   OR (d.magnitude_avg IS NULL AND m.magnitude_avg IS NOT NULL)
   OR (d.magnitude_avg IS NOT NULL AND m.magnitude_avg IS NULL)
   OR ABS(d.magnitude_avg - m.magnitude_avg) > 0.0001
   OR (d.magnitude_max IS NULL AND m.magnitude_max IS NOT NULL)
   OR (d.magnitude_max IS NOT NULL AND m.magnitude_max IS NULL)
   OR ABS(d.magnitude_max - m.magnitude_max) > 0.0001
   OR (d.depth_sum IS NULL AND m.depth_sum IS NOT NULL)
   OR (d.depth_sum IS NOT NULL AND m.depth_sum IS NULL)
   OR ABS(d.depth_sum - m.depth_sum) > 0.0001
   OR (d.depth_avg IS NULL AND m.depth_avg IS NOT NULL)
   OR (d.depth_avg IS NOT NULL AND m.depth_avg IS NULL)
   OR ABS(d.depth_avg - m.depth_avg) > 0.0001;

-- Q4. Deep Grain-Level Fact-to-Daily Metric Parity
-- Computes metrics directly from gold.event_current at the daily grain and compares
-- with v_event_daily across all statistical and KPI indicators.
-- Strictly checks NULL/non-NULL separation before numeric tolerance comparison.
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
   OR (f.fact_mag_sum IS NULL AND d.magnitude_sum IS NOT NULL)
   OR (f.fact_mag_sum IS NOT NULL AND d.magnitude_sum IS NULL)
   OR ABS(f.fact_mag_sum - d.magnitude_sum) > 0.0001
   OR (f.fact_mag_avg IS NULL AND d.magnitude_avg IS NOT NULL)
   OR (f.fact_mag_avg IS NOT NULL AND d.magnitude_avg IS NULL)
   OR ABS(f.fact_mag_avg - d.magnitude_avg) > 0.0001
   OR (f.fact_mag_max IS NULL AND d.magnitude_max IS NOT NULL)
   OR (f.fact_mag_max IS NOT NULL AND d.magnitude_max IS NULL)
   OR ABS(f.fact_mag_max - d.magnitude_max) > 0.0001
   OR (f.fact_depth_sum IS NULL AND d.depth_sum IS NOT NULL)
   OR (f.fact_depth_sum IS NOT NULL AND d.depth_sum IS NULL)
   OR ABS(f.fact_depth_sum - d.depth_sum) > 0.0001
   OR (f.fact_depth_avg IS NULL AND d.depth_avg IS NOT NULL)
   OR (f.fact_depth_avg IS NOT NULL AND d.depth_avg IS NULL)
   OR ABS(f.fact_depth_avg - d.depth_avg) > 0.0001
   OR f.event_date_key_jst IS NULL
   OR d.event_date_key_jst IS NULL;

-- Q5. Honest NULL & Mathematical Invariant Audit
-- Verifies bidirectional consistency for Honest NULL in both daily and monthly aggregates:
-- 1. When non_null_count = 0, sum/avg/max MUST be NULL.
-- 2. When non_null_count > 0, sum/avg/max MUST NOT be NULL (even when value is 0.0).
-- 3. event_count must be strictly positive (> 0).
-- Expected output: 0 rows.
SELECT
    'v_event_daily' AS source_view,
    year_month_jst,
    event_date_key_jst,
    region_key,
    magnitude_band_code,
    depth_band_code
FROM iceberg.gold.v_event_daily
WHERE (magnitude_non_null_count = 0 AND (magnitude_avg IS NOT NULL OR magnitude_max IS NOT NULL OR magnitude_sum IS NOT NULL))
   OR (magnitude_non_null_count > 0 AND (magnitude_avg IS NULL OR magnitude_max IS NULL OR magnitude_sum IS NULL))
   OR (depth_non_null_count = 0 AND (depth_avg IS NOT NULL OR depth_sum IS NOT NULL))
   OR (depth_non_null_count > 0 AND (depth_avg IS NULL OR depth_sum IS NULL))
   OR (event_count <= 0)
UNION ALL
SELECT
    'v_event_monthly' AS source_view,
    year_month_jst,
    NULL AS event_date_key_jst,
    region_key,
    magnitude_band_code,
    depth_band_code
FROM iceberg.gold.v_event_monthly
WHERE (magnitude_non_null_count = 0 AND (magnitude_avg IS NOT NULL OR magnitude_max IS NOT NULL OR magnitude_sum IS NOT NULL))
   OR (magnitude_non_null_count > 0 AND (magnitude_avg IS NULL OR magnitude_max IS NULL OR magnitude_sum IS NULL))
   OR (depth_non_null_count = 0 AND (depth_avg IS NOT NULL OR depth_sum IS NOT NULL))
   OR (depth_non_null_count > 0 AND (depth_avg IS NULL OR depth_sum IS NULL))
   OR (event_count <= 0);

-- Q6. Daily Granularity Uniqueness Audit
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

-- Q7. Monthly Granularity Uniqueness Audit
-- Verifies that each monthly grouping grain (year_month_jst, region_key, magnitude_band_code, depth_band_code, source_coverage_code)
-- is strictly unique (no duplicate rows).
-- Expected output: 0 rows.
SELECT
    year_month_jst,
    region_key,
    magnitude_band_code,
    depth_band_code,
    source_coverage_code,
    COUNT(*) AS row_occurrences
FROM iceberg.gold.v_event_monthly
GROUP BY
    year_month_jst,
    region_key,
    magnitude_band_code,
    depth_band_code,
    source_coverage_code
HAVING COUNT(*) > 1;
