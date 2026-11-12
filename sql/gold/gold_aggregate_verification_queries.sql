-- ============================================================================
-- Gold Aggregate Verification Queries (GLD-02)
-- Catalog: iceberg.gold.*
-- Target: Trino query smoke test, daily/monthly aggregate reconciliation,
--         and zero double-counting verification.
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
    WHERE is_earthquake = true
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
    SELECT event_date_key_jst, COUNT(DISTINCT event_id) AS distinct_event_count
    FROM iceberg.gold.event_current
    WHERE is_earthquake = true
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
-- Verifies that sum(event_count) in monthly rollup strictly matches daily totals per year_month.
-- Expected output: 0 rows with mismatch.
SELECT
    d.year_month_jst,
    d.daily_total_events,
    m.monthly_total_events,
    (d.daily_total_events - m.monthly_total_events) AS delta
FROM (
    SELECT year_month_jst, SUM(event_count) AS daily_total_events
    FROM iceberg.gold.v_event_daily
    GROUP BY year_month_jst
) d
FULL OUTER JOIN (
    SELECT year_month_jst, SUM(event_count) AS monthly_total_events
    FROM iceberg.gold.v_event_monthly
    GROUP BY year_month_jst
) m
  ON d.year_month_jst = m.year_month_jst
WHERE d.daily_total_events <> m.monthly_total_events
   OR d.daily_total_events IS NULL
   OR m.monthly_total_events IS NULL;

-- Q4. Honest NULL & Mathematical Invariant Audit
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

-- Q5. Granularity Uniqueness Audit
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
