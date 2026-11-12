-- ============================================================================
-- Sample Schema & Fixture Data for Power BI / BI-02 Early Development (BI-01)
-- Dialect: ANSI SQL / SQLite compatible for local standalone development.
-- ============================================================================

-- 1. Fact Table: event_current (Gold Canonical Events)
DROP TABLE IF EXISTS event_current;
CREATE TABLE event_current (
    event_id TEXT PRIMARY KEY,
    canonical_event_id TEXT NOT NULL,
    event_date_key_jst INTEGER NOT NULL,
    event_date_jst TEXT NOT NULL,
    event_time_utc TEXT NOT NULL,
    latitude REAL NOT NULL,
    longitude REAL NOT NULL,
    depth_km REAL,
    magnitude REAL,
    region_key TEXT,
    region_name TEXT,
    region_category TEXT,
    magnitude_band_code TEXT,
    depth_band_code TEXT,
    source_coverage_code TEXT NOT NULL,
    is_earthquake INTEGER NOT NULL,
    is_in_study_area INTEGER NOT NULL,
    is_strong_earthquake INTEGER NOT NULL,
    tsunami_flag INTEGER,
    alert_level TEXT,
    jma_max_intensity TEXT
);

-- 2. Dimension Tables
DROP TABLE IF EXISTS dim_date;
CREATE TABLE dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date TEXT NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name TEXT NOT NULL,
    day INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name TEXT NOT NULL,
    is_weekend INTEGER NOT NULL
);

DROP TABLE IF EXISTS dim_region;
CREATE TABLE dim_region (
    region_key TEXT PRIMARY KEY,
    region_name TEXT NOT NULL,
    region_category TEXT NOT NULL,
    latitude_min REAL,
    latitude_max REAL,
    longitude_min REAL,
    longitude_max REAL
);

DROP TABLE IF EXISTS dim_magnitude_band;
CREATE TABLE dim_magnitude_band (
    band_code TEXT PRIMARY KEY,
    band_label TEXT NOT NULL,
    min_magnitude REAL,
    max_magnitude REAL,
    sort_order INTEGER NOT NULL
);

DROP TABLE IF EXISTS dim_depth_band;
CREATE TABLE dim_depth_band (
    band_code TEXT PRIMARY KEY,
    band_label TEXT NOT NULL,
    min_depth_km REAL,
    max_depth_km REAL,
    sort_order INTEGER NOT NULL
);

-- 3. Sample Dimension Data
INSERT INTO dim_magnitude_band VALUES
    ('LT_3', '< M3.0', 0.0, 3.0, 1),
    ('M3_TO_LT4', 'M3.0 - M3.9', 3.0, 4.0, 2),
    ('M4_TO_LT5', 'M4.0 - M4.9', 4.0, 5.0, 3),
    ('M5_TO_LT6', 'M5.0 - M5.9', 5.0, 6.0, 4),
    ('M6_TO_LT7', 'M6.0 - M6.9', 6.0, 7.0, 5),
    ('GE_7', '>= M7.0', 7.0, 10.0, 6),
    ('UNKNOWN', 'Unknown Magnitude', NULL, NULL, 99);

INSERT INTO dim_depth_band VALUES
    ('NEGATIVE', 'Negative / Surface (< 0 km)', -10.0, 0.0, 1),
    ('SHALLOW', 'Shallow (0 - 69 km)', 0.0, 70.0, 2),
    ('INTERMEDIATE', 'Intermediate (70 - 299 km)', 70.0, 300.0, 3),
    ('DEEP', 'Deep (>= 300 km)', 300.0, 1000.0, 4),
    ('UNKNOWN', 'Unknown Depth', NULL, NULL, 99);

INSERT INTO dim_region VALUES
    ('reg-tohoku', 'Tohoku Region', 'INLAND', 37.0, 41.5, 139.0, 142.5),
    ('reg-kanto', 'Kanto Region', 'INLAND', 34.9, 37.2, 138.5, 141.0),
    ('reg-kansai', 'Kansai Region', 'INLAND', 33.5, 36.0, 134.0, 136.5),
    ('OFFSHORE', 'Offshore Japan Trench', 'OFFSHORE', 30.0, 45.0, 142.0, 148.0),
    ('UNKNOWN', 'Unclassified Region', 'UNKNOWN', NULL, NULL, NULL, NULL);

INSERT INTO dim_date VALUES
    (20230101, '2023-01-01', 2023, 1, 'January', 1, 7, 'Sunday', 1),
    (20230105, '2023-01-05', 2023, 1, 'January', 5, 4, 'Thursday', 0),
    (20230110, '2023-01-10', 2023, 1, 'January', 10, 2, 'Tuesday', 0),
    (20230115, '2023-01-15', 2023, 1, 'January', 15, 7, 'Sunday', 1),
    (20230201, '2023-02-01', 2023, 2, 'February', 1, 3, 'Wednesday', 0),
    (20230205, '2023-02-05', 2023, 2, 'February', 5, 7, 'Sunday', 1);

-- 4. Sample Fact Data (Canonical Earthquakes)
INSERT INTO event_current VALUES
    ('ev-001', 'can-001', 20230101, '2023-01-01', '2023-01-01T03:15:00Z', 35.68, 139.76, 45.0, 4.2, 'reg-kanto', 'Kanto Region', 'INLAND', 'M4_TO_LT5', 'SHALLOW', 'BOTH_USGS_AND_JMA', 1, 1, 0, 0, NULL, '2'),
    ('ev-002', 'can-002', 20230101, '2023-01-01', '2023-01-01T08:20:00Z', 35.70, 139.80, 50.0, 4.8, 'reg-kanto', 'Kanto Region', 'INLAND', 'M4_TO_LT5', 'SHALLOW', 'USGS_ONLY', 1, 1, 0, 0, NULL, NULL),
    ('ev-003', 'can-003', 20230105, '2023-01-05', '2023-01-05T12:00:00Z', 38.30, 142.40, 25.0, 6.4, 'OFFSHORE', 'Offshore Japan Trench', 'OFFSHORE', 'M6_TO_LT7', 'SHALLOW', 'BOTH_USGS_AND_JMA', 1, 1, 1, 1, 'YELLOW', '4'),
    ('ev-004', 'can-004', 20230110, '2023-01-10', '2023-01-10T14:30:00Z', 39.50, 141.20, 120.0, 5.2, 'reg-tohoku', 'Tohoku Region', 'INLAND', 'M5_TO_LT6', 'INTERMEDIATE', 'BOTH_USGS_AND_JMA', 1, 1, 0, 0, 'GREEN', '3'),
    ('ev-005', 'can-005', 20230115, '2023-01-15', '2023-01-15T01:10:00Z', 34.69, 135.50, 15.0, 3.5, 'reg-kansai', 'Kansai Region', 'INLAND', 'M3_TO_LT4', 'SHALLOW', 'JMA_ONLY', 1, 1, 0, 0, NULL, '1'),
    ('ev-006', 'can-006', 20230115, '2023-01-15', '2023-01-15T18:45:00Z', 37.80, 143.50, 380.0, 7.1, 'OFFSHORE', 'Offshore Japan Trench', 'OFFSHORE', 'GE_7', 'DEEP', 'BOTH_USGS_AND_JMA', 1, 1, 1, 1, 'RED', '5+'),
    ('ev-007', 'can-007', 20230201, '2023-02-01', '2023-02-01T04:00:00Z', 36.10, 140.10, 55.0, 4.5, 'reg-kanto', 'Kanto Region', 'INLAND', 'M4_TO_LT5', 'SHALLOW', 'BOTH_USGS_AND_JMA', 1, 1, 0, 0, NULL, '2'),
    ('ev-008', 'can-008', 20230201, '2023-02-01', '2023-02-01T09:30:00Z', 35.00, 139.00, -2.0, NULL, 'UNKNOWN', 'Unclassified Region', 'UNKNOWN', 'UNKNOWN', 'NEGATIVE', 'USGS_ONLY', 1, 1, 0, 0, NULL, NULL),
    ('ev-009', 'can-009', 20230205, '2023-02-05', '2023-02-05T22:15:00Z', 38.00, 141.00, 40.0, 5.8, 'reg-tohoku', 'Tohoku Region', 'INLAND', 'M5_TO_LT6', 'SHALLOW', 'BOTH_USGS_AND_JMA', 1, 1, 0, 0, 'GREEN', '4'),
    -- Non-natural events (should be filtered out by view logic)
    ('ev-998', 'can-998', 20230105, '2023-01-05', '2023-01-05T02:00:00Z', 35.00, 139.00, 0.0, 2.2, 'reg-kanto', 'Kanto Region', 'INLAND', 'LT_3', 'SHALLOW', 'USGS_ONLY', 0, 1, 0, 0, NULL, NULL),
    -- Out of study area event (should be filtered out by view logic)
    ('ev-999', 'can-999', 20230105, '2023-01-05', '2023-01-05T03:00:00Z', 10.00, 120.00, 10.0, 7.5, 'UNKNOWN', 'Unclassified Region', 'UNKNOWN', 'GE_7', 'SHALLOW', 'USGS_ONLY', 1, 0, 1, 1, 'RED', NULL);

-- 5. Daily Aggregate View: v_event_daily
DROP VIEW IF EXISTS v_event_daily;
CREATE VIEW v_event_daily AS
SELECT
    e.event_date_key_jst,
    e.event_date_jst,
    CAST(SUBSTR(CAST(e.event_date_key_jst AS TEXT), 1, 6) AS INTEGER) AS year_month_jst,
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
    SUM(CASE WHEN e.is_strong_earthquake = 1 THEN 1 ELSE 0 END) AS strong_earthquake_count,
    SUM(CASE WHEN e.tsunami_flag = 1 THEN 1 ELSE 0 END) AS tsunami_flagged_count,
    SUM(CASE WHEN e.alert_level IS NOT NULL THEN 1 ELSE 0 END) AS alerted_event_count,
    SUM(CASE WHEN e.jma_max_intensity IS NOT NULL THEN 1 ELSE 0 END) AS intensity_reported_count,
    SUM(CASE WHEN e.region_category IN ('OFFSHORE', 'UNKNOWN') THEN 1 ELSE 0 END) AS offshore_or_unknown_count
FROM event_current e
WHERE e.is_earthquake = 1
  AND e.is_in_study_area = 1
GROUP BY
    e.event_date_key_jst,
    e.event_date_jst,
    CAST(SUBSTR(CAST(e.event_date_key_jst AS TEXT), 1, 6) AS INTEGER),
    COALESCE(e.region_key, 'UNKNOWN'),
    COALESCE(e.region_name, 'Unknown'),
    COALESCE(e.region_category, 'UNKNOWN'),
    COALESCE(e.magnitude_band_code, 'UNKNOWN'),
    COALESCE(e.depth_band_code, 'UNKNOWN'),
    e.source_coverage_code;

-- 6. Monthly Aggregate View: v_event_monthly
DROP VIEW IF EXISTS v_event_monthly;
CREATE VIEW v_event_monthly AS
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
        THEN SUM(d.magnitude_sum) / CAST(SUM(d.magnitude_non_null_count) AS REAL)
        ELSE NULL
    END AS magnitude_avg,
    MAX(d.magnitude_max) AS magnitude_max,
    SUM(d.depth_non_null_count) AS depth_non_null_count,
    SUM(d.depth_sum) AS depth_sum,
    CASE
        WHEN SUM(d.depth_non_null_count) > 0
        THEN SUM(d.depth_sum) / CAST(SUM(d.depth_non_null_count) AS REAL)
        ELSE NULL
    END AS depth_avg,
    SUM(d.strong_earthquake_count) AS strong_earthquake_count,
    SUM(d.tsunami_flagged_count) AS tsunami_flagged_count,
    SUM(d.alerted_event_count) AS alerted_event_count,
    SUM(d.intensity_reported_count) AS intensity_reported_count,
    SUM(d.offshore_or_unknown_count) AS offshore_or_unknown_count
FROM v_event_daily d
GROUP BY
    d.year_month_jst,
    d.region_key,
    d.region_name,
    d.region_category,
    d.magnitude_band_code,
    d.depth_band_code,
    d.source_coverage_code;
