-- TRINO QA BEGIN
-- Check this row before reading the exact serving view below.
SELECT publication_id, serving_view, max_event_time_utc, event_count
FROM iceberg.gold.publication_status
WHERE publication_id = 'pub-202302-001'
  AND verify_status = 'PASSED'
  AND serving_view = 'iceberg.gold.v_bndl20230201sha256test01_earthquake_event_current';

-- Apply the same slicer predicates as Desktop to the input CTE for filtered QA.
WITH events AS (
    SELECT * FROM iceberg.gold.v_bndl20230201sha256test01_earthquake_event_current
)
SELECT count(DISTINCT canonical_event_id) AS total,
       count_if(magnitude >= 5) AS strong,
       avg(magnitude) AS average_magnitude,
       avg(depth_km) AS average_depth_km,
       count_if(lower(trim(max_intensity_code)) NOT IN ('', 'none')) AS intensity_reported,
       count_if(tsunami_flag) AS tsunami_flagged,
       count_if(source_coverage_code = 'USGS_ONLY') AS usgs_only,
       count_if(source_coverage_code = 'JMA_ONLY') AS jma_only,
       count_if(source_coverage_code = 'USGS_JMA') AS usgs_jma,
       count_if(latitude IS NOT NULL AND longitude IS NOT NULL) AS map_points
FROM events;

SELECT region_key, region_name, source_coverage_code, count(*) AS events
FROM iceberg.gold.v_bndl20230201sha256test01_earthquake_event_current
GROUP BY region_key, region_name, source_coverage_code
ORDER BY region_key, source_coverage_code;

SELECT depth_band_code, magnitude_band_code, count(*) AS events
FROM iceberg.gold.v_bndl20230201sha256test01_earthquake_event_current
GROUP BY depth_band_code, magnitude_band_code
ORDER BY depth_band_code, magnitude_band_code;

-- This deterministic event can be looked up by canonical_event_id in Desktop.
SELECT canonical_event_id, event_time_jst, region_name, place_name, magnitude, depth_km,
       latitude, longitude, has_usgs, has_jma, source_coverage_code, max_intensity_code, tsunami_flag
FROM iceberg.gold.v_bndl20230201sha256test01_earthquake_event_current
ORDER BY canonical_event_id
FETCH FIRST 1 ROW ONLY;

-- Compare record counts with distinct coordinate pairs, independently of map zoom.
WITH events AS (
    SELECT latitude, longitude, depth_band_code
    FROM iceberg.gold.v_bndl20230201sha256test01_earthquake_event_current
), map_scope AS (
    SELECT 'baseline' AS case_name, latitude, longitude FROM events
    UNION ALL
    SELECT 'shallow', latitude, longitude FROM events WHERE depth_band_code = 'SHALLOW'
), coordinate_groups AS (
    SELECT case_name, latitude, longitude, count(*) AS events
    FROM map_scope
    GROUP BY case_name, latitude, longitude
)
SELECT case_name,
       sum(events) AS canonical_events,
       sum(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN events ELSE 0 END) AS geocoded_events,
       count_if(latitude IS NOT NULL AND longitude IS NOT NULL) AS distinct_coordinate_pairs,
       coalesce(max(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN events END), 0)
           AS max_events_per_coordinate
FROM coordinate_groups
GROUP BY case_name
ORDER BY case_name;

-- Simulate one missing coordinate in a projection, without mutating the QA catalog.
WITH ranked AS (
    SELECT canonical_event_id, latitude, longitude,
           row_number() OVER (ORDER BY canonical_event_id) AS event_number
    FROM iceberg.gold.v_bndl20230201sha256test01_earthquake_event_current
), projected AS (
    SELECT canonical_event_id, CASE WHEN event_number <> 1 THEN latitude END AS map_latitude,
           longitude
    FROM ranked
)
SELECT count(DISTINCT canonical_event_id) AS canonical_events,
       count_if(map_latitude IS NOT NULL AND longitude IS NOT NULL) AS map_points
FROM projected;
-- TRINO QA END

-- SQLITE FIXTURE BEGIN
-- Load powerbi/sample_schema.sql into an in-memory database before these statements.
SELECT canonical_event_id, region_category, source_coverage_code, magnitude, depth_band_code, tsunami_flag
FROM v_bndl20230201sha256test01_earthquake_event_current
WHERE canonical_event_id IN ('can-003', 'can-008')
ORDER BY canonical_event_id;

WITH projected AS (
    SELECT canonical_event_id, CASE WHEN canonical_event_id <> 'can-003' THEN latitude END AS map_latitude,
           longitude
    FROM v_bndl20230201sha256test01_earthquake_event_current
)
SELECT count(DISTINCT canonical_event_id) AS canonical_events,
       count(CASE WHEN map_latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 END) AS map_points
FROM projected;
-- SQLITE FIXTURE END
