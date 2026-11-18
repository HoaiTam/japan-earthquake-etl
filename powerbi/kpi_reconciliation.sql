-- Replace the literal bundle view only after resolving PinnedPublicationId in ActiveBundle.
-- Set the filter values to those selected in Power BI. NULL means no filter.
WITH filters AS (
    SELECT
        CAST(NULL AS INTEGER) AS start_date_key_jst,
        CAST(NULL AS INTEGER) AS end_date_key_jst_exclusive,
        CAST(NULL AS VARCHAR) AS region_key,
        CAST(NULL AS VARCHAR) AS source_coverage_code,
        CAST(NULL AS VARCHAR) AS magnitude_band_code,
        CAST(NULL AS VARCHAR) AS depth_band_code
)
SELECT
    COUNT(DISTINCT e.canonical_event_id) AS total,
    AVG(e.magnitude) AS avg_magnitude,
    MAX(e.magnitude) AS max_magnitude,
    AVG(e.depth_km) AS avg_depth_km,
    COUNT(CASE WHEN e.magnitude >= 5.0 THEN 1 END) AS strong,
    COUNT(CASE WHEN e.tsunami_flag = TRUE THEN 1 END) AS tsunami,
    COUNT(CASE WHEN LOWER(TRIM(e.max_intensity_code)) NOT IN ('', 'none') THEN 1 END)
        AS intensity_reported,
    COUNT(CASE WHEN e.source_coverage_code = 'USGS_ONLY' THEN 1 END) AS usgs_only,
    COUNT(CASE WHEN e.source_coverage_code = 'JMA_ONLY' THEN 1 END) AS jma_only,
    COUNT(CASE WHEN e.source_coverage_code = 'USGS_JMA' THEN 1 END) AS both
FROM iceberg.gold.v_bndl20230201sha256test01_earthquake_event_current AS e
CROSS JOIN filters AS f
WHERE (f.start_date_key_jst IS NULL OR e.event_date_key_jst >= f.start_date_key_jst)
  AND (f.end_date_key_jst_exclusive IS NULL OR e.event_date_key_jst < f.end_date_key_jst_exclusive)
  AND (f.region_key IS NULL OR e.region_key = f.region_key)
  AND (f.source_coverage_code IS NULL OR e.source_coverage_code = f.source_coverage_code)
  AND (f.magnitude_band_code IS NULL OR e.magnitude_band_code = f.magnitude_band_code)
  AND (f.depth_band_code IS NULL OR e.depth_band_code = f.depth_band_code);
