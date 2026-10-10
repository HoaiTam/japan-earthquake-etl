-- Read-only evidence queries for the exact QA bundle; not production namespace.
-- Never resolve a newer snapshot after a dataset is pinned.
-- aggregate_count expected=0
SELECT abs((SELECT coalesce(sum(source_count),0) FROM iceberg.gold_qa_2d155a4ff190443bb76e2e186bb39e7e.event_current FOR VERSION AS OF 1901025108914282099)-(SELECT count(*) FROM iceberg.gold_qa_2d155a4ff190443bb76e2e186bb39e7e.event_source_bridge FOR VERSION AS OF 5431466615262399871));
-- bridge_lineage expected=0
SELECT count(*) FROM iceberg.gold_qa_2d155a4ff190443bb76e2e186bb39e7e.event_source_bridge FOR VERSION AS OF 5431466615262399871 b LEFT JOIN iceberg.gold_qa_2d155a4ff190443bb76e2e186bb39e7e.event_current FOR VERSION AS OF 1901025108914282099 e ON b.canonical_event_id=e.canonical_event_id WHERE e.canonical_event_id IS NULL OR b.source_system NOT IN ('USGS','JMA_BULLETIN') OR b.source_system IS NULL OR b.source_record_key IS NULL OR b.raw_object_uri IS NULL OR NOT starts_with(b.raw_object_uri,'s3://') OR b.bronze_manifest_id IS NULL OR b.is_primary IS NULL OR (b.source_system='JMA_BULLETIN' AND b.catalog_release IS NULL) OR b.event_month_utc <> format_datetime(e.event_time_utc,'yyyy-MM');
-- changed_run_scope expected=0
SELECT count(*) FROM iceberg.gold_qa_2d155a4ff190443bb76e2e186bb39e7e.event_current FOR VERSION AS OF 1901025108914282099 WHERE gold_run_id='gld03-qa-e281d2fb056a4b37a66181966af76032' AND format_datetime(event_time_utc AT TIME ZONE 'UTC','yyyy-MM') NOT IN ('2023-01');
-- count:dim_date expected=3
SELECT count(*) FROM iceberg.gold_qa_2d155a4ff190443bb76e2e186bb39e7e.dim_date FOR VERSION AS OF 4773099487833932227;
-- count:dim_depth_band expected=5
SELECT count(*) FROM iceberg.gold_qa_2d155a4ff190443bb76e2e186bb39e7e.dim_depth_band FOR VERSION AS OF 6942070522731508153;
-- count:dim_magnitude_band expected=7
SELECT count(*) FROM iceberg.gold_qa_2d155a4ff190443bb76e2e186bb39e7e.dim_magnitude_band FOR VERSION AS OF 6354756532596391536;
-- count:dim_region expected=1
SELECT count(*) FROM iceberg.gold_qa_2d155a4ff190443bb76e2e186bb39e7e.dim_region FOR VERSION AS OF 6772357552088406235;
-- count:event_current expected=16
SELECT count(*) FROM iceberg.gold_qa_2d155a4ff190443bb76e2e186bb39e7e.event_current FOR VERSION AS OF 1901025108914282099;
-- count:event_source_bridge expected=16
SELECT count(*) FROM iceberg.gold_qa_2d155a4ff190443bb76e2e186bb39e7e.event_source_bridge FOR VERSION AS OF 5431466615262399871;
-- current:dim_date expected=4773099487833932227
SELECT snapshot_id FROM iceberg.gold_qa_2d155a4ff190443bb76e2e186bb39e7e."dim_date$refs" WHERE name='main';
-- current:dim_depth_band expected=6942070522731508153
SELECT snapshot_id FROM iceberg.gold_qa_2d155a4ff190443bb76e2e186bb39e7e."dim_depth_band$refs" WHERE name='main';
-- current:dim_magnitude_band expected=6354756532596391536
SELECT snapshot_id FROM iceberg.gold_qa_2d155a4ff190443bb76e2e186bb39e7e."dim_magnitude_band$refs" WHERE name='main';
-- current:dim_region expected=6772357552088406235
SELECT snapshot_id FROM iceberg.gold_qa_2d155a4ff190443bb76e2e186bb39e7e."dim_region$refs" WHERE name='main';
-- current:event_current expected=1901025108914282099
SELECT snapshot_id FROM iceberg.gold_qa_2d155a4ff190443bb76e2e186bb39e7e."event_current$refs" WHERE name='main';
-- current:event_source_bridge expected=5431466615262399871
SELECT snapshot_id FROM iceberg.gold_qa_2d155a4ff190443bb76e2e186bb39e7e."event_source_bridge$refs" WHERE name='main';
-- dimensions expected=0
SELECT count(*) FROM iceberg.gold_qa_2d155a4ff190443bb76e2e186bb39e7e.event_current FOR VERSION AS OF 1901025108914282099 e LEFT JOIN iceberg.gold_qa_2d155a4ff190443bb76e2e186bb39e7e.dim_date FOR VERSION AS OF 4773099487833932227 d ON e.event_date_key_utc=d.date_key LEFT JOIN iceberg.gold_qa_2d155a4ff190443bb76e2e186bb39e7e.dim_date FOR VERSION AS OF 4773099487833932227 j ON e.event_date_key_jst=j.date_key LEFT JOIN iceberg.gold_qa_2d155a4ff190443bb76e2e186bb39e7e.dim_region FOR VERSION AS OF 6772357552088406235 r ON e.region_key=r.region_key LEFT JOIN iceberg.gold_qa_2d155a4ff190443bb76e2e186bb39e7e.dim_magnitude_band FOR VERSION AS OF 6354756532596391536 m ON e.magnitude_band_code=m.band_code LEFT JOIN iceberg.gold_qa_2d155a4ff190443bb76e2e186bb39e7e.dim_depth_band FOR VERSION AS OF 6942070522731508153 b ON e.depth_band_code=b.band_code WHERE d.date_key IS NULL OR j.date_key IS NULL OR r.region_key IS NULL OR m.band_code IS NULL OR b.band_code IS NULL OR d.calendar_date<>e.event_date_utc OR j.calendar_date<>e.event_date_jst OR r.region_name<>e.region_name OR r.region_category<>e.region_category;
-- identity:dim_date expected=1
SELECT count(*) FROM iceberg.gold_qa_2d155a4ff190443bb76e2e186bb39e7e."dim_date$snapshots" WHERE snapshot_id=4773099487833932227 AND summary['gold.operation_id']='gld03-qa-e281d2fb056a4b37a66181966af76032' AND summary['gold.identity_sha256']='82af73a4e6f8ed2b48a46079185e4108ef731a52ae4b4790a82f781a93f42caa';
-- identity:dim_depth_band expected=1
SELECT count(*) FROM iceberg.gold_qa_2d155a4ff190443bb76e2e186bb39e7e."dim_depth_band$snapshots" WHERE snapshot_id=6942070522731508153 AND summary['gold.operation_id']='gld03-qa-e281d2fb056a4b37a66181966af76032' AND summary['gold.identity_sha256']='82af73a4e6f8ed2b48a46079185e4108ef731a52ae4b4790a82f781a93f42caa';
-- identity:dim_magnitude_band expected=1
SELECT count(*) FROM iceberg.gold_qa_2d155a4ff190443bb76e2e186bb39e7e."dim_magnitude_band$snapshots" WHERE snapshot_id=6354756532596391536 AND summary['gold.operation_id']='gld03-qa-e281d2fb056a4b37a66181966af76032' AND summary['gold.identity_sha256']='82af73a4e6f8ed2b48a46079185e4108ef731a52ae4b4790a82f781a93f42caa';
-- identity:dim_region expected=1
SELECT count(*) FROM iceberg.gold_qa_2d155a4ff190443bb76e2e186bb39e7e."dim_region$snapshots" WHERE snapshot_id=6772357552088406235 AND summary['gold.operation_id']='gld03-qa-e281d2fb056a4b37a66181966af76032' AND summary['gold.identity_sha256']='82af73a4e6f8ed2b48a46079185e4108ef731a52ae4b4790a82f781a93f42caa';
-- identity:event_current expected=1
SELECT count(*) FROM iceberg.gold_qa_2d155a4ff190443bb76e2e186bb39e7e."event_current$snapshots" WHERE snapshot_id=1901025108914282099 AND summary['gold.operation_id']='gld03-qa-e281d2fb056a4b37a66181966af76032' AND summary['gold.identity_sha256']='82af73a4e6f8ed2b48a46079185e4108ef731a52ae4b4790a82f781a93f42caa';
-- identity:event_source_bridge expected=1
SELECT count(*) FROM iceberg.gold_qa_2d155a4ff190443bb76e2e186bb39e7e."event_source_bridge$snapshots" WHERE snapshot_id=5431466615262399871 AND summary['gold.operation_id']='gld03-qa-e281d2fb056a4b37a66181966af76032' AND summary['gold.identity_sha256']='82af73a4e6f8ed2b48a46079185e4108ef731a52ae4b4790a82f781a93f42caa';
-- outside:event_current expected=0
SELECT 0;
-- outside:event_source_bridge expected=0
SELECT 0;
-- primary_provenance expected=0
SELECT count(*) FROM iceberg.gold_qa_2d155a4ff190443bb76e2e186bb39e7e.event_current FOR VERSION AS OF 1901025108914282099 e LEFT JOIN iceberg.gold_qa_2d155a4ff190443bb76e2e186bb39e7e.event_source_bridge FOR VERSION AS OF 5431466615262399871 b ON e.canonical_event_id=b.canonical_event_id AND b.is_primary WHERE b.source_observation_id IS NULL OR b.source_observation_id<>e.primary_observation_id OR b.source_system<>e.canonical_source_system;
-- required_fields expected=0
SELECT count(*) FROM iceberg.gold_qa_2d155a4ff190443bb76e2e186bb39e7e.event_current FOR VERSION AS OF 1901025108914282099 WHERE schema_version IS NULL OR canonical_event_id IS NULL OR primary_observation_id IS NULL OR canonical_source_system IS NULL OR event_time_utc IS NULL OR event_time_jst IS NULL OR event_date_utc IS NULL OR event_date_jst IS NULL OR event_date_key_utc IS NULL OR event_date_key_jst IS NULL OR latitude IS NULL OR longitude IS NULL OR event_type_code IS NULL OR is_natural_earthquake IS NULL OR is_in_study_area IS NULL OR region_key IS NULL OR region_name IS NULL OR region_category IS NULL OR magnitude_band_code IS NULL OR depth_band_code IS NULL OR source_count IS NULL OR has_usgs IS NULL OR has_jma IS NULL OR source_coverage_code IS NULL OR link_status IS NULL OR quality_status IS NULL OR canonical_model_version IS NULL OR gold_run_id IS NULL OR record_updated_at_utc IS NULL OR latitude NOT BETWEEN -90 AND 90 OR longitude NOT BETWEEN -180 AND 180 OR NOT is_finite(latitude) OR NOT is_finite(longitude) OR (magnitude IS NOT NULL AND NOT is_finite(magnitude)) OR (depth_km IS NOT NULL AND NOT is_finite(depth_km)) OR schema_version <> '1.0' OR source_count NOT IN (1,2) OR quality_status NOT IN ('VALID','WARNING') OR canonical_source_system NOT IN ('USGS','JMA_BULLETIN') OR region_category NOT IN ('PREFECTURE','OFFSHORE','UNKNOWN') OR source_coverage_code <> CASE WHEN has_usgs AND has_jma THEN 'USGS_JMA' WHEN has_usgs THEN 'USGS_ONLY' ELSE 'JMA_ONLY' END OR (has_jma AND (catalog_era IS NULL OR catalog_era NOT IN ('LEGACY','UNIFIED'))) OR (NOT has_jma AND catalog_era IS NOT NULL) OR is_natural_earthquake <> (event_type_code = 'EARTHQUAKE');
-- serving_count expected=16
SELECT count(*) FROM iceberg.gold_qa_2d155a4ff190443bb76e2e186bb39e7e.v_d03dad056f8ea9b4af290ddf_earthquake_event_current;
-- source_counts expected=0
SELECT count(*) FROM iceberg.gold_qa_2d155a4ff190443bb76e2e186bb39e7e.event_current FOR VERSION AS OF 1901025108914282099 e LEFT JOIN (SELECT canonical_event_id,count(*) n,count_if(is_primary) p,count_if(source_system='USGS') u,count_if(source_system='JMA_BULLETIN') j FROM iceberg.gold_qa_2d155a4ff190443bb76e2e186bb39e7e.event_source_bridge FOR VERSION AS OF 5431466615262399871 GROUP BY canonical_event_id) b ON e.canonical_event_id=b.canonical_event_id WHERE b.n IS NULL OR b.n<>e.source_count OR b.p<>1 OR b.u>1 OR b.j>1 OR e.has_usgs<>(b.u>0) OR e.has_jma<>(b.j>0);
-- stable_serving_count expected=16
SELECT count(*) FROM iceberg.gold_qa_2d155a4ff190443bb76e2e186bb39e7e.earthquake_event_current;
-- unique:dim_date expected=0
SELECT count(*)-count(DISTINCT date_key) FROM iceberg.gold_qa_2d155a4ff190443bb76e2e186bb39e7e.dim_date FOR VERSION AS OF 4773099487833932227;
-- unique:dim_depth_band expected=0
SELECT count(*)-count(DISTINCT band_code) FROM iceberg.gold_qa_2d155a4ff190443bb76e2e186bb39e7e.dim_depth_band FOR VERSION AS OF 6942070522731508153;
-- unique:dim_magnitude_band expected=0
SELECT count(*)-count(DISTINCT band_code) FROM iceberg.gold_qa_2d155a4ff190443bb76e2e186bb39e7e.dim_magnitude_band FOR VERSION AS OF 6354756532596391536;
-- unique:dim_region expected=0
SELECT count(*)-count(DISTINCT region_key) FROM iceberg.gold_qa_2d155a4ff190443bb76e2e186bb39e7e.dim_region FOR VERSION AS OF 6772357552088406235;
-- unique:event_current expected=0
SELECT count(*)-count(DISTINCT canonical_event_id) FROM iceberg.gold_qa_2d155a4ff190443bb76e2e186bb39e7e.event_current FOR VERSION AS OF 1901025108914282099;
-- unique:event_source_bridge expected=0
SELECT count(*)-count(DISTINCT source_observation_id) FROM iceberg.gold_qa_2d155a4ff190443bb76e2e186bb39e7e.event_source_bridge FOR VERSION AS OF 5431466615262399871;
-- utc_jst expected=0
SELECT count(*) FROM iceberg.gold_qa_2d155a4ff190443bb76e2e186bb39e7e.event_current FOR VERSION AS OF 1901025108914282099 WHERE event_time_jst<>CAST(event_time_utc AT TIME ZONE 'Asia/Tokyo' AS timestamp(6)) OR event_date_utc<>CAST(event_time_utc AT TIME ZONE 'UTC' AS date) OR event_date_jst<>CAST(event_time_jst AS date) OR event_date_key_utc<>CAST(format_datetime(event_time_utc AT TIME ZONE 'UTC','yyyyMMdd') AS integer) OR event_date_key_jst<>CAST(format_datetime(event_time_jst,'yyyyMMdd') AS integer) OR (canonical_source_system='JMA_BULLETIN' AND catalog_era<>CASE WHEN event_time_jst<TIMESTAMP '1997-10-01 00:00:00' THEN 'LEGACY' ELSE 'UNIFIED' END);
-- MLD-01 resolve a published dataset input, then persist all pins before reading.
SELECT publication_id,gold_run_id,snapshot_id,gold_table_name,physical_gold_table_name,snapshot_bundle_json,event_count,schema_version,verified_at_utc,published_at_utc,serving_view FROM iceberg.gold_qa_2d155a4ff190443bb76e2e186bb39e7e.publication_status WHERE publication_id='pub_d03dad056f8ea9b4af290ddf92009042d123bf9db0512e549ca0e5e180449b9f' AND verify_status='PASSED';
-- Consumer sample query uses immutable natural/ROI serving view.
SELECT canonical_event_id,event_time_utc,event_time_jst,magnitude,depth_km,source_count FROM iceberg.gold_qa_2d155a4ff190443bb76e2e186bb39e7e.v_d03dad056f8ea9b4af290ddf_earthquake_event_current ORDER BY event_time_utc,canonical_event_id LIMIT 5;
