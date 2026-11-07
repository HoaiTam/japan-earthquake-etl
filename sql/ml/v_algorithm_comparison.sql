-- Trino ML Serving View: v_algorithm_comparison
-- Multi-model comparative view across WINDOW, DBSCAN, HDBSCAN_GLOBAL, and HDBSCAN_ADAPTIVE.
-- Enforces publication gate (CANDIDATE/APPROVED only).
-- Preserves denominator integrity: failure windows are retained in total window count and failure rate.
-- Preserves NULL policy: metrics that are not applicable (e.g. DBCV for WINDOW) remain NULL and are never coerced to 0.

CREATE OR REPLACE VIEW iceberg.ml.v_algorithm_comparison AS
SELECT
    e.dataset_id,
    e.algorithm_name,
    e.model_config_version,
    e.experiment_status,
    COUNT(*) AS total_windows,
    SUM(CASE WHEN s.window_result_status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_windows,
    SUM(CASE WHEN s.window_result_status = 'FAILED' THEN 1 ELSE 0 END) AS failure_windows,
    SUM(CASE WHEN s.window_result_status = 'SKIPPED' THEN 1 ELSE 0 END) AS skipped_windows,
    CAST(SUM(CASE WHEN s.window_result_status = 'FAILED' THEN 1 ELSE 0 END) AS double) / NULLIF(COUNT(*), 0) AS failure_rate,
    SUM(s.candidate_count) AS total_candidates,
    SUM(s.sequence_member_count) AS total_sequence_members,
    SUM(s.aftershock_count) AS raw_aftershock_memberships,
    SUM(s.foreshock_candidate_count) AS total_foreshock_candidates,
    AVG(s.noise_ratio) AS avg_noise_ratio,
    AVG(s.dbcv_score) AS avg_dbcv_score,
    COUNT(s.dbcv_score) AS dbcv_computed_windows,
    AVG(s.omori_p) AS avg_omori_p,
    COUNT(s.omori_p) AS omori_fitted_windows,
    AVG(s.runtime_seconds) AS avg_runtime_seconds,
    AVG(s.peak_memory_mb) AS avg_peak_memory_mb
FROM iceberg.ml.experiment_run e
JOIN iceberg.ml.sequence_summary s
  ON e.experiment_run_id = s.experiment_run_id
WHERE e.experiment_status IN ('CANDIDATE', 'APPROVED')
GROUP BY
    e.dataset_id,
    e.algorithm_name,
    e.model_config_version,
    e.experiment_status;
