-- Trino ML Serving View: v_failure_windows
-- Explicit view of failed or skipped mainshock windows for auditing and quality control.
-- Ensures that failure cases (e.g. EXP_MAINSHOCK_CLASSIFIED_AS_NOISE) are never hidden from scientific evaluation.
-- Enforces publication gate (CANDIDATE/APPROVED only).

CREATE OR REPLACE VIEW iceberg.ml.v_failure_windows AS
SELECT
    s.dataset_id,
    s.experiment_run_id,
    s.algorithm_name,
    s.model_config_version,
    s.mainshock_event_id,
    s.window_result_status,
    s.failure_reason_code,
    s.candidate_count,
    s.runtime_seconds,
    s.created_at_utc
FROM iceberg.ml.sequence_summary s
JOIN iceberg.ml.experiment_run e
  ON s.experiment_run_id = e.experiment_run_id
WHERE e.experiment_status IN ('CANDIDATE', 'APPROVED')
  AND s.window_result_status IN ('FAILED', 'SKIPPED');
