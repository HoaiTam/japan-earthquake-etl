-- Trino ML Serving View: v_experiment_runs
-- Exposes valid experiment runs joined with dataset manifest and Gold snapshot lineage.
-- Enforces publication gate: only runs with status 'CANDIDATE' or 'APPROVED' are visible.
-- Compatible with Iceberg catalog in Trino (iceberg.ml.*).

CREATE OR REPLACE VIEW iceberg.ml.v_experiment_runs AS
SELECT
    e.experiment_run_id,
    e.algorithm_name,
    e.model_config_version,
    e.algorithm_version,
    e.package_version,
    e.code_version,
    e.experiment_status,
    d.dataset_id,
    d.dataset_split,
    d.gold_table_name,
    d.gold_snapshot_id,
    d.period_start_utc,
    d.period_end_utc_exclusive,
    d.feature_version,
    d.scaling_config_version,
    e.membership_row_count,
    e.summary_row_count,
    e.artifact_uri,
    e.result_bundle_sha256,
    e.completed_at_utc,
    e.created_at_utc
FROM iceberg.ml.experiment_run e
JOIN iceberg.ml.dataset_manifest d
  ON e.dataset_id = d.dataset_id
WHERE e.experiment_status IN ('CANDIDATE', 'APPROVED');
