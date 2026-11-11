-- ============================================================================
-- ML Serving & Publication Gate Verification Queries (MLI-04)
-- Catalog: iceberg.ml.*
-- Target: Offline verification harness, Trino smoke, and production gating.
-- ============================================================================

-- Q1. Publication Gate Audit: Verify that NO unapproved experiments leak into serving views.
-- Expected output: 0 rows.
SELECT
    experiment_run_id,
    algorithm_name,
    experiment_status
FROM iceberg.ml.v_experiment_runs
WHERE experiment_status NOT IN ('CANDIDATE', 'APPROVED');

-- Q2. Denominator Integrity & Summary Candidate Count Parity.
-- Verifies that candidate_count in sequence_summary matches exact membership row counts.
-- Expected output: 0 rows with mismatch.
SELECT
    s.experiment_run_id,
    s.mainshock_event_id,
    s.candidate_count AS summary_candidate_count,
    COUNT(m.candidate_event_id) AS membership_row_count
FROM iceberg.ml.sequence_summary s
LEFT JOIN iceberg.ml.sequence_membership m
  ON s.experiment_run_id = m.experiment_run_id
 AND s.mainshock_event_id = m.mainshock_event_id
JOIN iceberg.ml.experiment_run e
  ON s.experiment_run_id = e.experiment_run_id
WHERE e.experiment_status IN ('CANDIDATE', 'APPROVED')
GROUP BY s.experiment_run_id, s.mainshock_event_id, s.candidate_count
HAVING s.candidate_count <> COUNT(m.candidate_event_id);

-- Q3. Physical Consistency: Aftershock Count Reconciliation.
-- Verifies that aftershock_count in sequence_summary matches count of POST sequence members.
-- Excludes FAILED or SKIPPED windows where aftershock_count is expected to be NULL.
-- Expected output: 0 rows with mismatch.
SELECT
    s.experiment_run_id,
    s.mainshock_event_id,
    s.aftershock_count AS summary_aftershocks,
    COUNT(CASE WHEN m.event_role_candidate = 'POST' AND m.is_sequence_member = true THEN 1 END) AS membership_aftershocks
FROM iceberg.ml.sequence_summary s
LEFT JOIN iceberg.ml.sequence_membership m
  ON s.experiment_run_id = m.experiment_run_id
 AND s.mainshock_event_id = m.mainshock_event_id
JOIN iceberg.ml.experiment_run e
  ON s.experiment_run_id = e.experiment_run_id
WHERE e.experiment_status IN ('CANDIDATE', 'APPROVED')
  AND s.window_result_status = 'SUCCESS'
GROUP BY s.experiment_run_id, s.mainshock_event_id, s.aftershock_count
HAVING COALESCE(s.aftershock_count, 0) <> COUNT(CASE WHEN m.event_role_candidate = 'POST' AND m.is_sequence_member = true THEN 1 END);

-- Q4. Failure Window Reason Code Completeness.
-- Verifies that every FAILED or SKIPPED window has a non-null failure_reason_code.
-- Expected output: 0 rows with NULL reason code.
SELECT
    experiment_run_id,
    mainshock_event_id,
    window_result_status,
    failure_reason_code
FROM iceberg.ml.sequence_summary
WHERE window_result_status IN ('FAILED', 'SKIPPED')
  AND failure_reason_code IS NULL;

-- Q5. Algorithm Metric Policy Check (WINDOW DBCV NULL Enforcement).
-- Verifies that WINDOW baseline never fabricates density DBCV scores.
-- Expected output: 0 rows where WINDOW has non-null dbcv_score.
SELECT
    experiment_run_id,
    mainshock_event_id,
    algorithm_name,
    dbcv_score
FROM iceberg.ml.sequence_summary
WHERE algorithm_name = 'WINDOW'
  AND dbcv_score IS NOT NULL;

-- Q6. Multi-Sequence Resolution Audit: Raw Memberships vs Unique Candidates.
-- Verifies that resolved unique aftershocks strictly require resolution_rank = 1.
-- Rows with resolution_rank IS NULL (e.g. AMBIGUOUS) or rank > 1 are not counted.
SELECT
    m.experiment_run_id,
    COUNT(CASE WHEN m.is_sequence_member = true AND m.event_role_candidate = 'POST' THEN 1 END) AS raw_post_memberships,
    COUNT(DISTINCT CASE WHEN m.is_sequence_member = true AND m.event_role_candidate = 'POST' AND m.resolution_rank = 1 THEN m.candidate_event_id END) AS unique_resolved_aftershocks
FROM iceberg.ml.sequence_membership m
JOIN iceberg.ml.experiment_run e
  ON m.experiment_run_id = e.experiment_run_id
WHERE e.experiment_status IN ('CANDIDATE', 'APPROVED')
GROUP BY m.experiment_run_id;

-- Q7. Gold & Dataset Lineage Integrity Check.
-- Verifies that all experiment runs reference valid datasets registered in dataset_manifest.
-- Expected output: 0 orphan runs.
SELECT
    e.experiment_run_id,
    e.dataset_id
FROM iceberg.ml.experiment_run e
LEFT JOIN iceberg.ml.dataset_manifest d
  ON e.dataset_id = d.dataset_id
WHERE d.dataset_id IS NULL;
