-- Trino ML Serving View: v_sequence_memberships_resolved
-- Granular sequence membership view linked to candidate event lineage.
-- Enforces publication gate (CANDIDATE/APPROVED only).
-- Separates raw membership records from resolved unique aftershocks to avoid double-counting.
-- Winner definition: strictly requires is_sequence_member = true, event_role_candidate = 'POST', and resolution_rank = 1.
-- Rows with resolution_rank IS NULL (e.g. AMBIGUOUS or unassigned) or resolution_rank > 1 are NEVER considered winners.

CREATE OR REPLACE VIEW iceberg.ml.v_sequence_memberships_resolved AS
SELECT
    m.experiment_run_id,
    m.dataset_id,
    m.mainshock_event_id,
    m.candidate_event_id,
    m.algorithm_name,
    m.model_config_version,
    m.cluster_id,
    m.is_noise,
    m.is_mainshock_cluster_member,
    m.is_sequence_member,
    m.membership_probability,
    m.event_role_candidate,
    m.normalized_distance,
    m.multi_sequence_status,
    m.resolution_rank,
    CASE
        WHEN m.is_sequence_member = true
         AND m.event_role_candidate = 'POST'
         AND m.resolution_rank = 1
        THEN true
        ELSE false
    END AS is_resolved_unique_aftershock,
    m.created_at_utc
FROM iceberg.ml.sequence_membership m
JOIN iceberg.ml.experiment_run e
  ON m.experiment_run_id = e.experiment_run_id
WHERE e.experiment_status IN ('CANDIDATE', 'APPROVED');
