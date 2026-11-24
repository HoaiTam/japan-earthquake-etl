-- Render the __...__ placeholders from the evidence described in the MLQ-01 runbook.
-- SELECT only. All relations use the exact snapshots used to issue the receipt.
WITH pins AS (
    SELECT '__DATASET_ID__' AS dataset_id, '__RUN_ID__' AS run_id,
           '__MAINSHOCK_ID__' AS mainshock_id, '__EVENT_ID__' AS event_id,
           '__BUNDLE_SHA256__' AS bundle_sha256,
           '__GOLD_PUBLICATION_ID__' AS gold_publication_id,
           '__GOLD_TABLE__' AS physical_gold_table_name,
           '__DATASET_MANIFEST_SHA256__' AS dataset_manifest_sha256
),
gold_matches AS (
    SELECT count(*) AS gold_rows
    FROM __GOLD_TABLE__ FOR VERSION AS OF __GOLD_SNAPSHOT__
    CROSS JOIN pins p
    WHERE canonical_event_id = p.event_id
),
dataset_matches AS (
    SELECT count(*) AS dataset_rows,
           count(CASE WHEN d.dataset_status = 'EXPORTED' AND d.gold_snapshot_id = __GOLD_SNAPSHOT__
                        AND d.gold_publication_id = p.gold_publication_id
                        AND d.physical_gold_table_name = p.physical_gold_table_name
                        AND d.candidate_snapshot_id = __CANDIDATE_SNAPSHOT__
                        AND d.mainshock_snapshot_id = __MAINSHOCK_SNAPSHOT__ THEN 1 END) AS exported_rows
    FROM __DATASET_TABLE__ FOR VERSION AS OF __DATASET_SNAPSHOT__ d
    CROSS JOIN pins p
    WHERE d.dataset_id = p.dataset_id
),
registry_matches AS (
    SELECT count(*) AS registry_rows,
           count(CASE WHEN e.dataset_id = p.dataset_id
                       AND e.experiment_status IN ('CANDIDATE', 'APPROVED')
                       AND e.result_bundle_sha256 = p.bundle_sha256
                       AND e.dataset_registry_snapshot_id = CAST(__DATASET_SNAPSHOT__ AS varchar)
                       AND e.candidate_source_snapshot_id = CAST(__CANDIDATE_SNAPSHOT__ AS varchar)
                       AND e.mainshock_source_snapshot_id = CAST(__MAINSHOCK_SNAPSHOT__ AS varchar)
                       AND e.dataset_manifest_sha256 = p.dataset_manifest_sha256
                       AND e.membership_snapshot_id = __MEMBERSHIP_SNAPSHOT__
                       AND e.summary_snapshot_id = __SUMMARY_SNAPSHOT__ THEN 1 END) AS serving_rows
    FROM __RUN_TABLE__ FOR VERSION AS OF __RUN_SNAPSHOT__ e
    CROSS JOIN pins p
    WHERE e.experiment_run_id = p.run_id
),
candidates AS (
    SELECT c.candidate_event_id, c.relative_time_role
    FROM __CANDIDATE_TABLE__ FOR VERSION AS OF __CANDIDATE_SNAPSHOT__ c
    CROSS JOIN pins p
    WHERE c.dataset_id = p.dataset_id AND c.mainshock_event_id = p.mainshock_id
),
memberships AS (
    SELECT m.candidate_event_id, m.event_role_candidate, m.is_sequence_member, m.resolution_rank
    FROM __MEMBERSHIP_TABLE__ FOR VERSION AS OF __MEMBERSHIP_SNAPSHOT__ m
    CROSS JOIN pins p
    WHERE m.dataset_id = p.dataset_id AND m.experiment_run_id = p.run_id
      AND m.mainshock_event_id = p.mainshock_id
),
candidate_counts AS (
    SELECT count(*) AS candidate_rows, count(DISTINCT c.candidate_event_id) AS candidate_keys,
           count(CASE WHEN c.relative_time_role = 'MAINSHOCK' THEN 1 END) AS candidate_total_mainshock_rows,
           count(CASE WHEN c.candidate_event_id = p.mainshock_id
                       AND c.relative_time_role = 'MAINSHOCK' THEN 1 END) AS candidate_mainshock_rows,
           count(CASE WHEN c.candidate_event_id = p.event_id AND c.relative_time_role = 'POST' THEN 1 END) AS target_candidates
    FROM candidates c CROSS JOIN pins p
),
membership_counts AS (
    SELECT count(*) AS membership_rows, count(DISTINCT m.candidate_event_id) AS membership_keys,
           count(CASE WHEN m.event_role_candidate = 'MAINSHOCK' THEN 1 END) AS membership_total_mainshock_rows,
           count(CASE WHEN m.candidate_event_id = p.mainshock_id AND m.event_role_candidate = 'MAINSHOCK'
                       AND m.is_sequence_member THEN 1 END) AS membership_mainshock_rows,
           count(CASE WHEN m.is_sequence_member AND m.event_role_candidate = 'POST' THEN 1 END) AS post_members,
           count(CASE WHEN m.candidate_event_id = p.event_id AND m.is_sequence_member
                       AND m.event_role_candidate = 'POST' AND m.resolution_rank = 1 THEN 1 END) AS target_resolved
    FROM memberships m CROSS JOIN pins p
),
matched_grain AS (
    SELECT count(*) AS role_matched_rows
    FROM candidates c JOIN memberships m
      ON c.candidate_event_id = m.candidate_event_id AND c.relative_time_role = m.event_role_candidate
),
summary_counts AS (
    SELECT count(*) AS summary_rows,
           count(CASE WHEN s.window_result_status = 'SUCCESS' THEN 1 END) AS success_rows,
           max(s.candidate_count) AS summary_candidates, max(s.aftershock_count) AS summary_post_members
    FROM __SUMMARY_TABLE__ FOR VERSION AS OF __SUMMARY_SNAPSHOT__ s
    CROSS JOIN pins p
    WHERE s.dataset_id = p.dataset_id AND s.experiment_run_id = p.run_id
      AND s.mainshock_event_id = p.mainshock_id
)
SELECT p.dataset_id, p.run_id, p.mainshock_id, p.event_id,
       g.gold_rows, d.dataset_rows, d.exported_rows, r.registry_rows, r.serving_rows,
       c.candidate_rows, c.candidate_keys, c.candidate_total_mainshock_rows, c.candidate_mainshock_rows,
       c.target_candidates,
       m.membership_rows, m.membership_keys, m.membership_total_mainshock_rows, m.membership_mainshock_rows,
       m.post_members, m.target_resolved,
       a.role_matched_rows, s.summary_rows, s.success_rows, s.summary_candidates, s.summary_post_members,
       CASE WHEN g.gold_rows = 1 AND d.dataset_rows = 1 AND d.exported_rows = 1
               AND r.registry_rows = 1 AND r.serving_rows = 1
               AND c.target_candidates = 1 AND m.target_resolved = 1
               AND c.candidate_total_mainshock_rows = 1 AND c.candidate_mainshock_rows = 1
               AND m.membership_total_mainshock_rows = 1 AND m.membership_mainshock_rows = 1
               AND c.candidate_rows = c.candidate_keys AND m.membership_rows = m.membership_keys
               AND c.candidate_rows = m.membership_rows AND a.role_matched_rows = c.candidate_rows
               AND s.summary_rows = 1 AND s.success_rows = 1
               AND s.summary_candidates = c.candidate_rows AND s.summary_post_members = m.post_members
            THEN 'PASS' ELSE 'FAIL' END AS lineage_check
FROM pins p CROSS JOIN gold_matches g CROSS JOIN dataset_matches d CROSS JOIN registry_matches r
CROSS JOIN candidate_counts c CROSS JOIN membership_counts m CROSS JOIN matched_grain a
CROSS JOIN summary_counts s
