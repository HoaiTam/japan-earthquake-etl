# QA-02 - Rerun, duplicate and late revision

Status: **PREPARED**

Matrix SHA-256: `3f98cac860dfd78f4367ce6fa370382e229cc015bb89b0a656696ba050b595d7`

ORC-07 is Done. This is a PREPARED template, not a runtime pass; see PIPELINE_QA_ACCEPTANCE.md.

| Case | Category | Operation | Status |
|---|---|---|---|
| rerun_same_input | retry | `qa02-rerun-001` | PENDING_RUNTIME |
| duplicate_same_source | duplicate | `qa02-duplicate-001` | PENDING_RUNTIME |
| usgs_late_revision | late_revision | `qa02-usgs-revision-001` | PENDING_RUNTIME |
| jma_release_replacement | late_revision | `qa02-jma-release-001` | PENDING_RUNTIME |
| overlap_usgs_jma | source_overlap | `qa02-overlap-001` | PENDING_RUNTIME |
| event_moves_month | partition_move | `qa02-month-move-001` | PENDING_RUNTIME |
| jma_record_withdrawn | withdrawal | `qa02-withdraw-001` | PENDING_RUNTIME |
| retention_outside_scope | retention | `qa02-retention-001` | PENDING_RUNTIME |

## Reconciliation to record

- logical keys and current/history rows
- canonical count and source overlap
- partition contents and exact snapshot IDs
- KPI before/after and retained old snapshots
