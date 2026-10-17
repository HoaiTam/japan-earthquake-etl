# QA-02 - Rerun, duplicate and late revision

Status: **PREPARED**

Matrix SHA-256: `cb4e1a6d80c873d6348e5ace76f6979315db5b361a889cb9020b4d3a8960f67f`

Runtime acceptance is pending ORC-07 real adapters. Synthetic cases are a test plan, not a pass.

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
