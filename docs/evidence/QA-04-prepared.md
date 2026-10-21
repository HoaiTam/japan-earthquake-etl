# QA-04 prepared evidence

Status: `PREPARED`

Runtime request: `qa-04-runtime-v1`; runtime report: `qa-04-report-v1`.

ORC-07 and MLI-03 are Done; this template is still PREPARED, not runtime acceptance.
QA-04 requires its own fault/recovery evidence on real storage.

| Case | Fault | Status |
|---|---|---|
| `fetch_timeout` | `FETCH_TIMEOUT` | `PENDING_RUNTIME` |
| `archive_invalid` | `JMA_ARCHIVE_CHECKSUM_INVALID` | `PENDING_RUNTIME` |
| `silver_failure` | `SILVER_QUALITY_FAILED` | `PENDING_RUNTIME` |
| `gold_commit_partial` | `GOLD_COMMIT_PARTIAL` | `PENDING_RUNTIME` |
| `gold_commit_receipt_lost` | `GOLD_RECEIPT_LOST` | `PENDING_RUNTIME` |
| `trino_verify_failure` | `TRINO_VERIFY_FAILED` | `PENDING_RUNTIME` |
| `publication_metadata_failure` | `PUBLICATION_READBACK_FAILED` | `PENDING_RUNTIME` |
| `ml_bundle_invalid` | `ML_BUNDLE_CHECKSUM_OR_SCHEMA_INVALID` | `PENDING_RUNTIME` |
| `ml_import_partial` | `ML_COMMIT_PARTIAL` | `PENDING_RUNTIME` |
| `lease_timeout_contender` | `TIMEOUT_LEASE_RETAINED` | `PENDING_RUNTIME` |
