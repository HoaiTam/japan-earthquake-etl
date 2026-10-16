# QA-04 prepared evidence

Status: `PREPARED`

The bounded matrix and offline control-plane tests are committed. Runtime
evidence remains pending until ORC-07 and MLI-03 are complete.

| Case | Fault stage | Status |
|---|---|---|
| `fetch_timeout` | `FETCH` | `PENDING_RUNTIME` |
| `archive_invalid` | `ARCHIVE` | `PENDING_RUNTIME` |
| `silver_failure` | `SILVER` | `PENDING_RUNTIME` |
| `gold_commit_partial` | `GOLD` | `PENDING_RUNTIME` |
| `gold_commit_receipt_lost` | `GOLD` | `PENDING_RUNTIME` |
| `trino_verify_failure` | `TRINO` | `PENDING_RUNTIME` |
| `publication_metadata_failure` | `PUBLICATION` | `PENDING_RUNTIME` |
| `ml_bundle_invalid` | `ML_IMPORT` | `PENDING_RUNTIME` |
| `ml_import_partial` | `ML_IMPORT` | `PENDING_RUNTIME` |
| `lease_timeout_contender` | `LEASE` | `PENDING_RUNTIME` |

The offline checks use synthetic, bounded state and do not claim live storage,
Trino, deployed credentials or the full ORC-07 adapter chain.

## Checks run on Windows

- `python -m unittest airflow.tests.test_qa_04_failure_recovery` — 5/5 passed.
- Java component gate (`GoldIcebergWriterTest`, `GoldPublicationTest`,
  `TrinoSqlClientTest`, `ResultBundleContractTest`) — 18/18 passed.
- `python -m unittest discover -s airflow/tests -p
  'test_backfill_observability.py'` — 18/18 passed.
- `scripts/check-task-status.sh` — passed; 76 task files agree with index and
  tracking sections.
- `scripts/check-week-5-plan.sh` — passed; QA-04 remains week 6 and the week 5
  budget remains 72h.
- `git diff --check` — passed.

The existing recovery discovery reached 2/11 portable assertions; 9 cases
cannot start on this Windows host because those legacy tests invoke `sh`.
The security discovery reached 30/31 tests; its remaining case has the same
shell prerequisite. CI workflow discovery reached 33/34 tests; the remaining
case needs the optional `PyYAML` package. These are environment prerequisites,
not QA-04 assertion failures; GitHub CI remains the hosted gate.
