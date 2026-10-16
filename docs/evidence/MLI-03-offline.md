# MLI-03 offline evidence

Branch: `feat/mli-03-validate-import-iceberg`
Baseline: `main` at `accdb245`
Status: `In Progress`

## Checks

- `python -m unittest discover -s airflow/tests -p 'test_mli_03_import_results.py'` — 3/3 passed.
- `./mvnw.cmd --batch-mode --no-transfer-progress -pl spark -am -Dtest=ResultBundleContractTest,MlResultImportJobTest,TrustedDatasetResolverTest,MlResultPublicationIcebergTest -Dsurefire.failIfNoSpecifiedTests=false test` — 17/17 passed (includes 2 real local Spark/Iceberg publication/recovery cases).
- `python scripts/docker-runtime.py build` — Docker images and Java artifacts prepared; Spark container suite 390/390 passed.
- `python scripts/docker-runtime.py up --component ml` followed by `python scripts/docker-runtime.py check --component ml --client mli-03-import-results` — preflight passed for the earlier image; after the final catalog-config change, the next check correctly requires `docker-runtime.py up` again and is not claimed as current runtime evidence.
- `./mvnw.cmd --batch-mode --no-transfer-progress -pl spark -am -DskipTests compile` — `BUILD SUCCESS`.
- `python -m py_compile` for MLI-03 control-plane modules — passed.
- `git diff --check` — passed.
- `python scripts/audit-security.py --output staging/mli03-security-report.json` — 0 findings, exit 2 with pending config/runtime/notebook-export coverage; `can_mark_done=false`.

The tests cover trusted exported manifest pins, exact Parquet schema, checksums,
lineage, grain, candidate counts, pilot manifest rejection, same-fingerprint
retry, changed-fingerprint reuse rejection, no-CANDIDATE after a Trino blocker,
and recovery after an unknown outcome following the membership commit. The
writer pins operation/fingerprint in Iceberg snapshot properties and a durable
`mli03-journal-v2`; the reader verifies exact snapshot IDs before `CANDIDATE`.

## Coverage pending

`make` is not installed. The local `.env` still contains `change-me-*` values
and an invalid Fernet key, so `check-config-local` fails and a runtime import
must not be claimed until the operator replaces those values. Compose config,
task status, week-5 plan and `git diff --check` passed. The container Maven
suite ran 390/390 tests. The host full Maven suite previously ran 389 tests;
383 passed and 6 remain Windows environment-only failures (missing
`HADOOP_HOME`/winutils, NTFS symlink privilege, and an existing cross-process
fixture). Airflow's full 94-test suite remains blocked by Windows
`fcntl`/path limitations. Exact EXPORTED registry seeding, runtime import,
Trino exact-snapshot readback and deployed credential/permission checks are
still pending; keep MLI-03 `In Progress` until that evidence is added.
