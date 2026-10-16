# MLI-03 offline evidence

Branch: `feat/mli-03-validate-import-iceberg`
Baseline: `main` at `accdb245`
Status: `In Progress`

## Checks

- `python -m unittest discover -s airflow/tests -p 'test_mli_03_import_results.py'` — 3/3 passed.
- `./mvnw.cmd --batch-mode --no-transfer-progress -pl spark -am -Dtest=ResultBundleContractTest,MlResultImportJobTest -Dsurefire.failIfNoSpecifiedTests=false test` — 11/11 passed.
- `./mvnw.cmd --batch-mode --no-transfer-progress -pl spark -am -DskipTests compile` — `BUILD SUCCESS`.
- `python -m py_compile` for MLI-03 control-plane modules — passed.
- `git diff --check` — passed.

The tests cover exact Parquet schema, checksums, lineage, grain, candidate
counts, pilot manifest rejection, same-fingerprint retry, changed-fingerprint
reuse rejection and recovery after a failed commit. The writer pins operation
and fingerprint in Iceberg snapshot properties and the reader verifies exact
snapshot IDs before `CANDIDATE`.

## Coverage pending

Docker is installed but the Docker daemon is unavailable on this machine, and
`make` is not installed. MinIO/Catalog/Trino runtime import, namespace QA seed,
Trino exact-snapshot verification and deployed credential/permission checks are
therefore not claimed. Keep MLI-03 `In Progress` until that evidence is added.
