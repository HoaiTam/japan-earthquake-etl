# MLI-03 offline evidence

Branch: `feat/mli-03-validate-import-iceberg`
PR baseline: commit `1e881e0`
Status tại mốc offline lịch sử: `In Progress`. Trạng thái hiện tại xem
[runtime acceptance 19/10/2026](./MLI-03_RUNTIME_ACCEPTANCE.md) và
[task MLI-03](../task/tasks/MLI-03.md); không dùng các giới hạn lịch sử bên dưới
để suy PR #79 vẫn mở hoặc runtime hiện tại chưa chạy.

## PR #79 review fixes

- Docker lifecycle fixture now includes `compose/airflow/mli-import-runner.sh`
  and the `mli-03-import-results` QA client; `source_sha()` safety checks are
  unchanged.
- `ml.experiment_run` reserves an experiment ID and result fingerprint before
  membership/summary writes. A changed fingerprint is rejected before result
  snapshots change.
- Failed retries write unique evidence under `attempts/`; they do not replace a
  canonical `CANDIDATE` or discard its fingerprint and snapshot IDs. Retries
  consult the namespace reservation and exact snapshot readback rather than
  returning the local journal as proof of publication.
- Airflow pins the request bytes and SHA-256 to the actual DagRun identity.
  Downstream steps validate that context; the lease owner is separate from
  import and experiment IDs. Release and completion both require exact owner
  confirmation.

## Checks run

- Java/Spark MLI-03 command:
  `.\mvnw.cmd --batch-mode --no-transfer-progress -pl spark -am '-Dtest=ResultBundleContractTest,MlResultImportJobTest,TrustedDatasetResolverTest,MlResultPublicationIcebergTest' '-Dsurefire.failIfNoSpecifiedTests=false' test` — **19/19 passed** on Windows after the reservation and changed-fingerprint regressions were added. The suite includes local Spark/Iceberg tests.
- Airflow suite in the repository Airflow image:
  `python3 -m unittest discover -s airflow/tests` — **223/223 passed**.
- CI contract suite in the same image:
  `python3 -m unittest discover -s tests/ci -p 'test_*.py'` — **37/37 passed**.
- DAG import in the Airflow image registered `mli_03_import_results` with
  `resolve_request`, `acquire_lease`, `stage`, `resolve_dataset_validate`,
  `commit_results`, `trino_verify`, `publish_readback`, `release_lease`, and
  `completion_gate`.
- Focused Docker lifecycle check:
  `python3 -m unittest discover -s tests -p test_docker_runtime.py -k mli_import_client`
  in the Airflow image — **1/1 passed**; preflight used verified mounts and did
  not build, pull, start, or initialize services.
- `python -m py_compile airflow/dags/mli_03_import_results.py scripts/docker-runtime.py`
  and `git diff --check` — passed.
- Security audit found **0 findings**, with **3 pending coverage items** and
  `can_mark_done=false`; runtime/notebook/export acceptance remains unverified.

## Limits and pending evidence

- The complete Docker lifecycle suite did not pass in this workspace. The
  Windows run is blocked by missing POSIX shell/symlink privileges; the
  container run could not execute the repository's `make -n` probes because
  `make` is unavailable there. The focused MLI-03 client preflight passed.
  GitHub Actions Project CI run #54 on implementation commit `e46faa3`
  completed successfully, including `docs-contracts` and all applicable Java,
  Airflow, and pilot suites.
- No MinIO/Catalog/Trino import or exact-snapshot runtime verification was run
  for these review fixes. Do not treat local Spark/Iceberg or mocked lease tests
  as runtime acceptance.
- PR #79 remains open and unmerged. This work is not committed or pushed yet.
  Keep MLI-03 `In Progress` until runtime acceptance and independent review are
  recorded.
