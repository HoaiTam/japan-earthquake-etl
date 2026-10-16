# MLI-03 - Result import runbook

## Phạm vi

`mli_03_import_results` nhận exact artifact references và một reference tới
manifest trong trusted registry. Java/Spark tự resolve trạng thái `EXPORTED`,
candidate/mainshock snapshot pins và lineage từ manifest đã kiểm checksum;
caller không được khai `dataset_status` hoặc candidate IDs. Python giữ lease
cho toàn run theo identity của Airflow DagRun thật. Import `run_id` và
`experiment_run_id` là operation/publication identity riêng, không được dùng
làm lease owner. Task resolve pin request bytes và SHA-256 vào context bền vững;
mọi task kiểm tra checksum context và nguồn request trước khi chạy tiếp. Java
runner nhận lease owner nội bộ do DagRun tạo, không nhận owner từ request ngoài.

Writer reserve `experiment_run_id` trong `ml.experiment_run` ở trạng thái
`IMPORT_VALIDATING` trước khi append membership/summary. Namespace reservation
là nguồn kiểm tra ID/fingerprint kể cả khi retry dùng journal directory khác.
Fingerprint khác bị từ chối trước mọi result-table write. Result tables được
commit trước, Trino verify trên exact snapshots, rồi mới cập nhật cùng dòng
registry thành `CANDIDATE`; importer không ghi `APPROVED`.

## Request tối thiểu

```json
{
  "dag_id": "mli_03_import_results",
  "run_id": "mli03-qa-001",
  "experiment_run_id": "exp_fixture",
  "dataset_id": "ds_fixture",
  "dataset_manifest_ref": "registry/ds_fixture.json",
  "dataset_manifest_sha256": "<sha256 của file manifest trong registry>",
  "artifact_root": "/opt/pipeline/staging/ml-artifacts/exp_fixture",
  "journal_root": "/opt/pipeline/staging/ml-artifacts/journal/exp_fixture",
  "ml_namespace": "iceberg.ml_qa_0123456789abcdef",
  "algorithm_name": "WINDOW",
  "model_config_version": "1",
  "import_run_id": "mli03-qa-001"
}
```

Manifest phải có `dataset_status=EXPORTED`, `export_manifest_ref`,
`export_manifest_sha256`, registry snapshot ID, candidate snapshot ID,
mainshock snapshot ID và candidate rows. Dataset chưa có các pins hoặc bị
đổi bytes sẽ bị reject; không fallback sang bảng current.

`MLI_IMPORT_RUNNER_COMMAND` chỉ lấy từ môi trường vận hành và được chạy
không qua shell. Image Airflow đóng gói `/opt/pipeline/bin/mli-import-runner`
và Spark/Iceberg runtime; runner phải nhận request path cuối cùng, tạo
`journal_root/import-report.json` và kết thúc với exit code 0. Report phải có
`experiment_status=CANDIDATE`, ba count readback dương và đúng
`experiment_run_id`/`dataset_id`. Request, report và journal không chứa
credential.

## Bảng và idempotency

Writer tạo `ml.sequence_membership`, `ml.sequence_summary` và
`ml.experiment_run` trong namespace đã pin. Hai bảng kết quả được partition theo
`experiment_run_id`; registry run giữ một dòng cho mỗi run và lưu fingerprint,
model/config versions, artifact URI, row counts cùng snapshot IDs. Cùng
experiment ID và fingerprint là retry; namespace reservation phát hiện
fingerprint khác trước mọi append và trả `IMP_EXPERIMENT_RUN_REUSED`. Lỗi
checksum/schema/lineage hoặc Trino quality blocker là `REJECTED` terminal.
Timeout, mất receipt hoặc outcome hạ tầng chưa rõ giữ `IMPORT_VALIDATING`,
ghi journal `mli03-journal-v2` theo từng bảng và resume bằng cùng operation /
fingerprint. Không append lại snapshot đã readback được.

Canonical `experiment-run-<id>.json` chỉ đổi theo lifecycle của đúng identity.
Lỗi một attempt được ghi riêng dưới `attempts/`; một retry bundle lỗi không
được ghi đè journal `CANDIDATE` hoặc làm mất identity/snapshot đã pin. Lease chỉ
được release sau khi xác nhận owner hiện tại là đúng DagRun; completion yêu cầu
report `CANDIDATE` và receipt release `RELEASED` trùng owner.

## Kiểm tra offline

```powershell
python -m unittest airflow.tests.test_mli_03_import_results
./mvnw.cmd --batch-mode --no-transfer-progress -pl spark -am -Dtest=ResultBundleContractTest,MlResultImportJobTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Runtime Iceberg/Trino cần namespace QA riêng có MinIO/Catalog/Trino đang chạy.
Không nâng fixture `DEVELOPMENT_PILOT` thành `EXPORTED`; acceptance trên storage
thật và verify Trino phải được ghi bổ sung vào evidence trước khi chuyển task
`Done`.
