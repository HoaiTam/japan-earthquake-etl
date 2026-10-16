# MLI-03 - Result import runbook

## Phạm vi

`mli_03_import_results` nhận một request có dataset đã `EXPORTED`, exact
artifact references và candidate lineage. Python giữ lease cho toàn run và
chuyển request vào staging; Java/Spark đọc bundle, kiểm tra checksum/schema,
validate lineage và ghi ba bảng Iceberg. Chỉ readback trên các snapshot vừa
commit mới được ghi registry ở trạng thái `CANDIDATE`; importer không ghi
`APPROVED`.

## Request tối thiểu

```json
{
  "dag_id": "mli_03_import_results",
  "run_id": "mli03-qa-001",
  "experiment_run_id": "exp_fixture",
  "dataset_id": "ds_fixture",
  "dataset_status": "EXPORTED",
  "artifact_root": "/opt/pipeline/staging/ml-artifacts/exp_fixture",
  "journal_root": "/opt/pipeline/staging/ml-artifacts/journal/exp_fixture",
  "ml_namespace": "iceberg.ml_qa_0123456789abcdef",
  "algorithm_name": "WINDOW",
  "model_config_version": "1",
  "candidates": [
    {"mainshock_event_id": "main", "candidate_event_id": "main", "role": "MAINSHOCK"}
  ]
}
```

`MLI_IMPORT_RUNNER_COMMAND` chỉ lấy từ môi trường vận hành và được chạy
không qua shell. Runner phải nhận request path cuối cùng, tạo
`journal_root/import-report.json` và kết thúc với exit code 0. Report phải có
`experiment_status=CANDIDATE`, ba count readback dương và đúng
`experiment_run_id`/`dataset_id`. Request, report và journal không chứa
credential.

## Bảng và idempotency

Writer tạo `ml.sequence_membership`, `ml.sequence_summary` và
`ml.experiment_run` trong namespace đã pin. Hai bảng kết quả được partition theo
`experiment_run_id`; registry run giữ một dòng cho mỗi run và lưu fingerprint,
model/config versions, artifact URI, row counts cùng snapshot IDs. Cùng run và
cùng fingerprint là retry; fingerprint khác bị `IMP_EXPERIMENT_RUN_REUSED`.
Lỗi giữa các bước giữ journal và lease để recovery, không được đổi thành
`CANDIDATE`.

## Kiểm tra offline

```powershell
python -m unittest airflow.tests.test_mli_03_import_results
./mvnw.cmd --batch-mode --no-transfer-progress -pl spark -am -Dtest=ResultBundleContractTest,MlResultImportJobTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Runtime Iceberg/Trino cần namespace QA riêng có MinIO/Catalog/Trino đang chạy.
Không nâng fixture `DEVELOPMENT_PILOT` thành `EXPORTED`; acceptance trên storage
thật và verify Trino phải được ghi bổ sung vào evidence trước khi chuyển task
`Done`.
