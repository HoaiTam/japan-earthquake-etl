# MLI-03 - Result import runbook

Nghiệm thu component ngày 19/10/2026: [runtime evidence](../evidence/MLI-03_RUNTIME_ACCEPTANCE.md).

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
`import_run_id` giữ operation ID trong request (fallback `run_id`), không tự lấy
experiment ID. Lease owner vẫn là DagRun thật, tách biệt cả hai operation IDs.

Iceberg không có transaction chung cho ba bảng. Vì vậy raw result table có thể
có snapshot của attempt dở dang; **đó không phải publication**. Consumer phải
resolve một dòng registry `CANDIDATE` cùng membership/summary snapshot pins và
đọc các pins đó, như `MlResultSnapshotReader.serving()`. Không query bảng result
current rồi coi mọi row là đã publish. Retry vẫn chạy independent Trino gate
trước khi trả receipt; không dùng journal hoặc Spark count thay Trino.

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

### Physical mapping v1

`SparkMlIcebergWriter` tạo schema trực tiếp từ `result-bundle-v1.json` cho
membership/summary; schema registry nằm ở `experimentFrame()`. Có thể xem đầy
đủ field/key/null rules tại [logical model](../specs/ML_DATA_MODEL.md) và
[machine-readable contract](../../spark/src/main/resources/ml/result-bundle-v1.json).

| Logical / Parquet input | Iceberg v1 | Quy tắc |
|---|---|---|
| string / json_string | STRING | JSON đã validate; không chứa credential |
| timestamp_utc / TIMESTAMP(MILLIS,true) | STRING | Canonical ISO-8601 UTC `Z` sau validate; không phải local time |
| boolean | BOOLEAN | Không thay null bằng false |
| integer / long | INT / BIGINT | Không đổi grain hoặc ID |
| double | DOUBLE | Chỉ hữu hạn; probability theo algorithm |
| array<string> | ARRAY<STRING> | Giữ thứ tự và null policy của contract |

Cả ba bảng `sequence_membership`, `sequence_summary`, `experiment_run` dùng
Iceberg format v2, Parquet, identity partition `experiment_run_id`. Result append
một lần cho mỗi fingerprint; registry overwrite **đúng partition experiment**
để chuyển `IMPORT_VALIDATING` → `CANDIDATE` hoặc `REJECTED`. Snapshot properties
`ml.operation_id`, `ml.result_bundle_sha256`, `ml.expected_row_count` phục vụ
readback/recovery. Không overwrite Gold, partition dataset khác hoặc experiment khác.

Registry bổ sung lineage vật lý: `membership_snapshot_id`, `summary_snapshot_id`,
`dataset_registry_snapshot_id`, `candidate_source_snapshot_id`,
`mainshock_source_snapshot_id`, `dataset_manifest_sha256`, `export_manifest_sha256`.
Experiment snapshot ID nằm ở receipt/journal, tránh self-reference trong chính row.
Không tuning file size/compaction bằng cách rewrite warehouse trong smoke này.

## Kiểm tra offline

```bash
make test-mli-import
```

Java/Spark tests cần Java 17; nếu máy chỉ có JDK mới hơn thì dùng bước build
Docker Java 17 bên dưới, không coi Maven chạy sai JDK là nghiệm thu đạt.

## Nghiệm thu trên storage thật

Chuẩn bị runtime **riêng, chỉ khi image/code chưa được chuẩn bị**:

```bash
make docker-build
make up-ml WAIT_TIMEOUT=600
make smoke-mli-import-acceptance
```

`up-ml` chuẩn bị MinIO/Catalog/Trino và init cần thiết, không
khởi động DAG import. Smoke chỉ preflight rồi chạy one-off service đã kiểm
`mli-03-import-results`, `--no-deps --pull never`; không build/start/init/reset.
Nếu preflight báo stale thì quay lại bước chuẩn bị, không bỏ freshness guard.

`mli_import_qa.py` giữ shared whole-run lease. Java tạo namespace mới
`<catalog>.ml_qa_<32 hex>`, seed hai candidate/ một mainshock/ một registry
`EXPORTED` từ fixture synthetic, rồi pin/verify ba snapshots qua Trino. Không
đọc nhầm fixture local như dữ liệu trên MinIO và không nhận pilot làm EXPORTED.
QA kiểm tra reject checksum/schema/grain/unknown/NaN trước result writes,
reject BUILDING/DEVELOPMENT_PILOT, first/rerun/fingerprint conflict, lost commit
outcome recovery và Trino blocker. Namespace hậu tố `ab`/`cd` dành cho failure
cases; không đụng production `ml` hoặc Gold. Kết thúc chạy lại cùng bundle qua
**production Python → shell runner → Java → Trino**, không dùng runner giả.

Thành công khi terminal có JSON `task=MLI-03`, `status=PASSED`,
`synthetic_component_qa=true`, `scientific_approval=false`; counts là membership
2 / summary 1 / experiment 1, ba publication snapshot IDs dương, checks đều đạt,
`control_plane.status=CANDIDATE` và `lease_release.status=RELEASED`.
Evidence giữ tại `report_path` trong shared staging volume. Đây là **component
acceptance bằng synthetic fixture trên storage thật**, không phải official
research dataset hoặc scientific approval. Mỗi lần smoke tạo namespace riêng;
không tự xóa evidence/table/warehouse sau test.

### Chạy đủ graph Airflow sau component storage

Khi component trên đã PASSED, lấy `import-request.json` nằm cùng thư mục với
`report_path` (không tự chọn request production khác). Chuẩn bị Airflow bằng
`make up-airflow WAIT_TIMEOUT=600` nếu deployment chưa khớp image/config hiện tại,
rồi chạy `make smoke-mli-import-dag MLI_IMPORT_REQUEST=<exact QA import-request.json>`.
Target này chỉ preflight và exec trong DAG processor đã chạy; không tự start/build.
Make không export `MLI_IMPORT_REQUEST` vào môi trường tính hash Compose: đây là
input của từng run, được truyền bằng argv/`compose run -e`, không phải config
khởi động Airflow/init. Đổi request không yêu cầu recreate init hoặc deployment.

Harness chỉ nhận accepted QA request dưới `mli-03-qa`, verify manifest SHA và
artifact/journal scope, rồi chạy một **DAG.test mới**. Nó dùng guard đã có của
ORC-07 để từ chối logical date tồn tại hoặc DagRun đang active trước khi gọi
`DAG.test`; không clear task/run, không mark-success, không pause/unpause.
Thành công yêu cầu đúng 9 task đều `success` (kể cả completion gate), result pins
và counts không đổi; receipt riêng `dag-acceptance-<id>.json` ghi DagRun ID.
Đây không phải cron/scheduled acceptance của QA-01 và không thay Gold→ML E2E.

Lỗi có evidence trong đúng thư mục `/opt/pipeline/staging/mli-03-qa/<run-id>/`.
Không chia sẻ `*.private.log`: chỉ kiểm tra local, trích reason code đã redacted.
Local JVM đã exit thì wrapper release đúng owner; timeout/outcome chưa rõ giữ
lease. Không xóa `owner.json` hoặc release owner của run khác để test lại.

## Import một request cụ thể

```bash
make smoke-mli-import MLI_IMPORT_REQUEST=/opt/pipeline/staging/ml-artifacts/request.json
```

Đây là control-plane component run, không phải scheduler DagRun. Muốn theo dõi
trong Airflow, trigger manual DAG `mli_03_import_results` với conf
`{"request_path":"/opt/pipeline/staging/ml-artifacts/request.json"}`. Registry
root của executor phải resolve đúng manifest ref đã pin. Chín task là graph
điều phối; stage/resolve/commit/trino placeholders hiện kiểm lease/context,
còn Java thực hiện validate/commit/Trino trong task `publish_readback` theo
thứ tự nội bộ. Không diễn giải task placeholder xanh là một commit độc lập.

## Giới hạn downstream

- [MLD-05 — Validate feature snapshot/export](../task/tasks/MLD-05.md) và
  [MLI-02 — Build dataset DAG](../task/tasks/MLI-02.md): còn nghiệm thu official
  coverage/export; component fixtures không chứng minh Gold→ML export thật.
- [EXP-02 — Baselines](../task/tasks/EXP-02.md),
  [EXP-03 — Adaptive/multi-sequence](../task/tasks/EXP-03.md),
  [EXP-04 — Scientific evaluation](../task/tasks/EXP-04.md): official model runs,
  Colab parity và scientific review riêng; importer không tự APPROVED.
- [MLI-04 — Trino/static report](../task/tasks/MLI-04.md),
  [MLQ-01 — ML E2E](../task/tasks/MLQ-01.md): consumer/report và whole-flow
  acceptance; phải dùng CANDIDATE pins, không raw current result rows.
