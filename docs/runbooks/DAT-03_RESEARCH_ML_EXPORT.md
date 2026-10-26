# DAT-03 — Checkpointed official collection build/export

Version `dat03-research-dataset-build-v1`, gọi lại10 fixed steps MLI-02 bằng
Java/Spark, không thêm Python ETL. Hiện controller/component tests đã có;
**chưa có real full-period acceptance**. Không chạy trước gate catalog288tháng.

## Input và phạm vi

- `prepare` kiểm exact frozen catalog request, full contiguous288 checkpoints,
  final accepted session/result/execution hashes và own release trước tạo
  attempt. Observation phải là retained copy trong đúng catalog host folder.
- Request pin catalog root/request SHA, exact prebuilt runner SHA, scaling
  config SHA, observation cutoff UTC và một split. REPRODUCTION/EXTENSION
  là hai attempts/dataset IDs riêng, giữ shared source pins và exact full Gold.
- Reuse selection/window/scaling config đã version hóa; natural/Japan/JMA/
  UNIFIED filters, full-split MAXC Mc, strict window coverage. Không caller Mc,
  sample, truncate, nới limits mù hoặc đổi scientific baseline để chạy nhanh.
- Java kiểm288 record/phase/source scope rồi đọc lại original stored final
  commit/publication; MLI-02 independently resolve publication và coverage.
  Không biến offline JSON hoặc research_ready boolean thành source proof.

## Checkpoint và lease

Controller host7200s, default Java6000s; control-plane chỉ nhận4200..6600s,
để dư startup so với reserve3600s trước step mới. Driver local[1], heap768MiB,
direct256MiB, shuffle16. Không build/pull/start/reset trong controller.
Mỗi host run/resume kiểm >=4GiB đĩa vật lý trước Docker; không thay phép đo
đó bằng logical free bên trong VM. Worker kiểm dependencies/volume reserve
trước tạo execution/acquire. Thiếu chỗ dừng, không tự xóa artifacts.

Request +8 input files được hash tại host và bootstrap trong container;
`-B -X pycache_prefix=/dev/null` không đọc/write cache cũ. Private stdout/
stderr và execution nằm `execution-sessions/<uuid>`, Java report ở
`sessions/<uuid-with-hyphens>.json`, receipts/receipt-chain immutable theo step.

- Clean checkpoint giữ đúng lease `mli_02_build_dataset/dat03-collection-<uuid>`;
  chỉ resume khi process/query/DagRun idle và lease vẫn đúng owner. Không
  tự acquire lại nếu checkpoint lease đã mất, không lấy foreign lease.
- Mỗi previous execution phải có acceptance, code/request/report/execution SHA
  khớp. Java đối soát plan + receipt-chain trước skip completed prefix.
- Failed/nonzero/timeout giữ lease/logs, không retry tự động. Timeout chỉ
  TERM/KILL process group được worker tạo; không cancel query hoặc restart.
- Chỉ release own lease sau10steps, fresh final mark_dataset_exported readback,
  EXPORTED/quality/counts/trusted registry được xác nhận và quiescence. Không
  chỉ lấy persisted completed_steps=10 để chứng nhận một handoff mới.
- Đây là explicit isolated adapter execution, **không scheduled Airflow run**.
  Không chỉnh cron/pause/DAG state hoặc thay deployed runner của default DAG.

## Cách dùng sau khi catalog thật đủ gate

Chạy từ checkout code đã kiểm thử. Dùng roots/observation/session **thực tế đã
đối soát**, không copy namespace/pins của example khác vào attempt mới.

```bash
make test-research-ml-export
make prepare-research-ml-export RESEARCH_DEPLOYMENT_ROOT="$dat03_deployment_root" RESEARCH_CATALOG_FOLDER="$dat03_catalog_folder" RESEARCH_ML_CATALOG_OBSERVATION="$dat03_catalog_observation" RESEARCH_ML_CATALOG_SESSION="$dat03_final_session" RESEARCH_ML_SPLIT=REPRODUCTION
```

Prepare in exact new folder; gán folder đó cho `dat03_ml_folder` rồi:

```bash
make run-research-ml-export RESEARCH_DEPLOYMENT_ROOT="$dat03_deployment_root" RESEARCH_ML_FOLDER="$dat03_ml_folder"
make resume-research-ml-export RESEARCH_DEPLOYMENT_ROOT="$dat03_deployment_root" RESEARCH_ML_FOLDER="$dat03_ml_folder"
```

Lệnh resume **chỉ** dùng cho checkpoint có acceptance, không failed/unknown.
EXTENSION dùng prepare riêng với RESEARCH_ML_SPLIT=EXTENSION sau split trước
đã kết thúc/release. Không chạy hai attempts đồng thời, không release thủ công
để nhường tài nguyên khi còn job/query chưa rõ kết quả.

## Evidence và phần còn lại

12Python mock tests: gate partial catalog, typed budgets, exact input files,
foreign/own lease, missing dependencies, success/checkpoint/fail/timeout/false
handoff. Hai Java pure config/full-catalog rejection tests không Spark/lake.
Chúng không thay capacity/full ETL/ML export acceptance trên dữ liệu nghiên cứu.

[DAT-03](../task/tasks/DAT-03.md) còn actual full288 catalog, hai split export,
offline all-shard loader/rerun/real resource measurements, immutable research
kit và ba máy verifier receipts. Scientific experiment/approval thuộc
[EXP-02](../task/tasks/EXP-02.md)/[EXP-03](../task/tasks/EXP-03.md), không tự cấp
trong controller này. Giữ giới hạn và evidence failed; không gọi Done từ code.
