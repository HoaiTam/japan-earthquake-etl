# QA-03 - JMA historical backfill

QA-03 hiện có CLI/helper để dựng coverage matrix và kiểm tra runtime receipts.
Đây không phải Airflow DAG, và helper không thay thế real adapter của ORC-07.

## Chuẩn bị offline

Matrix v2 tại `airflow/dags/fixtures/qa_03_jma_historical_matrix.json` pin SHA-256
của `config/jma/hypocenter_archives_v1.csv`. Validator đối chiếu slot năm/segment,
tháng UTC, native bounds JST và archive cần cho halo ±16 giây. Tháng `1997-09`
cần cả `h199701.zip` và `h199710.zip`; việc đọc archive hỗ trợ không mở rộng
partition được ghi.

```powershell
python airflow/dags/qa_03_jma_historical_backfill.py `
  --matrix airflow/dags/fixtures/qa_03_jma_historical_matrix.json `
  --json staging/qa-03/qa-03.json `
  --markdown staging/qa-03/qa-03.md
```

Hoặc chạy `make test-qa-03`. Prepared report luôn là `PREPARED` với các case
`PENDING_RUNTIME`; giá trị capacity để `null` cho đến khi có phép đo thật.

## Request runtime

Sau khi ORC-07 đóng gói adapter, request JSON cần có:

- `task_id: QA-03`, `request_version: qa-03-runtime-v1`.
- `run_id`, `operation_id`, `namespace`.
- `matrix_path`, `matrix_sha256`, `inventory_path`, `inventory_sha256`.
- `source_cutoff_utc` dạng UTC `Z` và `input_refs` là danh sách `{uri, sha256}` chính xác.
- `report_path` mới bên trong thư mục staging đã mount; helper tạo evidence ở
  `<report_path>.evidence/`. Không dùng symlink hoặc đường dẫn có `..`.

Runtime report v3 phải khớp toàn bộ identity/scope, bốn coverage month và archive
pins đã verify, sáu case cùng operation IDs, snapshots trước/sau, evidence JSON
có checksum, và các phép đo capacity hữu hạn. Changed release phải đối chiếu
table/snapshot/release cũ và mới với receipts trước/sau, đồng thời đọc lại được
snapshot cũ. Retention phải dùng partition tháng hợp lệ nằm ngoài write scope,
đối chiếu count/content và readback snapshot với receipts.

Để ghi nhận PASS, report còn phải kèm commit receipt và publication report GLD-04
cho đủ bốn tháng. Mỗi cặp phải khớp checksum, Gold run, tháng/scope, namespace,
publication ID và bundle SHA; bundle gồm đủ sáu bảng và snapshot. Publication phải
có `engine=TRINO`, `verify_status=PASSED`, `published=true`, cùng các check Trino
bằng expected values. Snapshot tổng của report phải đúng bundle các tháng đó.
Report chỉ tới Silver, thiếu publication hoặc thiếu một gate Trino sẽ bị từ chối.

## Smoke Bronze-to-Published

Lệnh này chỉ preflight MinIO, Catalog, Trino, image/JAR/mount freshness rồi gọi
runner đã cấu hình trong container `qa-03-jma-historical-backfill`. Lệnh không
build, pull, start hoặc init service:

```powershell
make smoke-qa-03 `
  QA_03_RUNTIME_REQUEST=/opt/pipeline/staging/qa-03/request.json `
  QA_03_RUNTIME_COMMAND="$env:QA_03_RUNTIME_COMMAND"
```

ORC-07 hiện `Ready`; chưa có real adapters/evidence nối một run Bronze → Gold
Published. Không chạy smoke bằng executable fixture rồi ghi nhận thành runtime
acceptance. Dùng namespace QA riêng và chỉ request scope đã pin.

## Evidence

Template hiện ở [QA-03 JSON](../evidence/QA-03-prepared.json) và [Markdown](../evidence/QA-03-prepared.md).
Prepared evidence hiện chỉ là fixture kiểm tra validator, không phải Gold runtime
receipt. Historical report/capacity note chỉ hoàn tất sau khi có output adapter thật,
readback snapshots và reconciliation cho từng case. Bốn tháng đại diện không
đại diện toàn bộ 40 năm.
