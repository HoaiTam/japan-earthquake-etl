# QA-02 - Rerun, duplicate and late revision

## Chuẩn bị fixture

Ma trận tổng hợp tại
`airflow/dags/fixtures/qa_02_rerun_revision_matrix.json` bao phủ retry cùng
input, duplicate cùng nguồn, USGS revision đến trễ, JMA release replacement,
overlap hai nguồn, event đổi tháng, record bị rút và retention ngoài scope.
Mỗi case có operation identity riêng; fixture không gọi mạng và không ghi Gold.

```powershell
python airflow/dags/qa_02_rerun_revision.py `
  --matrix airflow/dags/fixtures/qa_02_rerun_revision_matrix.json `
  --json staging/qa-02/qa-02.json `
  --markdown staging/qa-02/qa-02.md
```

Evidence template hiện lưu tại [QA-02 JSON](../evidence/QA-02-prepared.json) và [Markdown](../evidence/QA-02-prepared.md). Dùng `make test-qa-02` để chạy unit test và tạo report template bằng `QA_02_MATRIX`, `QA_02_JSON` và `QA_02_MARKDOWN`.

Script là CLI/helper chuẩn bị và kiểm tra report; nó chưa phải Airflow DAG. Runtime request dùng `qa-02-runtime-v1` và phải có `run_id`, `namespace`, `operation_id`, đường dẫn matrix/report cùng SHA-256 của matrix. Controller kiểm tra các field này, đọc matrix trước khi gọi runner và từ chối request thiếu field, sai checksum hoặc sai namespace.

Runtime report dùng contract `qa-02-report-v2`. Controller chỉ nhận report khi dùng đúng matrix checksum, đủ cả tám case với operation ID khớp, mỗi case có `PASS` và input pins đúng. `snapshots.before/after` phải là danh sách không rỗng gồm `{table, snapshot_id}` hợp lệ; `evidence_ref` phải trỏ tới file đã kiểm tra trong cùng namespace report, kèm SHA-256 và identity của run/case/operation. Reconciliation phải có `check` khớp assertion trong matrix, dữ liệu `before/after` không null/rỗng và phép đối soát tương ứng: count/key không đổi, current row duy nhất, revision/release mới, history/lineage giữ lại, partition state, scope ngoài phạm vi hoặc snapshot cũ đọc được. Summary phải khớp kết quả từng case và không còn `FAIL` hoặc `PENDING_RUNTIME`; cờ PASS tổng không được dùng làm bằng chứng duy nhất.

- `old_snapshot_readable`: cả `before` và `after` phải chỉ đúng cùng `{table, snapshot_id}`. ID là số nguyên dương trong phạm vi signed 64-bit (hoặc chuỗi thập phân tương ứng); boolean, ID thiếu và `latest` bị từ chối. Reference phải xuất hiện trong `case.snapshots.before` và `case.snapshots.after` tương ứng, cùng với `after.readable=true`. Adapter ghi reference snapshot cũ đã readback vào danh sách sau recovery, kể cả khi current snapshot đã đổi.
- `old_release.auditable`: `before.release_id` và `after.release_id` phải là cùng một release không rỗng, khớp `reconciliation.release.before.release_id`; `after.auditable=true`. Ví dụ replacement `jma-old` → `jma-new` phải chứng minh vẫn audit được `jma-old`; assertion `release` kiểm tra release mới riêng.

Smoke runtime sau khi ORC-07 sẵn sàng:

```powershell
make smoke-qa-02 QA_02_RUNTIME_REQUEST=/opt/pipeline/staging/qa-02/request.json QA_02_RUNTIME_COMMAND=/opt/pipeline/bin/qa-02-runner
```

Target chỉ preflight image/mount và chạy service `qa-02-rerun-revision`; build, start, init và credential setup là bước chuẩn bị riêng.

## Nghiệm thu sau ORC-07

Chạy cùng request trong QA namespace qua adapters thật, lưu input pins, release
và cutoff. Đối chiếu logical keys, current/history, canonical count, partition
contents, exact snapshots và KPI trước/sau. Retry giữ operation identity;
revision mới dùng operation mới với baseline/scope đã pin. Snapshot cũ phải còn
đọc được. Report chỉ chuyển `PASS` khi đủ Bronze → Published; trước đó giữ
`PREPARED`/`PENDING_RUNTIME`.

Các lỗi upstream phải ghi testcase tái hiện và mã task sở hữu lỗi; component
test không được dùng làm evidence whole-pipeline.
