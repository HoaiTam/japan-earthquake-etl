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

Evidence template hiện lưu tại [QA-02 JSON](../evidence/QA-02-prepared.json) và [Markdown](../evidence/QA-02-prepared.md). Dùng `make test-qa-02` để chạy unit test và tạo report template bằng
`QA_02_MATRIX`, `QA_02_JSON` và `QA_02_MARKDOWN`.

## Nghiệm thu sau ORC-07

Chạy cùng request trong QA namespace qua adapters thật, lưu input pins, release
và cutoff. Đối chiếu logical keys, current/history, canonical count, partition
contents, exact snapshots và KPI trước/sau. Retry giữ operation identity;
revision mới dùng operation mới với baseline/scope đã pin. Snapshot cũ phải còn
đọc được. Report chỉ chuyển `PASS` khi đủ Bronze → Published; trước đó giữ
`PREPARED`/`PENDING_RUNTIME`.

Các lỗi upstream phải ghi testcase tái hiện và mã task sở hữu lỗi; component
test không được dùng làm evidence whole-pipeline.
