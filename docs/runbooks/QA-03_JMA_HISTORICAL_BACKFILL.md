# QA-03 - JMA historical backfill

## Chuẩn bị matrix

Fixture tại `airflow/dags/fixtures/qa_03_jma_historical_matrix.json` chọn hai
segment của năm 1997, reproduction 2000 và extension 2023. Scope ban đầu là
`1997-09`, `1997-10`, `2000-01`, `2023-09`, kèm coverage/halo cần được pin bằng
request. Revision trong fixture là synthetic và được đánh dấu rõ.

```powershell
python airflow/dags/qa_03_jma_historical_backfill.py `
  --matrix airflow/dags/fixtures/qa_03_jma_historical_matrix.json `
  --json staging/qa-03/qa-03.json `
  --markdown staging/qa-03/qa-03.md
```

Evidence template hiện lưu tại [QA-03 JSON](../evidence/QA-03-prepared.json) và [Markdown](../evidence/QA-03-prepared.md). Dùng `make test-qa-03` với `QA_03_MATRIX`, `QA_03_JSON` và
`QA_03_MARKDOWN`.

## Nghiệm thu sau ORC-07

Verify toàn archive Bronze; chạy từng batch tháng đã đóng với resource guard.
Đo duration, peak memory, resource failures, download/reuse counts và resume
sau lỗi. Kiểm tra changed release, cùng release khác bytes, retention ngoài
scope và snapshot cũ. Chỉ report `PASS` khi chạy Bronze → Published trong QA
namespace và đối chiếu được counts/snapshots/scope. Vài tháng đại diện chỉ là
evidence phạm vi đã chạy; không đại diện cho toàn bộ 40 năm.

Bàn giao historical report, capacity note và thứ tự rollout các batch còn lại.
