# Giới hạn hiện hành và phần việc còn thiếu

Đồng bộ task tracking ngày **22/10/2026**, từ main `3e8625e` (PR #94), theo
phân công gần nhất. Bảng dưới phản ánh trạng thái task/evidence đã lưu, không
phải runtime re-audit ngày 22/10. [Task index](./task/tasks/README.md) và từng
file task là nguồn chính; merge/CI xanh không thay live/scientific acceptance.

Mốc DOC-01 gốc 19/10, MLI-03 acceptance 19/10, QA-02/03/04 và MLD-05 acceptance
21/10 được giữ trong evidence riêng. Bản đồng bộ này không sửa logs, counts,
pins, reviewer hoặc approval của các lần chạy đó. MLI-02 chuyển owner rosy179;
DOC-01/QA-04/EXP-03 chuyển lịch tuần 5, không đổi scope hoặc acceptance.

| Task / trạng thái tại 22/10 | Đã có | Còn thiếu / tác động |
|---|---|---|
| [QA-01 — Daily E2E](./task/tasks/QA-01.md), In Progress | Collector, offline tests; scheduled run cũ SourcesReady được nhận diện NOT_READY | Deployment ORC-07 mới, eligible request và scheduled run Published + KPI SQL thật; không dùng DAG.test thay cron |
| [ORC-07 — Real ETL adapters](./task/tasks/ORC-07.md), Done | Bounded real MinIO/Iceberg/Trino/Airflow DAG.test và rerun | Không có nghĩa deployment source-only trên máy đã được cập nhật; xem QA-01 |
| [MLD-05 — Validate feature snapshot/export](./task/tasks/MLD-05.md), Done (component, 21/10) | Coverage/audit gate, pinned snapshots, portable export/reuse, trusted handoff và local loader đã kiểm | Catalog thật toàn kỳ chưa nghiệm thu; MLI-02 còn live build DAG, EXP-02 còn official fit/Colab; pilot không thay official dataset |
| [MLI-02 — Build dataset DAG](./task/tasks/MLI-02.md), In Progress; owner rosy179 | Graph, exact identity, runner boundary và mock tests; MLD-05 exporter component đã Done | Nối real exporter/registry và nghiệm thu live build DAG; mock export không mở Colab/official lifecycle |
| [MLI-03 — Result import](./task/tasks/MLI-03.md), Done | [Synthetic QA trên MinIO/Catalog/Trino + production runner + 9/9 Airflow tasks](./evidence/MLI-03_RUNTIME_ACCEPTANCE.md); rejects/rerun/recovery đạt | Official dataset/model run còn thuộc MLD-05/MLI-02/EXP-02..04; không APPROVED hoặc Gold→ML E2E |
| [EXP-02 — Baselines](./task/tasks/EXP-02.md), In Progress | Ba baseline per-window, local smoke/gate; DOC-01 đã chạy fresh-kernel pilot thật | Colab parity và official run sau MLD-05; không có kết luận thuật toán nào tốt hơn |
| [EXP-03 — Adaptive/multi-sequence](./task/tasks/EXP-03.md), In Progress | Implementation/config/tests và notebook | Reproduction run/fresh-kernel evidence còn thiếu; dependency EXP-02 chưa Done. isolated_exec/fallback không thay local gate; reviewer unassigned, không có approval |
| [EXP-04 — Scientific evaluation](./task/tasks/EXP-04.md), Backlog; [EXP-05 — Extension](./task/tasks/EXP-05.md), Backlog | Contracts/config baseline | DBCV/noise/Omori/stability và extension; không gọi local PASSED là APPROVED |
| [MLI-04 — Trino/static report](./task/tasks/MLI-04.md), Backlog; [MLQ-01 — ML E2E](./task/tasks/MLQ-01.md), Backlog | Logical model và result contracts | Serving/report và Gold→ML E2E thật; notebook JSON không thay Core static report |
| [QA-02 — Rerun/revision](./task/tasks/QA-02.md), Done | [Runtime acceptance 8/8](./evidence/QA-02_RUNTIME_ACCEPTANCE.md), six publications và Trino readback | Bounded runtime scope, không thay QA-01 scheduled deployment hoặc full research data |
| [QA-03 — Historical backfill](./task/tasks/QA-03.md), Done | [Runtime acceptance 6/6](./evidence/QA-03_RUNTIME_ACCEPTANCE.md), archive đại diện/resume/changed release/retention | Không chứng minh đã nạp 40 năm hoặc catalog thật toàn kỳ đã research-ready |
| [QA-04 — Failure/recovery](./task/tasks/QA-04.md), Done | [Runtime acceptance 10/10](./evidence/QA-04_RUNTIME_ACCEPTANCE.md), tám publication và ML pinned readback | Synthetic ML/component faults không thay official ML E2E hoặc production chaos |
| [SEC-01 — Security review](./task/tasks/SEC-01.md), Review | Static/runtime permission checks | Retained logs re-audit và actual ML handoff gate; không chia sẻ private log/.env |
| [OPS-01 — Backup/restore](./task/tasks/OPS-01.md), Backlog/Stretch | Persistence layout đã chốt | Restore acceptance trên môi trường tách biệt; không suy backup file là phục hồi đã test |
| [GLD-02](./task/tasks/GLD-02.md), [BI-01](./task/tasks/BI-01.md)..[BI-05](./task/tasks/BI-05.md), Backlog/Stretch | Logical analytics contract | Aggregate/ODBC/dashboard/refresh; không chặn Core HDBSCAN/static report |

## Các giới hạn phải nói rõ khi chạy

- **Chưa có task** chốt current-month/as-of-cutoff và JMA sau 2023. ORC-06/07
  v1 chỉ complete closed months đủ halo. Không bật daily publication 2026 bằng
  request historical; vẫn có thể ingest USGS và đạt SourcesReady độc lập.
- **DAT-02 — Development pilot** có 279 candidate rows / 2 mainshock, input JMA
  chỉ khoảng 14 ngày đầu năm 2000. PARTIAL và research_ready=false; không phải
  toàn năm, toàn 40 năm hoặc chứng minh catalog completeness. Counts là grain
  mainshock/candidate, không phải 279 event độc lập bắt buộc.
- **EXP-01 — Environment/harness** có requirements-lock.txt pin direct packages,
  không phải lock mọi transitive dependency có hashes/wheels cho mọi OS.
  HDBSCAN 0.8.40 trên Python 3.13 macOS có thể cần compiler/build wheel; ghi OS,
  Python và resolved packages của lần chạy, không tuyên bố bitwise portability.
- **EXP-01/02/03 — Notebook fixtures**: không truyền PILOT_BUNDLE hoặc path không
  tồn tại có thể fallback synthetic. Muốn test real phải xác nhận thư mục tồn
  tại và đối chiếu pilot_id/checksums/counts trong actual local-smoke-report.
- Fallback execution warning khác fresh-kernel acceptance; `exit=0` của script
  không tự đủ để mở Colab gate. Dùng actual EXP-02 local-smoke-report, không
  dùng mock gate demo trong notebook EXP-01.
- Mainshock noise/no productive sequence là outcome phải giữ, không chọn cluster
  khác để làm đẹp. `failed_windows` do case noise cần đọc reason/counts, không
  đồng nghĩa kernel/pipeline crash hoặc bằng chứng scientific failure.
- **Chưa có task riêng** nghiệm thu backfill catalog thật toàn kỳ nghiên cứu.
  MLD-05 synthetic component QA không tạo bằng chứng thay cho phạm vi này;
  ORC-03/06 là execution/state, QA-03 là bounded backfill acceptance.

## Cách cập nhật

PR đổi hành vi phải cập nhật code/contract/task evidence cùng lúc. Không sửa
evidence lịch sử thành kết quả mới; thêm mốc mới và link vào đây. Không tự đổi
task khác Done/Needs Update trong PR tài liệu nếu chưa có acceptance audit tương ứng.
Reproduction đã chạy của DOC-01 nằm tại [evidence](./evidence/DOC-01.md).
