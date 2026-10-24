# Giới hạn hiện hành và phần việc còn thiếu

Đồng bộ task tracking ngày **22/10/2026**, từ main `3e8625e` (PR #94), theo
phân công gần nhất. Bảng dưới phản ánh trạng thái task/evidence đã lưu, không
phải runtime re-audit ngày 22/10. [Task index](./task/tasks/README.md) và từng
file task là nguồn chính; merge/CI xanh không thay live/scientific acceptance.

Cập nhật riêng QA-01 ngày **23/10/2026**: operational v2 đã có scheduled recovery
acceptance PASSED; các dòng task khác vẫn theo mốc/evidence riêng của branch này.
Cập nhật MLI-02 ngày **24/10/2026** theo [real acceptance](./evidence/MLI-02_RUNTIME_ACCEPTANCE.md),
không sửa evidence offline/lịch sử hoặc tự mở scientific gates.
Cập nhật DAT-03 ngày **24/10/2026**: pilot team kit, profiles 2000/2011/2016/2023,
scoped resolver v2 và strict-split opt-in đã có. [Đợt kỹ thuật](./evidence/DAT-03_TECHNICAL_PROGRESS.md)
phát hiện 18 depth-token rejects trong 2016; chưa chốt mapping, không bỏ dòng.
Task còn In Progress cho full research catalog và receipts ba máy.

Mốc DOC-01 gốc 19/10, MLI-03 acceptance 19/10, QA-02/03/04 và MLD-05 acceptance
21/10 được giữ trong evidence riêng. Bản đồng bộ này không sửa logs, counts,
pins, reviewer hoặc approval của các lần chạy đó. MLI-02 chuyển owner rosy179;
DOC-01/QA-04/EXP-03 chuyển lịch tuần 5, không đổi scope hoặc acceptance.

| Task / trạng thái (QA-01 23/10, MLI-02 24/10) | Đã có | Còn thiếu / tác động |
|---|---|---|
| [QA-01 — Daily E2E](./task/tasks/QA-01.md), Done — operational v2 | [Scheduled recovery acceptance 23/10](./evidence/QA-01_CURRENT_MONTH_ACCEPTANCE.md): 8/8 tasks, 58 events, six snapshots và 42 SQL readbacks PASSED | Explicit readonly prebuilt deployment; default cached-image Make preflight chưa đạt. Không first-attempt/on-time SLA hoặc official ML/research eligibility; daily hiện paused |
| [ORC-07 — Real ETL adapters](./task/tasks/ORC-07.md), Done | Bounded real MinIO/Iceberg/Trino/Airflow DAG.test và rerun | Không có nghĩa deployment source-only trên máy đã được cập nhật; xem QA-01 |
| [MLD-05 — Validate feature snapshot/export](./task/tasks/MLD-05.md), Done (component, 21/10) | Coverage/audit gate, pinned snapshots, portable export/reuse, trusted handoff và local loader đã kiểm | Catalog thật toàn kỳ thuộc DAT-03 In Progress; MLI-02 technical DAG đã Done, EXP-02 còn official fit/Colab; pilot không thay official dataset |
| [MLI-02 — Build dataset DAG](./task/tasks/MLI-02.md), Done technical component 24/10; owner rosy179 | Real Java/Spark + Airflow first/rerun 12/12, registry/pins/12 SQL mỗi run, reject unpublished input và local handoff đạt | Synthetic QA không chứng nhận research catalog thật hoặc fit/Colab. Giữ Docker shutdown/recovery validate try 2; deployment đã restore, daily/MLI paused. Không default cached-image hoặc first-attempt SLA; reviewer unassigned |
| [MLI-03 — Result import](./task/tasks/MLI-03.md), Done | [Synthetic QA trên MinIO/Catalog/Trino + production runner + 9/9 Airflow tasks](./evidence/MLI-03_RUNTIME_ACCEPTANCE.md); rejects/rerun/recovery đạt | DAT-03 còn catalog thật toàn kỳ; EXP-02..04 còn official fit/scientific gates. MLI-02 technical Done không chứng nhận APPROVED hoặc Gold→ML E2E |
| [DAT-03 — Catalog thật và team handoff](./task/tasks/DAT-03.md), In Progress | Kit pilot 279 rows/2 mainshock; profiles 2000/2011/2016/2023, resolver v2 và strict-split opt-in có tests; reader probe 2011 từng tháng đạt, gộp chạm cap | Chưa xử lý 18 depth tokens 2016, chưa source biên 2024/collection/sharding/full research backfill/MLI-02 export thật và receipts ba máy; pilot vẫn PARTIAL/development-only |
| [EXP-02 — Baselines](./task/tasks/EXP-02.md), In Progress | Ba baseline per-window, local smoke/gate; DOC-01 đã chạy fresh-kernel pilot thật | Colab parity và official run sau MLD-05; không có kết luận thuật toán nào tốt hơn |
| [EXP-03 — Adaptive/multi-sequence](./task/tasks/EXP-03.md), In Progress | Implementation/config/tests và notebook | Reproduction run/fresh-kernel evidence còn thiếu; dependency EXP-02 chưa Done. isolated_exec/fallback không thay local gate; reviewer unassigned, không có approval |
| [EXP-04 — Scientific evaluation](./task/tasks/EXP-04.md), Backlog; [EXP-05 — Extension](./task/tasks/EXP-05.md), Backlog | Contracts/config baseline | DBCV/noise/Omori/stability và extension; không gọi local PASSED là APPROVED |
| [MLI-04 — Trino/static report](./task/tasks/MLI-04.md), Backlog; [MLQ-01 — ML E2E](./task/tasks/MLQ-01.md), Backlog | Logical model và result contracts | Serving/report và Gold→ML E2E thật; notebook JSON không thay Core static report |
| [QA-02 — Rerun/revision](./task/tasks/QA-02.md), Done | [Runtime acceptance 8/8](./evidence/QA-02_RUNTIME_ACCEPTANCE.md), six publications và Trino readback | Bounded runtime scope, không thay QA-01 scheduled deployment hoặc full research data |
| [QA-03 — Historical backfill](./task/tasks/QA-03.md), Done | [Runtime acceptance 6/6](./evidence/QA-03_RUNTIME_ACCEPTANCE.md), archive đại diện/resume/changed release/retention | Không chứng minh đã nạp 40 năm hoặc catalog thật toàn kỳ đã research-ready |
| [QA-04 — Failure/recovery](./task/tasks/QA-04.md), Done | [Runtime acceptance 10/10](./evidence/QA-04_RUNTIME_ACCEPTANCE.md), tám publication và ML pinned readback | Synthetic ML/component faults không thay official ML E2E hoặc production chaos |
| [SEC-01 — Security review](./task/tasks/SEC-01.md), Done — local | [Audit 23/10](./evidence/SEC-01_LOCAL_ACCEPTANCE_20261023.md): source/history/full retained logs/runtime, 15 fresh permission checks và 5 pinned handoffs đạt | Colab/Drive chưa triển khai/chưa chứng nhận; mở lại SEC-01 trước migration hoặc khi phạm vi thay đổi; không chia sẻ private log/.env |
| [OPS-01 — Backup/restore](./task/tasks/OPS-01.md), Backlog/Stretch | Persistence layout đã chốt | Restore acceptance trên môi trường tách biệt; không suy backup file là phục hồi đã test |
| [GLD-02](./task/tasks/GLD-02.md), [BI-01](./task/tasks/BI-01.md)..[BI-05](./task/tasks/BI-05.md), Backlog/Stretch | Logical analytics contract | Aggregate/ODBC/dashboard/refresh; không chặn Core HDBSCAN/static report |

## Các giới hạn phải nói rõ khi chạy

- **QA-01 — Daily E2E** đã bổ sung [thiết kế current-month/as-of-cutoff](./specs/CURRENT_MONTH_AS_OF_CUTOFF.md)
  theo lựa chọn của owner ngày 22/10; policy/planner giữ DESIGN_ONLY. Owner đã
  yêu cầu làm tiếp Source/Silver trên PR #96; [component standalone](./runbooks/CURRENT_MONTH_SOURCE_SILVER.md)
  và [Gold/publication component](./runbooks/CURRENT_MONTH_GOLD_PUBLICATION.md)
  đã nối cùng daily owner và nghiệm thu MinIO/Trino thật ngày 23/10 sau owner
  cho recovery exact scheduled run. [Evidence](./evidence/QA-01_CURRENT_MONTH_ACCEPTANCE.md)
  ghi default Make stale-image gate, explicit mount/file SHA proof và lịch sử
  failed attempts; không bỏ freshness guard hoặc fake state. **Chưa có task riêng**
  cho JMA sau baseline/deletion/replan vượt budget. ORC-06/07 v1 vẫn chỉ complete closed months đủ halo;
  không bật daily publication 2026 bằng request historical hoặc nâng
  operational provisional thành ML-eligible. USGS ingest/SourcesReady vẫn độc lập.
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
- **DAT-03 — Catalog nghiên cứu thật và handoff** đang In Progress cho backfill/nghiệm thu toàn kỳ.
  MLD-05 synthetic component QA không tạo bằng chứng thay cho phạm vi này;
  ORC-03/06 là execution/state, QA-03 là bounded backfill acceptance.

## Cách cập nhật

PR đổi hành vi phải cập nhật code/contract/task evidence cùng lúc. Không sửa
evidence lịch sử thành kết quả mới; thêm mốc mới và link vào đây. Không tự đổi
task khác Done/Needs Update trong PR tài liệu nếu chưa có acceptance audit tương ứng.
Reproduction đã chạy của DOC-01 nằm tại [evidence](./evidence/DOC-01.md).
