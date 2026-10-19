# Kịch bản trình bày và giới hạn demo

DOC-01 đồng bộ **19/10/2026**: đây là hướng dẫn chọn evidence, không phải nghiệm
thu [DEMO-01](./task/tasks/DEMO-01.md)/[DEMO-02](./task/tasks/DEMO-02.md).
Core là ETL + ML hồi cứu + Trino/static report; Power BI chỉ Stretch.
[Known issues](./KNOWN_ISSUES.md) nêu phần hiện chưa đủ để tuyên bố full MVP.

## 1. Thông điệp

- Raw Bronze bất biến; Silver chuẩn hóa/revision/linking để tránh double count.
- Gold phải đủ full-month proof, commit sáu snapshots và Trino verify/publish.
- Gold snapshot → audit/Mc/windows/4-D feature → Jupyter trước Colab, cùng input.
- Official export/import có lifecycle/gate riêng; pilot không được tự nâng cấp.
- Output là sequence/aftershock candidate hồi cứu, không prediction/causal.

## 2. Chọn evidence theo ba chế độ

| Chế độ | Có thể trình bày | Không được tuyên bố |
|---|---|---|
| Notebook-only development | Pilot thật/ZIP hoặc synthetic có nhãn; fresh-kernel counts/roles/noise/report | Scheduled ETL Passed, full coverage hoặc scientific APPROVED |
| Bounded real ETL component | ORC-07 MinIO/Iceberg/Trino/DAG.test evidence và exact pins/snapshots | Cron scheduler đã chạy hoặc lịch sử 40 năm hoàn tất |
| Release E2E | Chỉ sau QA-01/MLQ-01/official export/import/report có evidence thật | Dùng smoke namespaces khác nhau hoặc stored output thay run đang chạy |

Không unpause/trigger pipeline dài hoặc build image giữa buổi trình bày.
Chuẩn bị trước theo [local runbook](./LOCAL_OPERATIONS_RUNBOOK.md);
nhóm notebook-only theo [onboarding](./runbooks/DOC-01_TEAM_ONBOARDING.md).
Ẩn .env/private logs/token Jupyter và mọi credential.

## 3. Trình tự 15–20 phút đề xuất

| Bước | Trình bày gì? | Evidence / expected |
|---|---|---|
| 1 | Bài toán, hai nguồn, architecture và retrospective candidate | Source coverage, canonical/lineage, không prediction |
| 2 | Airflow/source và Bronze | Exact run/type/interval, raw + manifest SHA; nói rõ SourcesReady hay Published |
| 3 | Silver/full-month/Gold | Current/history/reject/link counts, six snapshot IDs và pinned Trino verification |
| 4 | Dataset/experiment nhỏ | Đúng pilot_id/checksums/coverage, fresh kernel, ba baseline per-window, noise reasons |
| 5 | Official handoff/import/report gate | Chỉ output thật nếu task đã đủ gate; nếu chưa thì giải thích contract/known issues |
| 6 | Rerun/recovery và giới hạn | Exact identity, giữ journals/lease khi fail; không xóa lake để chạy lại |

Core static report cuối cùng còn [MLI-04](./task/tasks/MLI-04.md) và
[MLQ-01](./task/tasks/MLQ-01.md); notebook local-smoke-report không thay nó.
Nếu làm BI-01..05 Stretch, thêm dashboard/ODBC refresh/KPI cùng filter và pin,
không lấy việc thiếu Power BI làm lỗi Core.

## 4. Đọc kết quả cho đúng

Pilot JMA 279 candidate rows/2 mainshock, PARTIAL, không phải 279 event độc lập
hoặc toàn catalog. Technical gate có thể PASSED dù mainshock noise/no productive
sequence; không đổi config để ép có aftershock. Chạy script exit0 nhưng báo
FALLBACK_EXEC_WARNING không phải fresh-kernel acceptance hoặc quyền migrate Colab.

Bản thân snapshot COMMITTED chưa đủ Published; publication CANDIDATE của ML
chưa scientific APPROVED. Consumer không đọc raw current/latest thay snapshot
đã pin. Một COUNT(*) không chứng minh toàn bộ data quality.

## 5. Fallback và checklist bàn giao

Mạng/API lỗi: replay exact Bronze/captured data và nói rõ replay; không gọi output
lưu sẵn là run live đang chạy. Notebook dùng synthetic thì nói rõ synthetic.
Docker stale/thiếu RAM: trình bày evidence có ngày/commit và known issues, không
bỏ guard hoặc reset volume để kịp demo.

Bàn giao commit/config, run/type/interval, input SHAs, counts, snapshots,
dataset/pilot/experiment IDs, inspection/verification reports và limitations.
Reviewer/release approval chưa có thì không tự điền. QA-02/03/04, SEC-01,
OPS-01 và DEMO-01/02 giữ acceptance scope riêng.
