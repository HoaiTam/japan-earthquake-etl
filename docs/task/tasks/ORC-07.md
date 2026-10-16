---
task_id: "ORC-07"
status: "Done"
week: 5
block: "F - Điều phối"
workstream: "Real ETL adapter integration"
scope: "Core"
priority: "P0"
effort_hours: 8
assignee: "HoaiTam"
reviewer: "unassigned"
dependencies: ["ORC-01", "ORC-02", "ORC-06", "GLD-04"]
---

# ORC-07 - Kết nối real ETL adapters đến Gold Published

## Mục đích

Nối các job đã có thành ETL thật, trả receipts từ artifact/readback thật.
Không coi DAG mock hoặc các smoke namespace khác nhau là một run Published.

## Phạm vi công việc

- Đóng gói executable/wrapper cấu hình qua môi trường theo ORC-01 và scoped
  protocol ORC-03; không nhận arbitrary command từ dag_run.conf.
- Nối readiness → verified exact Bronze → ORC-06 full-month Silver → GLD-03
  six-table commit → GLD-04 snapshot-pinned Trino verification → publication.
- Map identity/context/upstream SHA, bundle/count/scope/observability fields từ
  outputs thật; không hard-code passed/readback/checks=true.
- Resolve context/input trước execution và giữ immutable qua retry. Nếu phải
  mở rộng protocol cho coverage/baseline, version request/receipt và cập nhật
  fixtures/consumer cùng PR, không ngầm sửa ORC-01 1.0.
- Giữ source_run_guard exact dag/run owner qua write/query/readback; reuse
  journal/snapshots đúng operation khi retry, không giả sáu commit là atomic.
- Daily source DAG giữ một cron owner; nối handoff vào real ETL có explicit
  interval/context. Manual backfill/reprocess dùng cùng adapters theo scoped
  protocol; không thêm lịch thứ hai hoặc để daily chờ notebook.
- Bổ sung Make targets/test runbook **chỉ preflight**, không tự build/pull/start/init
  lại services trong mỗi test. Runtime packaging/build/start là bước riêng.
- Mặc định real command chưa triển khai vẫn fail closed; chỉ bật cấu hình thật
  sau khi deployment và evidence đủ, không fallback sang mock khi lỗi.

## Thành phần cần có / Deliverable

- Packaged real phase/scoped runner, Java/Spark/Trino invocation mapping.
- Versioned requests/receipts và adapter failure/security/idempotency tests.
- DAG wiring, resource/lease boundaries, count reconciliation và run summary.
- Operator runbook/test commands; bounded real daily và manual scoped evidence
  liên kết cùng Bronze/Silver/Gold snapshots/Trino report/publication.

## Tiêu chí hoàn thành

- [x] Một daily interval thật đi đến Published với đủ sáu exact Gold snapshots,
      Trino blockers passed và publication metadata readback cùng bundle.
- [x] Full-month proof thiếu/sai bị chặn trước Gold write.
- [x] Mock vẫn MockComplete/published=false, không có mock artifact trong real receipt.
- [x] Phase lỗi không gọi downstream/publish; persisted summary giữ đúng reason/run context.
- [x] Retry cùng operation reuse exact artifacts/snapshots; stale baseline/context bị reject.
- [x] Manual scoped reprocess đi qua cùng adapters, không sửa tháng ngoài phạm vi.
- [x] Timeout/interruption giữ owner khi chưa chứng minh remote/JVM đã dừng;
      không foreign cleanup/delete metadata.
- [x] Lệnh test không build/start Docker lại, không chứa secret hoặc raw payload trong XCom.

## Hard dependency

- [ORC-01](./ORC-01.md) — phase protocol/skeleton.
- [ORC-02](./ORC-02.md) — source readiness/schedule runtime acceptance.
- [ORC-06](./ORC-06.md) — full-month state/proof.
- [GLD-04](./GLD-04.md) — verification/publication contract.

Cập nhật 2026-10-16: ORC-01/ORC-02/ORC-06/GLD-04 đều Done; ORC-07 đã
hoàn tất bounded real acceptance, xem [evidence](../../evidence/ORC-07.md).
ORC-06 bàn giao request/proof/resolver và bounded file-backed real-source QA;
MinIO/daily/manual integration và Published evidence thuộc task này đã đạt.
Version ORC-06 hiện chỉ nhận tháng đã đóng và đủ halo; adapter phải chọn
eligible scope. Nếu cần tháng đang mở as-of-cutoff, version hóa coverage/
request/receipt và tests trước khi bật, không đổi thiếu proof thành COMPLETE_MONTHS.
Đó là mở rộng ngoài scope v1, hiện chưa có task riêng cho phiên bản mới.

## Cách triển khai và phối hợp

HoaiTam, 8h trong [kế hoạch tuần 5](../WEEK_5_PARALLEL_PLAN.md).
Nhận ORC-06 request/coverage fixture ngày đầu; dùng SLV/GLD job outputs và
MLI-01-independent namespace, không chờ experiment. Branch triển khai đề xuất:
`feat/orc-07-real-etl-adapters` từ main mới nhất.

QA-01 là người kiểm chứng luồng được nối, **không sở hữu việc viết adapters**.
QA-02/03/04 chuẩn bị fault/revision/resume matrix song song. Reviewer chưa
chỉ định; planned cross-review không phải approval đã tồn tại.

## Ranh giới

- Không làm lại parser/linking/Gold algorithms hoặc thay data contract.
- Không dùng receipt giả thay Trino SQL readback; không expose raw current như Published.
- Không clear DAG/xóa lease/warehouse để retry và không đổi secret của operator.
- MLI-02/03 sở hữu hai DAG ML riêng; EXP-03/04/05 sở hữu adaptive/evaluation.
- QA-01/02/03/04 còn phải nghiệm thu E2E/revision/backfill/recovery sau task này.

## Theo dõi

- **Trạng thái:** Done
- **Assignee:** HoaiTam
- **Reviewer:** unassigned
- **Evidence / PR:** [ORC-07 implementation và acceptance](../../evidence/ORC-07.md), key `75b2a4a0d4ad4a49817ac8cde6b1f868`: daily 8/8 và manual 5/5 task success, Published qua actual MinIO/Iceberg/Trino; final-binary replay identical và owned partial-commit recovery đạt. Branch `feat/orc-07-real-etl-adapters`, từ main `accdb24` (ORC-06 / PR #78 đã merge). Chưa commit/push/PR; không có reviewer approval. Đây là production DAGs qua DAG.test, không scheduled deployment acceptance của QA-01.

## Checklist bàn giao

- [x] Adapter implementation, regression tests, runbook và bounded real evidence đạt.
- [x] Contract/fixtures/receipts đồng bộ và không làm yếu quality/publication gates.
- [x] Status/Theo dõi/index đồng bộ.
- [x] Không chứa secret, data dump hoặc build artifact.

## Tài liệu liên quan

- [ETL orchestration](../../specs/ETL_ORCHESTRATION_CONTRACT.md)
- [Gold commit](../../specs/GOLD_ICEBERG_COMMIT.md)
- [Trino publication](../../specs/GOLD_TRINO_PUBLICATION.md)
- [Real ETL adapter contract](../../specs/REAL_ETL_ADAPTER_CONTRACT.md)
- [Operator runbook](../../runbooks/ORC-07_REAL_ETL.md)
- [Evidence](../../evidence/ORC-07.md)
- [Kế hoạch tuần 5](../WEEK_5_PARALLEL_PLAN.md)
