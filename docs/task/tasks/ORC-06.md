---
task_id: "ORC-06"
status: "Done"
week: 5
block: "F - Điều phối"
workstream: "Full-month state resolution"
scope: "Core"
priority: "P0"
effort_hours: 8
assignee: "HoaiTam"
reviewer: "unassigned"
dependencies: ["SLV-09", "GLD-03", "ORC-03"]
---

# ORC-06 - Resolve affected months và dựng full-month Silver state

## Mục đích

Cung cấp current state đủ tháng cho Gold overwrite có phạm vi. Ngăn daily
overlap/sample nhỏ thay toàn partition và làm mất event không nằm trong delta.

## Phạm vi công việc

- Từ exact new/revised Bronze inputs và pinned baseline, resolve toàn bộ tháng
  UTC bị ảnh hưởng. Event chuyển tháng cần cả tháng cũ và mới.
- Chốt versioned resolver request/receipt: exact manifests/raw SHA, baseline
  snapshots/registry, source release/cutoff, affected months, output bundle SHA
  và coverage/completeness proof. Không suy baseline bằng scan latest.
- Ghép baseline đã verify và delta bằng Java/Spark, áp dụng revision/dedup/link
  contract hiện có; không phát minh key/canonical policy mới.
- Tạo đủ bốn Silver datasets và verify bundle phục vụ GoldInputReader. Với
  source link/canonical đổi vì revision, đưa toàn bộ thành phần liên quan vào
  scope; không cắt link state ở biên tháng một cách tùy ý.
- Chứng minh đủ coverage của current state cần thay: registry phải truy vết
  exact inputs/revisions và cả absence/empty month. Thiếu baseline/input/cutoff
  proof thì fail closed, không tự gắn COMPLETE_MONTHS.
- Bootstrap một phạm vi tháng hữu hạn từ input nguồn đã verify đầy đủ; không
  nhận pilot/sample thiếu tháng là baseline sản xuất.

## Thành phần cần có / Deliverable

- Java/Spark resolver/job và CLI request validator, coverage registry/proof có version.
- Exact full-month Silver bundle + count/revision/link reconciliation report.
- Fixtures: bootstrap, baseline+delta, unchanged month, moved event, empty month,
  missing source coverage, stale baseline, same identity/different bytes.
- Runbook cách chọn phạm vi, pin baseline, kiểm tra MinIO artifacts và handoff
  sang [ORC-07](./ORC-07.md); chỉ ghi lệnh khi implementation thực sự tồn tại.

## Tiêu chí hoàn thành

- [x] Event ngoài daily overlap nhưng trong affected month được giữ nguyên.
- [x] Revision chuyển tháng xóa logical current cũ và cập nhật tháng mới;
      không mất raw Bronze và không double count canonical.
- [x] Empty month được chứng minh explicit; thiếu coverage/baseline bị reject trước Gold write.
- [x] Rerun cùng identity/checksum deterministic; stale baseline hoặc identity conflict bị chặn.
- [x] Readback/checksum/count/grain của full-month bundle đạt, partitions ngoài scope không đổi.
- [x] Một bounded real input đủ tháng có evidence; pilot thiếu coverage chỉ test reject.
- [x] Receipt chứa đủ proof cho Gold COMPLETE_MONTHS, không tự Published.

## Hard dependency

- [SLV-09](./SLV-09.md) — verified multi-source bundle.
- [GLD-03](./GLD-03.md) — Gold input/overwrite/baseline contract.
- [ORC-03](./ORC-03.md) — explicit scoped planner/reprocess protocol.

## Cách triển khai và phối hợp

HoaiTam nhận triển khai trực tiếp ngày 2026-10-15; estimate 8h giữ nguyên.
Phân công ban đầu ThanhTris giữ làm lịch sử trong [kế hoạch tuần 5](../WEEK_5_PARALLEL_PLAN.md).
Khóa request/proof fixture nhỏ để ORC-07 có thể làm adapter tests độc lập.
Có thể dùng existing Silver fixtures để code ngay; chỉ integration cần source
input/baseline coverage thật. Ưu tiên phạm vi pilot **đủ tháng**, không toàn 40 năm.

Branch triển khai: `feat/orc-06-full-month-state`, từ main `f2d64ef`;
ba hard dependencies đã Done. Hoàn tất ngày 2026-10-16.
Reviewer độc lập chưa chỉ định, không ghi approval trước khi review.

Implementation Java/Spark replay exact Bronze baseline + delta, JMA replacement,
old/new link closure, registry/receipt immutable và production MinIO entrypoint.
Request/coverage/resource bounds nằm trong [contract](../../specs/FULL_MONTH_STATE_CONTRACT.md).
[Runbook](../../runbooks/ORC-06_FULL_MONTH_STATE.md) có lệnh test/QA/replay và
handoff production. [Evidence](../../evidence/ORC-06.md) ghi full-month 09/2023:
257.020 JMA physical records được verify, 22.499 current observations,
22.390 canonical events; rerun/readback đạt, không ghi Gold.

## Ranh giới

- Không sửa CON-03 schema/grain hoặc hạ gate COMPLETE_MONTHS của GLD-03.
- Không chọn wildcard/latest, silent truncate hoặc thay full-month bằng overlap.
- Không viết business transforms bằng Python, không xóa bucket/warehouse.
- ORC-07 sở hữu adapters/receipts/lease; QA-01/02/03 sở hữu nghiệm thu toàn chuỗi.
- Full research coverage thuộc QA-03 report/historical rollout; bounded monthly
  acceptance không chứng minh toàn reproduction/extension sẵn sàng cho ML.
- Version này chỉ chứng nhận tháng đã đóng, đủ coverage cả halo; không gắn
  COMPLETE_MONTHS cho tháng đang mở. ORC-07 cần chọn scope đủ điều kiện;
  nếu muốn current-month-as-of-cutoff, cần version hóa contract và tests trước,
  không bỏ quality gate hoặc suy coverage từ daily overlap.

## Theo dõi

- **Trạng thái:** Done
- **Assignee:** HoaiTam
- **Reviewer:** unassigned
- **Evidence / PR:** [Evidence ORC-06](../../evidence/ORC-06.md); branch `feat/orc-06-full-month-state`, chưa commit/push hoặc mở PR. File-backed public-capture QA, không phải whole-pipeline MinIO acceptance.

## Checklist bàn giao

- [x] Implementation/fixtures/docs có trong repository.
- [x] Acceptance criteria, offline tests và bounded real evidence đạt.
- [x] Status/Theo dõi/index đồng bộ; docs nêu task ID của giới hạn còn lại.
- [x] Không chứa secret/raw dump/artifact build.

## Tài liệu liên quan

- [Gold commit và COMPLETE_MONTHS](../../specs/GOLD_ICEBERG_COMMIT.md)
- [ETL phase contract](../../specs/ETL_ORCHESTRATION_CONTRACT.md)
- [Kế hoạch tuần 5](../WEEK_5_PARALLEL_PLAN.md)
