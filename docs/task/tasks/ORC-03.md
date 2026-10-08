---
task_id: "ORC-03"
status: "Done"
week: 4
block: "F - Điều phối"
workstream: "Backfill"
scope: "Core"
priority: "P0"
effort_hours: 5
assignee: "HoaiTam"
reviewer: "unassigned"
dependencies: ["ORC-01"]
---

# ORC-03 - Implement backfill và reprocessing

## Mục đích

Dùng để nạp lịch sử, sửa dữ liệu và phục hồi mà không phải xóa bucket hay tải lại toàn bộ.

## Phạm vi công việc

Hỗ trợ khoảng UTC cho USGS, danh sách năm/release cho JMA, tái sử dụng Bronze, preview phạm vi và giới hạn concurrency.

## Thành phần cần có

- **Đầu vào và contract:** [ORC-01](./ORC-01.md)
- **Phần triển khai:** Hỗ trợ khoảng UTC cho USGS, danh sách năm/release cho JMA, tái sử dụng Bronze, preview phạm vi và giới hạn concurrency.
- **Kết quả bàn giao:** Backfill parameters/workflow và hướng dẫn chạy.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- Backfill parameters/workflow và hướng dẫn chạy.

## Tiêu chí hoàn thành

- [x] Chạy lại không tạo duplicate: live source/reuse rerun giữ nguyên exact raw/manifest pins trong cùng operation; không suy ra Gold dedup đã hoàn tất.
- [x] không sửa partition ngoài phạm vi: immutable Bronze + old-object readback thật, scoped-adapter gates/test fixtures chặn scope sai; physical Silver/Gold acceptance thuộc SLV-09/GLD-03/GLD-04, chưa có adapter nên fail closed.
- [x] operator thấy trước input/output sẽ tác động: preview ba actions, UTC chunks/JMA segments/releases, existing-state/baselines và affected logical scopes tường minh.

## Hard dependency

- [ORC-01](./ORC-01.md)

## Cách triển khai và phối hợp

### Phân công tuần 4 mở rộng

- **Owner / effort:** HoaiTam, 5h Core; đưa từ tuần 5 lên [kế hoạch tuần 4](../WEEK_4_PARALLEL_PLAN.md), sau nhóm mở đường ORC-01.
- **Làm trước:** Planner/preview parameters và retry tests bằng exact-input fixture; chưa chạy downstream thật nếu chưa ready.
- **Làm gì / có gì:** USGS half-open UTC range, JMA year/segment/release selection, Bronze reuse và Silver/Gold affected-scope reprocess; gọi workflow JMA-04, không viết downloader thứ hai.
- **Dùng để làm gì:** Phục hồi có phạm vi, không xóa/rebuild lake hoặc tải lại nguồn không đổi.
- **Nghiệm thu / handoff:** Preview đúng input/output, revision/rerun không duplicate hoặc sửa partition ngoài scope; bàn giao commands/runbook và scope summary cho ORC-04/05. Test thật nhỏ sau các adapters sẵn sàng, không full 40 năm.

Có thể chuẩn bị interface, fixture, mock, test plan và tài liệu trước khi toàn bộ upstream chạy thật. Chỉ được chuyển sang `Done` khi hard dependency đã đạt và acceptance criteria được kiểm tra trên output phù hợp.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới từ `main`: `feat/orc-03-implement-backfill-va-reprocessing`.
3. Triển khai đúng phạm vi; dùng fixture nhỏ, xác định được và không phụ thuộc mạng cho unit test.
4. Chạy test/check phù hợp, đối chiếu acceptance criteria và cập nhật tài liệu nếu contract hoặc hành vi thay đổi.
5. Cập nhật `status`, `assignee`, `reviewer` và Evidence ngay trong file này khi mở PR hoặc hoàn tất review.

## Ranh giới

- Không tự mở rộng sang deliverable của task khác.
- Không đổi contract upstream trong PR implementation mà không cập nhật task contract liên quan và có review.
- Không commit secret, credential, payload nhạy cảm, data dump lớn hoặc artifact build không cần thiết.
- Không đánh dấu `Done` nếu chưa có evidence kiểm tra được.

## Theo dõi

- **Trạng thái:** Done
- **Assignee:** HoaiTam
- **Reviewer:** unassigned; chưa có approval độc lập.
- **Evidence / PR:** Branch `feat/orc-03-backfill-reprocessing` từ `origin/main`
  tại `ff4f460` (merge PR #45). [Evidence](../../evidence/ORC-03.md),
  [live runtime pins](../../evidence/ORC-03-runtime.json): 22 Python + 28 Java
  backfill tests; full checks 176 Java/111 Airflow/12 build-input tests; fresh
  image, real Bronze reuse/source pilot rerun và DagBag import đạt.
- **Kỹ năng phù hợp:** Airflow backfill, idempotency

## Checklist bàn giao

- [x] Deliverable đã có trong repository hoặc môi trường demo.
- [x] Acceptance criteria đã được kiểm tra trong phạm vi orchestration/source/reuse + scoped contract; không giả định downstream Published thật.
- [x] Test tự động đạt hoặc có evidence thủ công có thể lặp lại.
- [x] Tài liệu/contract đã cập nhật nếu schema, flow, cấu hình hoặc hành vi thay đổi.
- [x] Không chứa secret, dữ liệu nhạy cảm hoặc file build không cần thiết.
- [ ] P0/P1 có reviewer khác assignee xác nhận.

## Tài liệu liên quan

- [Runbook backfill/reprocessing và giới hạn có owner](../../specs/BACKFILL_AND_REPROCESSING.md)
- [ORC-03 evidence](../../evidence/ORC-03.md)

- [Kế hoạch tuần 4 mở rộng](../WEEK_4_PARALLEL_PLAN.md)
- [Kế hoạch 8 tuần](../README.md)
- [Các khối công việc](../WORK_BLOCKS.md)
- [Baseline MVP, KPI và Definition of Done](../../specs/MVP_SCOPE_KPI_AND_DOD.md)
