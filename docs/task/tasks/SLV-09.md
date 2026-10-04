---
task_id: "SLV-09"
status: "Backlog"
week: 4
block: "D - Silver đa nguồn"
workstream: "Integration QA"
scope: "Core"
priority: "P0"
effort_hours: 4
assignee: "unassigned"
reviewer: "unassigned"
dependencies: ["USG-05", "JMA-05", "SLV-02", "SLV-03", "SLV-04", "SLV-05", "SLV-06", "SLV-07", "SLV-08"]
---

# SLV-09 - Tích hợp và kiểm thử Silver đa nguồn

## Mục đích

Dùng làm mốc SilverReady để Gold dùng dữ liệu thật; các task SLV-02..08 vẫn có thể phát triển song song bằng fixture.

## Phạm vi công việc

Chạy manifest USGS/JMA qua parser, lineage, validation, dedup/link và publish; kiểm tra retry, revised record và reconciliation counts.

## Thành phần cần có

- **Đầu vào và contract:** [USG-05](./USG-05.md), [JMA-05](./JMA-05.md), [SLV-02](./SLV-02.md), [SLV-03](./SLV-03.md), [SLV-04](./SLV-04.md), [SLV-05](./SLV-05.md), [SLV-06](./SLV-06.md), [SLV-07](./SLV-07.md), [SLV-08](./SLV-08.md)
- **Phần triển khai:** Chạy manifest USGS/JMA qua parser, lineage, validation, dedup/link và publish; kiểm tra retry, revised record và reconciliation counts.
- **Kết quả bàn giao:** Silver integration suite và run-level reconciliation report.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- Silver integration suite và run-level reconciliation report.

## Tiêu chí hoàn thành

- [ ] Kết quả logic không đổi khi rerun fixture.
- [ ] parsed/valid/rejected/duplicate/canonical đối soát được.

## Hard dependency

- [USG-05](./USG-05.md)
- [JMA-05](./JMA-05.md)
- [SLV-02](./SLV-02.md)
- [SLV-03](./SLV-03.md)
- [SLV-04](./SLV-04.md)
- [SLV-05](./SLV-05.md)
- [SLV-06](./SLV-06.md)
- [SLV-07](./SLV-07.md)
- [SLV-08](./SLV-08.md)

## Cách triển khai và phối hợp

Đây là integration gate. Có thể chuẩn bị test plan, fixture và harness trước, nhưng chỉ được chuyển sang `Done` sau khi toàn bộ hard dependency cung cấp output thật và báo cáo đối soát đạt.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới từ `main`: `feat/slv-09-tich-hop-va-kiem-thu`.
3. Triển khai đúng phạm vi; dùng fixture nhỏ, xác định được và không phụ thuộc mạng cho unit test.
4. Chạy test/check phù hợp, đối chiếu acceptance criteria và cập nhật tài liệu nếu contract hoặc hành vi thay đổi.
5. Cập nhật `status`, `assignee`, `reviewer` và Evidence ngay trong file này khi mở PR hoặc hoàn tất review.

## Ranh giới

- Không tự mở rộng sang deliverable của task khác.
- Không đổi contract upstream trong PR implementation mà không cập nhật task contract liên quan và có review.
- Không commit secret, credential, payload nhạy cảm, data dump lớn hoặc artifact build không cần thiết.
- Không đánh dấu `Done` nếu chưa có evidence kiểm tra được.

## Theo dõi

- **Trạng thái:** Backlog
- **Assignee:** Chưa ghi lại
- **Reviewer:** Chưa ghi lại
- **Evidence / PR:** Chưa có
- **Kỹ năng phù hợp:** Spark integration, reconciliation

## Checklist bàn giao

- [ ] Deliverable đã có trong repository hoặc môi trường demo.
- [ ] Acceptance criteria đã được kiểm tra.
- [ ] Test tự động đạt hoặc có evidence thủ công có thể lặp lại.
- [ ] Tài liệu/contract đã cập nhật nếu schema, flow, cấu hình hoặc hành vi thay đổi.
- [ ] Không chứa secret, dữ liệu nhạy cảm hoặc file build không cần thiết.
- [ ] P0/P1 có reviewer khác assignee xác nhận.

## Tài liệu liên quan

- [Kế hoạch 8 tuần](../README.md)
- [Các khối công việc](../WORK_BLOCKS.md)
- [Baseline MVP, KPI và Definition of Done](../../specs/MVP_SCOPE_KPI_AND_DOD.md)
