---
task_id: "PLN-01"
status: "Done"
week: 1
block: "Foundation"
workstream: "Planning"
scope: "Core"
priority: "P0"
effort_hours: 4
assignee: "unassigned"
reviewer: "unassigned"
dependencies: []
---

# PLN-01 - Chốt scope, KPI và Definition of Done

## Mục đích

Cố định ranh giới MVP, KPI và Definition of Done để cả nhóm dùng cùng tiêu chuẩn khi triển khai và review.

## Phạm vi công việc

Rà tài liệu hiện tại, chốt phạm vi MVP, phần Stretch, KPI và tiêu chí bàn giao.

## Thành phần cần có

- **Đầu vào và contract:** Không có hard dependency.
- **Phần triển khai:** Rà tài liệu hiện tại, chốt phạm vi MVP, phần Stretch, KPI và tiêu chí bàn giao.
- **Kết quả bàn giao:** Biên bản scope và checklist DoD được cập nhật trong docs.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- Biên bản scope và checklist DoD được cập nhật trong docs.

## Tiêu chí hoàn thành

- [x] Ba thành viên thống nhất phạm vi.
- [x] không còn yêu cầu mơ hồ trên đường găng.

## Hard dependency

- Không có.

## Cách triển khai và phối hợp

Có thể bắt đầu ngay. Chốt output contract hoặc fixture nhỏ trước để các task tiêu thụ có thể làm song song.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới từ `main`: `docs/pln-01-chot-scope-kpi-va-definition`.
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
- **Assignee:** Chưa ghi lại
- **Reviewer:** Chưa ghi lại
- **Evidence / PR:** [PR #3](https://github.com/HoaiTam/japan-earthquake-etl/pull/3)
- **Kỹ năng phù hợp:** Phân tích yêu cầu, tài liệu

## Checklist bàn giao

- [x] Deliverable đã có trong repository hoặc môi trường demo.
- [x] Acceptance criteria đã được kiểm tra.
- [x] Test tự động đạt hoặc có evidence thủ công có thể lặp lại.
- [x] Tài liệu/contract đã cập nhật nếu schema, flow, cấu hình hoặc hành vi thay đổi.
- [x] Không chứa secret, dữ liệu nhạy cảm hoặc file build không cần thiết.
- [x] P0/P1 có reviewer khác assignee xác nhận.

## Tài liệu liên quan

- [Kế hoạch 6 tuần](../README.md)
- [Các khối công việc](../WORK_BLOCKS.md)
- [Baseline MVP, KPI và Definition of Done](../../specs/MVP_SCOPE_KPI_AND_DOD.md)
