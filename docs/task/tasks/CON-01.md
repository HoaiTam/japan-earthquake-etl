---
task_id: "CON-01"
status: "Done"
week: 2
block: "A - Hợp đồng dữ liệu"
workstream: "Source scope"
scope: "Core"
priority: "P0"
effort_hours: 5
assignee: "unassigned"
reviewer: "unassigned"
dependencies: []
---

# CON-01 - Chốt phạm vi USGS và JMA

## Mục đích

Dùng làm ranh giới chung để các block USGS, JMA, Silver và BI triển khai độc lập mà vẫn cùng một định nghĩa dữ liệu.

## Phạm vi công việc

Xác định USGS cho dữ liệu cập nhật hằng ngày, JMA cho kho lịch sử 40 năm; chốt vùng Nhật Bản, mốc thời gian, múi giờ, độ trễ và phần giao nhau giữa hai nguồn.

## Thành phần cần có

- **Đầu vào và contract:** Không có hard dependency.
- **Phần triển khai:** Xác định USGS cho dữ liệu cập nhật hằng ngày, JMA cho kho lịch sử 40 năm; chốt vùng Nhật Bản, mốc thời gian, múi giờ, độ trễ và phần giao nhau giữa hai nguồn.
- **Kết quả bàn giao:** Source coverage contract, source priority và bảng quyết định trường hợp overlap.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- [Source coverage contract](../../specs/SOURCE_COVERAGE.md), source priority và bảng quyết định trường hợp overlap.

## Tiêu chí hoàn thành

- [x] Mỗi nguồn có phạm vi, thời gian, timezone, tần suất cập nhật và quy tắc sử dụng rõ.
- [x] Không cộng trùng hai catalog.

## Hard dependency

- Không có.

## Cách triển khai và phối hợp

Có thể bắt đầu ngay. Chốt output contract hoặc fixture nhỏ trước để các task tiêu thụ có thể làm song song.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới từ `main`: `docs/con-01-chot-pham-vi-usgs-va`.
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
- **Evidence / PR:** [Static contract check](../../../scripts/check-source-coverage.sh); bổ sung link PR sau khi mở.
- **Kỹ năng phù hợp:** Phân tích dữ liệu, USGS, JMA

## Checklist bàn giao

- [x] Deliverable đã có trong repository hoặc môi trường demo.
- [x] Acceptance criteria đã được kiểm tra.
- [x] Test tự động đạt hoặc có evidence thủ công có thể lặp lại.
- [x] Tài liệu/contract đã cập nhật nếu schema, flow, cấu hình hoặc hành vi thay đổi.
- [x] Không chứa secret, dữ liệu nhạy cảm hoặc file build không cần thiết.
- [ ] Reviewer độc lập đã xác nhận (khuyến nghị, không chặn `Done`).

## Tài liệu liên quan

- [Source coverage contract](../../specs/SOURCE_COVERAGE.md)
- [Kế hoạch 6 tuần](../README.md)
- [Các khối công việc](../WORK_BLOCKS.md)
- [Baseline MVP, KPI và Definition of Done](../../specs/MVP_SCOPE_KPI_AND_DOD.md)
