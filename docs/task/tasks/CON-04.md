---
task_id: "CON-04"
status: "Ready"
week: 2
block: "A - Hợp đồng dữ liệu"
workstream: "Fixtures"
scope: "Core"
priority: "P1"
effort_hours: 3
assignee: "unassigned"
reviewer: "unassigned"
dependencies: []
---

# CON-04 - Chuẩn bị fixture và ma trận test dùng chung

## Mục đích

Dùng để các block parser, quality, Gold và BI bắt đầu song song mà không chờ tải dữ liệu thật.

## Phạm vi công việc

Tạo mẫu USGS GeoJSON và JMA fixed-width/ZIP nhỏ cho success, empty, invalid, duplicate, revised event, timezone và checksum mismatch.

## Thành phần cần có

- **Đầu vào và contract:** Không có hard dependency.
- **Phần triển khai:** Tạo mẫu USGS GeoJSON và JMA fixed-width/ZIP nhỏ cho success, empty, invalid, duplicate, revised event, timezone và checksum mismatch.
- **Kết quả bàn giao:** Fixture không phụ thuộc mạng và test matrix theo từng contract.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- Fixture không phụ thuộc mạng và test matrix theo từng contract.

## Tiêu chí hoàn thành

- [ ] Fixture nhỏ, xác định được, không chứa secret.
- [ ] mỗi case ghi rõ input, expected output và reason code.

## Hard dependency

- Không có.

## Cách triển khai và phối hợp

Có thể bắt đầu ngay. Chốt output contract hoặc fixture nhỏ trước để các task tiêu thụ có thể làm song song.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới từ `main`: `docs/con-04-chuan-bi-fixture-va-ma`.
3. Triển khai đúng phạm vi; dùng fixture nhỏ, xác định được và không phụ thuộc mạng cho unit test.
4. Chạy test/check phù hợp, đối chiếu acceptance criteria và cập nhật tài liệu nếu contract hoặc hành vi thay đổi.
5. Cập nhật `status`, `assignee`, `reviewer` và Evidence ngay trong file này khi mở PR hoặc hoàn tất review.

## Ranh giới

- Không tự mở rộng sang deliverable của task khác.
- Không đổi contract upstream trong PR implementation mà không cập nhật task contract liên quan và có review.
- Không commit secret, credential, payload nhạy cảm, data dump lớn hoặc artifact build không cần thiết.
- Không đánh dấu `Done` nếu chưa có evidence kiểm tra được.

## Theo dõi

- **Trạng thái:** Ready
- **Assignee:** Chưa ghi lại
- **Reviewer:** Chưa ghi lại
- **Evidence / PR:** Chưa có
- **Kỹ năng phù hợp:** Test design, JSON, fixed-width

## Checklist bàn giao

- [ ] Deliverable đã có trong repository hoặc môi trường demo.
- [ ] Acceptance criteria đã được kiểm tra.
- [ ] Test tự động đạt hoặc có evidence thủ công có thể lặp lại.
- [ ] Tài liệu/contract đã cập nhật nếu schema, flow, cấu hình hoặc hành vi thay đổi.
- [ ] Không chứa secret, dữ liệu nhạy cảm hoặc file build không cần thiết.
- [ ] P0/P1 có reviewer khác assignee xác nhận.

## Tài liệu liên quan

- [Kế hoạch 6 tuần](../README.md)
- [Các khối công việc](../WORK_BLOCKS.md)
- [Baseline MVP, KPI và Definition of Done](../../specs/MVP_SCOPE_KPI_AND_DOD.md)
