---
task_id: "QRY-01"
status: "Done"
week: 1
block: "Foundation"
workstream: "Serving"
scope: "Core"
priority: "P1"
effort_hours: 7
assignee: "unassigned"
reviewer: "unassigned"
dependencies: ["MIO-01", "SPK-01"]
---

# QRY-01 - Cấu hình Iceberg REST Catalog và Trino

## Mục đích

Cung cấp catalog Iceberg và lớp truy vấn Trino để kiểm tra Gold và phục vụ Power BI.

## Phạm vi công việc

Khởi động Catalog/Trino, khai báo warehouse MinIO và kiểm tra kết nối nền.

## Thành phần cần có

- **Đầu vào và contract:** [MIO-01](./MIO-01.md), [SPK-01](./SPK-01.md)
- **Phần triển khai:** Khởi động Catalog/Trino, khai báo warehouse MinIO và kiểm tra kết nối nền.
- **Kết quả bàn giao:** Catalog và Trino services với catalog config ban đầu.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- Catalog và Trino services với catalog config ban đầu.

## Tiêu chí hoàn thành

- [x] Trino khởi động, nhìn thấy catalog/schema kiểm thử và không lộ credential.

## Hard dependency

- [MIO-01](./MIO-01.md)
- [SPK-01](./SPK-01.md)

## Cách triển khai và phối hợp

Có thể chuẩn bị interface, fixture, mock, test plan và tài liệu trước khi toàn bộ upstream chạy thật. Chỉ được chuyển sang `Done` khi hard dependency đã đạt và acceptance criteria được kiểm tra trên output phù hợp.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới từ `main`: `feat/qry-01-cau-hinh-iceberg-rest-catalog`.
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
- **Evidence / PR:** [PR #10](https://github.com/HoaiTam/japan-earthquake-etl/pull/10)
- **Kỹ năng phù hợp:** Iceberg, Trino, MinIO

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
