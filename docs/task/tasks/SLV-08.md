---
task_id: "SLV-08"
status: "Backlog"
week: 4
block: "D - Silver đa nguồn"
workstream: "Silver storage"
scope: "Core"
priority: "P0"
effort_hours: 5
assignee: "unassigned"
reviewer: "unassigned"
dependencies: ["CON-03"]
---

# SLV-08 - Ghi Silver Parquet theo source và thời gian

## Mục đích

Dùng làm lớp dữ liệu chuẩn hóa tái sử dụng cho canonicalization, quality analysis và backfill.

## Phạm vi công việc

Ghi output tạm, verify schema/count, publish partition event_year/event_month/source_system và tạo output manifest.

## Thành phần cần có

- **Đầu vào và contract:** [CON-03](./CON-03.md)
- **Phần triển khai:** Ghi output tạm, verify schema/count, publish partition event_year/event_month/source_system và tạo output manifest.
- **Kết quả bàn giao:** Silver Parquet writer, partition convention và publish marker.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- Silver Parquet writer, partition convention và publish marker.

## Tiêu chí hoàn thành

- [ ] Không có output dở được coi là hoàn tất.
- [ ] retry không append duplicate.
- [ ] partition khớp event time UTC.

## Hard dependency

- [CON-03](./CON-03.md)

## Cách triển khai và phối hợp

Có thể chuẩn bị interface, fixture, mock, test plan và tài liệu trước khi toàn bộ upstream chạy thật. Chỉ được chuyển sang `Done` khi hard dependency đã đạt và acceptance criteria được kiểm tra trên output phù hợp.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới từ `main`: `feat/slv-08-ghi-silver-parquet-theo-source`.
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
- **Kỹ năng phù hợp:** Parquet, Spark, MinIO

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
