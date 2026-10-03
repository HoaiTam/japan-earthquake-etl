---
task_id: "GLD-03"
status: "Backlog"
week: 4
block: "E - Gold & Serving"
workstream: "Iceberg publish"
scope: "Core"
priority: "P0"
effort_hours: 6
assignee: "unassigned"
reviewer: "unassigned"
dependencies: ["GLD-01"]
---

# GLD-03 - Ghi Gold Iceberg và commit snapshot

## Mục đích

Dùng làm lớp bảng dữ liệu bền vững thay cho business database riêng; hỗ trợ snapshot và truy vấn nhất quán.

## Phạm vi công việc

Tạo schema/table Iceberg, write affected partitions, commit snapshot, ghi snapshot_id và không publish khi quality gate thất bại.

## Thành phần cần có

- **Đầu vào và contract:** [GLD-01](./GLD-01.md)
- **Phần triển khai:** Tạo schema/table Iceberg, write affected partitions, commit snapshot, ghi snapshot_id và không publish khi quality gate thất bại.
- **Kết quả bàn giao:** Iceberg tables, Spark write logic và commit metadata.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- Iceberg tables, Spark write logic và commit metadata.

## Tiêu chí hoàn thành

- [ ] Consumer chỉ thấy snapshot hoàn chỉnh.
- [ ] lỗi commit không đánh dấu Published.
- [ ] rerun không tạo duplicate logic.
- [ ] Snapshot ID/publication metadata đủ để `MLD-01` pin và time-travel.

## Hard dependency

- [GLD-01](./GLD-01.md)

## Cách triển khai và phối hợp

Có thể chuẩn bị interface, fixture, mock, test plan và tài liệu trước khi toàn bộ upstream chạy thật. Chỉ được chuyển sang `Done` khi hard dependency đã đạt và acceptance criteria được kiểm tra trên output phù hợp.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới từ `main`: `feat/gld-03-ghi-gold-iceberg-va-commit`.
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
- **Kỹ năng phù hợp:** Iceberg, Spark, MinIO

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
