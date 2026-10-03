---
task_id: "OPS-01"
status: "Backlog"
week: 6
block: "K - QA & Release"
workstream: "Backup and restore"
scope: "Stretch"
priority: "P2"
effort_hours: 4
assignee: "unassigned"
reviewer: "unassigned"
dependencies: ["GLD-03", "ORC-05"]
---

# OPS-01 - Smoke test backup và restore metadata

## Mục đích

Dùng để tăng khả năng phục hồi; có thể hoãn nếu thời gian chỉ đủ hoàn thành MVP Core.

## Phạm vi công việc

Backup manifest Bronze/Gold/ML quan trọng, Iceberg catalog metadata, experiment artifact registry và Airflow metadata cần thiết; restore trong môi trường thử.

## Thành phần cần có

- **Đầu vào và contract:** [GLD-03](./GLD-03.md), [ORC-05](./ORC-05.md)
- **Phần triển khai:** Backup manifest Bronze/Gold quan trọng, Iceberg catalog metadata và Airflow metadata cần thiết; restore trong môi trường thử.
- **Kết quả bàn giao:** Backup manifest và restore procedure đã thử.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- Backup manifest và restore procedure đã thử.

## Tiêu chí hoàn thành

- [ ] Restore đọc được Gold snapshot/query kiểm chứng.
- [ ] Restore truy vết được dataset/experiment manifest và bảng `ml.*` liên quan.
- [ ] backup không chứa secret chia sẻ.

## Hard dependency

- [GLD-03](./GLD-03.md)
- [ORC-05](./ORC-05.md)

## Cách triển khai và phối hợp

Có thể chuẩn bị interface, fixture, mock, test plan và tài liệu trước khi toàn bộ upstream chạy thật. Chỉ được chuyển sang `Done` khi hard dependency đã đạt và acceptance criteria được kiểm tra trên output phù hợp.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới từ `main`: `feat/ops-01-smoke-test-backup-va-restore`.
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
- **Kỹ năng phù hợp:** Backup, MinIO, operations

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
