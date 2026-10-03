---
task_id: "ORC-05"
status: "Backlog"
week: 5
block: "F - Điều phối"
workstream: "Recovery and resources"
scope: "Core"
priority: "P1"
effort_hours: 4
assignee: "unassigned"
reviewer: "unassigned"
dependencies: ["ORC-01"]
---

# ORC-05 - Chốt recovery, concurrency và tài nguyên

## Mục đích

Dùng để nhiều thành viên chạy task/backfill linh hoạt mà không tranh tài nguyên hoặc phá dữ liệu của nhau.

## Phạm vi công việc

Cấu hình retry boundary, Spark resources, task concurrency, staging cleanup an toàn và hướng xử lý lỗi ở từng tầng.

## Thành phần cần có

- **Đầu vào và contract:** [ORC-01](./ORC-01.md)
- **Phần triển khai:** Cấu hình retry boundary, Spark resources, task concurrency, staging cleanup an toàn và hướng xử lý lỗi ở từng tầng.
- **Kết quả bàn giao:** Runtime profile, recovery matrix và operator notes.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- Runtime profile, recovery matrix và operator notes.

## Tiêu chí hoàn thành

- [ ] Máy local không OOM ở data đại diện.
- [ ] retry đúng tầng.
- [ ] không cần xóa dữ liệu để phục hồi.

## Hard dependency

- [ORC-01](./ORC-01.md)

## Cách triển khai và phối hợp

Có thể chuẩn bị interface, fixture, mock, test plan và tài liệu trước khi toàn bộ upstream chạy thật. Chỉ được chuyển sang `Done` khi hard dependency đã đạt và acceptance criteria được kiểm tra trên output phù hợp.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới từ `main`: `feat/orc-05-chot-recovery-concurrency-va-tai`.
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
- **Kỹ năng phù hợp:** Airflow recovery, Docker, Spark

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
