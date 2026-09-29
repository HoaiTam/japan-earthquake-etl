---
task_id: "USG-02"
status: "Backlog"
week: 2
block: "B - USGS Bronze"
workstream: "HTTP client"
scope: "Core"
priority: "P0"
effort_hours: 6
assignee: "unassigned"
reviewer: "unassigned"
dependencies: ["USG-01"]
---

# USG-02 - Xây USGS HTTP client có retry an toàn

## Mục đích

Dùng để lấy dữ liệu cập nhật ổn định mà không làm Airflow phụ thuộc vào logic HTTP chi tiết.

## Phạm vi công việc

Cài timeout, retry/backoff cho 429/5xx, response size guard, count pre-check khi cần và log request metadata không chứa payload lớn.

## Thành phần cần có

- **Đầu vào và contract:** [USG-01](./USG-01.md)
- **Phần triển khai:** Cài timeout, retry/backoff cho 429/5xx, response size guard, count pre-check khi cần và log request metadata không chứa payload lớn.
- **Kết quả bàn giao:** Java HTTP client và unit test bằng mock server.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- Java HTTP client và unit test bằng mock server.

## Tiêu chí hoàn thành

- [ ] Retry đúng nhóm lỗi.
- [ ] 4xx cấu hình không lặp vô hạn.
- [ ] event window và request ID xuất hiện trong log.

## Hard dependency

- [USG-01](./USG-01.md)

## Cách triển khai và phối hợp

Có thể chuẩn bị interface, fixture, mock, test plan và tài liệu trước khi toàn bộ upstream chạy thật. Chỉ được chuyển sang `Done` khi hard dependency đã đạt và acceptance criteria được kiểm tra trên output phù hợp.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới từ `main`: `feat/usg-02-xay-usgs-http-client-co`.
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
- **Kỹ năng phù hợp:** Java HTTP, retry, testing

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
