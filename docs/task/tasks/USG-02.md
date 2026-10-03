---
task_id: "USG-02"
status: "Done"
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
- **Kết quả bàn giao:** Java HTTP client có timeout, retry/backoff giới hạn,
  response-size guard, count pre-check/pagination và unit test bằng mock server.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- [USGS HTTP client contract](../../specs/USGS_HTTP_CLIENT_CONTRACT.md).
- `UsgsHttpClient`, `UsgsHttpResponse`, `UsgsHttpException` và
  `UsgsResponseTooLargeException` trong package
  `vn.edu.uit.ie212.earthquake.spark.usgs`.
- Mock-server tests cho retry/backoff, `Retry-After`, 4xx, response-size guard,
  logging metadata và count pagination.

## Tiêu chí hoàn thành

- [x] Retry đúng nhóm lỗi.
- [x] 4xx cấu hình không lặp vô hạn.
- [x] Event window và request ID xuất hiện trong log.

## Hard dependency

- [USG-01](./USG-01.md)

## Cách triển khai và phối hợp

Có thể chuẩn bị interface, fixture, mock, test plan và tài liệu trước khi toàn bộ upstream chạy thật. Chỉ được chuyển sang `Done` khi hard dependency đã đạt và acceptance criteria được kiểm tra trên output phù hợp.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới: `feat/usg-02-http-client`.
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
- **Evidence / PR:** Chưa mở PR; evidence cục bộ bên dưới.
- **Kỹ năng phù hợp:** Java HTTP, retry, testing

## Checklist bàn giao

- [x] Deliverable đã có trong repository hoặc môi trường demo.
- [x] Acceptance criteria đã được kiểm tra.
- [x] Test tự động đạt hoặc có evidence thủ công có thể lặp lại.
- [x] Tài liệu/contract đã cập nhật nếu schema, flow, cấu hình hoặc hành vi thay đổi.
- [x] Không chứa secret, dữ liệu nhạy cảm hoặc file build không cần thiết.
- [ ] Reviewer độc lập đã xác nhận (khuyến nghị, không chặn `Done`).

## Tài liệu liên quan

- [Kế hoạch 8 tuần](../README.md)
- [Các khối công việc](../WORK_BLOCKS.md)
- [Baseline MVP, KPI và Definition of Done](../../specs/MVP_SCOPE_KPI_AND_DOD.md)
- [USGS request contract](../../specs/USGS_REQUEST_CONTRACT.md)
- [USGS HTTP client contract](../../specs/USGS_HTTP_CLIENT_CONTRACT.md)

## Evidence

- `sh -n scripts/check-config.sh scripts/check-repository-layout.sh` — đạt.
- `./scripts/check-config.sh` — đạt; xác nhận các biến retry/response guard mới.
- `./scripts/check-repository-layout.sh` — đạt.
- `./mvnw --batch-mode --no-transfer-progress -pl spark -am test` — đạt: 15
  tests, 0 failures, 0 errors; mock server chạy trên localhost.
- Test cover 503 retry, 429/`Retry-After`, 4xx no-retry, response-size guard,
  request metadata logs và count pagination.
