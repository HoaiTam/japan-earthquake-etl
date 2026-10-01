---
task_id: "USG-01"
status: "Done"
week: 2
block: "B - USGS Bronze"
workstream: "Request configuration"
scope: "Core"
priority: "P0"
effort_hours: 5
assignee: "unassigned"
reviewer: "unassigned"
dependencies: ["CON-01"]
---

# USG-01 - Đặc tả request USGS và cấu hình runtime

## Mục đích

Dùng để daily run và backfill tạo cùng một kiểu request có thể kiểm tra và tái chạy.

## Phạm vi công việc

Chốt endpoint FDSN, bounding box Nhật Bản, cửa sổ [start,end) UTC, overlap, limit, timeout và các biến môi trường cần thiết.

## Thành phần cần có

- **Đầu vào và contract:** [CON-01](./CON-01.md)
- **Phần triển khai:** Chốt endpoint FDSN, bounding box Nhật Bản, cửa sổ [start,end) UTC, overlap, limit, timeout và các biến môi trường cần thiết.
- **Kết quả bàn giao:** USGS request contract, immutable config validation và
  daily/backfill request planner không gọi mạng.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- [USGS request contract](../../specs/USGS_REQUEST_CONTRACT.md).
- `UsgsRequestConfig`, `UsgsRequestBuilder`, `UsgsRequestPlan` và `UsgsRequest`
  trong Spark package `vn.edu.uit.ie212.earthquake.spark.usgs`.
- Unit test offline cho daily overlap, seed clipping, backfill chunking và
  invalid configuration.

## Tiêu chí hoàn thành

- [x] Request tái lập được.
- [x] Thiếu/sai config dừng sớm.
- [x] Cửa sổ không tạo gap hoặc request vượt giới hạn mà không được chia nhỏ.

## Hard dependency

- [CON-01](./CON-01.md)

## Cách triển khai và phối hợp

Có thể chuẩn bị interface, fixture, mock, test plan và tài liệu trước khi toàn bộ upstream chạy thật. Chỉ được chuyển sang `Done` khi hard dependency đã đạt và acceptance criteria được kiểm tra trên output phù hợp.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới: `feat/usg-01-request-config`.
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
- **Kỹ năng phù hợp:** USGS API, Java config

## Checklist bàn giao

- [x] Deliverable đã có trong repository hoặc môi trường demo.
- [x] Acceptance criteria đã được kiểm tra.
- [x] Test tự động đạt hoặc có evidence thủ công có thể lặp lại.
- [x] Tài liệu/contract đã cập nhật nếu schema, flow, cấu hình hoặc hành vi thay đổi.
- [x] Không chứa secret, dữ liệu nhạy cảm hoặc file build không cần thiết.
- [ ] Reviewer độc lập đã xác nhận (khuyến nghị, không chặn `Done`).

## Tài liệu liên quan

- [Kế hoạch 6 tuần](../README.md)
- [Các khối công việc](../WORK_BLOCKS.md)
- [Baseline MVP, KPI và Definition of Done](../../specs/MVP_SCOPE_KPI_AND_DOD.md)
- [USGS request contract](../../specs/USGS_REQUEST_CONTRACT.md)

## Evidence

- `sh -n scripts/check-config.sh scripts/check-repository-layout.sh` — đạt.
- `./scripts/check-config.sh` — đạt; kiểm tra `.env.example` và secret hygiene,
  không cần local `.env`.
- `./scripts/check-repository-layout.sh` — đạt.
- `./scripts/check-foundation.sh` — đạt; toàn bộ static foundation checks.
- `./mvnw --batch-mode --no-transfer-progress -pl spark -am test` — đạt: 10
  tests, 0 failures, 0 errors.
- Test không gọi mạng và kiểm tra `minmagnitude` không được đưa vào query.
