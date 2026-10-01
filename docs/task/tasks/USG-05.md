---
task_id: "USG-05"
status: "Done"
week: 2
block: "B - USGS Bronze"
workstream: "QA"
scope: "Core"
priority: "P0"
effort_hours: 4
assignee: "unassigned"
reviewer: "unassigned"
dependencies: ["USG-04"]
---

# USG-05 - Kiểm thử USGS đến Bronze

## Mục đích

Dùng làm mốc BronzeReady của nguồn USGS để Silver và E2E tích hợp dữ liệu thật.

## Phạm vi công việc

Bao phủ success, empty, invalid JSON, timeout, 429/5xx, checksum mismatch và một integration run bằng fixture hoặc cửa sổ nhỏ.

## Thành phần cần có

- **Đầu vào và contract:** [USG-04](./USG-04.md)
- **Phần triển khai:** Bao phủ success, empty, invalid JSON, timeout, 429/5xx, checksum mismatch và một integration run bằng fixture hoặc cửa sổ nhỏ.
- **Kết quả bàn giao:** Unit/integration tests và evidence record-count theo run_id.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- Unit/integration tests và evidence record-count theo run_id.

## Tiêu chí hoàn thành

- [x] Test ổn định, không phụ thuộc mạng cho unit test.
- [x] object, manifest và log đối soát được.

## Hard dependency

- [USG-04](./USG-04.md)

## Cách triển khai và phối hợp

Đây là integration gate. Có thể chuẩn bị test plan, fixture và harness trước, nhưng chỉ được chuyển sang `Done` sau khi toàn bộ hard dependency cung cấp output thật và báo cáo đối soát đạt.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới cho task: `feat/usg-05-usgs-bronze-tests` trên nền output của `USG-04`.
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
- **Evidence / PR:** Đã hoàn tất trên branch `feat/usg-05-usgs-bronze-tests`; chưa commit/push hoặc mở PR.
- **Evidence kiểm thử:** `./scripts/check-shared-fixtures.sh` đạt 17 cases; `./mvnw --batch-mode --no-transfer-progress -pl spark -am test` đạt 25 tests, 0 failures, 0 errors. `UsgsBronzeFixtureAcceptanceTest` đối soát raw object, manifest, run_id, record count, checksum/readback và quarantine; `UsgsHttpClientTest` bao phủ timeout, 429, 5xx, 4xx, pagination và response-size guard.
- **Kỹ năng phù hợp:** JUnit, mocking, integration test

## Checklist bàn giao

- [x] Deliverable đã có trong repository hoặc môi trường demo.
- [x] Acceptance criteria đã được kiểm tra.
- [x] Test tự động đạt hoặc có evidence thủ công có thể lặp lại.
- [x] Tài liệu/contract đã cập nhật nếu schema, flow, cấu hình hoặc hành vi thay đổi.
- [x] Không chứa secret, dữ liệu nhạy cảm hoặc file build không cần thiết.
- [ ] P0/P1 có reviewer khác assignee xác nhận.

## Tài liệu liên quan

- [Kế hoạch 6 tuần](../README.md)
- [Các khối công việc](../WORK_BLOCKS.md)
- [Baseline MVP, KPI và Definition of Done](../../specs/MVP_SCOPE_KPI_AND_DOD.md)
- [USGS Bronze QA contract](../../specs/USGS_BRONZE_QA_CONTRACT.md)
- [USGS Bronze writer contract](../../specs/USGS_BRONZE_WRITER_CONTRACT.md)
- [USGS HTTP client contract](../../specs/USGS_HTTP_CLIENT_CONTRACT.md)
