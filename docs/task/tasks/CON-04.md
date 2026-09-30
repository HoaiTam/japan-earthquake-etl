---
task_id: "CON-04"
status: "Done"
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
- **Kết quả bàn giao:** [Shared source fixtures](../../../tests/fixtures/README.md), [`cases.json`](../../../tests/fixtures/cases.json) và [test matrix](../../../tests/fixtures/TEST_MATRIX.md) không phụ thuộc mạng.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- [Fixture USGS/JMA và test matrix](../../../tests/fixtures/README.md) không phụ thuộc mạng, có expected count/reason code theo contract.

## Tiêu chí hoàn thành

- [x] Fixture nhỏ, xác định được, không chứa secret.
- [x] Mỗi case ghi rõ input, expected output và reason code.

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

- **Trạng thái:** Done
- **Assignee:** Chưa ghi lại
- **Reviewer:** Chưa ghi lại
- **Evidence / PR:** [Shared fixture checker](../../../scripts/check-shared-fixtures.sh) đạt 17 case; ZIP/checksum tái tạo ổn định; `check-source-coverage.sh`, `check-bronze-contract.sh`, `check-data-model-contract.sh`, `check-repository-layout.sh`, local Markdown link check và `git diff --check` đều đạt.
- **Kỹ năng phù hợp:** Test design, JSON, fixed-width

## Checklist bàn giao

- [x] Deliverable đã có trong repository hoặc môi trường demo.
- [x] Acceptance criteria đã được kiểm tra.
- [x] Test tự động đạt hoặc có evidence thủ công có thể lặp lại.
- [x] Tài liệu/contract đã cập nhật nếu schema, flow, cấu hình hoặc hành vi thay đổi.
- [x] Không chứa secret, dữ liệu nhạy cảm hoặc file build không cần thiết.
- [ ] Reviewer độc lập đã xác nhận (khuyến nghị, không chặn `Done`).

## Tài liệu liên quan

- [Shared source fixtures](../../../tests/fixtures/README.md)
- [Machine-readable test cases](../../../tests/fixtures/cases.json)
- [Human-readable test matrix](../../../tests/fixtures/TEST_MATRIX.md)
- [Source coverage contract](../../specs/SOURCE_COVERAGE.md)
- [Bronze storage contract](../../specs/BRONZE_STORAGE_CONTRACT.md)
- [Silver/Gold logical model](../../specs/SILVER_GOLD_DATA_MODEL.md)
- [Kế hoạch 6 tuần](../README.md)
- [Các khối công việc](../WORK_BLOCKS.md)
- [Baseline MVP, KPI và Definition of Done](../../specs/MVP_SCOPE_KPI_AND_DOD.md)
