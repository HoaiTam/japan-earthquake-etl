---
task_id: "SLV-04"
status: "Done"
week: 3
block: "D - Silver đa nguồn"
workstream: "Lineage"
scope: "Core"
priority: "P0"
effort_hours: 4
assignee: "codex"
reviewer: "unassigned"
dependencies: ["CON-03"]
---

# SLV-04 - Tạo source key và lineage đa nguồn

## Mục đích

Dùng để audit, reprocess và liên kết nhiều observation về cùng một động đất mà không mất nguồn gốc.

## Phạm vi công việc

Định nghĩa source_system, source_record_key, raw_object_uri, raw checksum, line number, catalog release, parser version và ingest_run_id.

## Thành phần cần có

- **Đầu vào và contract:** [CON-03](./CON-03.md)
- **Phần triển khai:** Định nghĩa source_system, source_record_key, raw_object_uri, raw checksum, line number, catalog release, parser version và ingest_run_id.
- **Kết quả bàn giao:** Lineage transformation và key-generation tests.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- Lineage transformation và key-generation tests.

## Tiêu chí hoàn thành

- [x] Mỗi observation truy vết về đúng raw record.
- [x] key ổn định khi rerun cùng input.
- [x] không dùng time/coordinate như canonical event ID.

## Hard dependency

- [CON-03](./CON-03.md)

## Cách triển khai và phối hợp

Có thể chuẩn bị interface, fixture, mock, test plan và tài liệu trước khi toàn bộ upstream chạy thật. Chỉ được chuyển sang `Done` khi hard dependency đã đạt và acceptance criteria được kiểm tra trên output phù hợp.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới từ `main`: `feat/slv-04-tao-source-key-va-lineage`.
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
- **Assignee:** codex
- **Reviewer:** unassigned
- **Evidence / PR:** Hoàn tất trên branch `feat/slv-04-tao-source-key-va-lineage`; Maven test suite đạt 59/59 tests (bao gồm 7 tests mới trong `SilverLineageAndKeyTest`). Đã chuẩn hóa lớp `SourceKeyGenerator` và `SilverLineage` cho cả hai nguồn USGS và JMA; JMA key dùng thuật toán version `jma_k1` từ identity fields chính thức bảo đảm `same_source_record_key=true` khi cập nhật revision; `canonical_event_id` được sinh độc lập dưới dạng `evt_` + 32 hex chars, hoàn toàn không phụ thuộc thời gian hay tọa độ; `SilverLineage` hỗ trợ đối soát toàn vẹn byte-level về raw record locator/hash và raw object SHA-256. Kịch bản kiểm tra `check-week-3-plan.sh`, `check-repository-layout.sh`, `check-mvp-baseline.sh` và `git diff --check` đều đạt. Reviewer giữ `unassigned`.
- **Kỹ năng phù hợp:** Data lineage, hashing, Spark

## Checklist bàn giao

- [x] Deliverable đã có trong repository hoặc môi trường demo.
- [x] Acceptance criteria đã được kiểm tra.
- [x] Test tự động đạt hoặc có evidence thủ công có thể lặp lại.
- [x] Tài liệu/contract đã cập nhật nếu schema, flow, cấu hình hoặc hành vi thay đổi.
- [x] Không chứa secret, dữ liệu nhạy cảm hoặc file build không cần thiết.
- [ ] P0/P1 có reviewer khác assignee xác nhận.

## Tài liệu liên quan

- [Kế hoạch 8 tuần](../README.md)
- [Các khối công việc](../WORK_BLOCKS.md)
- [Baseline MVP, KPI và Definition of Done](../../specs/MVP_SCOPE_KPI_AND_DOD.md)
