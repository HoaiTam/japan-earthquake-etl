---
task_id: "USG-03"
status: "Done"
week: 2
block: "B - USGS Bronze"
workstream: "Bronze writer"
scope: "Core"
priority: "P0"
effort_hours: 7
assignee: "unassigned"
reviewer: "unassigned"
dependencies: ["CON-02", "USG-01"]
---

# USG-03 - Validate và lưu USGS raw vào Bronze

## Mục đích

Dùng để lưu bằng chứng nguồn bất biến cho reprocessing, audit và đối soát về sau.

## Phạm vi công việc

Kiểm tra GeoJSON/features, phân biệt empty hợp lệ với lỗi; ghi response nguyên bản, manifest và SHA-256 vào path theo ingest_date/run_id.

## Thành phần cần có

- **Đầu vào và contract:** [CON-02](./CON-02.md), [USG-01](./USG-01.md)
- **Phần triển khai:** Kiểm tra GeoJSON/features, phân biệt empty hợp lệ với lỗi; ghi response nguyên bản, manifest và SHA-256 vào path theo ingest_date/run_id.
- **Kết quả bàn giao:** USGS envelope validator, immutable Bronze writer, raw
  readback/SHA-256 và metadata manifest/quarantine.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- USGS validator, Bronze writer và metadata manifest.

## Tiêu chí hoàn thành

- [x] Object đọc lại được.
- [x] Checksum khớp.
- [x] Retry/rerun không mất raw response hoặc overwrite mơ hồ.
- [x] Invalid payload không được đánh dấu BronzeReady.

## Hard dependency

- [CON-02](./CON-02.md)
- [USG-01](./USG-01.md)

## Cách triển khai và phối hợp

Có thể chuẩn bị interface, fixture, mock, test plan và tài liệu trước khi toàn bộ upstream chạy thật. Chỉ được chuyển sang `Done` khi hard dependency đã đạt và acceptance criteria được kiểm tra trên output phù hợp.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới: `feat/usg-03-bronze-writer`.
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
- **Kỹ năng phù hợp:** GeoJSON, MinIO SDK, checksum

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
- [Bronze storage contract](../../specs/BRONZE_STORAGE_CONTRACT.md)
- [USGS Bronze writer contract](../../specs/USGS_BRONZE_WRITER_CONTRACT.md)

## Evidence

- `./scripts/check-config.sh` — đạt.
- `./scripts/check-repository-layout.sh` — đạt.
- `./scripts/check-bronze-contract.sh` — đạt.
- `./scripts/check-foundation.sh` — đạt: toàn bộ foundation contract và 20
  Maven tests, 0 failures, 0 errors.
- `./mvnw --batch-mode --no-transfer-progress -pl spark -am test` — đạt: 20
  tests, 0 failures, 0 errors.
- `UsgsBronzeWriterTest` kiểm tra success/empty/invalid GeoJSON, raw readback,
  SHA-256, idempotent rerun, quarantine và ambiguous overwrite.
