---
task_id: "JMA-03"
status: "Done"
week: 3
block: "C - JMA Bronze"
workstream: "Bronze writer"
scope: "Core"
priority: "P0"
effort_hours: 7
assignee: "HoaiTam"
reviewer: "unassigned"
dependencies: ["CON-02", "JMA-01"]
---

# JMA-03 - Validate và lưu JMA archive vào Bronze

## Mục đích

Dùng để giữ nguyên dữ liệu JMA phục vụ audit và cho phép parser được nâng cấp mà không tải lại nguồn.

## Phạm vi công việc

Kiểm tra ZIP mở được, file expected tồn tại, record fixed-width hợp lệ ở mức cấu trúc; lưu ZIP nguyên bản, manifest, checksum và record count sơ bộ.

## Thành phần cần có

- **Đầu vào và contract:** [CON-02](./CON-02.md), [JMA-01](./JMA-01.md)
- **Phần triển khai:** Kiểm tra ZIP mở được, file expected tồn tại, record fixed-width hợp lệ ở mức cấu trúc; lưu ZIP nguyên bản, manifest, checksum và record count sơ bộ.
- **Kết quả bàn giao:** JMA archive validator, Bronze writer và manifest theo year/catalog_release.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- JMA archive validator, Bronze writer và manifest theo year/catalog_release.
- [JMA Bronze writer contract](../../specs/JMA_BRONZE_WRITER_CONTRACT.md): giải thích thành phần, luồng kiểm tra/lưu raw, metadata, retry/revision, handoff và lệnh test.

## Tiêu chí hoàn thành

- [x] Archive đọc lại được.
- [x] file hỏng không BronzeReady.
- [x] không normalize/filter business row trong Bronze.

## Hard dependency

- [CON-02](./CON-02.md)
- [JMA-01](./JMA-01.md)

## Cách triển khai và phối hợp

Có thể chuẩn bị interface, fixture, mock, test plan và tài liệu trước khi toàn bộ upstream chạy thật. Chỉ được chuyển sang `Done` khi hard dependency đã đạt và acceptance criteria được kiểm tra trên output phù hợp.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới từ `main`: `feat/jma-03-bronze-writer`.
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
- **Assignee:** HoaiTam
- **Reviewer:** Chưa ghi lại
- **Branch / baseline:** `feat/jma-03-bronze-writer` từ main `b631cbd` (sau merge PR #36), giữ lại phần JMA-03 đang dở bằng stash riêng; không thay đổi `.env` hoặc `.metals/`. Chưa commit/push/mở PR cho task này.
- **Evidence 2026-10-07:** `make test-jma-bronze` đạt **21/21 tests** (validator 5, writer/handoff 16); full `make check` đạt **104/104 Java tests**, **15/15 Airflow tests**, toàn bộ contract/foundation/USG-06 static checks và checker **73 task**. Build bằng JDK 21.0.11, compiler `--release 17`; test dùng fixture/file store/mock, không gọi nguồn thật hoặc ghi MinIO thật. JDK 17/Spark runtime smoke chưa chạy; live orchestration/QA tiếp tục tại JMA-04/JMA-05.
- **Acceptance evidence:** ZIP readback bằng đúng input bytes/SHA; corrupted ZIP/CRC/member/95-byte/checksum/type/length/HTTP lỗi không publish Ready; record rỗng/duplicate/artificial/out-of-year vẫn giữ nguyên; hai segment 1997 có interval và lineage riêng; retry sau manifest upload failure, create race, revision, metadata tampering và SLV-01 handoff đã được kiểm tra. Status/reason đối chiếu fixture CON-04, không sửa shared fixture/contract.
- **Handoff / safety:** `make help` có target mới; `git diff --check` và configuration/secret hygiene đạt. Chưa có PR hoặc reviewer độc lập cho JMA-03, giữ `reviewer: unassigned`; không có migration/xóa dữ liệu bắt buộc. Docs giải thích giới hạn metadata JMA-02 và local manifest path của SLV-01 để JMA-04 tích hợp đúng.
- **Kỹ năng phù hợp:** ZIP, fixed-width, MinIO SDK

## Checklist bàn giao

- [x] Deliverable đã có trong repository hoặc môi trường demo.
- [x] Acceptance criteria đã được kiểm tra.
- [x] Test tự động đạt hoặc có evidence thủ công có thể lặp lại.
- [x] Tài liệu/contract đã cập nhật nếu schema, flow, cấu hình hoặc hành vi thay đổi.
- [x] Không chứa secret, dữ liệu nhạy cảm hoặc file build không cần thiết.
- [ ] P0/P1 có reviewer khác assignee xác nhận.

## Tài liệu liên quan

- [JMA Bronze writer contract](../../specs/JMA_BRONZE_WRITER_CONTRACT.md)
- [Bronze storage contract CON-02](../../specs/BRONZE_STORAGE_CONTRACT.md)
- [JMA archive inventory/source format](../../specs/JMA_ARCHIVE_INVENTORY.md)
- [Kế hoạch 8 tuần](../README.md)
- [Các khối công việc](../WORK_BLOCKS.md)
- [Baseline MVP, KPI và Definition of Done](../../specs/MVP_SCOPE_KPI_AND_DOD.md)
