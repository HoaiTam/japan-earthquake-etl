---
task_id: "CON-02"
status: "Done"
week: 2
block: "A - Hợp đồng dữ liệu"
workstream: "Bronze contract"
scope: "Core"
priority: "P0"
effort_hours: 5
assignee: "unassigned"
reviewer: "unassigned"
dependencies: []
---

# CON-02 - Thiết kế Bronze object và manifest

## Mục đích

Dùng làm giao diện bàn giao giữa nhóm ingest và nhóm xử lý; các writer/parser có thể phát triển bằng fixture trước khi pipeline hoàn chỉnh.

## Phạm vi công việc

Quy định path MinIO, file raw bất biến, manifest, checksum, source URL, retrieval time, run_id, catalog release và cách lưu archive/response của từng nguồn.

## Thành phần cần có

- **Đầu vào và contract:** Không có hard dependency.
- **Phần triển khai:** Quy định path MinIO, file raw bất biến, manifest, checksum, source URL, retrieval time, run_id, catalog release và cách lưu archive/response của từng nguồn.
- **Kết quả bàn giao:** [Bronze storage contract](../../specs/BRONZE_STORAGE_CONTRACT.md) và ví dụ object layout cho USGS/JMA.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- Bronze storage contract và ví dụ object layout cho USGS/JMA.

## Tiêu chí hoàn thành

- [x] Có thể truy vết từ manifest về đúng object raw.
- [x] retry không ghi đè mơ hồ.
- [x] file lỗi không được đánh dấu BronzeReady.

## Hard dependency

- Không có.

## Cách triển khai và phối hợp

Có thể bắt đầu ngay. Chốt output contract hoặc fixture nhỏ trước để các task tiêu thụ có thể làm song song.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới từ `main`: `docs/con-02-thiet-ke-bronze-object-va`.
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
- **Evidence / PR:** [Bronze storage contract check](../../../scripts/check-bronze-contract.sh); `check-source-coverage.sh`, `check-repository-layout.sh`, `git diff --check` đều đạt.
- **Kỹ năng phù hợp:** MinIO, metadata, data contract

## Checklist bàn giao

- [x] Deliverable đã có trong repository hoặc môi trường demo.
- [x] Acceptance criteria đã được kiểm tra.
- [x] Test tự động đạt hoặc có evidence thủ công có thể lặp lại.
- [x] Tài liệu/contract đã cập nhật nếu schema, flow, cấu hình hoặc hành vi thay đổi.
- [x] Không chứa secret, dữ liệu nhạy cảm hoặc file build không cần thiết.
- [ ] Reviewer độc lập đã xác nhận (khuyến nghị, không chặn `Done`).

## Tài liệu liên quan

- [Bronze storage contract](../../specs/BRONZE_STORAGE_CONTRACT.md)
- [MinIO storage và bootstrap contract](../../specs/MINIO_STORAGE.md)
- [Source coverage contract](../../specs/SOURCE_COVERAGE.md)
- [Kế hoạch 6 tuần](../README.md)
- [Các khối công việc](../WORK_BLOCKS.md)
- [Baseline MVP, KPI và Definition of Done](../../specs/MVP_SCOPE_KPI_AND_DOD.md)
