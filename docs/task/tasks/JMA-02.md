---
task_id: "JMA-02"
status: "Done"
week: 3
block: "C - JMA Bronze"
workstream: "Downloader"
scope: "Core"
priority: "P0"
effort_hours: 6
assignee: "ThanhTris"
reviewer: "unassigned"
dependencies: ["JMA-01"]
---

# JMA-02 - Xây JMA downloader và phát hiện file thay đổi

## Mục đích

Dùng cho initial backfill 40 năm và các lần JMA sửa catalog sau này.

## Phạm vi công việc

Tải từng archive theo inventory, giới hạn concurrency, hỗ trợ resume, lưu ETag/Last-Modified/size/SHA-256 và tạo phiên bản mới khi nguồn thay đổi.

## Thành phần cần có

- **Đầu vào và contract:** [JMA-01](./JMA-01.md)
- **Phần triển khai:** Tải từng archive theo inventory, giới hạn concurrency, hỗ trợ resume, lưu ETag/Last-Modified/size/SHA-256 và tạo phiên bản mới khi nguồn thay đổi.
- **Kết quả bàn giao:** JMA archive downloader và change-detection result.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- JMA archive downloader và change-detection result.

## Tiêu chí hoàn thành

- [x] Không tải mù toàn bộ khi rerun.
- [x] file thay đổi được nhận biết.
- [x] lỗi một năm không làm mất evidence của năm khác.

## Hard dependency

- [JMA-01](./JMA-01.md)

## Cách triển khai và phối hợp

Có thể chuẩn bị interface, fixture, mock, test plan và tài liệu trước khi toàn bộ upstream chạy thật. Chỉ được chuyển sang `Done` khi hard dependency đã đạt và acceptance criteria được kiểm tra trên output phù hợp.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới từ `main`: `feat/jma-02-xay-jma-downloader-va-phat`.
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
- **Assignee:** ThanhTris
- **Reviewer:** Chưa ghi lại
- **Evidence / PR:** Implementation trên branch `feat/jma-02-downloader`; JMA downloader tests 3/3 và toàn bộ Spark Maven tests 44/44 đạt với JDK 17/Maven 3.9.16; contract checks JMA/Bronze/repository/week-3 đạt. Reviewer vẫn `unassigned`.
- **Kỹ năng phù hợp:** Java HTTP, archive download, checksum

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
