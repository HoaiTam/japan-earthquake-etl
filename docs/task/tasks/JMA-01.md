---
task_id: "JMA-01"
status: "Ready"
week: 3
block: "C - JMA Bronze"
workstream: "Archive inventory"
scope: "Core"
priority: "P0"
effort_hours: 5
assignee: "unassigned"
reviewer: "unassigned"
dependencies: ["CON-01"]
---

# JMA-01 - Lập danh mục 40 năm archive JMA

## Mục đích

Dùng làm danh sách đầu vào có kiểm soát, tránh scrape HTML thành dữ liệu sự kiện hoặc tải thiếu năm.

## Phạm vi công việc

Xác định dải năm, URL/file name, catalog release, format 96-byte, timezone JST, geodetic datum, record type và điều khoản ghi nguồn.

## Thành phần cần có

- **Đầu vào và contract:** [CON-01](./CON-01.md)
- **Phần triển khai:** Xác định dải năm, URL/file name, catalog release, format 96-byte, timezone JST, geodetic datum, record type và điều khoản ghi nguồn.
- **Kết quả bàn giao:** JMA archive inventory và source-format contract.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- JMA archive inventory và source-format contract.

## Tiêu chí hoàn thành

- [ ] Mỗi năm có URL/version dự kiến.
- [ ] ghi rõ cách nhận biết file sửa.
- [ ] quy tắc citation và metadata không bị bỏ sót.

## Hard dependency

- [CON-01](./CON-01.md)

## Cách triển khai và phối hợp

Có thể chuẩn bị interface, fixture, mock, test plan và tài liệu trước khi toàn bộ upstream chạy thật. Chỉ được chuyển sang `Done` khi hard dependency đã đạt và acceptance criteria được kiểm tra trên output phù hợp.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới từ `main`: `feat/jma-01-lap-danh-muc-40-nam`.
3. Triển khai đúng phạm vi; dùng fixture nhỏ, xác định được và không phụ thuộc mạng cho unit test.
4. Chạy test/check phù hợp, đối chiếu acceptance criteria và cập nhật tài liệu nếu contract hoặc hành vi thay đổi.
5. Cập nhật `status`, `assignee`, `reviewer` và Evidence ngay trong file này khi mở PR hoặc hoàn tất review.

## Ranh giới

- Không tự mở rộng sang deliverable của task khác.
- Không đổi contract upstream trong PR implementation mà không cập nhật task contract liên quan và có review.
- Không commit secret, credential, payload nhạy cảm, data dump lớn hoặc artifact build không cần thiết.
- Không đánh dấu `Done` nếu chưa có evidence kiểm tra được.

## Theo dõi

- **Trạng thái:** Ready
- **Assignee:** Chưa ghi lại
- **Reviewer:** Chưa ghi lại
- **Evidence / PR:** Chưa có
- **Kỹ năng phù hợp:** JMA catalog, metadata, research

## Checklist bàn giao

- [ ] Deliverable đã có trong repository hoặc môi trường demo.
- [ ] Acceptance criteria đã được kiểm tra.
- [ ] Test tự động đạt hoặc có evidence thủ công có thể lặp lại.
- [ ] Tài liệu/contract đã cập nhật nếu schema, flow, cấu hình hoặc hành vi thay đổi.
- [ ] Không chứa secret, dữ liệu nhạy cảm hoặc file build không cần thiết.
- [ ] P0/P1 có reviewer khác assignee xác nhận.

## Tài liệu liên quan

- [Kế hoạch 8 tuần](../README.md)
- [Các khối công việc](../WORK_BLOCKS.md)
- [Baseline MVP, KPI và Definition of Done](../../specs/MVP_SCOPE_KPI_AND_DOD.md)
