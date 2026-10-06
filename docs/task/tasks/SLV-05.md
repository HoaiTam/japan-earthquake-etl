---
task_id: "SLV-05"
status: "Done"
week: 3
block: "D - Silver đa nguồn"
workstream: "Quality"
scope: "Core"
priority: "P0"
effort_hours: 5
assignee: "ThanhTris"
reviewer: "unassigned"
dependencies: ["CON-03", "CON-04"]
---

# SLV-05 - Áp dụng validation và reject metrics

## Mục đích

Dùng để ngăn dữ liệu lỗi vào analytics mà không xóa bằng chứng raw.

## Phạm vi công việc

Kiểm tra required fields, parseability, tọa độ, thời gian, range, JMA quality flags; tổng hợp reason count theo source và run.

## Thành phần cần có

- **Đầu vào và contract:** [CON-03](./CON-03.md), [CON-04](./CON-04.md)
- **Phần triển khai:** Kiểm tra required fields, parseability, tọa độ, thời gian, range, JMA quality flags; tổng hợp reason count theo source và run.
- **Kết quả bàn giao:** Silver validator, reject dataset và quality summary.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- Silver validator, reject dataset và quality summary.

## Tiêu chí hoàn thành

- [x] valid + rejected = parsed.
- [x] blocker dừng publish.
- [x] rejected vẫn truy vết được về Bronze.

## Hard dependency

- [CON-03](./CON-03.md)
- [CON-04](./CON-04.md)

## Cách triển khai và phối hợp

Có thể chuẩn bị interface, fixture, mock, test plan và tài liệu trước khi toàn bộ upstream chạy thật. Chỉ được chuyển sang `Done` khi hard dependency đã đạt và acceptance criteria được kiểm tra trên output phù hợp.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới từ `main`: `feat/slv-05-ap-dung-validation-va-reject`.
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
- **Evidence / PR:** [PR #32](https://github.com/HoaiTam/japan-earthquake-etl/pull/32) trên branch `feat/slv-05-quality-validation`; rebase lên `origin/main` tại `9003352` ngày 2026-10-06, không có conflict. Maven `-pl spark -am clean test` đạt 44/44 tests (quality 4/4) với JDK 17/Maven 3.9.16; contract checks data-model/repository/week-3 và `git diff --check` đạt. SLV-05 hiện là Java API, không có biến môi trường riêng; run ID và đường dẫn quality summary do caller truyền vào. Reviewer vẫn `unassigned`.
- **Kỹ năng phù hợp:** Data quality, Spark

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
