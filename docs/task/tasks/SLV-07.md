---
task_id: "SLV-07"
status: "Backlog"
week: 4
block: "D - Silver đa nguồn"
workstream: "Entity resolution"
scope: "Core"
priority: "P0"
effort_hours: 6
assignee: "unassigned"
reviewer: "unassigned"
dependencies: ["CON-01", "CON-03", "CON-04"]
---

# SLV-07 - Liên kết observation và chọn canonical event

## Mục đích

Dùng để định nghĩa một động đất duy nhất cho Gold trong khi vẫn bảo toàn dữ liệu từ cả USGS và JMA.

## Phạm vi công việc

Tạo candidate theo ngưỡng thời gian/khoảng cách/depth/magnitude; lưu link confidence và áp dụng source priority đã chốt thay vì UNION ALL để đếm.

## Thành phần cần có

- **Đầu vào và contract:** [CON-01](./CON-01.md), [CON-03](./CON-03.md), [CON-04](./CON-04.md)
- **Phần triển khai:** Tạo candidate theo ngưỡng thời gian/khoảng cách/depth/magnitude; lưu link confidence và áp dụng source priority đã chốt thay vì UNION ALL để đếm.
- **Kết quả bàn giao:** Source-link table, canonical selection rules và match report.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- Source-link table, canonical selection rules và match report.

## Tiêu chí hoàn thành

- [ ] Không double count overlap.
- [ ] giữ được hai observation gốc.
- [ ] unmatched event vẫn tồn tại.
- [ ] threshold có test biên.

## Hard dependency

- [CON-01](./CON-01.md)
- [CON-03](./CON-03.md)
- [CON-04](./CON-04.md)

## Cách triển khai và phối hợp

Có thể chuẩn bị interface, fixture, mock, test plan và tài liệu trước khi toàn bộ upstream chạy thật. Chỉ được chuyển sang `Done` khi hard dependency đã đạt và acceptance criteria được kiểm tra trên output phù hợp.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới từ `main`: `feat/slv-07-lien-ket-observation-va-chon`.
3. Triển khai đúng phạm vi; dùng fixture nhỏ, xác định được và không phụ thuộc mạng cho unit test.
4. Chạy test/check phù hợp, đối chiếu acceptance criteria và cập nhật tài liệu nếu contract hoặc hành vi thay đổi.
5. Cập nhật `status`, `assignee`, `reviewer` và Evidence ngay trong file này khi mở PR hoặc hoàn tất review.

## Ranh giới

- Không tự mở rộng sang deliverable của task khác.
- Không đổi contract upstream trong PR implementation mà không cập nhật task contract liên quan và có review.
- Không commit secret, credential, payload nhạy cảm, data dump lớn hoặc artifact build không cần thiết.
- Không đánh dấu `Done` nếu chưa có evidence kiểm tra được.

## Theo dõi

- **Trạng thái:** Backlog
- **Assignee:** Chưa ghi lại
- **Reviewer:** Chưa ghi lại
- **Evidence / PR:** Chưa có
- **Kỹ năng phù hợp:** Entity resolution, geospatial, Spark

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
