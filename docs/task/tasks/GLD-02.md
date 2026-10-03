---
task_id: "GLD-02"
status: "Backlog"
week: 4
block: "E - Gold & Serving"
workstream: "Aggregates"
scope: "Stretch"
priority: "P2"
effort_hours: 4
assignee: "unassigned"
reviewer: "unassigned"
dependencies: ["CON-03", "GLD-01"]
---

# GLD-02 - Tạo aggregate phục vụ dashboard

## Mục đích

Dùng để tối ưu dashboard tùy chọn sau khi đường găng HDBSCAN ổn định; không chặn Gold snapshot hoặc ML dataset.

## Phạm vi công việc

Tính count, avg/max magnitude, avg depth, tsunami/intensity/source counts theo ngày, tháng, region và band cần cho BI.

## Thành phần cần có

- **Đầu vào và contract:** [CON-03](./CON-03.md), [GLD-01](./GLD-01.md)
- **Phần triển khai:** Tính count, avg/max magnitude, avg depth, tsunami/intensity/source counts theo ngày, tháng, region và band cần cho BI.
- **Kết quả bàn giao:** Gold aggregate transformations hoặc views.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- Gold aggregate transformations hoặc views.

## Tiêu chí hoàn thành

- [ ] Aggregate khớp fact với cùng filter.
- [ ] không double count do source-link hoặc dimension join.

## Hard dependency

- [CON-03](./CON-03.md)
- [GLD-01](./GLD-01.md)

## Cách triển khai và phối hợp

Có thể chuẩn bị interface, fixture, mock, test plan và tài liệu trước khi toàn bộ upstream chạy thật. Chỉ được chuyển sang `Done` khi hard dependency đã đạt và acceptance criteria được kiểm tra trên output phù hợp.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới từ `main`: `feat/gld-02-tao-aggregate-phuc-vu-dashboard`.
3. Triển khai đúng phạm vi; dùng fixture nhỏ, xác định được và không phụ thuộc mạng cho unit test.
4. Chạy test/check phù hợp, đối chiếu acceptance criteria và cập nhật tài liệu nếu contract hoặc hành vi thay đổi.
5. Cập nhật `status`, `assignee`, `reviewer` và Evidence ngay trong file này khi mở PR hoặc hoàn tất review.

## Ranh giới

- Không tự mở rộng sang deliverable của task khác.
- Không được trở thành hard dependency của `GLD-03`, `MLD-*` hoặc `MLQ-01`.
- Không đổi contract upstream trong PR implementation mà không cập nhật task contract liên quan và có review.
- Không commit secret, credential, payload nhạy cảm, data dump lớn hoặc artifact build không cần thiết.
- Không đánh dấu `Done` nếu chưa có evidence kiểm tra được.

## Theo dõi

- **Trạng thái:** Backlog
- **Assignee:** Chưa ghi lại
- **Reviewer:** Chưa ghi lại
- **Evidence / PR:** Chưa có
- **Kỹ năng phù hợp:** Spark SQL, aggregation

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
