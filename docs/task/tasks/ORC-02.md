---
task_id: "ORC-02"
status: "Backlog"
week: 4
block: "F - Điều phối"
workstream: "Scheduling"
scope: "Core"
priority: "P1"
effort_hours: 4
assignee: "HoaiTam"
reviewer: "unassigned"
dependencies: ["ORC-01"]
---

# ORC-02 - Cấu hình lịch và readiness cho hai nguồn

## Mục đích

Dùng để hai nguồn có nhịp cập nhật khác nhau nhưng vẫn vào cùng pipeline có kiểm soát.

## Phạm vi công việc

Chốt daily schedule, timezone, overlap USGS, JMA catalog check, max active runs và điều kiện không chạy trùng daily/backfill.

## Thành phần cần có

- **Đầu vào và contract:** [ORC-01](./ORC-01.md)
- **Phần triển khai:** Chốt daily schedule, timezone, overlap USGS, JMA catalog check, max active runs và điều kiện không chạy trùng daily/backfill.
- **Kết quả bàn giao:** Runtime schedule profile và readiness sensors/checks.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- Runtime schedule profile và readiness sensors/checks.

## Tiêu chí hoàn thành

- [ ] Daily run ổn định.
- [ ] JMA chỉ kích hoạt năm thay đổi.
- [ ] timezone/data interval đúng.

## Hard dependency

- [ORC-01](./ORC-01.md)

## Cách triển khai và phối hợp

### Phân công tuần 4 mở rộng

- **Owner / effort:** HoaiTam, 4h Core; đưa từ tuần 5 lên khối vận hành của [kế hoạch tuần 4](../WEEK_4_PARALLEL_PLAN.md).
- **Làm trước:** Readiness/schedule config và data-interval fixtures bằng I/O ORC-01; task hiện Backlog, chỉ hoàn tất khi dependency đạt.
- **Làm gì / có gì:** USGS UTC daily window/three-day revision overlap, JMA changed-year readiness, timezone điều phối, max-active-runs và daily/backfill collision guards.
- **Dùng để làm gì:** Hai nguồn có nhịp khác nhau vào cùng ETL mà không chạy chồng hoặc xử lý sai ngày.
- **Handoff / nghiệm thu:** Profile/ready checks cho DAG và ORC-03/05; kiểm tra UTC/JST/host boundary, no-change JMA và daily/backfill contention. Daily ổn định cần evidence runtime phù hợp, không chỉ cron string/mock success.

Có thể chuẩn bị interface, fixture, mock, test plan và tài liệu trước khi toàn bộ upstream chạy thật. Chỉ được chuyển sang `Done` khi hard dependency đã đạt và acceptance criteria được kiểm tra trên output phù hợp.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới từ `main`: `feat/orc-02-cau-hinh-lich-va-readiness`.
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
- **Assignee:** HoaiTam
- **Reviewer:** Chưa ghi lại
- **Evidence / PR:** Chưa có
- **Kỹ năng phù hợp:** Airflow scheduling, config

## Checklist bàn giao

- [ ] Deliverable đã có trong repository hoặc môi trường demo.
- [ ] Acceptance criteria đã được kiểm tra.
- [ ] Test tự động đạt hoặc có evidence thủ công có thể lặp lại.
- [ ] Tài liệu/contract đã cập nhật nếu schema, flow, cấu hình hoặc hành vi thay đổi.
- [ ] Không chứa secret, dữ liệu nhạy cảm hoặc file build không cần thiết.
- [ ] P0/P1 có reviewer khác assignee xác nhận.

## Tài liệu liên quan

- [Kế hoạch tuần 4 mở rộng](../WEEK_4_PARALLEL_PLAN.md)
- [Kế hoạch 8 tuần](../README.md)
- [Các khối công việc](../WORK_BLOCKS.md)
- [Baseline MVP, KPI và Definition of Done](../../specs/MVP_SCOPE_KPI_AND_DOD.md)
