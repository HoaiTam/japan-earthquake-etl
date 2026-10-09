---
task_id: "GLD-04"
status: "In Progress"
week: 4
block: "E - Gold & Serving"
workstream: "Trino verification"
scope: "Core"
priority: "P0"
effort_hours: 4
assignee: "Trang"
reviewer: "unassigned"
dependencies: ["GLD-03"]
---

# GLD-04 - Tạo Trino views và verification SQL

## Mục đích

Dùng làm giao diện SQL ổn định để kiểm chứng Gold độc lập với Spark và cung cấp snapshot đã Published cho ML; Power BI chỉ là consumer tùy chọn.

## Phạm vi công việc

Khai báo schema/views Gold; kiểm tra uniqueness, completeness, source/catalog era, aggregate consistency và snapshot freshness; cung cấp query resolve snapshot cho `MLD-01`.

## Thành phần cần có

- **Đầu vào và contract:** [GLD-03](./GLD-03.md)
- **Phần triển khai:** Khai báo schema/views cho BI; kiểm tra uniqueness, completeness, aggregate consistency, snapshot freshness và sample queries.
- **Kết quả bàn giao:** Trino views, verification queries và query smoke tests.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- Trino views, verification queries và query smoke tests.

## Tiêu chí hoàn thành

- [ ] Trino đọc current snapshot.
- [ ] mọi blocker đạt trước Published.
- [ ] schema Gold/ML input khớp contract và snapshot ID truy vết được.

## Hard dependency

- [GLD-03](./GLD-03.md)

## Cách triển khai và phối hợp

### Phân công tuần 4

- **Owner / effort:** Trang, 4h Core; thuộc khối Gold + ML identity/contract/security trong [kế hoạch tuần 4](../WEEK_4_PARALLEL_PLAN.md). Chuyển từ ThanhTris để giữ chuỗi Gold → snapshot pin cùng owner; review SQL vẫn đề xuất ThanhTris, chưa ghi approval.
- **Làm trước:** Trino SQL, expected output và query harness theo CON-03 khi làm GLD-01/03; hiện Backlog vì chưa bắt đầu. Khi làm phần chuẩn bị được phép, ghi In Progress và dependency GLD-03 còn chờ.
- **Làm gì / có gì:** Natural/ROI serving views, uniqueness/completeness/provenance/source-era/schema queries, snapshot consistency và verification report gắn Gold run/snapshot.
- **Dùng để làm gì:** Kiểm chứng Gold độc lập với Spark; chỉ passed snapshot được Published và được MLD-01 pin. Aggregate dashboard GLD-02 không phải gate của task này.
- **Handoff:** Nhận table identity/snapshot/commit metadata từ GLD-03; trả verify status/reasons và publication evidence. Verify đúng snapshot vừa commit, không query latest mơ hồ rồi gắn một ID khác.
- **Nghiệm thu:** GLD-03 Done và Trino đọc snapshot thật; blocker lỗi chặn Published. Hạ tầng `make smoke-query` không thay cho Gold verification, mock SQL không đủ evidence hoàn thành.

Đây là integration gate. Có thể chuẩn bị test plan, fixture và harness trước, nhưng chỉ được chuyển sang `Done` sau khi toàn bộ hard dependency cung cấp output thật và báo cáo đối soát đạt.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới từ `main`: `feat/gld-04-tao-trino-views-va-verification`.
3. Triển khai đúng phạm vi; dùng fixture nhỏ, xác định được và không phụ thuộc mạng cho unit test.
4. Chạy test/check phù hợp, đối chiếu acceptance criteria và cập nhật tài liệu nếu contract hoặc hành vi thay đổi.
5. Cập nhật `status`, `assignee`, `reviewer` và Evidence ngay trong file này khi mở PR hoặc hoàn tất review.

## Ranh giới

- Không tự mở rộng sang deliverable của task khác.
- Không đổi contract upstream trong PR implementation mà không cập nhật task contract liên quan và có review.
- Không commit secret, credential, payload nhạy cảm, data dump lớn hoặc artifact build không cần thiết.
- Không đánh dấu `Done` nếu chưa có evidence kiểm tra được.

## Theo dõi

- **Trạng thái:** In Progress
- **Assignee:** Trang
- **Reviewer:** unassigned; chưa có review độc lập
- **Evidence / PR:** 5 fixture tests passed (staging/gld-04-tests.log); changed-file secret scan and git diff --check passed. SQL/mock harness only; GLD-03 needs live Iceberg commit/readback and SLV-09 needs ready-for-Gold input before live Trino verification. Reviewer unassigned. [PR #52](https://github.com/HoaiTam/japan-earthquake-etl/pull/52).
- **Kỹ năng phù hợp:** Trino, SQL, data quality

## Checklist bàn giao

- [ ] Deliverable đã có trong repository hoặc môi trường demo.
- [ ] Acceptance criteria đã được kiểm tra.
- [ ] Test tự động đạt hoặc có evidence thủ công có thể lặp lại.
- [ ] Tài liệu/contract đã cập nhật nếu schema, flow, cấu hình hoặc hành vi thay đổi.
- [ ] Không chứa secret, dữ liệu nhạy cảm hoặc file build không cần thiết.
- [ ] P0/P1 có reviewer khác assignee xác nhận.

## Tài liệu liên quan

- [Phân công tuần 4](../WEEK_4_PARALLEL_PLAN.md)
- [Silver/Gold logical model](../../specs/SILVER_GOLD_DATA_MODEL.md)
- [Iceberg REST Catalog và Trino](../../specs/ICEBERG_TRINO.md)
- [Kế hoạch 8 tuần](../README.md)
- [Các khối công việc](../WORK_BLOCKS.md)
- [Baseline MVP, KPI và Definition of Done](../../specs/MVP_SCOPE_KPI_AND_DOD.md)
