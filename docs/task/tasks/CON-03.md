---
task_id: "CON-03"
status: "Done"
week: 2
block: "A - Hợp đồng dữ liệu"
workstream: "Silver and Gold contract"
scope: "Core"
priority: "P0"
effort_hours: 6
assignee: "unassigned"
reviewer: "unassigned"
dependencies: ["CON-01"]
---

# CON-03 - Thiết kế mô hình dữ liệu Silver và Gold

## Mục đích

Dùng thay cho bước thiết kế database truyền thống: Silver chuẩn hóa observation, Gold là bảng Iceberg và Trino là lớp SQL phục vụ BI.

## Phạm vi công việc

Chốt observation theo nguồn, lineage, UTC/JST, tọa độ, magnitude/depth, quality flags, source record key, canonical event, dimension/band và KPI Gold.

## Thành phần cần có

- **Đầu vào và contract:** [CON-01](./CON-01.md)
- **Phần triển khai:** Chốt observation theo nguồn, lineage, UTC/JST, tọa độ, magnitude/depth, quality flags, source record key, canonical event, dimension/band và KPI Gold.
- **Kết quả bàn giao:** [Logical data model Silver/Gold](../../specs/SILVER_GOLD_DATA_MODEL.md), field mapping USGS/JMA, data dictionary và query examples; không khóa DDL database vật lý.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- [Logical data model Silver/Gold](../../specs/SILVER_GOLD_DATA_MODEL.md), field mapping USGS/JMA, data dictionary và query examples; không khóa DDL database vật lý.

## Tiêu chí hoàn thành

- [x] Parser, quality rule, canonicalization, Gold và Power BI dùng cùng tên trường, kiểu dữ liệu và null policy.

## Hard dependency

- [CON-01](./CON-01.md)

## Cách triển khai và phối hợp

Có thể chuẩn bị interface, fixture, mock, test plan và tài liệu trước khi toàn bộ upstream chạy thật. Chỉ được chuyển sang `Done` khi hard dependency đã đạt và acceptance criteria được kiểm tra trên output phù hợp.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới từ `main`: `docs/con-03-thiet-ke-mo-hinh-du`.
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
- **Evidence / PR:** [Data model contract check](../../../scripts/check-data-model-contract.sh); `check-bronze-contract.sh`, `check-source-coverage.sh`, `check-repository-layout.sh`, kiểm tra 540 local links trong 97 file Markdown và `git diff --check` đều đạt.
- **Kỹ năng phù hợp:** Data modeling, SQL, Iceberg

## Checklist bàn giao

- [x] Deliverable đã có trong repository hoặc môi trường demo.
- [x] Acceptance criteria đã được kiểm tra.
- [x] Test tự động đạt hoặc có evidence thủ công có thể lặp lại.
- [x] Tài liệu/contract đã cập nhật nếu schema, flow, cấu hình hoặc hành vi thay đổi.
- [x] Không chứa secret, dữ liệu nhạy cảm hoặc file build không cần thiết.
- [ ] Reviewer độc lập đã xác nhận (khuyến nghị, không chặn `Done`).

## Tài liệu liên quan

- [Logical data model Silver/Gold](../../specs/SILVER_GOLD_DATA_MODEL.md)
- [Source coverage contract](../../specs/SOURCE_COVERAGE.md)
- [Bronze storage contract](../../specs/BRONZE_STORAGE_CONTRACT.md)
- [Kế hoạch 6 tuần](../README.md)
- [Các khối công việc](../WORK_BLOCKS.md)
- [Baseline MVP, KPI và Definition of Done](../../specs/MVP_SCOPE_KPI_AND_DOD.md)
