---
task_id: "SLV-01"
status: "Done"
week: 3
block: "D - Silver đa nguồn"
workstream: "Input staging"
scope: "Core"
priority: "P0"
effort_hours: 3
assignee: "codex"
reviewer: "unassigned"
dependencies: ["CON-02"]
---

# SLV-01 - Resolve đúng Bronze input cho Spark

## Mục đích

Dùng làm giao diện chung để parser hai nguồn không cần biết cách Airflow tải dữ liệu.

## Phạm vi công việc

Đọc manifest theo run context, stage đúng object USGS/JMA, truyền source_system/catalog_release và không dùng wildcard rộng.

## Thành phần cần có

- **Đầu vào và contract:** [CON-02](./CON-02.md)
- **Phần triển khai:** Đọc manifest theo run context, stage đúng object USGS/JMA, truyền source_system/catalog_release và không dùng wildcard rộng.
- **Kết quả bàn giao:** Input resolver và staging manifest cho Spark.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- Input resolver và staging manifest cho Spark.

## Tiêu chí hoàn thành

- [x] Spark chỉ đọc object đã verify thuộc run.
- [x] retry tái sử dụng Bronze hợp lệ.
- [x] input có thể truy vết.

## Hard dependency

- [CON-02](./CON-02.md)

## Cách triển khai và phối hợp

Có thể chuẩn bị interface, fixture, mock, test plan và tài liệu trước khi toàn bộ upstream chạy thật. Chỉ được chuyển sang `Done` khi hard dependency đã đạt và acceptance criteria được kiểm tra trên output phù hợp.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới từ `main`: `feat/slv-01-resolve-ung-bronze-input-cho`.
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
- **Assignee:** codex
- **Reviewer:** Chưa ghi lại
- **Evidence / PR:** Implementation trên branch `feat/slv-01-bronze-input-resolver`; resolver tests 2/2 và toàn bộ Spark Maven tests 44/44 đạt với JDK 17/Maven 3.9.16; contract checks Bronze/data-model/repository/week-3 đạt. Reviewer vẫn `unassigned`.
- **Kỹ năng phù hợp:** Airflow, MinIO, staging

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
