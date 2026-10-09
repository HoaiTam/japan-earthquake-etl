---
task_id: "SLV-06"
status: "In Progress"
week: 4
block: "D - Silver đa nguồn"
workstream: "Source dedup"
scope: "Core"
priority: "P0"
effort_hours: 5
assignee: "ThanhTris"
reviewer: "unassigned"
dependencies: ["CON-03", "CON-04"]
---

# SLV-06 - Deduplicate và xử lý revision trong từng nguồn

## Mục đích

Dùng để làm sạch duplicate kỹ thuật trước bước đối chiếu USGS với JMA.

## Phạm vi công việc

USGS giữ updated mới nhất theo id; JMA xử lý cùng source key/catalog release và bản archive mới; chốt tie-break xác định.

## Thành phần cần có

- **Đầu vào và contract:** [CON-03](./CON-03.md), [CON-04](./CON-04.md)
- **Phần triển khai:** USGS giữ updated mới nhất theo id; JMA xử lý cùng source key/catalog release và bản archive mới; chốt tie-break xác định.
- **Kết quả bàn giao:** Source-local dedup transformation và late-revision fixtures.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- Source-local dedup transformation và late-revision fixtures.

## Tiêu chí hoàn thành

- [ ] Rerun cùng input cho cùng kết quả.
- [ ] revision mới thay đúng bản cũ.
- [ ] không dedup mơ hồ giữa hai nguồn.

## Hard dependency

- [CON-03](./CON-03.md)
- [CON-04](./CON-04.md)

## Cách triển khai và phối hợp

### Phân công tuần 4

- **Owner / effort:** ThanhTris, 5h Core; xem [kế hoạch tuần 4](../WEEK_4_PARALLEL_PLAN.md).
- **Bắt đầu ngay:** CON-03/CON-04 đã Done; dựng observations theo shared model để test mà không chờ ingest/live parser.
- **Làm gì / có gì:** Source-local window/tie-break theo [CON-03](../../specs/SILVER_GOLD_DATA_MODEL.md), USGS updated và JMA normalized release order; giữ history/current flags cùng metrics exact duplicate/superseded/current.
- **Dùng để làm gì:** Có đúng một current revision cho mỗi source key, không mất raw/history và không dedup USGS với JMA tại bước này.
- **Handoff:** Current observations cho SLV-07; history/current và reconciliation cho SLV-09. Giữ schema/key SLV-02/SLV-04, không tự đổi identity algorithm.
- **Nghiệm thu:** Rerun/permutation/tie-break, late revision và JMA release ordering; revision cũ còn audit được. Không coi superseded history là parse/quality reject.

Có thể chuẩn bị interface, fixture, mock, test plan và tài liệu trước khi toàn bộ upstream chạy thật. Chỉ được chuyển sang `Done` khi hard dependency đã đạt và acceptance criteria được kiểm tra trên output phù hợp.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới từ `main`: `feat/slv-06-deduplicate-va-xu-ly-revision`.
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
- **Assignee:** ThanhTris
- **Reviewer:** unassigned
- **Evidence / PR:** Đang triển khai trên branch `feat/slv-06-deduplicate-va-xu-ly-revision`
- **Kỹ năng phù hợp:** Spark window, idempotency

## Checklist bàn giao

- [ ] Deliverable đã có trong repository hoặc môi trường demo.
- [ ] Acceptance criteria đã được kiểm tra.
- [ ] Test tự động đạt hoặc có evidence thủ công có thể lặp lại.
- [ ] Tài liệu/contract đã cập nhật nếu schema, flow, cấu hình hoặc hành vi thay đổi.
- [ ] Không chứa secret, dữ liệu nhạy cảm hoặc file build không cần thiết.
- [ ] P0/P1 có reviewer khác assignee xác nhận.

## Tài liệu liên quan

- [Phân công tuần 4](../WEEK_4_PARALLEL_PLAN.md)
- [Silver/Gold model: dedup và revision](../../specs/SILVER_GOLD_DATA_MODEL.md)
- [Kế hoạch 8 tuần](../README.md)
- [Các khối công việc](../WORK_BLOCKS.md)
- [Baseline MVP, KPI và Definition of Done](../../specs/MVP_SCOPE_KPI_AND_DOD.md)
