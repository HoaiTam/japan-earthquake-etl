---
task_id: "SLV-06"
status: "Done"
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

- [x] Rerun cùng input cho cùng kết quả.
- [x] revision mới thay đúng bản cũ.
- [x] không dedup mơ hồ giữa hai nguồn.

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

- **Trạng thái:** Done
- **Assignee:** ThanhTris
- **Reviewer:** unassigned
- **Evidence / PR:** Hoàn tất trên branch `feat/slv-06-deduplicate-va-xu-ly-revision`. Đã triển khai đầy đủ `SourceDedupTransformer`, `SourceDedupMetrics`, `SourceDedupResult` và `JmaReleaseComparator` dưới package `ie212.earthquake.spark.silver`. Bổ sung method `withCurrentSourceRevision` cho `SilverObservation`. Phân hoạch source-local độc lập theo `(source_system, source_record_key)` bảo đảm 0 deduplication mơ hồ xuyên nguồn giữa USGS và JMA. Áp dụng chuẩn tie-break xác định theo CON-03 (USGS: `source_updated_at_utc DESC`, `processed_at_utc DESC`, `raw_record_hash ASC`; JMA: `catalog_release_at_utc DESC`, normalized release order DESC qua `JmaReleaseComparator`, `processed_at_utc DESC`, `raw_record_hash ASC`). Phân loại chính xác N-1 bản ghi loser thành exact duplicate (`DUPLICATE_SOURCE_RECORD`) hoặc superseded revision (`SOURCE_REVISION_SUPERSEDED`), bảo đảm bất biến số học `input = current + duplicate + superseded`, và giữ toàn vẹn lịch sử observations phục vụ audit ở SLV-09. Bổ sung các fixture late revision tại `spark/src/test/resources/fixtures/late_revision/`. Toàn bộ 201/201 tests trong module Spark đạt 100% (bao gồm 13 tests mới trong `SourceDedupTransformerTest` và 6 tests trong `JmaReleaseComparatorTest`, đối soát đầy đủ ma trận contract `FX-USGS-05`, `FX-USGS-06`, `FX-JMA-04`, `FX-JMA-05`). Kịch bản kiểm tra `check-spark.sh` (SPK-01 Spark static check), `check-week-3-plan.sh`, `check-repository-layout.sh`, `check-mvp-baseline.sh`, `check-task-status.sh` và `git diff --check` đều đạt. Reviewer giữ `unassigned`.
- **Kỹ năng phù hợp:** Spark window, idempotency

## Checklist bàn giao

- [x] Deliverable đã có trong repository hoặc môi trường demo.
- [x] Acceptance criteria đã được kiểm tra.
- [x] Test tự động đạt hoặc có evidence thủ công có thể lặp lại.
- [x] Tài liệu/contract đã cập nhật nếu schema, flow, cấu hình hoặc hành vi thay đổi.
- [x] Không chứa secret, dữ liệu nhạy cảm hoặc file build không cần thiết.
- [ ] P0/P1 có reviewer khác assignee xác nhận.

## Tài liệu liên quan

- [Phân công tuần 4](../WEEK_4_PARALLEL_PLAN.md)
- [Silver/Gold model: dedup và revision](../../specs/SILVER_GOLD_DATA_MODEL.md)
- [Kế hoạch 8 tuần](../README.md)
- [Các khối công việc](../WORK_BLOCKS.md)
- [Baseline MVP, KPI và Definition of Done](../../specs/MVP_SCOPE_KPI_AND_DOD.md)
