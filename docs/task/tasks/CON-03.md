---
task_id: "CON-03"
status: "Done"
week: 2
block: "A - Hợp đồng dữ liệu"
workstream: "Data model contract"
scope: "Core"
priority: "P0"
effort_hours: 6
assignee: "unassigned"
reviewer: "unassigned"
dependencies: ["CON-01"]
---

# CON-03 - Thiết kế mô hình dữ liệu Silver, Gold và ML

## Mục đích

Dùng thay cho bước thiết kế database truyền thống: Silver chuẩn hóa observation, Gold giữ canonical event, còn namespace `ml` version hóa dataset và kết quả thí nghiệm để Spark, Colab, Airflow và Trino dùng cùng grain/lineage.

## Phạm vi công việc

Giữ contract Silver/Gold hiện có và bổ sung logical schema cho `ml.dataset_manifest`, `ml.mainshock_candidate_snapshot`, `ml.sequence_candidate_snapshot`, `ml.experiment_run`, `ml.sequence_membership`, `ml.sequence_summary`, lifecycle, reason code và quan hệ snapshot/version.

## Thành phần cần có

- **Đầu vào và contract:** [CON-01](./CON-01.md)
- **Phần triển khai:** Mở rộng contract đã duyệt bằng namespace `ml`, khóa grain, field, null policy, dataset/experiment ID, snapshot lineage và trạng thái publish/reject; không trộn nhãn thí nghiệm vào Gold core.
- **Kết quả bàn giao:** Logical model Silver/Gold/ML, data dictionary, lifecycle và query examples; không khóa DDL database vật lý.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- Logical data model Silver/Gold/ML, data dictionary, lifecycle và query examples; phần hiện có nằm tại [Silver/Gold logical model](../../specs/SILVER_GOLD_DATA_MODEL.md) và phải được mở rộng hoặc tách contract ML có liên kết rõ.

## Tiêu chí hoàn thành

- [x] Parser, quality rule, canonicalization và Gold dùng cùng tên trường, kiểu dữ liệu và null policy.
- [x] Các bảng `ml.*` có grain, khóa, lineage, version và null policy rõ ràng.
- [x] Kết quả Window/DBSCAN/HDBSCAN cùng tồn tại theo `experiment_run_id` và không sửa grain Gold.
- [x] Trạng thái dataset/experiment và quality gate import được mô tả đủ cho Airflow, Spark và Trino.

## Hard dependency

- [CON-01](./CON-01.md)

## Cách triển khai và phối hợp

Có thể chuẩn bị interface, fixture, mock, test plan và tài liệu trước khi toàn bộ upstream chạy thật. Chỉ được chuyển sang `Done` khi hard dependency đã đạt và acceptance criteria được kiểm tra trên output phù hợp.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới từ `main`: `docs/con-03-ml-data-model-update`.
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
- **Evidence / PR:** Hoàn tất trên branch `docs/con-03-ml-data-model-update`; chưa commit/push hoặc mở PR. Contract `CON-03` v2 giữ Silver/Gold schema v1 và bổ sung [ML logical data model](../../specs/ML_DATA_MODEL.md) v1 theo kiểu additive.
- **Evidence kiểm thử:** `./scripts/check-data-model-contract.sh` kiểm tra Silver/Gold/ML grain, key, lifecycle, enum, reason code, publish gate và trạng thái task/index; `./scripts/check-mvp-baseline.sh` kiểm tra đồng bộ baseline/roadmap; `git diff --check` kiểm tra whitespace.
- **Kỹ năng phù hợp:** Data modeling, SQL, Iceberg

## Checklist bàn giao

- [x] Deliverable cập nhật đã có trong repository hoặc môi trường demo.
- [x] Acceptance criteria mới đã được kiểm tra.
- [x] Contract check đã mở rộng cho các bảng `ml.*`.
- [x] Consumer task và tài liệu liên quan đã đồng bộ.
- [x] Không chứa secret, dữ liệu nhạy cảm hoặc file build không cần thiết.
- [ ] Reviewer độc lập đã xác nhận (khuyến nghị, không chặn `Done`).

## Tài liệu liên quan

- [Contract index và logical model Silver/Gold](../../specs/SILVER_GOLD_DATA_MODEL.md)
- [ML logical data model](../../specs/ML_DATA_MODEL.md)
- [Roadmap HDBSCAN](../HDBSCAN_WORKSTREAM.md)
- [Source coverage contract](../../specs/SOURCE_COVERAGE.md)
- [Bronze storage contract](../../specs/BRONZE_STORAGE_CONTRACT.md)
- [Kế hoạch 8 tuần](../README.md)
- [Các khối công việc](../WORK_BLOCKS.md)
- [Baseline MVP, KPI và Definition of Done](../../specs/MVP_SCOPE_KPI_AND_DOD.md)
