---
task_id: "MLI-01"
status: "Done"
week: 4
block: "I - ML Integration"
workstream: "Result bundle contract"
scope: "Core"
priority: "P0"
effort_hours: 5
assignee: "Trang"
reviewer: "unassigned"
dependencies: ["CON-03"]
---

# MLI-01 - Khóa result bundle và experiment lifecycle

## Mục đích

Tạo giao diện ổn định giữa Colab và pipeline local để đội notebook/import phát
triển song song và kết quả thiếu/giả mạo không đi vào Iceberg.

## Phạm vi công việc

- Khóa layout `memberships.parquet`, `sequence_summary.parquet`, metrics/config, requirements lock và `_SUCCESS.json`.
- Khóa schema/grain/range/null rule, algorithm enum, event role, multi-sequence status và failure reason.
- Định nghĩa checksum, count, artifact URI, `dataset_id`, `experiment_run_id` uniqueness và lifecycle.
- Tạo result fixtures success/checksum mismatch/schema mismatch/duplicate/unknown event.

## Deliverable

- Machine-readable schema/contract và bundle validator interface.
- Valid/invalid result bundle fixtures nhỏ, không phụ thuộc Drive/network.
- Lifecycle `TRAINING_EXTERNAL → RESULT_READY → IMPORT_VALIDATING → CANDIDATE/REJECTED → APPROVED`.

## Tiêu chí hoàn thành

- [x] `_SUCCESS.json` đủ dataset/run/algorithm/version/count/status/checksum.
- [x] Membership grain `(run, mainshock, canonical_event)` và summary grain `(run, mainshock)` rõ.
- [x] Probability, noise/cluster convention, role và event IDs có validation rule.
- [x] `experiment_run_id` không được reuse cho artifact khác.
- [x] `CANDIDATE` không tự chuyển `APPROVED` chỉ vì import thành công.

## Hard dependency

- [CON-03](./CON-03.md)

## Cách làm song song

### Phân công tuần 4 mở rộng

- **Owner / effort:** Trang, 5h Core; đưa từ tuần 6 lên [kế hoạch tuần 4](../WEEK_4_PARALLEL_PLAN.md). CON-03 đã Done nên task Ready, có thể làm độc lập khi chờ live Silver/Gold.
- **Làm gì / có gì:** Machine-readable layout/schema/grain/null/enum/range/checksum/count/lifecycle cho result bundle, validator interface và fixtures success/invalid/ID reuse theo ML logical model.
- **Dùng để làm gì:** Khóa Colab/import handoff sớm; các owners notebook/import có fixture mà không cần model thật.
- **Handoff / nghiệm thu:** `_SUCCESS` chỉ RESULT_READY; import chỉ CANDIDATE, approval cần review khoa học. Test checksum/schema/duplicate/unknown event/probability/noise/role/count và run reuse; không triển khai algorithm hoặc ML import DAG, không fake notebook/model/APPROVED chưa có.

Contract/fixture không cần model thật, vì vậy `MLI-03` và `EXP-01` có thể dùng
ngay sau khi schema được review.

## Ranh giới

- Không implement algorithm hoặc Airflow DAG trong task này.
- Không cho Colab ghi trực tiếp bảng `ml.*` hoặc Gold.
- Không lưu model pickle như bằng chứng tái lập duy nhất.

## Theo dõi

- **Trạng thái:** Done
- **Assignee:** Trang
- **Reviewer:** Chưa ghi lại
- **Evidence / PR:** CON-03 aligned schema/layout, validator interface, seven synthetic Parquet fixture bundles. Full Spark suite 189 tests passed; final contract retest 7 passed; changed-file secret scan and git diff --check passed. No real model/import/approval claimed; reviewer unassigned. See docs/specs/ML_RESULT_BUNDLE_CONTRACT.md.
- **Kỹ năng phù hợp:** Data contracts, Parquet schema, validation

## Checklist bàn giao

- [x] Deliverable và fixtures/test tồn tại trong repository.
- [x] Acceptance criteria đã được kiểm tra.
- [x] Contract/docs đã cập nhật và không chứa secret/data dump.
- [ ] Reviewer độc lập được khuyến nghị cho P0.

## Tài liệu liên quan

- [Kế hoạch tuần 4 mở rộng](../WEEK_4_PARALLEL_PLAN.md)
- [Roadmap HDBSCAN](../HDBSCAN_WORKSTREAM.md)
- [ML logical data model](../../specs/ML_DATA_MODEL.md)
- [CON-03](./CON-03.md)
