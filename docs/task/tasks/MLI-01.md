---
task_id: "MLI-01"
status: "Backlog"
week: 6
block: "I - ML Integration"
workstream: "Result bundle contract"
scope: "Core"
priority: "P0"
effort_hours: 5
assignee: "unassigned"
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

- [ ] `_SUCCESS.json` đủ dataset/run/algorithm/version/count/status/checksum.
- [ ] Membership grain `(run, mainshock, canonical_event)` và summary grain `(run, mainshock)` rõ.
- [ ] Probability, noise/cluster convention, role và event IDs có validation rule.
- [ ] `experiment_run_id` không được reuse cho artifact khác.
- [ ] `CANDIDATE` không tự chuyển `APPROVED` chỉ vì import thành công.

## Hard dependency

- [CON-03](./CON-03.md)

## Cách làm song song

Contract/fixture không cần model thật, vì vậy `MLI-03` và `EXP-01` có thể dùng
ngay sau khi schema được review.

## Ranh giới

- Không implement algorithm hoặc Airflow DAG trong task này.
- Không cho Colab ghi trực tiếp bảng `ml.*` hoặc Gold.
- Không lưu model pickle như bằng chứng tái lập duy nhất.

## Theo dõi

- **Trạng thái:** Backlog
- **Assignee:** Chưa ghi lại
- **Reviewer:** Chưa ghi lại
- **Evidence / PR:** Chưa có
- **Kỹ năng phù hợp:** Data contracts, Parquet schema, validation

## Checklist bàn giao

- [ ] Deliverable và fixtures/test tồn tại trong repository.
- [ ] Acceptance criteria đã được kiểm tra.
- [ ] Contract/docs đã cập nhật và không chứa secret/data dump.
- [ ] Reviewer độc lập được khuyến nghị cho P0.

## Tài liệu liên quan

- [Roadmap HDBSCAN](../HDBSCAN_WORKSTREAM.md)
- [ML logical data model](../../specs/ML_DATA_MODEL.md)
- [CON-03](./CON-03.md)
