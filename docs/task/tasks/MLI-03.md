---
task_id: "MLI-03"
status: "Backlog"
week: 7
block: "I - ML Integration"
workstream: "Result import and Iceberg publish"
scope: "Core"
priority: "P0"
effort_hours: 7
assignee: "unassigned"
reviewer: "unassigned"
dependencies: ["MLI-01", "GLD-03"]
---

# MLI-03 - Validate/import kết quả và commit bảng ML Iceberg

## Mục đích

Đưa artifact external về lakehouse qua quality gate có thể audit; bundle lỗi
không được ghi partial table hoặc sửa Gold core.

## Phạm vi công việc

- Tạo Airflow import DAG: receive run → download/stage → verify checksum → validate dataset/config/schema/grain/lineage → commit → register artifacts.
- Spark validate ID tồn tại trong candidate snapshot, probability/range, noise/cluster, role/time và summary reconciliation.
- Commit idempotent `ml.sequence_membership`, `ml.sequence_summary` và
  `ml.experiment_run` metadata bằng Iceberg snapshot.
- Ghi rejected reason/evidence; chỉ publish `CANDIDATE` sau verify.

## Deliverable

- Import DAG, bundle validator implementation và Spark/Iceberg writers.
- ML table DDL/logical-to-physical mapping, partition strategy và tests.
- Artifact registry dưới prefix `ml-artifacts` hoặc config tương đương có quyền giới hạn.

## Tiêu chí hoàn thành

- [ ] Checksum/schema/grain/unknown ID/NaN lỗi bị reject trước commit.
- [ ] Membership/summary counts đối soát và mainshock/role nhất quán.
- [ ] Rerun cùng `experiment_run_id` idempotent; reuse với checksum khác bị reject.
- [ ] Failure commit không tạo trạng thái `CANDIDATE` hoặc snapshot partial visible.
- [ ] Không thêm cluster field thử nghiệm vào `gold.event_current`.

## Hard dependency

- [MLI-01](./MLI-01.md)
- [GLD-03](./GLD-03.md)

## Cách làm song song

Validator/DAG/writer có thể phát triển bằng valid/invalid fixtures của `MLI-01`
trước khi Colab sinh output thật.

## Ranh giới

- Không tự approve experiment theo scientific criteria.
- Không đặt Drive/MinIO credential trong bundle hoặc log.
- Không xóa warehouse/bucket để recovery.

## Theo dõi

- **Trạng thái:** Backlog
- **Assignee:** Chưa ghi lại
- **Reviewer:** Chưa ghi lại
- **Evidence / PR:** Chưa có
- **Kỹ năng phù hợp:** Airflow, Spark validation, Iceberg transactions

## Checklist bàn giao

- [ ] Deliverable và test/evidence tồn tại trong repository.
- [ ] Acceptance criteria đã được kiểm tra.
- [ ] Contract/docs đã cập nhật và không chứa secret/data dump.
- [ ] Reviewer độc lập được khuyến nghị cho P0.

## Tài liệu liên quan

- [Roadmap HDBSCAN](../HDBSCAN_WORKSTREAM.md)
- [ML logical data model](../../specs/ML_DATA_MODEL.md)
- [MLI-01](./MLI-01.md)
- [GLD-03](./GLD-03.md)
