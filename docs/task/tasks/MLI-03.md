---
task_id: "MLI-03"
status: "In Progress"
week: 5
block: "I - ML Integration"
workstream: "Result import and Iceberg publish"
scope: "Core"
priority: "P0"
effort_hours: 7
assignee: "ThanhTris"
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

## Phân công tuần 5

- **Owner / effort:** ThanhTris, 7h Core; [kế hoạch và handoff](../WEEK_5_PARALLEL_PLAN.md).
- **Làm gì / có gì:** Importer/validator/writer + physical ML mapping, same-run idempotency, partial commit/rejected states và snapshot-pinned readback.
- **Dùng để làm gì / output:** Namespace ml.* được quality-gate trước CANDIDATE, không thêm label vào Gold và không tự APPROVED.
- **Bắt đầu ngay / gate:** Dùng valid/invalid fixtures MLI-01 ngay; candidate IDs/dataset EXPORTED phải tồn tại ở QA namespace pin rõ. Fixture real-storage acceptance không là scientific approval, không chờ EXP-02 để code.
- **Theo dõi:** Đã bắt đầu trên branch `feat/mli-03-validate-import-iceberg`; giữ reviewer `unassigned` cho đến khi có review độc lập.

## Điều chỉnh Jupyter-first — 2026-10-14

Thêm acceptance case reject DAT-02 DEVELOPMENT_PILOT/pilot_result_manifest và source dataset chưa EXPORTED trước commit. Pilot local/Colab PASSED không được CANDIDATE; không nhận đổi tên pilot marker thành _SUCCESS để bypass official schema/lineage gates. Không thêm dependency DAT-02/EXP-02 cho phát triển importer bằng fixtures MLI-01.

- [DAT-02](./DAT-02.md)
- [Workflow Jupyter → Colab](../../specs/JUPYTER_PILOT_WORKFLOW.md)

## Theo dõi

- **Trạng thái:** In Progress
- **Assignee:** ThanhTris
- **Reviewer:** Chưa ghi lại
- **Evidence / PR:** [Offline evidence](../../evidence/MLI-03-offline.md); chưa tạo PR
- **Kỹ năng phù hợp:** Airflow, Spark validation, Iceberg transactions

## Checklist bàn giao

- [x] Importer, reader, writer, DAG và offline regression test đã có trong repository.
- [x] Checksum/schema/grain/lineage/count và same-run reuse đã được kiểm tra bằng fixture MLI-01.
- [x] Contract/docs/Make target đã cập nhật và không chứa secret/data dump.
- [ ] Acceptance runtime trên MinIO/Catalog/Trino với exact snapshots chưa chạy.
- [ ] Reviewer độc lập được khuyến nghị cho P0.

## Tài liệu liên quan

- [Roadmap HDBSCAN](../HDBSCAN_WORKSTREAM.md)
- [ML logical data model](../../specs/ML_DATA_MODEL.md)
- [MLI-01](./MLI-01.md)
- [GLD-03](./GLD-03.md)
- [MLI-03 result import runbook](../../runbooks/MLI-03_RESULT_IMPORT.md)

## Evidence hiện có

- Java: `ResultBundleContractTest` và `MlResultImportJobTest` đạt 9/9.
- Python: `test_mli_03_import_results.py` đạt 3/3 trên Windows Python 3.14.
- Compile: Maven Spark module với Java 17 đạt `BUILD SUCCESS`; `git diff --check` đạt.
- Chưa có evidence MinIO/Catalog/Trino; vì vậy task vẫn `In Progress` và chưa tạo PR.
