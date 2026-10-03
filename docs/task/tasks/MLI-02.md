---
task_id: "MLI-02"
status: "Backlog"
week: 6
block: "I - ML Integration"
workstream: "Dataset build orchestration"
scope: "Core"
priority: "P0"
effort_hours: 5
assignee: "unassigned"
reviewer: "unassigned"
dependencies: ["ORC-01", "MLD-05"]
---

# MLI-02 - Tạo Airflow DAG build ML dataset

## Mục đích

Điều phối Gold snapshot đến export bundle bằng run context rõ ràng mà không gắn
daily ETL với bước Colab thủ công.

## Phạm vi công việc

Tạo DAG/task groups:

```text
wait_gold_published → resolve_gold_snapshot → audit_gold_input → resolve_mc
→ select_mainshocks → build_candidate_windows → build_spacetime_features
→ validate_feature_snapshot → export_parquet → mark_dataset_exported
```

Truyền `dataset_id`, snapshot, period, config version, run ID, counts và artifact URI; hỗ trợ reproduction/extension parameter có validation.

## Deliverable

- Airflow DAG build dataset, runner boundary và static/unit tests.
- Retry/idempotency/publish gate cùng run summary.
- Operator runbook cho local staging/Drive handoff thủ công.

## Tiêu chí hoàn thành

- [ ] DAG không chạy nếu Gold chưa Published hoặc dataset config invalid.
- [ ] Retry dùng cùng logical dataset identity và không trộn export attempts.
- [ ] Failure dừng trước `DATASET_EXPORTED`; downstream Colab không được đánh dấu ready.
- [ ] DAG không tự trigger Colab miễn phí như một job service giả định.

## Hard dependency

- [ORC-01](./ORC-01.md)
- [MLD-05](./MLD-05.md)

## Cách làm song song

DAG structure/runner tests có thể dùng mock commands và fixture summaries trước
khi các Spark jobs được tích hợp thật.

## Ranh giới

- Không chứa thuật toán clustering hoặc import result.
- Không lưu payload/result lớn trong XCom.
- Không upload credential trong DAG config/log.

## Theo dõi

- **Trạng thái:** Backlog
- **Assignee:** Chưa ghi lại
- **Reviewer:** Chưa ghi lại
- **Evidence / PR:** Chưa có
- **Kỹ năng phù hợp:** Airflow, orchestration, idempotency

## Checklist bàn giao

- [ ] Deliverable và test/evidence tồn tại trong repository.
- [ ] Acceptance criteria đã được kiểm tra.
- [ ] Contract/docs đã cập nhật và không chứa secret/data dump.
- [ ] Reviewer độc lập được khuyến nghị cho P0.

## Tài liệu liên quan

- [Roadmap HDBSCAN](../HDBSCAN_WORKSTREAM.md)
- [MLD-05](./MLD-05.md)
- [ORC-01](./ORC-01.md)

