---
task_id: "MLI-02"
status: "In Progress"
week: 5
block: "I - ML Integration"
workstream: "Dataset build orchestration"
scope: "Core"
priority: "P0"
effort_hours: 5
assignee: "HoaiTam"
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

- [x] DAG không chạy nếu Gold chưa Published hoặc dataset config invalid.
- [x] Retry dùng cùng logical dataset identity và không trộn export attempts.
- [x] Failure dừng trước `DATASET_EXPORTED`; downstream Colab không được đánh dấu ready.
- [x] DAG không tự trigger Colab miễn phí như một job service giả định.

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

## Phân công tuần 5

- **Owner / effort:** HoaiTam, 5h Core; [kế hoạch và handoff](../WEEK_5_PARALLEL_PLAN.md).
- **Làm gì / có gì:** DAG pin/audit/Mc/window/feature/export, validate config/lifecycle, retry identity và run summary; Colab handoff thủ công.
- **Dùng để làm gì / output:** Một build DAG riêng với daily ETL; bundle sẵn sàng chỉ sau export verify, không giả Colab là job service.
- **Bắt đầu ngay / gate:** Graph/runner mocks làm ngay; Done sau MLD-05, Gold Published và lifecycle gates thật. Chưa có feature/export thì không đánh dấu DATASET_EXPORTED.
- **Theo dõi:** Phân công không có nghĩa đã triển khai; chỉ đổi In Progress khi bắt đầu, Done khi đủ acceptance/evidence. Reviewer hiện unassigned, chưa có approval.

## Theo dõi

- **Trạng thái:** In Progress
- **Assignee:** HoaiTam
- **Reviewer:** Chưa ghi lại
- **Evidence / PR:** [Offline Acceptance](../../evidence/MLI-02_OFFLINE_ACCEPTANCE.md)
- **Kỹ năng phù hợp:** Airflow, orchestration, idempotency

## Checklist bàn giao

- [x] Deliverable và test/evidence tồn tại trong repository.
- [x] Acceptance criteria đã được kiểm tra.
- [x] Contract/docs đã cập nhật và không chứa secret/data dump.
- [ ] Reviewer độc lập được khuyến nghị cho P0.

## Tài liệu liên quan

- [Runbook Dataset Build](../../runbooks/MLI-02_DATASET_BUILD.md)
- [Evidence Offline](../../evidence/MLI-02_OFFLINE_ACCEPTANCE.md)
- [Roadmap HDBSCAN](../HDBSCAN_WORKSTREAM.md)
- [MLD-05](./MLD-05.md)
- [ORC-01](./ORC-01.md)


