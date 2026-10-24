---
task_id: "MLI-02"
status: "Done"
week: 5
block: "I - ML Integration"
workstream: "Dataset build orchestration"
scope: "Core"
priority: "P0"
effort_hours: 5
assignee: "rosy179"
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
- Real Java/Spark step adapter đã nghiệm thu trên branch
  `feat/mli-02-real-export-orchestration` từ main `f3386ba` (đã gồm PR #96/#97),
  worktree riêng; deployment override tạm thời đã khôi phục sau nghiệm thu,
  không đổi source checkout daily. Giữ assignee rosy179 theo
  phân công hiện hành; reviewer unassigned. Nghiệm thu hệ thống dùng namespace
  ML QA riêng trên Published historical fixture có proof 225 tháng; không dùng
  operational PROVISIONAL của QA-01 hoặc nâng DAT-02 pilot thành official.

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

- **Owner / effort:** rosy179, 5h Core theo bảng phân công gần nhất; đồng bộ ngày 22/10/2026 trong [kế hoạch và handoff](../WEEK_5_PARALLEL_PLAN.md). HoaiTam là owner mốc cũ trong offline evidence; không sửa tác giả/lịch sử của evidence đó.
- **Làm gì / có gì:** DAG pin/audit/Mc/window/feature/export, validate config/lifecycle, retry identity và run summary; Colab handoff thủ công.
- **Dùng để làm gì / output:** Một build DAG riêng với daily ETL; bundle sẵn sàng chỉ sau export verify, không giả Colab là job service.
- **Gate đã chốt 24/10:** Real Airflow/Java/Spark first/rerun đều 12/12 success; exact registry/pins/checksums, negative unpublished-input và local loader đạt. Giữ scope synthetic component QA, không thay research-data/scientific gates.
- **Theo dõi:** Phân công không có nghĩa đã triển khai; chỉ đổi In Progress khi bắt đầu, Done khi đủ acceptance/evidence. Reviewer hiện unassigned, chưa có approval.

## Theo dõi

- **Trạng thái:** Done
- **Assignee:** rosy179
- **Reviewer:** unassigned; chưa có approval độc lập
- **Evidence / PR:** [Real acceptance 24/10](../../evidence/MLI-02_RUNTIME_ACCEPTANCE.md), [first](../../evidence/MLI-02_BUILD_FIRST.json), [rerun/recovery](../../evidence/MLI-02_BUILD_RERUN.json), [negative input](../../evidence/MLI-02_REJECTED_INPUT.json), [local loader](../../evidence/MLI-02_LOCAL_VERIFY.json). [Offline Acceptance](../../evidence/MLI-02_OFFLINE_ACCEPTANCE.md) và [PR #85](https://github.com/HoaiTam/japan-earthquake-etl/pull/85) giữ lịch sử, không dùng mock làm live evidence. Chưa tự commit/push/mở PR mới.
- **Kỹ năng phù hợp:** Airflow, orchestration, idempotency

## Checklist bàn giao

- [x] Deliverable và test/evidence tồn tại trong repository.
- [x] Real MLD-05 exporter, 12-task build DAG, registry, rerun/recovery, reject gate và local handoff đã nghiệm thu.
- [x] Contract/docs đã cập nhật và không chứa secret/data dump.
- [ ] Reviewer độc lập được khuyến nghị cho P0.

## Giới hạn / task tiếp theo

- QA dùng Published historical **synthetic** fixture đủ 225 tháng, không lấy
  PROVISIONAL current-month hoặc DAT-02 pilot làm input official.
- **DAT-03 — Catalog nghiên cứu thật và handoff** đang In Progress cho backfill/nghiệm thu toàn kỳ;
  MLD-05/MLI-02 component Done không thay bằng chứng dữ liệu nghiên cứu đó.
- **EXP-02** còn official fit/Colab parity; **EXP-03** còn scientific gates;
  **MLQ-01** còn full ML E2E, **MLI-04** còn report. Không mở Colab ready
  hoặc scientific approval từ export success.
- Runtime đã restore source/JAR trước phiên nghiệm thu, daily và MLI DAG
  paused. Sau merge cần chuẩn bị/deploy đúng artifact/pin một lần; không tự
  build/pull/start/init trong mỗi test.

## Tài liệu liên quan

- [Runbook Dataset Build](../../runbooks/MLI-02_DATASET_BUILD.md)
- [Evidence Offline](../../evidence/MLI-02_OFFLINE_ACCEPTANCE.md)
- [Roadmap HDBSCAN](../HDBSCAN_WORKSTREAM.md)
- [MLD-05](./MLD-05.md)
- [ORC-01](./ORC-01.md)
