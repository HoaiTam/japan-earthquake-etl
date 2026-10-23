---
task_id: "MLI-03"
status: "Done"
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

- [x] Checksum/schema/grain/unknown ID/NaN lỗi bị reject trước commit.
- [x] Membership/summary counts đối soát và mainshock/role nhất quán.
- [x] Rerun cùng `experiment_run_id` idempotent; reuse với checksum khác bị reject.
- [x] Failure commit không tạo trạng thái `CANDIDATE` hoặc snapshot partial được serving qua publication gate.
- [x] Không thêm cluster field thử nghiệm vào `gold.event_current`.

Iceberg không có transaction chung cho ba bảng: partial raw snapshots có thể
tồn tại để recovery, nhưng consumer chỉ được serving qua CANDIDATE registry
và exact pins. Không được đọc raw result current rồi coi là publication.

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
- **Theo dõi:** Hoàn thiện runtime trên branch `feat/mli-03-runtime-acceptance` từ main `b92bf2b`; giữ assignee ThanhTris và reviewer `unassigned`, không tự ghi nhận review chưa có.

## Điều chỉnh Jupyter-first — 2026-10-14

Thêm acceptance case reject DAT-02 DEVELOPMENT_PILOT/pilot_result_manifest và source dataset chưa EXPORTED trước commit. Pilot local/Colab PASSED không được CANDIDATE; không nhận đổi tên pilot marker thành _SUCCESS để bypass official schema/lineage gates. Không thêm dependency DAT-02/EXP-02 cho phát triển importer bằng fixtures MLI-01.

- [DAT-02](./DAT-02.md)
- [Workflow Jupyter → Colab](../../specs/JUPYTER_PILOT_WORKFLOW.md)

## Theo dõi

- **Trạng thái:** Done
- **Assignee:** ThanhTris
- **Reviewer:** unassigned — chưa ghi nhận independent approval
- **Evidence / PR:** [Runtime acceptance 19/10/2026](../../evidence/MLI-03_RUNTIME_ACCEPTANCE.md), [exact receipts](../../evidence/MLI-03-runtime.json); [offline evidence lịch sử](../../evidence/MLI-03-offline.md). PR #79 đã merge; phần hoàn thiện runtime ở branch `feat/mli-03-runtime-acceptance`, chưa mở PR mới.
- **Kỹ năng phù hợp:** Airflow, Spark validation, Iceberg transactions

## Checklist bàn giao

- [x] Importer, trusted dataset resolver, result writer, manual DAG graph và offline regression test đã có trong repository.
- [x] Checksum/schema/grain/lineage/count, publication gate và same-run recovery đã được kiểm tra bằng fixture MLI-01/local Iceberg.
- [x] Contract/docs/Make/Compose preflight đã cập nhật và không chứa secret/data dump.
- [x] Acceptance runtime trên MinIO/Catalog/Trino với exact snapshots, production runner và Airflow DAG.test đạt.
- [ ] Reviewer độc lập được khuyến nghị cho P0.

## Tài liệu liên quan

- [Roadmap HDBSCAN](../HDBSCAN_WORKSTREAM.md)
- [ML logical data model](../../specs/ML_DATA_MODEL.md)
- [MLI-01](./MLI-01.md)
- [GLD-03](./GLD-03.md)
- [MLI-03 result import runbook](../../runbooks/MLI-03_RESULT_IMPORT.md)

## Evidence hiện có

- Java 17 `clean verify`: 407 tests, 0 failure/error, 1 optional notebook cross-process probe skipped trong build context. Host chạy bổ sung 21/21 contract/import tests, không skip; không thay Java 17 verification.
- Full Airflow: 336/336; Docker lifecycle/Make: 53/53; CI contract: 38/38. Docs links/status và `git diff --check` đạt.
- `smoke-mli-import-acceptance`: PASSED trên namespace QA mới, có EXPORTED synthetic registry và candidate/mainshock snapshots thật; reject 5 loại bundle lỗi + non-EXPORTED/pilot + đổi tên marker; verify first/rerun/conflict/partial recovery/Trino blocker/Gold boundary.
- Membership/summary/experiment: 2/1/1 rows, exact result snapshots không đổi qua rerun/journal mới và production Python → shell → Java → Trino.
- `smoke-mli-import-dag`: fresh DagRun `manual__2026-10-19T16:50:52.703206+00:00`, 9/9 tasks success, completion COMPLETE, lease release đúng DagRun owner; không clear/mark-success hoặc giả scheduled run.
- Sửa smoke dùng đúng preflighted service/JAR; request CLI không làm đổi shared init config hash. Retry vẫn qua Trino exact count/lineage; metadata giữ import operation ID thay vì lấy experiment ID.
- Tại mốc nghiệm thu MLI-03, audit source/history còn 7 pattern flags và 3 coverage items thuộc SEC-01; không coi MLI-03 là security approval. Cập nhật 23/10: [SEC-01 — Security review](./SEC-01.md) đã có [acceptance local riêng](../../evidence/SEC-01_LOCAL_ACCEPTANCE_20261023.md), không chứng nhận Colab/Drive chưa triển khai. Reviewer độc lập vẫn khuyến nghị nhưng không chặn Done theo AGENTS.md.

## Giới hạn downstream

- [MLD-05 — Validate feature snapshot/export](./MLD-05.md), [MLI-02 — Build dataset DAG](./MLI-02.md): còn official export/coverage acceptance, không dùng DAT-02 pilot thay EXPORTED.
- [EXP-04 — Scientific evaluation](./EXP-04.md): scientific review và APPROVED riêng; fixture runtime chỉ chứng minh importer.
- [MLI-04 — Trino/static report](./MLI-04.md), [MLQ-01 — ML E2E](./MLQ-01.md): serving/report và toàn flow Gold→ML còn riêng. [QA-01 — Daily E2E](./QA-01.md) vẫn cần scheduled Published; DAG.test không thay cron evidence.
