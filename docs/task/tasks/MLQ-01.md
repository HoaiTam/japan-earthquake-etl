---
task_id: "MLQ-01"
status: "Backlog"
week: 8
block: "I - ML Integration"
workstream: "ML end-to-end QA"
scope: "Core"
priority: "P0"
effort_hours: 6
assignee: "unassigned"
reviewer: "unassigned"
dependencies: ["QA-01", "MLI-02", "MLI-04", "EXP-05"]
---

# MLQ-01 - Chạy E2E Gold snapshot đến ML report

## Mục đích

Làm integration gate cuối cho phương án HDBSCAN, chứng minh một record đi từ
Gold snapshot qua candidate/feature/experiment/import đến report có lineage.

## Phạm vi công việc

- Chạy reproduction/extension nhỏ nhưng đại diện qua DAG build dataset.
- Chạy đủ Window/DBSCAN/HDBSCAN global/adaptive bằng bundle thật.
- Import bundle, verify Iceberg/Trino/static report và lifecycle transition.
- Fault cases: checksum, duplicate run, unknown event, mainshock noise, partial bundle và rerun.

## Deliverable

- ML E2E report với dataset IDs, experiment run IDs, snapshot IDs, checksums và counts.
- Reconciliation table Gold → candidates → memberships → summaries/report.
- Known limitations/failure cases và release gate evidence.

## Tiêu chí hoàn thành

- [ ] Một canonical event được truy vết xuyên Gold snapshot, candidate, membership và report.
- [ ] Counts/grain/checksum/snapshot đối soát được; chênh lệch có reason.
- [ ] Bundle lỗi không tạo `CANDIDATE`; rerun hợp lệ không duplicate.
- [ ] Reproduction rule được giữ nguyên khi chạy extension.
- [ ] Báo cáo không tuyên bố prediction/causal aftershock.

## Hard dependency

- [QA-01](./QA-01.md)
- [MLI-02](./MLI-02.md)
- [MLI-04](./MLI-04.md)
- [EXP-05](./EXP-05.md)

## Cách làm song song

Test plan, fixtures và reconciliation query có thể chuẩn bị từ contract; chỉ
trạng thái `Done` mới buộc output thật của mọi hard dependency.

## Ranh giới

- Không dùng full 40 năm nếu run đại diện đã đủ evidence và resource guard.
- Không sửa data/model trong lúc QA để làm đẹp kết quả.
- Không thay review scientific bằng kiểm tra pipeline kỹ thuật.

## Theo dõi

- **Trạng thái:** Backlog
- **Assignee:** Chưa ghi lại
- **Reviewer:** Chưa ghi lại
- **Evidence / PR:** Chưa có
- **Kỹ năng phù hợp:** Integration testing, reconciliation, ML reproducibility

## Checklist bàn giao

- [ ] Deliverable và test/evidence tồn tại trong repository.
- [ ] Acceptance criteria đã được kiểm tra.
- [ ] Contract/docs đã cập nhật và không chứa secret/data dump.
- [ ] Reviewer độc lập được khuyến nghị cho P0.

## Tài liệu liên quan

- [Roadmap HDBSCAN](../HDBSCAN_WORKSTREAM.md)
- [MLI-04](./MLI-04.md)
- [EXP-05](./EXP-05.md)
