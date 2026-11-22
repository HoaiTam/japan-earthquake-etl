---
task_id: "MLQ-01"
status: "In Progress"
week: 8
block: "I - ML Integration"
workstream: "ML end-to-end QA"
scope: "Core"
priority: "P0"
effort_hours: 6
assignee: "HoaiTam"
reviewer: "unassigned"
dependencies: ["QA-01", "MLI-02", "MLI-04", "EXP-05"]
---

# MLQ-01 - Chạy E2E Gold snapshot đến ML report

## Giao việc hiện hành — 11/11/2026

- **Owner:** HoaiTam; **status:** In Progress.
- **Làm phần gì / dùng để làm gì:** Nghiệm thu ML E2E.
- **Bắt đầu độc lập:** Viết test plan, negative fixtures và reconciliation queries ngay.
- **Đầu ra và điều kiện Done:** Sau hard dependencies: truy vết Gold → dataset → experiment → import/report; reject bundle lỗi và rerun không duplicate.
- **Lịch:** chuẩn bị 11–15/11, nghiệm thu tuần9 16–22/11; xem
  [kế hoạch và setup](../WEEK_9_REPLAN_20261111.md). Giữ hard dependencies,
  scope/effort lịch sử; các mục tuần cũ dưới đây là lịch sử.

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

- **Trạng thái:** In Progress
- **Assignee:** HoaiTam
- **Reviewer:** unassigned
- **Evidence / PR:** [Test plan và truy vấn đối soát](../../runbooks/MLQ-01_E2E_RECONCILIATION.md), [fixture SQL](../../../tests/test_mlq01_reconciliation.py). Chuẩn bị trên nhánh `test/mlq-01-prepare-e2e-reconciliation`. Runtime chờ [MLI-04 — Tạo Trino views và static report cho experiment](./MLI-04.md) cung cấp receipt và evidence exact snapshot.
- **Kiểm tra chuẩn bị:** 3 test SQL fixture đạt, Trino local trả `lineage_check=PASS` với window tổng hợp; 32 Java/Spark/Iceberg và 41 Airflow tests nhóm import đạt. Receipt E2E official vẫn chờ.
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
