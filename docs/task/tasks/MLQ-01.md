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
- **Evidence / PR:** [Test plan và truy vấn đối soát](../../runbooks/MLQ-01_E2E_RECONCILIATION.md), [fixture SQL](../../../tests/test_mlq01_reconciliation.py). Chuẩn bị trên nhánh `test/mlq-01-prepare-e2e-reconciliation` ([PR #150](https://github.com/HoaiTam/japan-earthquake-etl/pull/150)). [MLI-03](./MLI-03.md) (`PR #149`, `633ac5a`) và [MLI-04](./MLI-04.md) (`PR #144`, `abd134d`, 8/8 shard REPRODUCTION `VERIFIED`) đã merge trên `main`; cấu trúc receipt hai cấp, ví dụ JSON tối thiểu và ràng buộc namespace QA của `MLQ-01` đã khóa khớp với [DEMO-01](./DEMO-01.md) (`PR #143`).
- **Kiểm tra chuẩn bị:** 9 test SQL fixture đạt, gồm tự tham chiếu MAINSHOCK duy nhất trong toàn window (chặn thêm event MAINSHOCK thứ hai), từng pin publication/lineage, không chứa dấu chấm phẩy `;` cho Trino HTTP API `/v1/statement` và khớp trường ánh xạ `trace` cho receipt `DEMO-01`. Fixture chạy trong CI `docs-contracts`. Live trace trên `REPRODUCTION` (`WINDOW`, `DBSCAN`, `HDBSCAN_GLOBAL`, `HDBSCAN_ADAPTIVE`) qua Trino HTTP API trả `lineage_check=PASS` (`20261118_022319_00104_v3i3r`..`20261118_022347_00107_v3i3r`); 32 Java/Spark/Iceberg và 41 Airflow tests nhóm import đã đạt ở lần kiểm thử trước. Phần runtime E2E còn lại để đóng `MLQ-01` là import/trace ca `EXTENSION` `WINDOW`, reject fault bundle không tạo `CANDIDATE` trong namespace QA (`ml_qa_*`), và receipt E2E tổng hợp (evidence rerun `REPRODUCTION` của `#149` có thể tái dùng trực tiếp bằng đúng SHA/pin, đủ 12 `query_id` phân biệt; ví dụ JSON trong runbook dùng placeholder minh họa schema). [Review #150 (MAINSHOCK)](https://github.com/HoaiTam/japan-earthquake-etl/pull/150#issuecomment-7102539216), [Review #150 (Trino HTTP API `;`)](https://github.com/HoaiTam/japan-earthquake-etl/pull/150#issuecomment-7122036067), [Review #150 (đồng bộ main và receipt)](https://github.com/HoaiTam/japan-earthquake-etl/pull/150#issuecomment-7131541152), [Review #150 (placeholder query ID)](https://github.com/HoaiTam/japan-earthquake-etl/pull/150#issuecomment-7136800576), [Review #143 (cấu trúc receipt MLQ-01)](https://github.com/HoaiTam/japan-earthquake-etl/pull/143#issuecomment-7127604628), [Review #143 (ràng buộc namespace)](https://github.com/HoaiTam/japan-earthquake-etl/pull/143#issuecomment-7131542575), [Review #143 (12 query IDs rerun)](https://github.com/HoaiTam/japan-earthquake-etl/pull/143#issuecomment-7136800289).
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
