---
task_id: "EXP-04"
status: "Backlog"
week: 7
block: "H - Experiment"
workstream: "Scientific evaluation"
scope: "Core"
priority: "P0"
effort_hours: 7
assignee: "unassigned"
reviewer: "unassigned"
dependencies: ["EXP-02", "EXP-03"]
---

# EXP-04 - Đánh giá quality, physical consistency và stability

## Mục đích

Đánh giá model bằng nhiều nhóm bằng chứng phù hợp với clustering không nhãn,
thay vì dùng accuracy/F1 hoặc chọn theo một metric duy nhất.

## Phạm vi công việc

- Clustering quality: DBCV, noise ratio, cluster size/probability, failure windows, runtime/peak memory.
- Physical consistency: counts/duration/spatial extent/largest aftershock và Modified Omori fit/error.
- Sensitivity: `Mc`, global/adaptive scaling, min cluster parameters, window changes, perturbation và bootstrap.
- Stability: Jaccard/ARI và các thành phần score được lưu riêng, không che bằng composite tùy ý.

## Deliverable

- Evaluation runner và metric schema/output.
- Reproduction comparison table cho Window/DBSCAN/HDBSCAN global/adaptive.
- Stability/physical consistency report với failure reason và uncertainty/limitation.

## Tiêu chí hoàn thành

- [ ] Không dùng accuracy/F1 như ground-truth metric.
- [ ] DBCV/noise, ít nhất một Omori metric và Jaccard stability được tính từ output thật.
- [ ] Metric null/failure có reason code; không bỏ window fail khỏi mẫu số im lặng.
- [ ] Báo cáo giữ metric thành phần, runtime và sample size.
- [ ] Không chọn model chỉ theo một metric hoặc số liệu minh họa.

## Hard dependency

- [EXP-02](./EXP-02.md)
- [EXP-03](./EXP-03.md)

## Cách làm song song

Clustering metrics, Omori analysis và stability harness có thể chia ba người
làm song song trên cùng membership fixture rồi hợp nhất bằng metric schema.

## Ranh giới

- Omori dùng để đánh giá consistency, không dùng tạo cluster.
- Không tạo score tổng hợp không công bố thành phần/trọng số.
- Không biến statistical candidate thành nhãn nhân quả.

## Theo dõi

- **Trạng thái:** Backlog
- **Assignee:** Chưa ghi lại
- **Reviewer:** Chưa ghi lại
- **Evidence / PR:** Chưa có
- **Kỹ năng phù hợp:** Clustering evaluation, seismology statistics, experiment design

## Checklist bàn giao

- [ ] Deliverable và test/evidence tồn tại trong repository.
- [ ] Acceptance criteria đã được kiểm tra.
- [ ] Contract/docs đã cập nhật và không chứa secret/data dump.
- [ ] Reviewer độc lập được khuyến nghị cho P0.

## Tài liệu liên quan

- [Roadmap HDBSCAN](../HDBSCAN_WORKSTREAM.md)
- [EXP-03](./EXP-03.md)

