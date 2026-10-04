---
task_id: "EXP-02"
status: "Backlog"
week: 6
block: "H - Experiment"
workstream: "Baseline algorithms"
scope: "Core"
priority: "P0"
effort_hours: 7
assignee: "unassigned"
reviewer: "unassigned"
dependencies: ["EXP-01"]
---

# EXP-02 - Chạy Window, DBSCAN và HDBSCAN global

## Mục đích

Tạo ba baseline trên cùng reproduction dataset để đánh giá liệu clustering có
giảm background tốt hơn window rule và HDBSCAN có lợi hơn DBSCAN hay không.

## Phạm vi công việc

- Window baseline: POST event trong counting window được đánh dấu candidate.
- Fit DBSCAN và HDBSCAN global riêng cho từng mainshock window trên feature 4-D.
- Lấy cluster chứa chính mainshock; nếu mainshock label `-1`, ghi no productive sequence.
- Gắn PRE/MAINSHOCK/POST, cluster/noise/probability và runtime/memory theo window.

## Deliverable

- Implementations/config cho `WINDOW`, `DBSCAN`, `HDBSCAN_GLOBAL`.
- Membership và sequence summary bundle theo contract.
- Unit/smoke cases: mainshock cluster hợp lệ, mainshock noise, nhiều cluster và empty POST.

## Tiêu chí hoàn thành

- [ ] Ba thuật toán đọc cùng `dataset_id` và không mutate feature input.
- [ ] HDBSCAN/DBSCAN chạy theo từng mainshock, không fit toàn catalog.
- [ ] Chỉ cluster chứa mainshock được coi là sequence; không chọn cluster gần nhất khi mainshock noise.
- [ ] Membership probability null đúng với thuật toán không cung cấp.
- [ ] Thuật ngữ output là candidate, không khẳng định causal aftershock.

## Hard dependency

- [EXP-01](./EXP-01.md)

## Cách làm song song

Ba implementation có thể giao riêng cho các thành viên nếu cùng dùng harness,
schema output và feature fixture do `EXP-01` khóa.

## Ranh giới

- Không tune bằng mắt riêng từng sequence.
- Không dùng accuracy/F1 khi chưa có ground truth.
- Không đưa magnitude vào vector baseline 4-D trong task này.

## Theo dõi

- **Trạng thái:** Backlog
- **Assignee:** Chưa ghi lại
- **Reviewer:** Chưa ghi lại
- **Evidence / PR:** Chưa có
- **Kỹ năng phù hợp:** DBSCAN, HDBSCAN, unsupervised learning

## Checklist bàn giao

- [ ] Deliverable và test/evidence tồn tại trong repository.
- [ ] Acceptance criteria đã được kiểm tra.
- [ ] Contract/docs đã cập nhật và không chứa secret/data dump.
- [ ] Reviewer độc lập được khuyến nghị cho P0.

## Tài liệu liên quan

- [Roadmap HDBSCAN](../HDBSCAN_WORKSTREAM.md)
- [EXP-01](./EXP-01.md)

