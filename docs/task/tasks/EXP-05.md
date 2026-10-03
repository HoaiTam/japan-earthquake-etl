---
task_id: "EXP-05"
status: "Backlog"
week: 7
block: "H - Experiment"
workstream: "Out-of-period evaluation"
scope: "Core"
priority: "P0"
effort_hours: 5
assignee: "unassigned"
reviewer: "unassigned"
dependencies: ["EXP-04"]
---

# EXP-05 - Chạy out-of-period extension 2018-2023

## Mục đích

Kiểm tra rule chọn trên reproduction period có giữ hành vi ổn định ở giai đoạn
sau hay chỉ phù hợp dữ liệu đã dùng để calibration.

## Phạm vi công việc

- Freeze window/scaling/parameter rule và `model_config_version` từ reproduction.
- Chạy Window, DBSCAN, HDBSCAN global/adaptive trên extension `2018-10-01..2023-12-31`.
- So sánh distribution metric, productive/failure rate, stability, runtime và data drift indicators.
- Ghi decision table cùng limitation; không chỉnh bằng mắt theo event extension.

## Deliverable

- Extension experiment bundle và reproduction-vs-extension comparison report.
- Evidence model config checksum/version giống rule đã khóa.
- Final method-selection recommendation có tiêu chí và failure cases.

## Tiêu chí hoàn thành

- [ ] Extension dùng dataset ID riêng nhưng cùng feature/model contract đã khóa.
- [ ] Không có parameter override dựa trên kết quả extension.
- [ ] So sánh đủ Window/DBSCAN/HDBSCAN global/adaptive và báo sample/failure windows.
- [ ] Kết luận phân biệt rõ generalization check với supervised train/test accuracy.

## Hard dependency

- [EXP-04](./EXP-04.md)

## Cách làm song song

Dataset extension có thể được `MLD` build song song từ sớm; task này chỉ chạy
sau khi reproduction rule/config đã freeze.

## Ranh giới

- Không dùng extension để quay lại tune reproduction mà không tạo experiment version mới.
- Không tuyên bố dự đoán tương lai từ out-of-period clustering.
- Không loại failure windows khỏi report.

## Theo dõi

- **Trạng thái:** Backlog
- **Assignee:** Chưa ghi lại
- **Reviewer:** Chưa ghi lại
- **Evidence / PR:** Chưa có
- **Kỹ năng phù hợp:** Temporal validation, experiment analysis, HDBSCAN

## Checklist bàn giao

- [ ] Deliverable và test/evidence tồn tại trong repository.
- [ ] Acceptance criteria đã được kiểm tra.
- [ ] Contract/docs đã cập nhật và không chứa secret/data dump.
- [ ] Reviewer độc lập được khuyến nghị cho P0.

## Tài liệu liên quan

- [Roadmap HDBSCAN](../HDBSCAN_WORKSTREAM.md)
- [EXP-04](./EXP-04.md)
