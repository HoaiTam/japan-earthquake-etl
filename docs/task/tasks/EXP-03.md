---
task_id: "EXP-03"
status: "Backlog"
week: 7
block: "H - Experiment"
workstream: "Adaptive HDBSCAN"
scope: "Core"
priority: "P0"
effort_hours: 6
assignee: "unassigned"
reviewer: "unassigned"
dependencies: ["EXP-02"]
---

# EXP-03 - Xây HDBSCAN adaptive và giải quyết multi-sequence

## Mục đích

So sánh rule thích nghi với global rule mà không chọn tham số thủ công cho từng
sequence, đồng thời tránh double count event xuất hiện trong nhiều window.

## Phạm vi công việc

- Xây adaptive rule từ candidate count, background rate, mainshock magnitude, window size và/hoặc `Mc` group.
- Khóa `model_config_version` trên reproduction period trước khi chạy extension.
- Giữ raw memberships cho mọi mainshock window; tạo resolution view/status theo probability → normalized distance → ambiguous.
- Gắn nested mainshock/parent evidence khi mainshock nằm trong sequence khác.

## Deliverable

- Versioned adaptive parameter resolver và tests.
- HDBSCAN adaptive run artifacts trên reproduction dataset.
- Multi-sequence/nested-mainshock resolution rules và before/after counts.

## Tiêu chí hoàn thành

- [ ] Adaptive rule deterministic và áp dụng đồng nhất, không có per-sequence manual override.
- [ ] Raw membership grain vẫn giữ mọi cặp mainshock/candidate.
- [ ] Aggregate unique aftershock không double count mà không ghi resolution status.
- [ ] Tie không giải được phải là `AMBIGUOUS_MULTI_SEQUENCE`, không chọn tùy ý.
- [ ] Rule/config được khóa trước `EXP-05`.

## Hard dependency

- [EXP-02](./EXP-02.md)

## Cách làm song song

Resolution rule có thể phát triển/test bằng fixture multi-window trong lúc
adaptive parameter analysis đang chạy, miễn cùng output contract.

## Ranh giới

- Không tune adaptive rule bằng extension period.
- Không xóa raw membership chỉ vì event được resolve sang sequence khác.
- Không tuyên bố adaptive tốt hơn trước khi `EXP-04` đánh giá.

## Theo dõi

- **Trạng thái:** Backlog
- **Assignee:** Chưa ghi lại
- **Reviewer:** Chưa ghi lại
- **Evidence / PR:** Chưa có
- **Kỹ năng phù hợp:** HDBSCAN, parameter policies, entity resolution

## Checklist bàn giao

- [ ] Deliverable và test/evidence tồn tại trong repository.
- [ ] Acceptance criteria đã được kiểm tra.
- [ ] Contract/docs đã cập nhật và không chứa secret/data dump.
- [ ] Reviewer độc lập được khuyến nghị cho P0.

## Tài liệu liên quan

- [Roadmap HDBSCAN](../HDBSCAN_WORKSTREAM.md)
- [EXP-02](./EXP-02.md)

