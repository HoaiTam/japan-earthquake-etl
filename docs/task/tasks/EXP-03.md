---
task_id: "EXP-03"
status: "In Progress"
week: 5
block: "H - Experiment"
workstream: "Adaptive HDBSCAN"
scope: "Core"
priority: "P0"
effort_hours: 6
assignee: "rosy179"
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

- [x] Adaptive rule deterministic và áp dụng đồng nhất, không có per-sequence manual override.
- [x] Raw membership grain vẫn giữ mọi cặp mainshock/candidate.
- [x] Aggregate unique aftershock không double count mà không ghi resolution status.
- [x] Tie không giải được phải là `AMBIGUOUS_MULTI_SEQUENCE`, không chọn tùy ý.
- [x] Rule/config được khóa trước `EXP-05`.

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

- **Trạng thái:** In Progress
- **Assignee:** rosy179
- **Reviewer:** unassigned
- **Evidence / PR:** [EXP-03 evidence](../../evidence/EXP-03.md), [PR #86](https://github.com/HoaiTam/japan-earthquake-etl/pull/86) đã merge implementation. Đồng bộ tuần 5 ngày 22/10/2026 theo phân công gần nhất. Evidence notebook hiện là isolated_exec/fallback, không thay fresh-kernel acceptance trên input hợp lệ; dependency EXP-02 còn In Progress.
- **Kỹ năng phù hợp:** HDBSCAN, parameter policies, entity resolution

## Checklist bàn giao

- [x] Deliverable và test/evidence tồn tại trong repository.
- [ ] Acceptance toàn task: unit/contract rules đã kiểm; còn reproduction run artifacts và nghiệm thu notebook fresh kernel trên input hợp lệ sau EXP-02.
- [x] Contract/docs đã cập nhật và không chứa secret/data dump.
- [ ] Reviewer độc lập xác nhận (khuyến nghị cho P0); reviewer unassigned, chưa có approval.

## Phân công tuần 5 — đồng bộ 22/10/2026

- Owner rosy179, 6h Core. Lịch ban đầu tuần 7; bảng phân công gần nhất kéo
  task lên tuần 5, không thêm task/effort hoặc giảm hard dependency.
- [EXP-02 — Chạy Window, DBSCAN và HDBSCAN global](./EXP-02.md) còn Colab
  parity/official acceptance. Giữ task này In Progress; fallback report không
  được dùng để mở Colab gate hoặc tuyên bố adaptive tốt hơn.

## Tài liệu liên quan

- [Roadmap HDBSCAN](../HDBSCAN_WORKSTREAM.md)
- [EXP-02](./EXP-02.md)
