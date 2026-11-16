---
task_id: "COL-01"
status: "Done"
week: 7
block: "H - Experiment"
workstream: "Colab pilot data inspection"
scope: "Core"
priority: "P1"
effort_hours: 3
assignee: "HoaiTam"
reviewer: "unassigned"
dependencies: ["DAT-02"]
---

# COL-01 - Khảo sát và giải thích pilot trên Colab

## Mục đích / phân loại

**New**, khối 1 tuần 7. Giúp nhóm hiểu grain/units/coverage trước khi diễn giải
cluster; không xây lại feature bằng Python hoặc gọi 279 rows là 279 event riêng.

## Đầu vào và setup

Đọc [kế hoạch tuần 7](../WEEK_7_COLAB_PLAN.md) và hoàn tất
[setup Colab](../../runbooks/WEEK_7_COLAB_SETUP.md). Dùng exact pilot ZIP/ID/SHA
ở runbook; chỉ cần Google account, quyền đọc release và Python runtime CPU,
không Docker/Java/.env. Inspection không fit nên không chờ EXP-02 cloud parity.

## Làm gì / gồm những gì / dùng để làm gì

1. Dùng `pilot_bundle.load_bundle` hoặc DEVELOPMENT loader đã có, không bỏ
   checksum/schema guard. Notebook nhận đường dẫn qua tham số.
2. Thống kê feature rows/mainshocks, unique candidate IDs và unique cặp
   mainshock/candidate; counts PRE/MAINSHOCK/POST theo từng window.
3. Kiểm null/nonfinite/trùng grain, self vector, units km/giờ và scaling version.
   Các validation vẫn giao shared loader; EDA giải thích kết quả, không sửa input.
4. Hiển thị actual available/required/missing coverage. Giải thích self-only
   window và selection bias; không extrapolate ra toàn Nhật Bản/toàn kỳ.
5. Lưu bảng JSON/CSV và hình nhỏ có dataset/pilot ID, checksum/code version;
   notebook `notebooks/col_01_pilot_eda.ipynb` gọi cùng CLI/module local.

## Tiêu chí nghiệm thu

- [x] Colab session mới → Run all đạt với input thật, có runtime metadata.
- [x] 279 candidate rows/2 mainshocks; unique event count được tính độc lập (local và Colab).
- [x] Báo rõ PARTIAL/DEVELOPMENT_ONLY/research_ready=false, không im lặng bỏ rows.
- [x] Input không đổi bytes; output ngoài kit, không chứa credentials (local và Colab).
- [x] Bảng/plots sinh từ dữ liệu, không nhập tay số minh họa làm kết quả thật (local và Colab).
- [x] Notebook source sạch output và có cách chạy lại theo runbook.

## Dependency / song song / ranh giới

[DAT-02](./DAT-02.md) đã Done theo scope pilot. Không chờ full DAT-03 hoặc
EXP-01 cloud setup mới để dùng loader hiện có. Không fit/tune trong task này,
không tự gán trạng thái EXPORTED/APPROVED hoặc tạo official marker.

## Theo dõi

- **Trạng thái:** Done
- **Assignee:** HoaiTam
- **Reviewer:** unassigned
- **Evidence / PR:** [Nghiệm thu local và Colab 02/11](../../evidence/COL-01_PILOT_EDA_20261102.md),
  [metadata/checksums](../../evidence/COL-01_PILOT_EDA_20261102.json),
  [runbook](../../runbooks/COL-01_PILOT_EDA.md). Fresh Colab session Run all đủ
  bốn code cells đạt; output ZIP tải về đã readback exact file-set và ba artifact
  SHA, PNG đã kiểm tra trực quan. Kết quả khớp local: 279 rows/2 mainshocks,
  không null/nonfinite/trùng grain; input không đổi bytes. 11 regressions và
  17 docs tests đạt. Đã sửa pip check cho venv without-pip và inherited
  MPLBACKEND trước import; giữ lịch sử lỗi trong evidence, không sửa input.
  Pilot vẫn PARTIAL/DEVELOPMENT_ONLY/research_ready=false; không fit model hoặc
  thay official lifecycle. Reviewer độc lập chưa được chỉ định.
