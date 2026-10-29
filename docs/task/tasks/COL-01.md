---
task_id: "COL-01"
status: "Backlog"
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
   đề xuất notebook mới `notebooks/col_01_pilot_eda.ipynb` (chưa triển khai).

## Tiêu chí nghiệm thu

- [ ] Colab session mới → Run all đạt với input thật, có runtime metadata.
- [ ] 279 candidate rows/2 mainshocks; unique event count được tính độc lập.
- [ ] Báo rõ PARTIAL/DEVELOPMENT_ONLY/research_ready=false, không im lặng bỏ rows.
- [ ] Input không đổi bytes; output ngoài kit, không chứa credentials.
- [ ] Bảng/plots sinh từ dữ liệu, không nhập tay số minh họa làm kết quả thật.
- [ ] Notebook source sạch output và có cách chạy lại theo runbook.

## Dependency / song song / ranh giới

[DAT-02](./DAT-02.md) đã Done theo scope pilot. Không chờ full DAT-03 hoặc
EXP-01 cloud setup mới để dùng loader hiện có. Không fit/tune trong task này,
không tự gán trạng thái EXPORTED/APPROVED hoặc tạo official marker.

## Theo dõi

- **Trạng thái:** Backlog
- **Assignee:** HoaiTam
- **Reviewer:** unassigned
- **Evidence / PR:** Chưa triển khai; giao việc không là bằng chứng nghiệm thu.
