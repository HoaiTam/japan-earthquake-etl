---
task_id: "COL-03"
status: "Backlog"
week: 7
block: "H - Experiment"
workstream: "Pilot baseline metrics and plots"
scope: "Core"
priority: "P1"
effort_hours: 4
assignee: "rosy179"
reviewer: "unassigned"
dependencies: ["DAT-02"]
---

# COL-03 - Metrics và trực quan hóa baseline pilot

## Mục đích / phân loại

**New**, khối 2. Biến artifact baseline thành bảng/hình kiểm chứng được, cung
cấp input cho EXP-04 của ThanhTris; không cùng sửa evaluation module của EXP-04.

## Setup / đầu vào

Dùng [setup chung](../../runbooks/WEEK_7_COLAB_SETUP.md) và outputs EXP-02/COL-02
được pin bằng pilot ID, code/config SHA và artifact checksums. Khi outputs chưa
có, chuẩn bị template/tests bằng fixture có nhãn rõ, không đợi người khác.
[Kế hoạch tuần](../WEEK_7_COLAB_PLAN.md).

## Công việc / deliverable

1. Đọc exact memberships/summary/config/metrics bằng schema hiện có; đối soát
   grain, counts/roles và mỗi algorithm đủ 2 windows với input pilot thật.
2. Xuất bảng theo mainshock/algorithm/config: candidate count, clusters/noise,
   sequence/POST counts, status/reason, runtime; giữ failure windows trong mẫu số.
3. DBCV chỉ tính khi thỏa điều kiện metric/labels; thiếu cluster/samples hoặc
   algorithm không phù hợp thì null + reason, không thay bằng 0 hoặc NaN JSON.
4. Vẽ relative space km và time-hours/depth; nếu vẽ lon/lat phải ghi đơn vị,
   không dùng độ làm Euclidean feature. Thể hiện mainshock/PRE/POST/noise rõ.
5. Bảng số liệu sinh tự động kèm CSV/JSON/PNG hoặc SVG nhỏ, README diễn giải.
   Pin plotting dependency trong thay đổi EXP-01 hoặc requirements riêng được
   review; không giả project đã có matplotlib/pandas trong model lock hiện tại.

## Tiêu chí nghiệm thu

- [ ] Số bảng/hình truy ngược được về exact artifacts, không số nhập tay.
- [ ] Không gọi noise hoặc no-productive-sequence mặc định là implementation lỗi.
- [ ] Metric null có reason; không accuracy/F1 khi chưa có ground truth.
- [ ] Không dùng runtime khác hardware để khẳng định model nhanh hơn.
- [ ] Notebook/report chạy từ session mới và giữ DEVELOPMENT_ONLY/PARTIAL.
- [ ] Handoff cho ThanhTris có checksum/pins/schema; không cần đổi input source.

## Dependency / giới hạn

[DAT-02](./DAT-02.md) là input đã có; actual baseline artifacts từ EXP-02 cần
cho nghiệm thu thật, fixture chỉ chặn template/tests. Task này không làm Omori/
stability của [EXP-04](./EXP-04.md), không scientific approval hoặc official
Trino report của [MLI-04](./MLI-04.md).

## Theo dõi

- **Trạng thái:** Backlog
- **Assignee:** rosy179
- **Reviewer:** unassigned
- **Evidence / PR:** Chưa triển khai; chưa có metric report hoặc plots mới.
