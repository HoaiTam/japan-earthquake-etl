---
task_id: "COL-02"
status: "Backlog"
week: 7
block: "H - Experiment"
workstream: "Bounded baseline parameter sweep"
scope: "Core"
priority: "P1"
effort_hours: 4
assignee: "rosy179"
reviewer: "unassigned"
dependencies: ["DAT-02"]
---

# COL-02 - Sweep tham số baseline giới hạn trên pilot

## Mục đích / phân loại

**New**, khối 2 tuần 7. Kiểm tra ảnh hưởng cấu hình và tính tái lập của
baseline, không tuyên bố tìm được tham số tốt nhất cho dữ liệu nghiên cứu.

## Setup / đầu vào

[Setup Colab](../../runbooks/WEEK_7_COLAB_SETUP.md), cùng pilot/feature checksum
và local gate như [EXP-02](./EXP-02.md). Không chờ notebook EDA; dùng shared
loader/module/config hiện có. [Kế hoạch tuần](../WEEK_7_COLAB_PLAN.md).

## Phạm vi / cách làm

1. Chạy ba default baselines của EXP-02 trước, lưu run IDs/pins làm mốc.
2. Khóa trước danh sách tối đa **8 cấu hình tổng**: DBSCAN tối đa 4 cấu hình
   với `eps`/`min_samples`, HDBSCAN_GLOBAL tối đa 4 cấu hình với
   `min_cluster_size`/`min_samples`. Window default là control, không sweep.
3. Lưu config JSON mới/version/SHA; dùng shared per-mainshock fit, cùng vector
   4-D, cùng candidate population. Không đổi scaling/window/input để cải thiện điểm.
4. Mỗi cấu hình một output/run ID mới. Lưu runtime/memory nếu đo được, noise,
   số cụm, productive/noise windows, failure và nguyên nhân; không overwrite default.
5. Chạy lại ít nhất một cấu hình bằng output mới để kiểm invariants, có tolerance
   và label-permutation policy công bố trước. Config mới phải thử local trước
   cloud, ghi exact pins và đối chiếu invariants; nếu đổi thuật toán/module,
   rerun fresh-kernel smoke cho scope mới. Không giữ report cũ cho code khác.

## Deliverable / nghiệm thu

- [ ] Notebook/module sweep (đề xuất `notebooks/col_02_parameter_sweep.ipynb`),
      matrix config có version và kết quả máy đọc được.
- [ ] Không quá 8 cấu hình, cùng pilot/code/scaling pins; đủ failure windows.
- [ ] Rerun không đổi input hoặc ghi đè result; comparison dựa grain/membership.
- [ ] PARTIAL/development-only và lý do không dùng pilot để scientific selection.
- [ ] Colab fresh-session evidence, config hashes và parity/rerun report.

## Dependency / song song / ranh giới

Hard input [DAT-02](./DAT-02.md) đã Done. EXP-02 default/parity gate cần đạt
trước khi nghiệm thu sweep cloud; fixtures/config matrix có thể chuẩn bị ngay.
Không đợi full DAT-03 nhưng cũng không dùng pilot cho official EXP-05 extension.
Không thay feature math, không import `ml.*`, không fit toàn catalog.

## Theo dõi

- **Trạng thái:** Backlog
- **Assignee:** rosy179
- **Reviewer:** unassigned
- **Evidence / PR:** Chưa triển khai; chưa có sweep hoặc cloud acceptance.
