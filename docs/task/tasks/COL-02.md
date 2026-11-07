---
task_id: "COL-02"
status: "Done"
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
Đường nghiệm thu riêng: [local handoff → Colab → readback](../../runbooks/COL-02_COLAB_ACCEPTANCE.md).

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

- [x] Notebook/module sweep (`notebooks/col_02_parameter_sweep.ipynb`, `notebooks/parameter_sweep.py`),
      matrix config có version và kết quả máy đọc được.
- [x] Không quá 8 cấu hình (chính xác 8 cấu hình: 4 DBSCAN, 4 HDBSCAN + 1 control WINDOW), cùng pilot/code/scaling pins; đủ failure windows.
- [x] Rerun không đổi input hoặc ghi đè result; comparison dựa grain/membership đạt RERUN_INVARIANTS_PASSED; mặc định fail-closed khi output đã có kết quả.
- [x] PARTIAL/development-only và lý do không dùng pilot để scientific selection.
- [x] Colab fresh-session evidence, config hashes và parity/rerun report;
      independent local readback đủ 75 files/10 comparisons PASSED
      ([cloud acceptance](../../evidence/COL-02_COLAB_20261104.md)).

## Dependency / song song / ranh giới

Hard input [DAT-02](./DAT-02.md) đã Done. EXP-02 default/parity gate cần đạt
trước khi nghiệm thu sweep cloud; fixtures/config matrix có thể chuẩn bị ngay.
Không đợi full DAT-03 nhưng cũng không dùng pilot cho official EXP-05 extension.
Không thay feature math, không import `ml.*`, không fit toàn catalog.

## Theo dõi

- **Trạng thái:** Done
- **Assignee:** rosy179
- **Reviewer:** unassigned
- **Evidence / PR:** [COL-02 Parameter Sweep Evidence](../../evidence/COL-02_PARAMETER_SWEEP_20261102.md)
- **04/11/2026:** tiếp tục cloud acceptance trên branch riêng từ main; giữ
  assignee rosy179, HoaiTam vận hành nghiệm thu. Actual Colab CPU fresh session
  đã đạt 9 configs + DBSCAN_C1 rerun; không nới tolerance để vượt lỗi dispatch.
  [Evidence phiên 04/11](../../evidence/COL-02_COLAB_20261104.md).
- **05/11/2026:** tải actual notebook/output về máy; 4 original code cells
  AST-identical, counts 1–4, không error; ZIP SHA khớp và independent readback
  đủ 75 files/10 parity PASSED. Chuyển Done cho scope pilot portability,
  không scientific approval hoặc official research acceptance. Reviewer vẫn
  unassigned; không coi operator HoaiTam là receipt của rosy179.
