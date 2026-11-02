# COL-03 — Pilot Baseline Metrics and Plots Evidence

- **Ngày kiểm chứng:** 2026-11-02
- **Tuần thực hiện:** Tuần 7 (02–08/11/2026), Khối 2
- **Assignee:** `rosy179`
- **Reviewer:** `unassigned`
- **Branch:** `feat/col-03-baseline-metrics-plots`
- **Tài liệu dẫn chiếu:** [COL-03 Task](../task/tasks/COL-03.md), [Kế hoạch tuần 7](../task/WEEK_7_COLAB_PLAN.md), [COL-02 Evidence](./COL-02_PARAMETER_SWEEP_20261102.md), [EXP-04 Task](../task/tasks/EXP-04.md)

---

## 1. Mục tiêu và bối cảnh

Task [COL-03 - Metrics và trực quan hóa baseline pilot](../task/tasks/COL-03.md) là công việc trọng tâm cuối cùng của Khối 2 (do `rosy179` phụ trách) trong tuần 7. Sau khi hoàn tất kiểm chứng tương đương trên Google Colab ([EXP-02](./EXP-02_COLAB_PARITY_20261102.md)) và khảo sát tham số giới hạn ([COL-02](./COL-02_PARAMETER_SWEEP_20261102.md)), COL-03 thực hiện:
1. Trích xuất bảng số liệu định lượng tự động từ các tệp Parquet và JSON thực tế, bao gồm số lượng ứng viên, số dư chấn, tỷ lệ nhiễu, trạng thái cửa sổ và chỉ số chất lượng phân cụm mật độ (DBCV).
2. Xây dựng các biểu đồ không gian tương đối ($km$) và thời gian - độ sâu ($hours, km$), phân biệt rõ ràng `MAINSHOCK`, `PRE`, `POST` và `noise`.
3. Khóa toàn bộ các tệp kết xuất thành gói bàn giao (Handoff Package) có mã băm SHA-256 xác định để chuyển giao cho ThanhTris phục vụ kiểm thử tính ổn định và định luật Omori tại [EXP-04](../task/tasks/EXP-04.md).

Toàn bộ quy trình tuân thủ nghiêm ngặt nguyên tắc: **100% số liệu và hình ảnh truy ngược trực tiếp từ artifact vật lý, không nhập số tay, không dùng runtime khác phần cứng để khẳng định mô hình nhanh hơn, và giữ nguyên phạm vi `DEVELOPMENT_ONLY` / `PARTIAL`**.

---

## 2. Chính sách số liệu định lượng và chỉ số DBCV

- **Tính khả thi của DBCV:** Chỉ số kiểm định phân cụm dựa trên mật độ (DBCV) chỉ được tính toán khi thuật toán có bản chất phân cụm mật độ (`DBSCAN`, `HDBSCAN_GLOBAL`) và tồn tại cụm chuỗi sau chấn hợp lệ ($\ge 1$ cụm, số điểm cụm $\ge 3$).
- **Chính sách giá trị NULL minh bạch:**
  - Thuật toán `WINDOW`: Thuần túy là quy tắc đếm thời gian, không có bản chất mật độ không gian -> `dbcv_score = null` kèm lý do giải thích `WINDOW_BASELINE_NOT_DENSITY_BASED`. Tuyệt đối không so sánh DBCV của Window như một giá trị có thật.
  - Các cửa sổ thất bại (mainshock rơi vào nhiễu hoặc không có chuỗi sau chấn): `dbcv_score = null` kèm lý do `NO_PRODUCTIVE_CLUSTERS`. Tuyệt đối không thay thế bằng 0 hoặc giá trị NaN giả tạo.
- **Quy chuẩn tọa độ trực quan hóa:**
  - Không gian tương đối được biểu diễn bằng khoảng cách $dx$ ($km$) và $dy$ ($km$) so với tâm chấn chính $(0, 0)$. Tuyệt đối không sử dụng tọa độ kinh vĩ độ (degrees) làm khoảng cách Euclidean không có đơn vị.
  - Trục thời gian biểu diễn theo số giờ trôi qua tương đối (`delta_time_hours`) so với thời điểm xảy ra chấn chính ($t = 0$). Trục độ sâu thể hiện độ sâu chấn tiêu (`candidate_depth_km`) với hướng nghịch đảo độ sâu.

---

## 3. Bảng số liệu định lượng trích xuất (Baseline Metrics Table)

Được trích xuất từ 9 cấu hình baseline/sweep trên bộ pilot 279 dòng / 2 cửa sổ chấn chính (xuất ra `baseline_metrics_table.csv` và `baseline_metrics_table.json`):

| Thuật toán | Phiên bản cấu hình | Mã chấn chính | Ứng viên | Thành viên chuỗi | Dư chấn | Điểm nhiễu | Chỉ số DBCV | Trạng thái cửa sổ | Lý do nếu không thành công |
|---|---|---|---:|---:|---:|---:|:---:|:---:|---|
| **DBSCAN** | `dbscan-sweep-c1-v1` | `evt_d47fcaaf451c..` | 1 | 0 | 0 | 1 | `null` | `NO_PRODUCTIVE_SEQUENCE` | `MAINSHOCK_CLASSIFIED_AS_NOISE` |
| **DBSCAN** | `dbscan-sweep-c1-v1` | `evt_f0300848e0b7..` | 278 | 251 | 240 | 8 | **0.6370** | `SUCCESS` | — |
| **DBSCAN** | `dbscan-sweep-c2-v1` | `evt_d47fcaaf451c..` | 1 | 0 | 0 | 1 | `null` | `NO_PRODUCTIVE_SEQUENCE` | `MAINSHOCK_CLASSIFIED_AS_NOISE` |
| **DBSCAN** | `dbscan-sweep-c2-v1` | `evt_f0300848e0b7..` | 278 | 251 | 240 | 17 | **0.7753** | `SUCCESS` | — |
| **DBSCAN** | `dbscan-sweep-c3-v1` | `evt_d47fcaaf451c..` | 1 | 0 | 0 | 1 | `null` | `NO_PRODUCTIVE_SEQUENCE` | `MAINSHOCK_CLASSIFIED_AS_NOISE` |
| **DBSCAN** | `dbscan-sweep-c3-v1` | `evt_f0300848e0b7..` | 278 | 231 | 229 | 25 | **0.6697** | `SUCCESS` | — |
| **DBSCAN** | `dbscan-sweep-c4-v1` | `evt_d47fcaaf451c..` | 1 | 0 | 0 | 1 | `null` | `NO_PRODUCTIVE_SEQUENCE` | `MAINSHOCK_CLASSIFIED_AS_NOISE` |
| **DBSCAN** | `dbscan-sweep-c4-v1` | `evt_f0300848e0b7..` | 278 | 270 | 252 | 3 | **0.6738** | `SUCCESS` | — |
| **HDBSCAN** | `hdbscan-sweep-c1-v1` | `evt_d47fcaaf451c..` | 1 | 0 | 0 | 1 | `null` | `NO_PRODUCTIVE_SEQUENCE` | `MAINSHOCK_CLASSIFIED_AS_NOISE` |
| **HDBSCAN** | `hdbscan-sweep-c1-v1` | `evt_f0300848e0b7..` | 278 | 0 | 0 | 107 | 0.3742 | `NO_PRODUCTIVE_SEQUENCE` | `MAINSHOCK_CLASSIFIED_AS_NOISE` |
| **HDBSCAN** | `hdbscan-sweep-c2-v1` | `evt_d47fcaaf451c..` | 1 | 0 | 0 | 1 | `null` | `NO_PRODUCTIVE_SEQUENCE` | `MAINSHOCK_CLASSIFIED_AS_NOISE` |
| **HDBSCAN** | `hdbscan-sweep-c2-v1` | `evt_f0300848e0b7..` | 278 | 216 | 215 | 18 | **0.4650** | `SUCCESS` | — |
| **HDBSCAN** | `hdbscan-sweep-c3-v1` | `evt_d47fcaaf451c..` | 1 | 0 | 0 | 1 | `null` | `NO_PRODUCTIVE_SEQUENCE` | `MAINSHOCK_CLASSIFIED_AS_NOISE` |
| **HDBSCAN** | `hdbscan-sweep-c3-v1` | `evt_f0300848e0b7..` | 278 | 217 | 216 | 18 | **0.5239** | `SUCCESS` | — |
| **HDBSCAN** | `hdbscan-sweep-c4-v1` | `evt_d47fcaaf451c..` | 1 | 0 | 0 | 1 | `null` | `NO_PRODUCTIVE_SEQUENCE` | `MAINSHOCK_CLASSIFIED_AS_NOISE` |
| **HDBSCAN** | `hdbscan-sweep-c4-v1` | `evt_f0300848e0b7..` | 278 | 216 | 215 | 25 | **0.5407** | `SUCCESS` | — |
| **WINDOW** | `window-baseline-v1` | `evt_d47fcaaf451c..` | 1 | 0 | 0 | 0 | `null` | `NO_PRODUCTIVE_SEQUENCE` | `EMPTY_SEQUENCE` |
| **WINDOW** | `window-baseline-v1` | `evt_f0300848e0b7..` | 278 | 257 | 255 | 0 | `null` | `SUCCESS` | *(DBCV null: WINDOW_BASELINE_NOT_DENSITY_BASED)* |

---

## 4. Các biểu đồ trực quan hóa kết xuất

Tất cả biểu đồ được sinh tự động và lưu trữ tại `staging/ml-experiments/col03-metrics-plots/`:

1. **Biểu đồ phân bố không gian tương đối (`spatial_distribution_*.png`):**
   - Thể hiện vị trí tương đối theo $dx$ (km) và $dy$ (km) xoay quanh tâm chấn chính $(0, 0)$ (đánh dấu bằng ngôi sao đỏ to).
   - Phân biệt rõ ràng các sự kiện chuỗi sau chấn (chấm tròn xanh), sự kiện tiền chấn và các điểm nhiễu nền (dấu chữ thập xám).
2. **Biểu đồ diễn tiến thời gian và độ sâu (`timeline_depth_*.png`):**
   - Thể hiện trục thời gian từ trước khi chấn chính xảy ra ($t < 0$) đến giai đoạn sau chấn ($t > 0$), kết hợp độ sâu chấn tiêu ($km$).
3. **Biểu đồ cột so sánh tổng thể (`baseline_comparison_summary.png`):**
   - Đối chiếu trực quan số lượng dư chấn được giữ lại và số lượng điểm nhiễu được loại bỏ giữa tất cả các cấu hình baseline.

---

## 5. Gói bàn giao cho ThanhTris (EXP-04 Handoff Package)

File `staging/ml-experiments/col03-metrics-plots/handoff_manifest.json` ghi nhận đầy đủ chữ ký kiểm chứng mã băm SHA-256 cho toàn bộ 11 artifacts phục vụ task [EXP-04](../task/tasks/EXP-04.md):

* `baseline_metrics_table.csv` — `e54ff8dc6513cda4214a526c939c8ac465fdc2090141669b26d414b5184c8c87`
* `baseline_metrics_table.json` — `74d33693273f2fde910dfdda4fb60b89e35d3093bf075572815aeb06cdd6baf2`
* `spatial_distribution_dbscan_c1.png` — `6712054ddb0753c424f3ec88a94ea55984a6dfe28029d266a686b6f5a05efdba`
* `timeline_depth_dbscan_c1.png` — `254a685fff390e3ad74a18d0b4d2025ec03a6cde33bba9c90d5b01f5762e74a8`
* `spatial_distribution_dbscan_c2.png` — `58f0ed50d1643710fc5ce586420693df8e3e51eb7fb8aaee1af6615e3d9b53dd`
* `timeline_depth_dbscan_c2.png` — `ea62ba4cbe1fe91382f3100717d0673f93e75ffcf7db235be341c408e6b8e007`
* `spatial_distribution_dbscan_c3.png` — `6052d402c49013c53af904e95371a4911fe6955fab783c4c854f5286df8bdb9d`
* `timeline_depth_dbscan_c3.png` — `bce93d813259749c111dcf36332d731c797bfd98f160795b8983121f48f40d5c`
* `spatial_distribution_dbscan_c4.png` — `493de997cf0f50308fe4b18b6215ba14e702a861a384f7e0bfaee6d143fb5363`
* `timeline_depth_dbscan_c4.png` — `f412948dc93180d0fdb492c45cd9cc43297b1ca289937ee1c820d483acb241f4`
* `baseline_comparison_summary.png` — `e98ac3a11d3bc904fb340824968d3a1570cc2ee7c7a635644260bc0895c6318c`

---

## 6. Danh mục tệp mã nguồn và kiểm thử

- **Mã nguồn:**
  - Module trực quan hóa: `notebooks/baseline_visualization.py`
  - Notebook: `notebooks/col_03_baseline_metrics_plots.ipynb`
  - Script Headless Runner: `scripts/inspect-col03-metrics.py`
- **Bộ kiểm thử tự động:**
  - `tests/test_baseline_visualization.py` (6/6 tests PASS).
- **Metadata Bằng chứng:**
  - JSON Metadata: [COL-03_BASELINE_METRICS_PLOTS_20261102.json](./COL-03_BASELINE_METRICS_PLOTS_20261102.json)

---

## 7. Giới hạn / Rủi ro / Bước tiếp theo

- **Bàn giao cho Khối 3:** Dữ liệu bảng số liệu và manifest được chuyển giao trực tiếp cho ThanhTris để triển khai [EXP-04 - Đánh giá quality, physical consistency và stability](../task/tasks/EXP-04.md).
- **Nghiên cứu chính thức toàn kỳ:** Toàn bộ bảng số liệu và biểu đồ ở đây thuộc phạm vi thử nghiệm pilot kỹ thuật (`DEVELOPMENT_ONLY`, `PARTIAL`); không đại diện cho toàn bộ 40 năm catalog nghiên cứu. Nghiên cứu toàn kỳ cần dataset chính thức từ [DAT-03 - Chuẩn bị catalog nghiên cứu thật](../task/tasks/DAT-03.md) và [MLD-05 - Validate feature snapshot và export Parquet bundle](../task/tasks/MLD-05.md).
- **Trạng thái task:** Đã chuyển `Done` trong [COL-03.md](../task/tasks/COL-03.md).
- **Reviewer:** Reviewer độc lập cho task P1 này hiện là `unassigned`.
