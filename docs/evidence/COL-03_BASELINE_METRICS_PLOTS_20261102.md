# COL-03 — Pilot Baseline Metrics and Plots Evidence

- **Ngày kiểm chứng:** 2026-11-02 (Cập nhật phản hồi review: 2026-11-11)
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

Toàn bộ quy trình tuân thủ nghiêm ngặt nguyên tắc: **100% số liệu và hình ảnh truy ngược trực tiếp từ artifact vật lý, không nhập số tay, không dùng runtime khác phần cứng để khẳng định mô hình nhanh hơn, fail-closed khi thiếu hoặc lệch feature grain, và giữ nguyên phạm vi `DEVELOPMENT_ONLY` / `PARTIAL`**.

---

## 2. Chính sách số liệu định lượng và chỉ số DBCV

- **Tính khả thi của DBCV:** Chỉ số kiểm định phân cụm dựa trên mật độ (DBCV, Moulavi et al. 2014) chỉ được tính toán khi thuật toán có bản chất phân cụm mật độ (`DBSCAN`, `HDBSCAN_GLOBAL`), cửa sổ thành công và tồn tại ít nhất 2 cụm hợp lệ để đánh giá độ tách biệt mật độ ($DSPC$).
- **Trọng số quần thể mẫu bao gồm Noise (Noise Population Parity):**
  - Theo đúng định nghĩa gốc của Moulavi et al. (2014) và cài đặt chuẩn `hdbscan.validity.validity_index`, trọng số tổng hợp của từng cụm được chuẩn hóa trên **toàn bộ kích thước cửa sổ $N$** (bao gồm cả các điểm nhiễu mang nhãn `-1`):
    $$\text{DBCV} = \sum_{i} \frac{|C_i|}{N} V_C(C_i)$$
  - Việc đưa toàn bộ vector và nhãn (giữ nguyên `-1` cho nhiễu) vào đánh giá đảm bảo mô hình gán quá nhiều điểm thành nhiễu sẽ bị phạt tỷ lệ nghịch chính xác theo tỷ lệ nhiễu, không bị đánh đồng với mô hình bảo toàn được dữ liệu.
  - Cài đặt fallback tự viết `compute_dbcv` tuân thủ đúng định nghĩa đồ thị MST nội bộ (`internal_nodes` có bậc $> 1$) và đạt độ tương đương tuyệt đối ($\Delta < 10^{-14}$) với `hdbscan.validity.validity_index`.
- **Chính sách giá trị NULL minh bạch:**
  - Thuật toán `WINDOW`: Thuần túy là quy tắc đếm thời gian, không có bản chất mật độ không gian -> `dbcv_score = null` kèm lý do giải thích `WINDOW_BASELINE_NOT_DENSITY_BASED`. Tuyệt đối không so sánh DBCV của Window như một giá trị có thật.
  - Các cửa sổ thất bại (mainshock rơi vào nhiễu hoặc không có chuỗi sau chấn / empty sequence): `dbcv_score = null` kèm lý do `NO_PRODUCTIVE_CLUSTERS`. Tuyệt đối không thay thế bằng 0, NaN JSON hoặc tính score trên các cụm độc lập khi chuỗi chính đã thất bại.
  - Trường hợp chỉ có 1 cụm hợp lệ: Không thể tính khoảng cách tách biệt giữa các cụm -> `dbcv_score = null` kèm lý do `DBCV_REQUIRES_AT_LEAST_TWO_CLUSTERS`.
  - Chỉ số phân tách Silhouette được theo dõi ở trường độc lập (`silhouette_score`, `silhouette_reason`), tuyệt đối không gán nhãn DBCV.
- **Fail-closed kiểm tra dữ liệu đặc trưng (Feature Grain):**
  - Mọi bản ghi trong `memberships.parquet` phải đối soát chính xác 1-1 với `(mainshock_event_id, candidate_event_id)` trong feature bundle.
  - Nếu phát hiện candidate ID lạ, trùng grain, thiếu vector đặc trưng hoặc chứa giá trị NaN/Inf, hệ thống lập tức từ chối (`ValueError`) trước khi thực hiện tính toán số liệu hoặc kết xuất biểu đồ. Tuyệt đối không tự ý gán vector 0.0 hoặc âm thầm bỏ qua bản ghi.
- **Quy chuẩn tọa độ trực quan hóa:**
  - Không gian tương đối được biểu diễn bằng khoảng cách $dx$ ($km$) và $dy$ ($km$) so với tâm chấn chính $(0, 0)$. Tuyệt đối không sử dụng tọa độ kinh vĩ độ (degrees) làm khoảng cách Euclidean không có đơn vị.
  - Trục thời gian biểu diễn theo số giờ trôi qua tương đối (`delta_time_hours`) so với thời điểm xảy ra chấn chính ($t = 0$). Trục độ sâu thể hiện độ sâu chấn tiêu (`candidate_depth_km`) với hướng nghịch đảo độ sâu.

---

## 3. Bảng số liệu định lượng trích xuất (Baseline Metrics Table)

Được trích xuất từ 9 cấu hình baseline/sweep trên bộ pilot 279 dòng / 2 cửa sổ chấn chính (xuất ra `baseline_metrics_table.csv` và `baseline_metrics_table.json`):

| Thuật toán | Phiên bản cấu hình | Mã chấn chính | Ứng viên | Thành viên chuỗi | Dư chấn | Điểm nhiễu | Chỉ số DBCV | Trạng thái cửa sổ | Lý do nếu không thành công |
|---|---|---|---:|---:|---:|---:|:---:|:---:|---|
| **DBSCAN** | `dbscan-sweep-c1-v1` | `evt_d47fcaaf451c..` | 1 | 0 | 0 | 1 | `null` | `NO_PRODUCTIVE_SEQUENCE` | `EMPTY_SEQUENCE` *(null: NO_PRODUCTIVE_CLUSTERS)* |
| **DBSCAN** | `dbscan-sweep-c1-v1` | `evt_f0300848e0b7..` | 278 | 251 | 240 | 8 | **0.1676** | `SUCCESS` | — |
| **DBSCAN** | `dbscan-sweep-c2-v1` | `evt_d47fcaaf451c..` | 1 | 0 | 0 | 1 | `null` | `NO_PRODUCTIVE_SEQUENCE` | `EMPTY_SEQUENCE` *(null: NO_PRODUCTIVE_CLUSTERS)* |
| **DBSCAN** | `dbscan-sweep-c2-v1` | `evt_f0300848e0b7..` | 278 | 251 | 240 | 17 | **0.4073** | `SUCCESS` | — |
| **DBSCAN** | `dbscan-sweep-c3-v1` | `evt_d47fcaaf451c..` | 1 | 0 | 0 | 1 | `null` | `NO_PRODUCTIVE_SEQUENCE` | `EMPTY_SEQUENCE` *(null: NO_PRODUCTIVE_CLUSTERS)* |
| **DBSCAN** | `dbscan-sweep-c3-v1` | `evt_f0300848e0b7..` | 278 | 231 | 229 | 25 | **0.3237** | `SUCCESS` | — |
| **DBSCAN** | `dbscan-sweep-c4-v1` | `evt_d47fcaaf451c..` | 1 | 0 | 0 | 1 | `null` | `NO_PRODUCTIVE_SEQUENCE` | `EMPTY_SEQUENCE` *(null: NO_PRODUCTIVE_CLUSTERS)* |
| **DBSCAN** | `dbscan-sweep-c4-v1` | `evt_f0300848e0b7..` | 278 | 270 | 252 | 3 | **0.1271** | `SUCCESS` | — |
| **HDBSCAN** | `hdbscan-sweep-c1-v1` | `evt_d47fcaaf451c..` | 1 | 0 | 0 | 1 | `null` | `NO_PRODUCTIVE_SEQUENCE` | `EMPTY_SEQUENCE` *(null: NO_PRODUCTIVE_CLUSTERS)* |
| **HDBSCAN** | `hdbscan-sweep-c1-v1` | `evt_f0300848e0b7..` | 278 | 0 | 0 | 107 | `null` | `NO_PRODUCTIVE_SEQUENCE` | `EMPTY_SEQUENCE` *(null: NO_PRODUCTIVE_CLUSTERS)* |
| **HDBSCAN** | `hdbscan-sweep-c2-v1` | `evt_d47fcaaf451c..` | 1 | 0 | 0 | 1 | `null` | `NO_PRODUCTIVE_SEQUENCE` | `EMPTY_SEQUENCE` *(null: NO_PRODUCTIVE_CLUSTERS)* |
| **HDBSCAN** | `hdbscan-sweep-c2-v1` | `evt_f0300848e0b7..` | 278 | 216 | 215 | 18 | **0.2044** | `SUCCESS` | — |
| **HDBSCAN** | `hdbscan-sweep-c3-v1` | `evt_d47fcaaf451c..` | 1 | 0 | 0 | 1 | `null` | `NO_PRODUCTIVE_SEQUENCE` | `EMPTY_SEQUENCE` *(null: NO_PRODUCTIVE_CLUSTERS)* |
| **HDBSCAN** | `hdbscan-sweep-c3-v1` | `evt_f0300848e0b7..` | 278 | 217 | 216 | 18 | **-0.0196** | `SUCCESS` | — |
| **HDBSCAN** | `hdbscan-sweep-c4-v1` | `evt_d47fcaaf451c..` | 1 | 0 | 0 | 1 | `null` | `NO_PRODUCTIVE_SEQUENCE` | `EMPTY_SEQUENCE` *(null: NO_PRODUCTIVE_CLUSTERS)* |
| **HDBSCAN** | `hdbscan-sweep-c4-v1` | `evt_f0300848e0b7..` | 278 | 216 | 215 | 25 | **0.2051** | `SUCCESS` | — |
| **WINDOW** | `window-baseline-v1` | `evt_d47fcaaf451c..` | 1 | 1 | 0 | 0 | `null` | `SUCCESS` | *(null: WINDOW_BASELINE_NOT_DENSITY_BASED)* |
| **WINDOW** | `window-baseline-v1` | `evt_f0300848e0b7..` | 278 | 256 | 255 | 0 | `null` | `SUCCESS` | *(null: WINDOW_BASELINE_NOT_DENSITY_BASED)* |

---

## 4. Các biểu đồ trực quan hóa kết xuất

Tất cả biểu đồ được sinh tự động và lưu trữ tại `staging/ml-experiments/col03-metrics-plots/`:

1. **Quy tắc chọn cấu hình đại diện theo `algorithm_name` trong manifest (`select_plot_result_dirs`):**
   - Dùng chung hàm `select_plot_result_dirs` giữa CLI (`scripts/inspect-col03-metrics.py`) và notebook (`notebooks/col_03_baseline_metrics_plots.ipynb`).
   - Gom nhóm theo `algorithm_name` trong `pilot_result_manifest.json` theo thứ tự chuẩn `WINDOW` → `DBSCAN` → `HDBSCAN_GLOBAL`, ưu tiên cấu hình có cửa sổ `SUCCESS` trong từng họ thuật toán và chọn luân phiên (round-robin), đảm bảo cả ba thuật toán đều có mặt (`WINDOW_CONTROL`, `DBSCAN_C1`, `HDBSCAN_C2`, `DBSCAN_C2`), không bị thứ tự tên thư mục theo bảng chữ cái lấn át.
2. **Phân nhóm hiển thị đầy đủ 5 nhóm độc lập (MECE) và đối soát số điểm được vẽ:**
   - Hàm `classify_candidate_plot_groups` phân hoạch toàn bộ candidate grains thành 5 nhóm rời nhau và vét cạn 100%:
     - `Mainshock` (`event_role_candidate == "MAINSHOCK"`, ngôi sao đỏ `*`).
     - `Sequence Members` (các candidate `POST` thuộc chuỗi chính `is_sequence_member == True`, không chứa `MAINSHOCK`, chấm tròn xanh `#1f77b4` / `#2ca02c`).
     - `PRE Candidates` (các sự kiện tiền chấn `PRE` không phải nhiễu tại $t < 0$, hình tam giác tím `^`).
     - `Non-Sequence POST` (các candidate `POST` nằm ngoài chuỗi chính nhưng không phải nhiễu, ví dụ thuộc cụm phụ hoặc cửa sổ `WINDOW`/`DBSCAN`/`HDBSCAN` ngoài sequence, hình vuông cam `s`).
     - `Noise Candidates` (các điểm nhiễu `is_noise == True` ngoài mainshock, dấu chữ thập xám `x`).
   - Kiểm tra fail-closed ngay khi vẽ: tổng số điểm thực tế trên `ax.collections` của cả hai biểu đồ không gian và thời gian bắt buộc khớp 1-1 với tổng số dòng trong feature bundle (`279` điểm).
3. **Biểu đồ phân bố không gian tương đối (`spatial_distribution_*.png`):**
   - Thể hiện vị trí tương đối theo $dx$ (km) và $dy$ (km) xoay quanh tâm chấn chính $(0, 0)$.
4. **Biểu đồ diễn tiến thời gian và độ sâu (`timeline_depth_*.png`):**
   - Thể hiện trục thời gian từ trước khi chấn chính xảy ra ($t < 0$) đến giai đoạn sau chấn ($t > 0$), kết hợp độ sâu chấn tiêu ($km$).
5. **Biểu đồ cột so sánh tổng thể (`baseline_comparison_summary.png`):**
   - Đối chiếu trực quan số lượng dư chấn được giữ lại và số lượng điểm nhiễu được loại bỏ giữa tất cả các cấu hình baseline.

---

## 5. Gói bàn giao cho ThanhTris (EXP-04 Handoff Package)

File `staging/ml-experiments/col03-metrics-plots/handoff_manifest.json` (sinh từ thực thi fresh kernel `nbclient` của `notebooks/col_03_baseline_metrics_plots.ipynb` đồng bộ với CLI `scripts/inspect-col03-metrics.py`) ghi nhận đầy đủ chữ ký kiểm chứng mã băm SHA-256 cho toàn bộ 11 artifacts cùng `source_provenance` của 9 cấu hình gốc phục vụ task [EXP-04](../task/tasks/EXP-04.md):

* `baseline_metrics_table.csv` — `9465f7a237e72b6ae4ac275feb946b920217fef19e765ef9f7817c09a92424c7`
* `baseline_metrics_table.json` — `dfa95fb06154676772bf857226d63ed56cb9f8079acb2502a00fbb89b16272c9`
* `spatial_distribution_window_control.png` — `cc3656b09059a418726871f9484bb2e2f745e0009b7624d7104c0a24526e909a`
* `timeline_depth_window_control.png` — `ad135ec4e8e91829238860c53e972de1b17b25fa43658c5730ca0e62ad8bf090`
* `spatial_distribution_dbscan_c1.png` — `1f55cfc5545ea9549bcb77992e6d803249a56014411c4f0e134c741cfc99957d`
* `timeline_depth_dbscan_c1.png` — `16f58523ddf0f6e4babe479f8fc354aae6050fce842d62f0c6e8e8fe6144b43d`
* `spatial_distribution_hdbscan_c2.png` — `56b8e300ba0c864ca7d63a1c2b986407ff589c3c17c5b3c3edb9393547580613`
* `timeline_depth_hdbscan_c2.png` — `db428ad1cb7b4b5000135264a482593d9cad3dbf8c88b20b8b6a449df7ab9c70`
* `spatial_distribution_dbscan_c2.png` — `83f9458606895f0d70e05d7ac2d11e3d5a35119297477c8627194dc096b59b5a`
* `timeline_depth_dbscan_c2.png` — `3272b294af2518b130dc3c42e3fa5be8a1d542ff7506800572aff9eabc7dc059`
* `baseline_comparison_summary.png` — `e98ac3a11d3bc904fb340824968d3a1570cc2ee7c7a635644260bc0895c6318c`
* `handoff_manifest.json` — `e55830114445e7e250648e185acf8614908685dd836959a1902673fa239a59a2`

---

## 6. Danh mục tệp mã nguồn, môi trường và kiểm thử

- **Mã nguồn:**
  - Module trực quan hóa: `notebooks/baseline_visualization.py`
  - Notebook: `notebooks/col_03_baseline_metrics_plots.ipynb` (SHA-256: `8f9d254cba591c1dbd608e78947a1beb5d5824ca864cf144892c71f5abc0f515`, `RESTART_RUN_ALL_PASSED` trên fresh kernel `nbclient`)
  - Script Headless Runner: `scripts/inspect-col03-metrics.py`
- **Dependencies:**
  - Local / Notebook venv: `notebooks/requirements-colab-lock.txt` (`pandas==2.2.3`, `matplotlib==3.10.0` và toàn bộ pins từ `notebooks/requirements-lock.txt`).
  - CI runner: `tests/ci/requirements.txt` (đã đồng bộ pin `pandas==2.2.3` và `matplotlib==3.10.0`).
- **Bộ kiểm thử tự động:**
  - `tests/test_baseline_visualization.py` (39/39 tests PASS, bao gồm đối soát toàn bộ 5 nhóm candidate grains được vẽ trên biểu đồ, chọn cấu hình đại diện theo `algorithm_name` giữa CLI và notebook, end-to-end notebook provenance test và dependency alignment test).
- **Metadata Bằng chứng:**
  - JSON Metadata: [COL-03_BASELINE_METRICS_PLOTS_20261102.json](./COL-03_BASELINE_METRICS_PLOTS_20261102.json)

---

## 7. Giới hạn / Rủi ro / Bước tiếp theo

- **Bàn giao cho Khối 3:** Dữ liệu bảng số liệu và manifest được chuyển giao trực tiếp cho ThanhTris để triển khai [EXP-04 - Đánh giá quality, physical consistency và stability](../task/tasks/EXP-04.md).
- **Nghiên cứu chính thức toàn kỳ:** Toàn bộ bảng số liệu và biểu đồ ở đây thuộc phạm vi thử nghiệm pilot kỹ thuật (`DEVELOPMENT_ONLY`, `PARTIAL`); không đại diện cho toàn bộ 40 năm catalog nghiên cứu. Nghiên cứu toàn kỳ cần dataset chính thức từ [DAT-03 - Chuẩn bị catalog nghiên cứu thật](../task/tasks/DAT-03.md) và [MLD-05 - Validate feature snapshot và export Parquet bundle](../task/tasks/MLD-05.md).
- **Trạng thái task:** Đang ở `In Progress` trong [COL-03.md](../task/tasks/COL-03.md) theo review acceptance thực tế; chờ reviewer độc lập / chủ dự án nghiệm thu.
- **Reviewer:** Reviewer độc lập cho task P1 này hiện là `unassigned`.
