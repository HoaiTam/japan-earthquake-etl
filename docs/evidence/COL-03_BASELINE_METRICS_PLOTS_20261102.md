# COL-03 — Pilot Baseline Metrics and Plots Evidence

- **Ngày kiểm chứng:** 2026-11-02 (Cập nhật phản hồi review: 2026-11-07)
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
| **DBSCAN** | `dbscan-sweep-c1-v1` | `evt_f0300848e0b7..` | 278 | 240 | 240 | 8 | **-0.3639** | `SUCCESS` | — |
| **DBSCAN** | `dbscan-sweep-c2-v1` | `evt_d47fcaaf451c..` | 1 | 0 | 0 | 1 | `null` | `NO_PRODUCTIVE_SEQUENCE` | `EMPTY_SEQUENCE` *(null: NO_PRODUCTIVE_CLUSTERS)* |
| **DBSCAN** | `dbscan-sweep-c2-v1` | `evt_f0300848e0b7..` | 278 | 240 | 240 | 17 | **-0.0346** | `SUCCESS` | — |
| **DBSCAN** | `dbscan-sweep-c3-v1` | `evt_d47fcaaf451c..` | 1 | 0 | 0 | 1 | `null` | `NO_PRODUCTIVE_SEQUENCE` | `EMPTY_SEQUENCE` *(null: NO_PRODUCTIVE_CLUSTERS)* |
| **DBSCAN** | `dbscan-sweep-c3-v1` | `evt_f0300848e0b7..` | 278 | 229 | 229 | 25 | **0.0862** | `SUCCESS` | — |
| **DBSCAN** | `dbscan-sweep-c4-v1` | `evt_d47fcaaf451c..` | 1 | 0 | 0 | 1 | `null` | `NO_PRODUCTIVE_SEQUENCE` | `EMPTY_SEQUENCE` *(null: NO_PRODUCTIVE_CLUSTERS)* |
| **DBSCAN** | `dbscan-sweep-c4-v1` | `evt_f0300848e0b7..` | 278 | 252 | 252 | 3 | **-0.4788** | `SUCCESS` | — |
| **HDBSCAN** | `hdbscan-sweep-c1-v1` | `evt_d47fcaaf451c..` | 1 | 0 | 0 | 1 | `null` | `NO_PRODUCTIVE_SEQUENCE` | `EMPTY_SEQUENCE` *(null: NO_PRODUCTIVE_CLUSTERS)* |
| **HDBSCAN** | `hdbscan-sweep-c1-v1` | `evt_f0300848e0b7..` | 278 | 0 | 0 | 107 | `null` | `NO_PRODUCTIVE_SEQUENCE` | `EMPTY_SEQUENCE` *(null: NO_PRODUCTIVE_CLUSTERS)* |
| **HDBSCAN** | `hdbscan-sweep-c2-v1` | `evt_d47fcaaf451c..` | 1 | 0 | 0 | 1 | `null` | `NO_PRODUCTIVE_SEQUENCE` | `EMPTY_SEQUENCE` *(null: NO_PRODUCTIVE_CLUSTERS)* |
| **HDBSCAN** | `hdbscan-sweep-c2-v1` | `evt_f0300848e0b7..` | 278 | 215 | 215 | 18 | **-0.3853** | `SUCCESS` | — |
| **HDBSCAN** | `hdbscan-sweep-c3-v1` | `evt_d47fcaaf451c..` | 1 | 0 | 0 | 1 | `null` | `NO_PRODUCTIVE_SEQUENCE` | `EMPTY_SEQUENCE` *(null: NO_PRODUCTIVE_CLUSTERS)* |
| **HDBSCAN** | `hdbscan-sweep-c3-v1` | `evt_f0300848e0b7..` | 278 | 216 | 216 | 18 | **-0.4048** | `SUCCESS` | — |
| **HDBSCAN** | `hdbscan-sweep-c4-v1` | `evt_d47fcaaf451c..` | 1 | 0 | 0 | 1 | `null` | `NO_PRODUCTIVE_SEQUENCE` | `EMPTY_SEQUENCE` *(null: NO_PRODUCTIVE_CLUSTERS)* |
| **HDBSCAN** | `hdbscan-sweep-c4-v1` | `evt_f0300848e0b7..` | 278 | 215 | 215 | 25 | **-0.3776** | `SUCCESS` | — |
| **WINDOW** | `window-baseline-v1` | `evt_d47fcaaf451c..` | 1 | 0 | 0 | 0 | `null` | `NO_PRODUCTIVE_SEQUENCE` | `EMPTY_SEQUENCE` *(null: WINDOW_BASELINE_NOT_DENSITY_BASED)* |
| **WINDOW** | `window-baseline-v1` | `evt_f0300848e0b7..` | 278 | 255 | 255 | 0 | `null` | `SUCCESS` | *(null: WINDOW_BASELINE_NOT_DENSITY_BASED)* |

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

* `baseline_metrics_table.csv` — `6f2c3b3fe54d428db18535301ae5830e38197a74dccaa0f854eb02ce90a6e8b1`
* `baseline_metrics_table.json` — `9bbd0ecfdd52c06f71f566606c857aac1f092e789732bdf7e2e65719af6680e9`
* `spatial_distribution_dbscan_c1.png` — `fa86d64e0d6a5ba2b6e1ea95864806cda379273a05121f454e06f1b498be2063`
* `timeline_depth_dbscan_c1.png` — `623e8e2838a56799e7cda23f2dd56f75555d2fa0ccfd751575156f1de63b33cd`
* `spatial_distribution_dbscan_c2.png` — `a43a9d0bf256270a04dce124322222b24b7b51b82a51ca3383bb1d8cc758bfeb`
* `timeline_depth_dbscan_c2.png` — `4b2eeb20110024991782b8afa426b10bd16250f4337d46d2f6c69ad4417e82c5`
* `spatial_distribution_dbscan_c3.png` — `6b78d24506d56530a0f53ecd4bfa34e7951b5aff1123c8147feb5e2061bbb966`
* `timeline_depth_dbscan_c3.png` — `bd5e3f26a54920ca6013522fa8d620779c3e4d2adc41d57001e3c4273f10d407`
* `spatial_distribution_dbscan_c4.png` — `215c32a1e9041d0931c8d2286a35645b13c5bb5ddbad37ddcc237b2c9615299c`
* `timeline_depth_dbscan_c4.png` — `fb09628207f0b67d3615e5af3bee36c731a91344a5a33a269522fb99483c7618`
* `baseline_comparison_summary.png` — `177634e496391bbbd6374bb9228806cd2ac276fa61cd204a350c472e03c52f68`

---

## 6. Danh mục tệp mã nguồn và kiểm thử

- **Mã nguồn:**
  - Module trực quan hóa: `notebooks/baseline_visualization.py`
  - Notebook: `notebooks/col_03_baseline_metrics_plots.ipynb`
  - Script Headless Runner: `scripts/inspect-col03-metrics.py`
- **Bộ kiểm thử tự động:**
  - `tests/test_baseline_visualization.py` (12/12 tests PASS).
- **Metadata Bằng chứng:**
  - JSON Metadata: [COL-03_BASELINE_METRICS_PLOTS_20261102.json](./COL-03_BASELINE_METRICS_PLOTS_20261102.json)

---

## 7. Giới hạn / Rủi ro / Bước tiếp theo

- **Bàn giao cho Khối 3:** Dữ liệu bảng số liệu và manifest được chuyển giao trực tiếp cho ThanhTris để triển khai [EXP-04 - Đánh giá quality, physical consistency và stability](../task/tasks/EXP-04.md).
- **Nghiên cứu chính thức toàn kỳ:** Toàn bộ bảng số liệu và biểu đồ ở đây thuộc phạm vi thử nghiệm pilot kỹ thuật (`DEVELOPMENT_ONLY`, `PARTIAL`); không đại diện cho toàn bộ 40 năm catalog nghiên cứu. Nghiên cứu toàn kỳ cần dataset chính thức từ [DAT-03 - Chuẩn bị catalog nghiên cứu thật](../task/tasks/DAT-03.md) và [MLD-05 - Validate feature snapshot và export Parquet bundle](../task/tasks/MLD-05.md).
- **Trạng thái task:** Đang ở `In Progress` trong [COL-03.md](../task/tasks/COL-03.md) theo review acceptance thực tế; chờ reviewer độc lập / chủ dự án nghiệm thu.
- **Reviewer:** Reviewer độc lập cho task P1 này hiện là `unassigned`.
