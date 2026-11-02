# COL-02 — Bounded Baseline Parameter Sweep on Development Pilot

- **Ngày kiểm chứng:** 2026-11-02
- **Tuần thực hiện:** Tuần 7 (02–08/11/2026), Khối 2
- **Assignee:** `rosy179`
- **Reviewer:** `unassigned`
- **Branch:** `feat/col-02-parameter-sweep`
- **Tài liệu dẫn chiếu:** [COL-02 Task](../task/tasks/COL-02.md), [Kế hoạch tuần 7](../task/WEEK_7_COLAB_PLAN.md), [EXP-02 Colab Parity Evidence](./EXP-02_COLAB_PARITY_20261102.md)

---

## 1. Mục tiêu và bối cảnh

Task [COL-02 - Sweep tham số baseline giới hạn trên pilot](../task/tasks/COL-02.md) thuộc Khối 2 trong kế hoạch Tuần 7, do `rosy179` phụ trách. Sau khi hoàn tất kiểm chứng parity cho các baseline mặc định ở [EXP-02](./EXP-02_COLAB_PARITY_20261102.md), COL-02 thực hiện khảo sát tham số có kiểm soát để:
1. Đánh giá độ nhạy của các thuật toán phân cụm mật độ (`DBSCAN`, `HDBSCAN_GLOBAL`) trước các thay đổi siêu tham số trong không gian 4-D chuẩn hóa `[x_scaled, y_scaled, z_scaled, time_scaled]`.
2. Kiểm tra tính tái lập tuyệt đối (deterministic invariants) khi chạy lại (rerun) trên cùng tập ứng viên pilot.
3. Cung cấp dữ liệu đa cấu hình máy đọc được làm đầu vào cho [COL-03 - Metrics và trực quan hóa baseline pilot](../task/tasks/COL-03.md).

Toàn bộ thực nghiệm tuân thủ chặt chẽ ranh giới: giữ nguyên tập đặc trưng đầu vào (279 dòng, 2 mainshock windows, pilot ID `pilot_196b6d40...`), không sửa đổi scaling, không tune tham số bằng mắt cho từng sequence, và không dùng bộ pilot nhỏ (coverage PARTIAL ~14 ngày) để tuyên bố tìm ra cấu hình "tối ưu" cho catalog nghiên cứu 40 năm.

---

## 2. Ma trận 8 cấu hình được khóa trước (Bounded Sweep Matrix)

Hệ thống khóa trước danh sách tối đa **8 cấu hình thực nghiệm** (4 DBSCAN, 4 HDBSCAN) cùng 1 cấu hình đối chứng cố định (`WINDOW_CONTROL`):

| Nhóm | Mã cấu hình | Phiên bản cấu hình | Tham số chính | Mục đích kiểm tra |
|---|---|---|---|---|
| **Control** | `WINDOW_CONTROL` | `window-baseline-v1` | Mặc định | Đối chứng đếm thời gian thuần túy (control) |
| **DBSCAN** | `DBSCAN_C1` | `dbscan-sweep-c1-v1` | `eps=0.5, min_samples=3` | Baseline mặc định |
| **DBSCAN** | `DBSCAN_C2` | `dbscan-sweep-c2-v1` | `eps=0.5, min_samples=5` | Tăng số điểm lân cận lõi (lọc nhiễu chặt hơn) |
| **DBSCAN** | `DBSCAN_C3` | `dbscan-sweep-c3-v1` | `eps=0.3, min_samples=3` | Giảm bán kính lân cận không-thời gian |
| **DBSCAN** | `DBSCAN_C4` | `dbscan-sweep-c4-v1` | `eps=0.7, min_samples=3` | Mở rộng bán kính lân cận không-thời gian |
| **HDBSCAN** | `HDBSCAN_C1` | `hdbscan-sweep-c1-v1` | `min_cluster_size=3, min_samples=2` | Baseline mặc định |
| **HDBSCAN** | `HDBSCAN_C2` | `hdbscan-sweep-c2-v1` | `min_cluster_size=3, min_samples=3` | Tăng mẫu lõi ước lượng mật độ |
| **HDBSCAN** | `HDBSCAN_C3` | `hdbscan-sweep-c3-v1` | `min_cluster_size=5, min_samples=2` | Tăng kích thước cụm tối thiểu lên 5 |
| **HDBSCAN** | `HDBSCAN_C4` | `hdbscan-sweep-c4-v1` | `min_cluster_size=5, min_samples=3` | Kết hợp kích thước cụm và mẫu lõi chặt hơn |

Mỗi cấu hình được lưu thành file JSON riêng biệt trong `config/ml/sweeps/`, có mã băm nội dung SHA-256 xác định và viết kết quả ra thư mục cô lập riêng biệt.

---

## 3. Kết quả thực nghiệm trên Pilot Bundle

Thực thi qua script headless runner `scripts/inspect-col02-sweep.py` và notebook `notebooks/col_02_parameter_sweep.ipynb` trên tập pilot 279 dòng / 2 mainshocks:

| Mã cấu hình | Thuật toán | Phiên bản | Ứng viên | Cụm hợp lệ | Thành viên chuỗi | Dư chấn | Điểm nhiễu | Trạng thái |
|---|---|---|---:|---:|---:|---:|---:|:---:|
| `WINDOW_CONTROL` | `WINDOW` | `window-baseline-v1` | 279 | 2 | 257 | 255 | 0 | 2/2 PASS |
| `DBSCAN_C1` | `DBSCAN` | `dbscan-sweep-c1-v1` | 279 | 1 | 251 | 240 | 9 | 1/2 PASS |
| `DBSCAN_C2` | `DBSCAN` | `dbscan-sweep-c2-v1` | 279 | 1 | 251 | 240 | 18 | 1/2 PASS |
| `DBSCAN_C3` | `DBSCAN` | `dbscan-sweep-c3-v1` | 279 | 1 | 231 | 229 | 26 | 1/2 PASS |
| `DBSCAN_C4` | `DBSCAN` | `dbscan-sweep-c4-v1` | 279 | 1 | 270 | 252 | 4 | 1/2 PASS |
| `HDBSCAN_C1` | `HDBSCAN_GLOBAL` | `hdbscan-sweep-c1-v1` | 279 | 0 | 0 | 0 | 108 | 0/2 PASS |
| `HDBSCAN_C2` | `HDBSCAN_GLOBAL` | `hdbscan-sweep-c2-v1` | 279 | 1 | 216 | 215 | 19 | 1/2 PASS |
| `HDBSCAN_C3` | `HDBSCAN_GLOBAL` | `hdbscan-sweep-c3-v1` | 279 | 1 | 217 | 216 | 19 | 1/2 PASS |
| `HDBSCAN_C4` | `HDBSCAN_GLOBAL` | `hdbscan-sweep-c4-v1` | 279 | 1 | 216 | 215 | 26 | 1/2 PASS |

---

## 4. Phân tích kỹ thuật và phát hiện thực nghiệm

1. **Độ nhạy bán kính lân cận `eps` trong DBSCAN:**
   - Khi giảm `eps` từ $0.5$ xuống $0.3$ (`DBSCAN_C3`): số lượng nhiễu tăng từ $9$ lên $26$, số dư chấn được nhận diện giảm từ $240$ xuống $229$. Điều này phản ánh các điểm phân tán xa tâm chấn bị loại bỏ.
   - Khi tăng `eps` lên $0.7$ (`DBSCAN_C4`): bán kính mở rộng làm giảm điểm nhiễu xuống chỉ còn $4$, số dư chấn tăng lên $252$ (tiệm cận kết quả của Window $255$).
2. **Khắc phục hiện tượng Mainshock Noise trong HDBSCAN:**
   - Tại cấu hình mặc định (`HDBSCAN_C1` với `min_cluster_size=3, min_samples=2`), cả 2 cửa sổ đều thất bại do mainshock bị phân loại là nhiễu (`total_sequence_members = 0`).
   - Tuy nhiên, khi điều chỉnh cấu hình phù hợp với mật độ cục bộ của pilot:
     - `HDBSCAN_C2` (`min_samples=3`): Mainshock tại cửa sổ 1 đã được nhận diện vào cụm chuỗi chính, cô lập được $215$ dư chấn và $19$ điểm nhiễu.
     - `HDBSCAN_C3` (`min_cluster_size=5, min_samples=2`): Tương tự, cô lập được $216$ dư chấn và $19$ điểm nhiễu.
     - `HDBSCAN_C4` (`min_cluster_size=5, min_samples=3`): Lọc nhiễu chặt hơn với $26$ điểm nhiễu và $215$ dư chấn.
3. **Tính bất biến của cửa sổ thứ hai:**
   - Trong toàn bộ các cấu hình DBSCAN và HDBSCAN, cửa sổ thứ hai luôn có mainshock mang nhãn `-1` (nhiễu). Đây là do đặc tính mật độ thưa của cửa sổ này trong pilot JMA. Hệ thống tuân thủ nghiêm ngặt nguyên tắc: **ghi nhận failed_windows trung thực, không ép chọn cụm gần nhất**.

---

## 5. Kiểm chứng tính bất biến khi Rerun (Rerun Invariance Check)

Cấu hình `DBSCAN_C1` được chạy lại trong một thư mục cô lập độc lập (`staging/ml-experiments/col02-sweep/rerun_check/dbscan_c1`):
* **Số lượng ứng viên:** $279/279$ khớp tuyệt đối.
* **Grain & Nhãn vai trò:** Khớp $100\%$ các trường `event_role_candidate`, `is_noise`, `is_mainshock_cluster_member`.
* **Phân hoạch cụm (Cluster Partitions):** Cấu trúc tập hợp cụm (frozenset of member keys) giữa hai lần chạy hoàn toàn đồng nhất, không bị ảnh hưởng bởi việc hoán đổi nhãn số.
* **Sai số số học:** Tuân thủ ngưỡng $rtol = 10^{-6}, atol = 10^{-8}$.
* **Trạng thái:** `RERUN_INVARIANTS_PASSED`.

---

## 6. Danh mục Artifacts và Bằng chứng

- **Mã nguồn và Cấu hình:**
  - Module Sweep: `notebooks/parameter_sweep.py`
  - Notebook Runner: `notebooks/col_02_parameter_sweep.ipynb`
  - Script Headless Runner: `scripts/inspect-col02-sweep.py`
  - Bộ 8 cấu hình sweep: `config/ml/sweeps/dbscan_sweep_c1..c4.json`, `config/ml/sweeps/hdbscan_sweep_c1..c4.json`
- **Unit Test Suite:**
  - `tests/test_parameter_sweep.py` (5/5 tests PASS).
- **Kết quả thực nghiệm:**
  - Ma trận JSON: `staging/ml-experiments/col02-sweep/sweep_results_matrix.json`
  - Báo cáo tổng hợp: `staging/ml-experiments/col02-sweep/col02-sweep-report.json`
  - Metadata nghiệm thu: [COL-02_PARAMETER_SWEEP_20261102.json](./COL-02_PARAMETER_SWEEP_20261102.json)

---

## 7. Giới hạn / Rủi ro / Bước tiếp theo

- **Trực quan hóa và bảng metrics (COL-03):** Kết xuất bảng so sánh chi tiết và đồ thị không gian - thời gian tương đối phục vụ handoff cho ThanhTris thuộc về [COL-03 - Metrics và trực quan hóa baseline pilot](../task/tasks/COL-03.md).
- **Đánh giá tính ổn định / Omori (EXP-04):** Đánh giá decay law và độ ổn định chuỗi theo thời gian thuộc về [EXP-04 - Đánh giá quality, physical consistency và stability](../task/tasks/EXP-04.md) (ThanhTris).
- **Nghiên cứu chính thức toàn kỳ:** Pilot mang tính chất kỹ thuật (`DEVELOPMENT_ONLY`, `PARTIAL`); việc chạy nghiên cứu toàn kỳ cần catalog chính thức từ [DAT-03 - Chuẩn bị catalog nghiên cứu thật](../task/tasks/DAT-03.md) và [MLD-05 - Validate feature snapshot và export Parquet bundle](../task/tasks/MLD-05.md).
- **Reviewer:** Reviewer độc lập cho task P1 này hiện là `unassigned`.
