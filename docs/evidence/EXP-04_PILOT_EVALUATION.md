# EXP-04: Đánh giá Khoa học và So sánh Mô hình trên Pilot JMA

## 1. Thông tin Thực nghiệm và Dữ liệu Đầu vào

- **Mã Task:** [EXP-04](../task/tasks/EXP-04.md) (Tuần 7, Khối H - Experiment)
- **Thời điểm đánh giá (UTC):** `2026-11-05T16:48:37Z`
- **Pilot Dataset ID:** `pilot_196b6d4081d347e1c289e331022658fbbe572dd33181ad987900895c1f1b189c`
- **Git Commit Pin:** `887d1b5640c7e89b6a8b853f90135dd791d79d3c` (clean)
- **Random Seed:** `42`
- **Số lượng ứng viên đầu vào:** `279` dòng (ở grain mainshock/candidate)
- **Số lượng cửa sổ Mainshock:** `2` cửa sổ
- **Môi trường thực thi:** Python `3.12.16`
- **Phạm vi dữ liệu:** `DEVELOPMENT_ONLY`, `research_ready=false` (Pilot 14 ngày, độ phủ `PARTIAL`)

### Cấu hình thuật toán (Config Pins):
- **WINDOW:** `config/ml/models/window_baseline_v1.json` (Version: `window-baseline-v1`, SHA: `78feda8112fd4a68...`)
- **DBSCAN:** `config/ml/models/dbscan_global_dev_v1.json` (Version: `dbscan-global-dev-v1`, SHA: `f089d928f4ace1c6...`)
- **HDBSCAN_GLOBAL:** `config/ml/models/hdbscan_global_dev_v1.json` (Version: `hdbscan-global-dev-v1`, SHA: `10d2b375d66c8c95...`)
- **HDBSCAN_ADAPTIVE:** `config/ml/models/hdbscan_adaptive_dev_v1.json` (Version: `hdbscan-adaptive-dev-v1`, SHA: `41a4232ad6e4ae50...`)

## 2. Bảng Đối soát So sánh 4 Thuật toán (Reproduction Comparison Table)

| Algorithm | Total Windows | Success Windows | Failure Windows | Failure Rate | Total Aftershocks | Mean Noise Ratio | Mean DBCV | Mean Omori p | Omori Fitted Windows | Mean Jaccard Stability | Mean ARI Stability | Runtime (s) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| WINDOW | 2 | 2 | 0 | 0.0% | 255 | 0.0% | N/A | 0.89 | 1/2 | 1.0000 | 1.0000 | 0.98 |
| DBSCAN | 2 | 1 | 1 | 50.0% | 240 | 2.9% | 0.1676 | 0.92 | 1/2 | 0.9880 | 0.9483 | 0.90 |
| HDBSCAN_GLOBAL | 2 | 0 | 2 | 100.0% | 0 | N/A | 0.1509 | N/A | 0/2 | 0.5977 | 0.6339 | 0.91 |
| HDBSCAN_ADAPTIVE | 2 | 0 | 2 | 100.0% | 0 | N/A | N/A | N/A | 0/2 | 1.0000 | 0.6714 | 0.88 |

### Ghi chú quy tắc đánh giá:
- **Tuyệt đối không dùng Accuracy / F1:** Bài toán phân cụm chuỗi sau chấn là phân cụm mật độ không nhãn, không có ground-truth tuyệt đối.
- **Bảo toàn mẫu số (Denominator Integrity):** Toàn bộ các cửa sổ thất bại (`failure_windows`) được giữ nguyên trong mẫu số tính tỷ lệ thất bại (`failure_rate = failure_windows / total_windows`), không bị loại bỏ ngầm khỏi báo cáo.
- **DBCV (Density-Based Clustering Validation):** Chỉ áp dụng cho các thuật toán phân cụm mật độ (DBSCAN, HDBSCAN). Thuật toán không gian - thời gian đơn thuần `WINDOW` có DBCV là `N/A` kèm mã `METRIC_NOT_APPLICABLE`.
- **Modified Omori Law:** Khớp phương trình suy giảm $n(t) = \frac{K}{(t + c)^p}$ qua phi tuyến bình phương tối thiểu (`scipy.optimize.curve_fit`). Chỉ fit khi cửa sổ có tối thiểu $N \ge 5$ dư chấn sau chấn (`POST`). Cửa sổ thiếu mẫu được ghi nhận `N/A` kèm `METRIC_INSUFFICIENT_MEMBERS`.
- **Stability Harness:** Áp dụng nhiễu Gaussian feature jitter nhỏ ($\sigma = 0.01$ trên 4-D scaled space) qua 5 lần lặp với `seed=42`. Đo chỉ số tương đồng cặp **Jaccard index** (trên tập dư chấn) và **Adjusted Rand Index (ARI)** (trên phân vùng cụm). Phân biệt rõ lỗi thực thi (báo `METRIC_FIT_FAILED`) với kết quả nhiễu vật lý.

## 3. Phân tích Chi tiết Từng Thuật toán

### WINDOW

- **Tổng số cửa sổ:** `2` (Thành công: `2`, Thất bại: `0`, Tỷ lệ lỗi: `0.0%`)
- **Lý do thất bại ghi nhận:** `['None']`
- **Tổng dư chấn tìm thấy:** `255`
- **Tỷ lệ nhiễu trung bình (Noise ratio):** `0.0%`
- **DBCV trung bình:** `N/A` (Số cửa sổ tính được: `0/2`)
- **Modified Omori decay exponent p:** `0.89` (R^2 fit: `1.00`, số cửa sổ fit: `1/2`)
- **Độ ổn định Jaccard (Mean Jaccard):** `1.0000`
- **Độ ổn định ARI (Mean ARI):** `1.0000`
- **Thời gian thực thi:** `0.98` giây

### DBSCAN

- **Tổng số cửa sổ:** `2` (Thành công: `1`, Thất bại: `1`, Tỷ lệ lỗi: `50.0%`)
- **Lý do thất bại ghi nhận:** `['EXP_MAINSHOCK_CLASSIFIED_AS_NOISE']`
- **Tổng dư chấn tìm thấy:** `240`
- **Tỷ lệ nhiễu trung bình (Noise ratio):** `2.9%`
- **DBCV trung bình:** `0.1676` (Số cửa sổ tính được: `1/2`)
- **Modified Omori decay exponent p:** `0.92` (R^2 fit: `1.00`, số cửa sổ fit: `1/2`)
- **Độ ổn định Jaccard (Mean Jaccard):** `0.9880`
- **Độ ổn định ARI (Mean ARI):** `0.9483`
- **Thời gian thực thi:** `0.90` giây

### HDBSCAN_GLOBAL

- **Tổng số cửa sổ:** `2` (Thành công: `0`, Thất bại: `2`, Tỷ lệ lỗi: `100.0%`)
- **Lý do thất bại ghi nhận:** `['EXP_MAINSHOCK_CLASSIFIED_AS_NOISE', 'EXP_MAINSHOCK_CLASSIFIED_AS_NOISE']`
- **Tổng dư chấn tìm thấy:** `0`
- **Tỷ lệ nhiễu trung bình (Noise ratio):** `N/A`
- **DBCV trung bình:** `0.1509` (Số cửa sổ tính được: `1/2`)
- **Modified Omori decay exponent p:** `N/A` (R^2 fit: `N/A`, số cửa sổ fit: `0/2`)
- **Độ ổn định Jaccard (Mean Jaccard):** `0.5977`
- **Độ ổn định ARI (Mean ARI):** `0.6339`
- **Thời gian thực thi:** `0.91` giây

### HDBSCAN_ADAPTIVE

- **Tổng số cửa sổ:** `2` (Thành công: `0`, Thất bại: `2`, Tỷ lệ lỗi: `100.0%`)
- **Lý do thất bại ghi nhận:** `['EXP_MAINSHOCK_CLASSIFIED_AS_NOISE', 'EXP_MAINSHOCK_CLASSIFIED_AS_NOISE']`
- **Tổng dư chấn tìm thấy:** `0`
- **Tỷ lệ nhiễu trung bình (Noise ratio):** `N/A`
- **DBCV trung bình:** `N/A` (Số cửa sổ tính được: `0/2`)
- **Modified Omori decay exponent p:** `N/A` (R^2 fit: `N/A`, số cửa sổ fit: `0/2`)
- **Độ ổn định Jaccard (Mean Jaccard):** `1.0000`
- **Độ ổn định ARI (Mean ARI):** `0.6714`
- **Thời gian thực thi:** `0.88` giây

## 4. Nhận xét và Giới hạn Thực nghiệm (Pilot Limitations)

1. **Phân tích so sánh thực tế giữa các mô hình trên Pilot Bundle:**
   - **WINDOW:** Thu nhận toàn bộ ứng viên xảy ra sau mainshock trong hộp không-thời gian (255 sau chấn trên 2 cửa sổ), tỷ lệ nhiễu đạt 0% nhưng không lọc được chấn động nền ngẫu nhiên không gắn kết mật độ.
   - **DBSCAN:** Lọc nhiễu hiệu quả (tỷ lệ nhiễu trung bình 2.9%), giữ lại 240 sau chấn với 1/2 cửa sổ thành công. Độ ổn định Jaccard đạt 0.9880 và ARI đạt 0.9483.
   - **HDBSCAN_GLOBAL:** Đạt 0/2 cửa sổ thành công trên cấu hình mặc định (tỷ lệ thất bại 100.0%, lý do chính: EXP_MAINSHOCK_CLASSIFIED_AS_NOISE, EXP_MAINSHOCK_CLASSIFIED_AS_NOISE).
   - **HDBSCAN_ADAPTIVE:** Đạt 0/2 cửa sổ thành công trên cấu hình dev (tỷ lệ thất bại 100.0%, lý do: EXP_MAINSHOCK_CLASSIFIED_AS_NOISE, EXP_MAINSHOCK_CLASSIFIED_AS_NOISE). Cần tinh chỉnh tham số thích ứng (adaptive policy / min_cluster_size) cho các cụm thưa trước khi áp dụng trên tập dữ liệu chính thức.

2. **Tính nhất quán vật lý (Modified Omori Law):**
   - Các mô hình có cửa sổ sau chấn đủ mẫu ($N \ge 5$) đã thực hiện khớp phân rã Omori: `WINDOW` (p=0.89, R²=1.00), `DBSCAN` (p=0.92, R²=1.00). Các cửa sổ ít hơn 5 sau chấn trả về `METRIC_INSUFFICIENT_MEMBERS` để bảo toàn tính trung thực thống kê.

3. **Giới hạn Pilot & Bước tiếp theo:**
   - Dữ liệu pilot 14 ngày (2 mainshocks, 279 dòng) chỉ phục vụ kiểm tra tính khả thi và hoàn thiện harness đánh giá (`DEVELOPMENT_ONLY`).
   - Kết quả phân cụm chỉ mang tính ứng viên chuỗi sau chấn (candidate aftershock sequences) hồi cứu, không tuyên bố dự đoán động đất hoặc quan hệ nhân quả vật lý.
   - Việc đánh giá và so sánh khoa học chính thức trên toàn bộ 288 tháng thuộc [EXP-04](../task/tasks/EXP-04.md) và [MLI-04](../task/tasks/MLI-04.md) sau khi official dataset export được nghiệm thu.
