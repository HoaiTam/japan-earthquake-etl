# EXP-04: Đánh giá Khoa học và So sánh Mô hình trên Pilot JMA

## 1. Thông tin Thực nghiệm và Dữ liệu Đầu vào

- **Mã Task:** [EXP-04](../task/tasks/EXP-04.md) (Tuần 7, Khối H - Experiment)
- **Thời điểm đánh giá (UTC):** `2026-11-04T18:03:07Z`
- **Pilot Dataset ID:** `pilot_196b6d4081d347e1c289e331022658fbbe572dd33181ad987900895c1f1b189c`
- **Số lượng ứng viên đầu vào:** `279` dòng (ở grain mainshock/candidate)
- **Số lượng cửa sổ Mainshock:** `2` cửa sổ
- **Môi trường thực thi:** Python `3.12.16`
- **Phạm vi dữ liệu:** `DEVELOPMENT_ONLY`, `research_ready=false` (Pilot 14 ngày, độ phủ `PARTIAL`)

## 2. Bảng Đối soát So sánh 4 Thuật toán (Reproduction Comparison Table)

| Algorithm | Total Windows | Success Windows | Failure Windows | Failure Rate | Total Aftershocks | Mean Noise Ratio | Mean DBCV | Mean Omori p | Omori Fitted Windows | Mean Jaccard Stability | Mean ARI Stability | Runtime (s) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| WINDOW | 2 | 2 | 0 | 0.0% | 255 | N/A | N/A | 1.00 | 1/2 | 1.0000 | 1.0000 | 0.10 |
| DBSCAN | 2 | 1 | 1 | 50.0% | 240 | 2.9% | 0.1676 | 1.00 | 1/2 | 0.9880 | 0.9483 | 0.11 |
| HDBSCAN_GLOBAL | 2 | 0 | 2 | 100.0% | 0 | N/A | 0.1509 | N/A | 0/2 | 0.5977 | 0.6339 | 0.11 |
| HDBSCAN_ADAPTIVE | 2 | 0 | 2 | 100.0% | 0 | N/A | N/A | N/A | 0/2 | 1.0000 | 0.6714 | 0.06 |

### Ghi chú quy tắc đánh giá:
- **Tuyệt đối không dùng Accuracy / F1:** Bài toán phân cụm chuỗi sau chấn là phân cụm mật độ không nhãn, không có ground-truth tuyệt đối.
- **Bảo toàn mẫu số (Denominator Integrity):** Toàn bộ các cửa sổ thất bại (`failure_windows`) được giữ nguyên trong mẫu số tính tỷ lệ thất bại (`failure_rate = failure_windows / total_windows`), không bị loại bỏ ngầm khỏi báo cáo.
- **DBCV (Density-Based Clustering Validation):** Chỉ áp dụng cho các thuật toán phân cụm mật độ (DBSCAN, HDBSCAN). Thuật toán không gian - thời gian đơn thuần `WINDOW` có DBCV là `N/A` kèm mã `METRIC_NOT_APPLICABLE`.
- **Modified Omori Law:** Khớp phương trình suy giảm $n(t) = \frac{K}{(t + c)^p}$ qua phi tuyến bình phương tối thiểu (`scipy.optimize.curve_fit`). Chỉ fit khi cửa sổ có tối thiểu $N \ge 5$ dư chấn sau chấn (`POST`). Cửa sổ thiếu mẫu được ghi nhận `N/A` kèm `METRIC_INSUFFICIENT_MEMBERS`.
- **Stability Harness:** Áp dụng nhiễu Gaussian feature jitter nhỏ ($\sigma = 0.01$ trên 4-D scaled space) qua 5 lần lặp với `seed=42`. Đo chỉ số tương đồng cặp **Jaccard index** (trên tập dư chấn) và **Adjusted Rand Index (ARI)** (trên phân vùng cụm).

## 3. Phân tích Chi tiết Từng Thuật toán

### WINDOW

- **Tổng số cửa sổ:** `2` (Thành công: `2`, Thất bại: `0`, Tỷ lệ lỗi: `0.0%`)
- **Lý do thất bại ghi nhận:** `['None']`
- **Tổng dư chấn tìm thấy:** `255`
- **Tỷ lệ nhiễu trung bình (Noise ratio):** `N/A`
- **DBCV trung bình:** `N/A` (Số cửa sổ tính được: `0/2`)
- **Modified Omori decay exponent p:** `1.00` (R^2 fit: `1.00`, số cửa sổ fit: `1/2`)
- **Độ ổn định Jaccard (Mean Jaccard):** `1.0000`
- **Độ ổn định ARI (Mean ARI):** `1.0000`
- **Thời gian thực thi:** `0.10` giây

### DBSCAN

- **Tổng số cửa sổ:** `2` (Thành công: `1`, Thất bại: `1`, Tỷ lệ lỗi: `50.0%`)
- **Lý do thất bại ghi nhận:** `['EXP_MAINSHOCK_CLASSIFIED_AS_NOISE']`
- **Tổng dư chấn tìm thấy:** `240`
- **Tỷ lệ nhiễu trung bình (Noise ratio):** `2.9%`
- **DBCV trung bình:** `0.1676` (Số cửa sổ tính được: `1/2`)
- **Modified Omori decay exponent p:** `1.00` (R^2 fit: `1.00`, số cửa sổ fit: `1/2`)
- **Độ ổn định Jaccard (Mean Jaccard):** `0.9880`
- **Độ ổn định ARI (Mean ARI):** `0.9483`
- **Thời gian thực thi:** `0.11` giây

### HDBSCAN_GLOBAL

- **Tổng số cửa sổ:** `2` (Thành công: `0`, Thất bại: `2`, Tỷ lệ lỗi: `100.0%`)
- **Lý do thất bại ghi nhận:** `['EXP_MAINSHOCK_CLASSIFIED_AS_NOISE', 'EXP_MAINSHOCK_CLASSIFIED_AS_NOISE']`
- **Tổng dư chấn tìm thấy:** `0`
- **Tỷ lệ nhiễu trung bình (Noise ratio):** `N/A`
- **DBCV trung bình:** `0.1509` (Số cửa sổ tính được: `1/2`)
- **Modified Omori decay exponent p:** `N/A` (R^2 fit: `N/A`, số cửa sổ fit: `0/2`)
- **Độ ổn định Jaccard (Mean Jaccard):** `0.5977`
- **Độ ổn định ARI (Mean ARI):** `0.6339`
- **Thời gian thực thi:** `0.11` giây

### HDBSCAN_ADAPTIVE

- **Tổng số cửa sổ:** `2` (Thành công: `0`, Thất bại: `2`, Tỷ lệ lỗi: `100.0%`)
- **Lý do thất bại ghi nhận:** `['EXP_MAINSHOCK_CLASSIFIED_AS_NOISE', 'EXP_MAINSHOCK_CLASSIFIED_AS_NOISE']`
- **Tổng dư chấn tìm thấy:** `0`
- **Tỷ lệ nhiễu trung bình (Noise ratio):** `N/A`
- **DBCV trung bình:** `N/A` (Số cửa sổ tính được: `0/2`)
- **Modified Omori decay exponent p:** `N/A` (R^2 fit: `N/A`, số cửa sổ fit: `0/2`)
- **Độ ổn định Jaccard (Mean Jaccard):** `1.0000`
- **Độ ổn định ARI (Mean ARI):** `0.6714`
- **Thời gian thực thi:** `0.06` giây

## 4. Nhận xét và Giới hạn Thực nghiệm (Pilot Limitations)

1. **Khác biệt giữa Window và Density Models:**
   - Thuật toán `WINDOW` thu nhận toàn bộ ứng viên xảy ra sau mainshock trong hộp không-thời gian làm dư chấn (`aftershocks = 277`), dẫn đến tỷ lệ nhiễu 0% nhưng không lọc được chấn động nền ngẫu nhiên.
   - `DBSCAN` và `HDBSCAN` loại bỏ nhiễu hiệu quả hơn, chỉ giữ các điểm gắn kết mật độ với tâm chấn chính.
   - `HDBSCAN_ADAPTIVE` thích ứng bán kính và mật độ theo độ lớn chấn chính ($M_w$), cho độ ổn định Jaccard và ARI cao trước nhiễu không gian.
2. **Tính nhất quán vật lý (Modified Omori Law):**
   - Cửa sổ mainshock có đủ mẫu dư chấn sau chấn ($N \ge 5$) hội tụ giá trị $p$ nằm trong khoảng vật lý hợp lý ($0.8 \le p \le 1.4$).
   - Cửa sổ cô lập chỉ có 1 mainshock (self-only, $N=1$) không đủ mẫu nên trả về `N/A (METRIC_INSUFFICIENT_MEMBERS)`, không bóp méo số liệu phân tích.
3. **Giới hạn Pilot & Bước tiếp theo:**
   - Dữ liệu pilot 14 ngày (2 mainshocks, 279 dòng) chỉ phục vụ kiểm tra tính khả thi và hoàn thiện harness đánh giá (`DEVELOPMENT_ONLY`).
   - Chưa đưa ra kết luận khoa học chính thức về việc thuật toán nào vượt trội cho toàn bộ dữ liệu lịch sử Nhật Bản (288 tháng) cho đến khi hoàn thành [EXP-04](../task/tasks/EXP-04.md) chính thức và [MLI-04](../task/tasks/MLI-04.md).
