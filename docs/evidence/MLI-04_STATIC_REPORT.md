# Báo cáo Thực nghiệm Pilot So sánh Mô hình Phân cụm Chuỗi Động đất (DEVELOPMENT_ONLY)

## 1. Siêu dữ liệu Thực nghiệm và Cổng Kiểm định (Metadata & Publication Gate)

- **Mã Task:** [MLI-04](../task/tasks/MLI-04.md) / [EXP-04](../task/tasks/EXP-04.md)
- **Phạm vi Báo cáo:** `DEVELOPMENT_ONLY (research_ready=false)`
- **Thời điểm Khởi tạo (UTC):** `2026-11-06T12:21:57Z`
- **Dataset ID:** `pilot_196b6d4081d347e1c289e331022658fbbe572dd33181ad987900895c1f1b189c`
- **Git Commit Pin:** `5bc1aff7617b99e3eb219c8b14ac148153657fd4`
- **Random Seed:** `42`
- **Số lượng Cửa sổ Mainshock:** `2`
- **Tổng số Sự kiện Ứng viên:** `279`

### Cấu hình Thuật toán đã Ghim (Algorithm Config Pins):
- **DBSCAN:** `config/ml/models/dbscan_global_dev_v1.json` (Phiên bản: `dbscan-global-dev-v1`, File SHA: `f089d928f4ace1c6...`, Canon SHA: `0f280869d059dd2d...`)
- **HDBSCAN_ADAPTIVE:** `config/ml/models/hdbscan_adaptive_dev_v1.json` (Phiên bản: `hdbscan-adaptive-dev-v1`, File SHA: `41a4232ad6e4ae50...`, Canon SHA: `55c8077ef11023de...`)
- **HDBSCAN_GLOBAL:** `config/ml/models/hdbscan_global_dev_v1.json` (Phiên bản: `hdbscan-global-dev-v1`, File SHA: `10d2b375d66c8c95...`, Canon SHA: `52672a9974954e05...`)
- **WINDOW:** `config/ml/models/window_baseline_v1.json` (Phiên bản: `window-baseline-v1`, File SHA: `78feda8112fd4a68...`, Canon SHA: `2307ac48bb5583f0...`)

> [!IMPORTANT]
> **RANH GIỚI KHOA HỌC BẮT BUỘC:**
> 1. **Kết quả hồi cứu (Retrospective Candidates):** Tất cả các cụm chuỗi được phân loại là **ứng viên chuỗi sau chấn (candidate aftershock sequences)** mang tính thống kê hồi cứu trên dữ liệu quan trắc lịch sử.
> 2. **Tuyệt đối KHÔNG dự đoán động đất:** Mô hình hoàn toàn không dự báo thời gian, tọa độ hoặc độ lớn của các trận động đất trong tương lai, và không đưa ra tuyên bố về quan hệ nhân quả vật lý trực tiếp.
> 3. **Không dùng Accuracy / F1:** Bài toán phân cụm mật độ không giám sát không có nhãn ground-truth tự nhiên tuyệt đối. Đánh giá dựa trên tính nhất quán vật lý (Luật Omori), chất lượng phân cụm (DBCV), độ ổn định (Jaccard, ARI) và tỷ lệ lỗi.
> 4. **Bảo toàn mẫu số (Denominator Integrity):** Toàn bộ các cửa sổ thất bại được giữ nguyên trong mẫu số tính tỷ lệ lỗi, không bị ẩn khỏi báo cáo.

## 2. Bảng Đối soát So sánh 4 Thuật toán (Algorithm Comparison Table)

| Thuật toán | Tổng Cửa sổ | Thành công | Thất bại | Tỷ lệ Lỗi | Tổng Sau chấn | Nhiễu TB | DBCV TB | Omori p TB | Cửa sổ Omori | Độ ổn định Jaccard | Độ ổn định ARI | Thời gian (s) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **WINDOW** | 2 | 2 | 0 | 0.0% | 255 | 0.0% | N/A | 0.89 | 1/2 | 1.0000 | 1.0000 | 1.76 |
| **DBSCAN** | 2 | 1 | 1 | 50.0% | 240 | 2.9% | 0.1676 | 0.92 | 1/2 | 0.9880 | 0.9483 | 1.39 |
| **HDBSCAN_GLOBAL** | 2 | 0 | 2 | 100.0% | 0 | N/A | 0.1509 | N/A | 0/2 | 0.5977 | 0.6339 | 1.54 |
| **HDBSCAN_ADAPTIVE** | 2 | 0 | 2 | 100.0% | 0 | N/A | N/A | N/A | 0/2 | 1.0000 | 0.6714 | 0.97 |

## 3. Phân tích Chi tiết Từng Mô hình

### Mô hình DBSCAN

- **Tổng số cửa sổ:** `2` (Thành công: `1`, Thất bại: `1`, Tỷ lệ lỗi: `50.0%`)
- **Lý do thất bại ghi nhận:** `['EXP_MAINSHOCK_CLASSIFIED_AS_NOISE']`
- **Tổng số sau chấn (POST sequence members):** `240`
- **Tỷ lệ nhiễu trung bình:** `2.9%`
- **DBCV trung bình:** `0.1676` (Cửa sổ tính được: `1`)
- **Modified Omori law decay exponent p:** `0.92` (R² fit: `1.00`, Cửa sổ fit: `1/2`)
- **Độ ổn định Jaccard (Feature Perturbation):** `0.9880`
- **Độ ổn định ARI (Cluster Partitioning):** `0.9483`
- **Thời gian thực thi:** `1.39` giây

### Mô hình HDBSCAN_ADAPTIVE

- **Tổng số cửa sổ:** `2` (Thành công: `0`, Thất bại: `2`, Tỷ lệ lỗi: `100.0%`)
- **Lý do thất bại ghi nhận:** `['EXP_MAINSHOCK_CLASSIFIED_AS_NOISE', 'EXP_MAINSHOCK_CLASSIFIED_AS_NOISE']`
- **Tổng số sau chấn (POST sequence members):** `0`
- **Tỷ lệ nhiễu trung bình:** `N/A`
- **DBCV trung bình:** `N/A` (Cửa sổ tính được: `0`)
- **Modified Omori law decay exponent p:** `N/A` (R² fit: `N/A`, Cửa sổ fit: `0/2`)
- **Độ ổn định Jaccard (Feature Perturbation):** `1.0000`
- **Độ ổn định ARI (Cluster Partitioning):** `0.6714`
- **Thời gian thực thi:** `0.97` giây

### Mô hình HDBSCAN_GLOBAL

- **Tổng số cửa sổ:** `2` (Thành công: `0`, Thất bại: `2`, Tỷ lệ lỗi: `100.0%`)
- **Lý do thất bại ghi nhận:** `['EXP_MAINSHOCK_CLASSIFIED_AS_NOISE', 'EXP_MAINSHOCK_CLASSIFIED_AS_NOISE']`
- **Tổng số sau chấn (POST sequence members):** `0`
- **Tỷ lệ nhiễu trung bình:** `N/A`
- **DBCV trung bình:** `0.1509` (Cửa sổ tính được: `1`)
- **Modified Omori law decay exponent p:** `N/A` (R² fit: `N/A`, Cửa sổ fit: `0/2`)
- **Độ ổn định Jaccard (Feature Perturbation):** `0.5977`
- **Độ ổn định ARI (Cluster Partitioning):** `0.6339`
- **Thời gian thực thi:** `1.54` giây

### Mô hình WINDOW

- **Tổng số cửa sổ:** `2` (Thành công: `2`, Thất bại: `0`, Tỷ lệ lỗi: `0.0%`)
- **Lý do thất bại ghi nhận:** `['Không có']`
- **Tổng số sau chấn (POST sequence members):** `255`
- **Tỷ lệ nhiễu trung bình:** `0.0%`
- **DBCV trung bình:** `N/A` (Cửa sổ tính được: `0`)
- **Modified Omori law decay exponent p:** `0.89` (R² fit: `1.00`, Cửa sổ fit: `1/2`)
- **Độ ổn định Jaccard (Feature Perturbation):** `1.0000`
- **Độ ổn định ARI (Cluster Partitioning):** `1.0000`
- **Thời gian thực thi:** `1.76` giây

## 4. Đối soát Trino Views và Phân giải Đa chuỗi (Lineage & Resolution Notes)

1. **Khớp nối Trino Serving Views:**
   - Toàn bộ các chỉ số trong bảng đối soát trên khớp chính xác với kết quả truy vấn từ các serving view `iceberg.ml.v_algorithm_comparison` và `iceberg.ml.v_experiment_runs`.
   - Cổng công bố `experiment_status IN ('CANDIDATE', 'APPROVED')` ngăn ngừa rò rỉ dữ liệu thử nghiệm chưa kiểm định vào lớp phục vụ.
2. **Phân giải Đa chuỗi (Multi-Sequence Resolution):**
   - Báo cáo phân biệt rõ giữa **Raw Aftershock Memberships** (tổng số lần gán nhãn POST độc lập theo từng cửa sổ) và **Unique Resolved Aftershocks** (`resolution_rank = 1` gán cho một mainshock duy nhất).
   - Không dùng `COUNT(*)` đơn thuần để suy ra tổng số dư chấn duy nhất trên toàn danh mục.
3. **Bảo tồn Trạng thái Thất bại:**
   - Các cửa sổ có mainshock bị phân cụm mật độ gán nhãn `-1` (nhiễu) được ghi nhận mã `EXP_MAINSHOCK_CLASSIFIED_AS_NOISE` và bảo toàn trong mẫu số.
