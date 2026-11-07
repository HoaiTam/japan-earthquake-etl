---
document_id: "ML-DICT-01"
document_version: "1.0"
status: "Active"
related_tasks: ["MLI-04", "EXP-04", "MLI-03", "CON-03-ML"]
---

# Từ điển Dữ liệu Serving Views và Báo cáo Thực nghiệm ML (ML Serving Data Dictionary)

Tài liệu này định nghĩa chi tiết ngữ nghĩa, kiểu dữ liệu, quy tắc xử lý giá trị khuyết thiếu (`NULL`), các mã lý do (`reason codes`) và nguyên tắc diễn giải khoa học cho lớp serving view Trino và báo cáo thực nghiệm tĩnh thuộc namespace `ml.*`.

---

## 1. Nguyên tắc Cốt lõi và Ranh giới Khoa học

> [!IMPORTANT]
> 1. **Bản chất hồi cứu (Retrospective Candidates):** Tất cả các cụm chuỗi động đất được phân lớp là các **ứng viên cụm chuỗi sau chấn (candidate aftershock sequence clusters)** mang tính thống kê hồi cứu trên dữ liệu quan trắc lịch sử. Mô hình **tuyệt đối không dự đoán thời gian, địa điểm, độ lớn của động đất tương lai** và **không khẳng định quan hệ nhân quả vật lý trực tiếp** giữa các chấn động.
> 2. **Tuyệt đối không dùng Accuracy / F1:** Bài toán phân cụm chuỗi động đất không có nhãn ground-truth tuyệt đối trong tự nhiên. Việc đánh giá model bắt buộc dựa trên đa chiều bằng chứng: chất lượng phân cụm (DBCV), tính nhất quán vật lý (Modified Omori Law), độ ổn định nhiễu (Jaccard / ARI) và tỷ lệ thất bại của thuật toán.
> 3. **Bảo toàn mẫu số (Denominator Integrity):** Toàn bộ các cửa sổ thất bại (`FAILED`) hoặc bị bỏ qua (`SKIPPED`) phải được giữ nguyên trong mẫu số tính toán tỷ lệ lỗi. Nghiêm cấm loại bỏ ngầm các cửa sổ lỗi khỏi bảng tổng hợp hoặc chỉ báo cáo trên tập con thành công.
> 4. **Chính sách NULL trung thực:** Không bao giờ chuyển đổi các metric không áp dụng hoặc tính toán thất bại thành `0`. Metric thiếu phải giữ `NULL` và ghi nhận mã lý do tương ứng (`METRIC_NOT_APPLICABLE`, `METRIC_INSUFFICIENT_MEMBERS`, `METRIC_FIT_FAILED`).
> 5. **Cổng công bố (Publication Gate):** Trino serving views chỉ phục vụ các thực nghiệm đã vượt qua kiểm định lakehouse với trạng thái `CANDIDATE` hoặc `APPROVED`. Kết quả thử nghiệm pilot phải được gắn nhãn tách biệt `DEVELOPMENT_ONLY`.

---

## 2. Danh mục Trino Serving Views

| View Name | Tệp Định nghĩa | Mục đích Nghiệp vụ / Khoa học |
| :--- | :--- | :--- |
| `iceberg.ml.v_experiment_runs` | `sql/ml/v_experiment_runs.sql` | Danh sách các lần chạy thực nghiệm hợp lệ kèm lineage về Gold snapshot |
| `iceberg.ml.v_algorithm_comparison` | `sql/ml/v_algorithm_comparison.sql` | Bảng tổng hợp so sánh 4 thuật toán trên cùng một dataset |
| `iceberg.ml.v_sequence_memberships_resolved` | `sql/ml/v_sequence_memberships_resolved.sql` | Chi tiết gán nhãn thành viên, phân tách raw membership và resolved unique candidate |
| `iceberg.ml.v_failure_windows` | `sql/ml/v_failure_windows.sql` | Chi tiết các cửa sổ thất bại kèm mã lý do phục vụ audit chất lượng |

---

## 3. Chi tiết Thuộc tính Từng View

### 3.1. `iceberg.ml.v_experiment_runs`

Grain: Một dòng cho mỗi `experiment_run_id`.

| Tên Cột | Kiểu Trino | Cho phép NULL | Diễn giải & Quy tắc Nghiệp vụ |
| :--- | :--- | :---: | :--- |
| `experiment_run_id` | `VARCHAR` | Không | Định danh duy nhất của lần chạy thực nghiệm (VD: `exp_196b6d...`). |
| `algorithm_name` | `VARCHAR` | Không | Tên thuật toán (`WINDOW`, `DBSCAN`, `HDBSCAN_GLOBAL`, `HDBSCAN_ADAPTIVE`). |
| `model_config_version` | `VARCHAR` | Không | Phiên bản cấu hình tham số mô hình (VD: `hdbscan-adaptive-dev-v1`). |
| `algorithm_version` | `VARCHAR` | Không | Phiên bản logic thuật toán triển khai. |
| `package_version` | `VARCHAR` | Không | Phiên bản gói thư viện ML / runtime. |
| `code_version` | `VARCHAR` | Không | Git commit SHA hoặc mã băm phiên bản code thi hành. |
| `experiment_status` | `VARCHAR` | Không | Trạng thái vòng đời (`CANDIDATE`, `APPROVED`). View lọc cố định 2 trạng thái này. |
| `dataset_id` | `VARCHAR` | Không | FK tham chiếu tới `ml.dataset_manifest`. |
| `dataset_split` | `VARCHAR` | Không | Phân vùng thời gian (`REPRODUCTION` hoặc `EXTENSION`). |
| `gold_table_name` | `VARCHAR` | Không | Tên bảng Gold được sử dụng làm đầu vào (VD: `iceberg.gold.earthquake_event_current`). |
| `gold_snapshot_id` | `BIGINT` | Không | Snapshot ID bất biến của bảng Gold được pin khi build dataset. |
| `period_start_utc` | `TIMESTAMP(6) WITH TIME ZONE` | Không | Biên thời gian bắt đầu cửa sổ dữ liệu (UTC, bao gồm). |
| `period_end_utc_exclusive` | `TIMESTAMP(6) WITH TIME ZONE` | Không | Biên thời gian kết thúc cửa sổ dữ liệu (UTC, không bao gồm). |
| `feature_version` | `VARCHAR` | Không | Phiên bản đặc trưng 4-D chuẩn hóa không-thời gian. |
| `scaling_config_version` | `VARCHAR` | Không | Phiên bản trọng số tỷ lệ khoảng cách - thời gian. |
| `membership_row_count` | `BIGINT` | Không | Tổng số dòng ghi nhận trong bảng `ml.sequence_membership`. |
| `summary_row_count` | `BIGINT` | Không | Tổng số cửa sổ mainshock được tổng hợp trong `ml.sequence_summary`. |
| `artifact_uri` | `VARCHAR` | Không | Đường dẫn URI lưu trữ bundle kết quả trên MinIO / S3. |
| `result_bundle_sha256` | `VARCHAR` | Không | Checksum SHA-256 xác thực tính toàn vẹn của artifact bundle. |
| `completed_at_utc` | `TIMESTAMP(6) WITH TIME ZONE` | Không | Thời điểm hoàn thành huấn luyện/chạy mô hình (UTC). |
| `created_at_utc` | `TIMESTAMP(6) WITH TIME ZONE` | Không | Thời điểm commit bản ghi vào Iceberg catalog (UTC). |

---

### 3.2. `iceberg.ml.v_algorithm_comparison`

Grain: `(dataset_id, algorithm_name, model_config_version, experiment_status)`.

| Tên Cột | Kiểu Trino | Cho phép NULL | Diễn giải & Quy tắc Nghiệp vụ |
| :--- | :--- | :---: | :--- |
| `dataset_id` | `VARCHAR` | Không | Định danh tập dữ liệu đầu vào. |
| `algorithm_name` | `VARCHAR` | Không | Thuật toán so sánh. |
| `model_config_version` | `VARCHAR` | Không | Phiên bản cấu hình thuật toán. |
| `experiment_status` | `VARCHAR` | Không | Trạng thái công bố (`CANDIDATE` hoặc `APPROVED`). |
| `total_windows` | `BIGINT` | Không | Mẫu số toàn phần: Tổng số cửa sổ mainshock được đánh giá. |
| `success_windows` | `BIGINT` | Không | Số lượng cửa sổ có `window_result_status = 'SUCCESS'`. |
| `failure_windows` | `BIGINT` | Không | Số lượng cửa sổ có `window_result_status = 'FAILED'`. |
| `skipped_windows` | `BIGINT` | Không | Số lượng cửa sổ bị bỏ qua (`SKIPPED`). |
| `failure_rate` | `DOUBLE` | Có | Tỷ lệ lỗi: `failure_windows / total_windows`. NULL nếu total_windows = 0. |
| `total_candidates` | `BIGINT` | Không | Tổng số sự kiện ứng viên nằm trong các hộp không gian - thời gian. |
| `total_sequence_members` | `BIGINT` | Có | Tổng số sự kiện được mô hình gán nhãn thành viên chuỗi (`PRE` + `POST`). |
| `raw_aftershock_memberships` | `BIGINT` | Có | Tổng số bản ghi thành viên sau chấn (`POST`) trước khi giải quyết chồng lấn. |
| `total_foreshock_candidates` | `BIGINT` | Có | Tổng số bản ghi tiền chấn (`PRE`) được thuật toán xác định. |
| `avg_noise_ratio` | `DOUBLE` | Có | Tỷ lệ nhiễu trung bình trên các cửa sổ. NULL đối với `WINDOW`. |
| `avg_dbcv_score` | `DOUBLE` | Có | Điểm DBCV trung bình. NULL đối với `WINDOW` hoặc cửa sổ không tính được. |
| `dbcv_computed_windows` | `BIGINT` | Không | Số lượng cửa sổ tính được điểm DBCV hợp lệ. |
| `avg_omori_p` | `DOUBLE` | Có | Tham số suy giảm Omori $p$ trung bình. NULL nếu không có cửa sổ nào fit được. |
| `omori_fitted_windows` | `BIGINT` | Không | Số lượng cửa sổ đạt điều kiện $N \ge 5$ và khớp hội tụ thành công. |
| `avg_runtime_seconds` | `DOUBLE` | Không | Thời gian thực thi trung bình mỗi cửa sổ (giây). |
| `avg_peak_memory_mb` | `DOUBLE` | Có | Dung lượng RAM đỉnh trung bình đo được (MB). NULL nếu môi trường không hỗ trợ. |

---

### 3.3. `iceberg.ml.v_sequence_memberships_resolved`

Grain: `(experiment_run_id, mainshock_event_id, candidate_event_id)`.

| Tên Cột | Kiểu Trino | Cho phép NULL | Diễn giải & Quy tắc Nghiệp vụ |
| :--- | :--- | :---: | :--- |
| `experiment_run_id` | `VARCHAR` | Không | Định danh lần chạy thực nghiệm. |
| `dataset_id` | `VARCHAR` | Không | Định danh tập dữ liệu. |
| `mainshock_event_id` | `VARCHAR` | Không | ID chấn động chính định vị cửa sổ. |
| `candidate_event_id` | `VARCHAR` | Không | ID sự kiện ứng viên (khớp với Gold `canonical_event_id`). |
| `cluster_id` | `BIGINT` | Có | ID cụm gán bởi thuật toán phân cụm mật độ. `-1` là nhiễu, NULL cho WINDOW. |
| `is_noise` | `BOOLEAN` | Có | Cờ xác định sự kiện là nhiễu (`true`/`false`). NULL cho WINDOW. |
| `is_mainshock_cluster_member`| `BOOLEAN` | Có | Sự kiện có thuộc cùng cụm với mainshock hay không. `false` nếu mainshock là nhiễu. |
| `is_sequence_member` | `BOOLEAN` | Không | Kết luận sự kiện có thuộc chuỗi động đất hay không theo quy tắc thuật toán. |
| `membership_probability` | `DOUBLE` | Có | Xác suất thuộc cụm `[0, 1]` (chỉ có ở HDBSCAN). NULL cho WINDOW/DBSCAN. |
| `event_role_candidate` | `VARCHAR` | Không | Vai trò theo thời gian đối với mainshock: `PRE`, `MAINSHOCK`, `POST`. |
| `normalized_distance` | `DOUBLE` | Không | Khoảng cách không-thời gian 4-D chuẩn hóa tới mainshock (dùng tie-break). |
| `multi_sequence_status` | `VARCHAR` | Không | Trạng thái đa chuỗi (`NOT_MEMBER`, `UNIQUE`, `MULTIPLE`, `AMBIGUOUS`). |
| `resolution_rank` | `INTEGER` | Có | Thứ hạng ưu tiên khi giải quyết đa chuỗi. `1` là sự kiện thắng cuộc. |
| `is_resolved_unique_aftershock`| `BOOLEAN`| Không | Cờ boolean xác định sự kiện là dư chấn duy nhất sau khi phân giải chồng lấn. |
| `created_at_utc` | `TIMESTAMP(6) WITH TIME ZONE` | Không | Thời điểm bản ghi được tạo. |

---

### 3.4. `iceberg.ml.v_failure_windows`

Grain: `(experiment_run_id, mainshock_event_id)`.

| Tên Cột | Kiểu Trino | Cho phép NULL | Diễn giải & Quy tắc Nghiệp vụ |
| :--- | :--- | :---: | :--- |
| `dataset_id` | `VARCHAR` | Không | Định danh tập dữ liệu. |
| `experiment_run_id` | `VARCHAR` | Không | Định danh lần chạy thực nghiệm. |
| `algorithm_name` | `VARCHAR` | Không | Thuật toán gặp lỗi. |
| `model_config_version` | `VARCHAR` | Không | Phiên bản cấu hình thuật toán. |
| `mainshock_event_id` | `VARCHAR` | Không | ID của mainshock có cửa sổ bị lỗi hoặc bị bỏ qua. |
| `window_result_status` | `VARCHAR` | Không | Trạng thái kết quả cửa sổ (`FAILED` hoặc `SKIPPED`). |
| `failure_reason_code` | `VARCHAR` | Không | Mã lý do kỹ thuật/vật lý giải thích thất bại. Bắt buộc không được NULL. |
| `candidate_count` | `BIGINT` | Không | Số lượng ứng viên trong cửa sổ tại thời điểm xảy ra lỗi. |
| `runtime_seconds` | `DOUBLE` | Không | Thời gian thực thi tiêu tốn trước khi kết thúc cửa sổ. |
| `created_at_utc` | `TIMESTAMP(6) WITH TIME ZONE` | Không | Thời điểm ghi nhận lỗi. |

---

## 4. Bảng Tra Cứu Mã Lý Do (Reason Codes Taxonomy)

### 4.1. Mã Thất bại Cửa sổ Thực nghiệm (`failure_reason_code`)

| Mã Lý Do | Ngữ cảnh Xuất hiện | Giải thích Chi tiết |
| :--- | :--- | :--- |
| `EXP_MAINSHOCK_CLASSIFIED_AS_NOISE` | DBSCAN / HDBSCAN | Thuật toán phân cụm mật độ gán nhãn `-1` (nhiễu) cho chính mainshock. Khi tâm chấn chính bị coi là nhiễu, toàn bộ cửa sổ không thể hình thành chuỗi dư chấn hợp lệ và chuyển trạng thái `FAILED`. |
| `EXP_EMPTY_POST_SEQUENCE` | Mọi thuật toán | Không có sự kiện ứng viên nào xảy ra sau mainshock vượt qua ngưỡng độ đầy đủ (`Mc`). Cửa sổ kết luận không có dư chấn. |
| `EXP_WINDOW_FAILED` | Mọi thuật toán | Lỗi ngoại lệ trong quá trình tính toán (ví dụ: tràn bộ nhớ ma trận khoảng cách, lỗi thư viện). |
| `ML_RESOURCE_LIMIT_EXCEEDED` | Tiền xử lý | Số lượng ứng viên trong cửa sổ vượt ngưỡng giới hạn an toàn (hard guard limit). |

### 4.2. Mã Lý Do Thuộc tính Khoa học (`metric_reason_codes`)

| Mã Lý Do | Metric Bị Ảnh Hưởng | Giải thích Chi tiết |
| :--- | :--- | :--- |
| `METRIC_NOT_APPLICABLE` | `dbcv_score`, `noise_ratio` | Metric không có ý nghĩa toán học cho mô hình (ví dụ: DBCV cho thuật toán `WINDOW` dựa trên hình học hộp đơn thuần). |
| `METRIC_INSUFFICIENT_MEMBERS` | `omori_p`, `omori_fit_metric` | Số lượng dư chấn sau chấn (`POST`) không đạt ngưỡng tối thiểu thống kê ($N < 5$) để khớp phương trình vi phân suy giảm Omori. |
| `METRIC_FIT_FAILED` | `omori_p`, `stability_components_json` | Thuật toán tối ưu hóa phi tuyến (`curve_fit`) không hội tụ hoặc gặp lỗi ma trận hiệp phương sai. |
| `METRIC_RUNTIME_UNAVAILABLE` | `peak_memory_mb` | Môi trường hệ điều hành hoặc phần cứng không cung cấp bộ đếm bộ nhớ tiến trình chuẩn. |

---

## 5. Hướng dẫn Phân biệt Raw Membership và Resolved Unique Candidates

Khi tổng hợp số lượng dư chấn trên quy mô toàn bộ danh mục động đất (`catalog-wide`), một sự kiện động đất có thể rơi vào vùng giao nhau của hai cửa sổ mainshock liên tiếp:

1. **Raw Membership Count (`raw_aftershock_memberships`):**
   - Đếm số dòng có `is_sequence_member = true AND event_role_candidate = 'POST'` trong `ml.sequence_membership`.
   - Phản ánh tổng số lần các thuật toán gán nhãn sự kiện là dư chấn độc lập theo từng cửa sổ riêng lẻ.
   - Thường lớn hơn số sự kiện thực tế nếu có nhiều mainshock xảy ra gần nhau về không-thời gian.

2. **Resolved Unique Candidates (`unique_resolved_aftershocks`):**
   - Đếm số sự kiện duy nhất (`COUNT(DISTINCT candidate_event_id)`) có `is_sequence_member = true AND event_role_candidate = 'POST' AND resolution_rank = 1`.
   - Sử dụng quy tắc phân giải khoảng cách chuẩn hóa nhỏ nhất (`normalized_distance`) để gán dứt khoát sự kiện cho đúng một chuỗi chính.
   - Phục vụ báo cáo khoa học không trùng lặp (không double-count).
