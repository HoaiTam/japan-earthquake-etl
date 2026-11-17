# EXP-04: Đánh giá Khoa học & So sánh 4 Mô hình trên Official REPRODUCTION

## 1. Thông tin Thực nghiệm và Danh tính Dataset Chính thức

- **Mã Task:** [EXP-04](../task/tasks/EXP-04.md) (Khối H - Experiment, P0 Core)
- **Trạng thái:** `PASSED` (`OFFICIAL_RESEARCH`)
- **Thời điểm đánh giá (UTC):** `2026-11-17T18:39:52Z`
- **Dataset Split / Status:** `REPRODUCTION` (`EXPORTED`)
- **Dataset ID:** `ds_907fe748fafdfc0f072ce4db4e1cf0aa619ed18877129e17c8440b43062c3d87`
- **Collection ID:** `col_bd10c69365da841b2cf6a62172e2350a797001eac04ce4bc6f2277a4d150ad98` (`2` shards)
- **Gold Publication ID:** `pub_832ce36ac875f546528718c3545442a77a0097f7573379f30ed92f2a43c481f4` (Snapshot: `7580315846938041321`)
- **Số lượng ứng viên đầu vào:** `156710` dòng (`194` mainshock windows, Global $M_c = 0.5$)
- **Manifest / Checksums SHA-256:** `d3ee1334bfc50005a527f7b32a8988fcb1bdd7659a7dd4e84de326a7735139ae` / `4768e500754c569bc951f23e2ac14fa9b9703ae69fbd5f78de06eacbb72f7ae9`
- **Team Handoff SHA-256:** `6cf0af7c2a3d8e8ec4c993f84ed6d86cdc417324102133dd3068eb2ebc40d297`
- **Git Commit Pin:** `a9c0d4ef07616279f8b2bf487a294258a8021e03` (modified/tracked)
- **Random Seed / Repeats / Sigma:** `42` / `5` / `0.01`
- **Python / Packages:** Python `3.12.10`, `scikit-learn==1.6.1`, `hdbscan==0.8.40`, `scipy==1.15.1`
- **Scientific Approval:** `false`

### Cấu hình Thuật toán được Khóa (`frozen_extension_rule` cho EXP-05):
- **WINDOW:** `config/ml/models/window_baseline_v1.json` (Version: `window-baseline-v1`, File SHA: `78feda8112fd4a68...`, Canonical SHA: `2307ac48bb5583f0...`)
- **DBSCAN:** `config/ml/models/dbscan_global_dev_v1.json` (Version: `dbscan-global-dev-v1`, File SHA: `f089d928f4ace1c6...`, Canonical SHA: `0f280869d059dd2d...`)
- **HDBSCAN_GLOBAL:** `config/ml/models/hdbscan_global_dev_v1.json` (Version: `hdbscan-global-dev-v1`, File SHA: `10d2b375d66c8c95...`, Canonical SHA: `52672a9974954e05...`)
- **HDBSCAN_ADAPTIVE:** `config/ml/models/hdbscan_adaptive_dev_v1.json` (Version: `hdbscan-adaptive-dev-v1`, File SHA: `41a4232ad6e4ae50...`, Canonical SHA: `55c8077ef11023de...`)

## 2. Bảng Đối soát So sánh 4 Thuật toán (Official Reproduction Comparison Table)

| Algorithm | Total Windows | Success Windows | Failure Windows | Failure Rate | Total Aftershocks | Mean Noise Ratio | Mean DBCV | Mean Omori p | Omori Fitted Windows | Mean Jaccard Stability | Mean ARI Stability | Runtime (s) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| WINDOW | 194 | 194 | 0 | 0.0% | 135853 | 0.0% | N/A | 0.55 | 155/194 | 1.0000 | 1.0000 | 24.08 |
| DBSCAN | 194 | 128 | 66 | 34.0% | 118487 | 20.6% | 0.1195 | 0.70 | 106/194 | 0.9743 | 0.9456 | 148.84 |
| HDBSCAN_GLOBAL | 194 | 142 | 52 | 26.8% | 15118 | 16.0% | 0.3244 | 0.82 | 99/194 | 0.8718 | 0.8164 | 203.51 |
| HDBSCAN_ADAPTIVE | 194 | 131 | 63 | 32.5% | 98795 | 31.9% | 0.3178 | 0.71 | 124/194 | 0.9342 | 0.8727 | 222.01 |

### Nguyên tắc Đánh giá & Toàn vẹn Mẫu số:
- **Không dùng Accuracy / F1:** Clustering chuỗi sau chấn không có ground-truth nhãn cố định.
- **Bảo toàn mẫu số (`Denominator Integrity`):** Toàn bộ `194` windows (bao gồm window `FAILED` / mainshock noise) giữ nguyên trong mẫu số `failure_rate = failure_windows / total_windows`.
- **Honest NULL & Reason Codes:** Mọi giá trị thiếu/không áp dụng (`WINDOW` DBCV -> `METRIC_NOT_APPLICABLE`, thiếu mẫu Omori $N<5$ hoặc all-noise -> `METRIC_INSUFFICIENT_MEMBERS`, fit thất bại -> `METRIC_FIT_FAILED`) giữ `NULL`/`N/A`, tuyệt đối không ép về `0`.
- **DBCV Memory-Safe Bound:** Sử dụng `hdbscan.validity.validity_index` trực tiếp cho cụm kích thước $\le 2000$ điểm và thuật toán chunked $O(N)$-memory theo cùng công thức Moulavi (2014) cho cụm lớn hơn để tránh bùng nổ bộ nhớ trên cụm DBSCAN lớn.

## 3. Phân tích Chi tiết Từng Thuật toán (kèm Reason Codes & Sample Sizes)

### WINDOW

- **Cửa sổ:** `194` (Thành công: `194`, Thất bại: `0`, Tỷ lệ thất bại: `0.0%`)
- **Phân bổ Failure Reason:** `{}`
- **Tổng dư chấn POST per-window:** `135853` | **Noise ratio trung bình:** `0.0%`
- **DBCV trung bình:** `N/A` (tính được trên `0/194` windows; N/A reasons: `{"METRIC_NOT_APPLICABLE": 194}`)
- **Modified Omori decay:** mean `p` = `0.5475`, mean $R^2$ = `0.9565` (fit được trên `155/194` windows; N/A reasons: `{"METRIC_INSUFFICIENT_MEMBERS": 39}`)
- **Độ ổn định (5x Gaussian jitter $\sigma=0.01$):** Mean Jaccard = `1.0000`, Mean ARI = `1.0000` (`194/194` windows; reasons: `{}`)
- **Runtime (gồm fit + DBCV + Omori + 5x stability):** `24.08`s

### DBSCAN

- **Cửa sổ:** `194` (Thành công: `128`, Thất bại: `66`, Tỷ lệ thất bại: `34.0%`)
- **Phân bổ Failure Reason:** `{"EXP_MAINSHOCK_CLASSIFIED_AS_NOISE": 66}`
- **Tổng dư chấn POST per-window:** `118487` | **Noise ratio trung bình:** `20.6%`
- **DBCV trung bình:** `0.1195` (tính được trên `98/194` windows; N/A reasons: `{"METRIC_FIT_FAILED": 49, "METRIC_INSUFFICIENT_MEMBERS": 47}`)
- **Modified Omori decay:** mean `p` = `0.7013`, mean $R^2$ = `0.9744` (fit được trên `106/194` windows; N/A reasons: `{"METRIC_INSUFFICIENT_MEMBERS": 88}`)
- **Độ ổn định (5x Gaussian jitter $\sigma=0.01$):** Mean Jaccard = `0.9743`, Mean ARI = `0.9456` (`194/194` windows; reasons: `{}`)
- **Runtime (gồm fit + DBCV + Omori + 5x stability):** `148.84`s

### HDBSCAN_GLOBAL

- **Cửa sổ:** `194` (Thành công: `142`, Thất bại: `52`, Tỷ lệ thất bại: `26.8%`)
- **Phân bổ Failure Reason:** `{"EXP_MAINSHOCK_CLASSIFIED_AS_NOISE": 52}`
- **Tổng dư chấn POST per-window:** `15118` | **Noise ratio trung bình:** `16.0%`
- **DBCV trung bình:** `0.3244` (tính được trên `101/194` windows; N/A reasons: `{"METRIC_FIT_FAILED": 65, "METRIC_INSUFFICIENT_MEMBERS": 28}`)
- **Modified Omori decay:** mean `p` = `0.8220`, mean $R^2$ = `0.9540` (fit được trên `99/194` windows; N/A reasons: `{"METRIC_INSUFFICIENT_MEMBERS": 95}`)
- **Độ ổn định (5x Gaussian jitter $\sigma=0.01$):** Mean Jaccard = `0.8718`, Mean ARI = `0.8164` (`194/194` windows; reasons: `{}`)
- **Runtime (gồm fit + DBCV + Omori + 5x stability):** `203.51`s

### HDBSCAN_ADAPTIVE

- **Cửa sổ:** `194` (Thành công: `131`, Thất bại: `63`, Tỷ lệ thất bại: `32.5%`)
- **Phân bổ Failure Reason:** `{"EXP_MAINSHOCK_CLASSIFIED_AS_NOISE": 63}`
- **Tổng dư chấn POST per-window:** `98795` | **Noise ratio trung bình:** `31.9%`
- **DBCV trung bình:** `0.3178` (tính được trên `32/194` windows; N/A reasons: `{"METRIC_FIT_FAILED": 125, "METRIC_INSUFFICIENT_MEMBERS": 37}`)
- **Modified Omori decay:** mean `p` = `0.7114`, mean $R^2$ = `0.9542` (fit được trên `124/194` windows; N/A reasons: `{"METRIC_INSUFFICIENT_MEMBERS": 70}`)
- **Độ ổn định (5x Gaussian jitter $\sigma=0.01$):** Mean Jaccard = `0.9342`, Mean ARI = `0.8727` (`194/194` windows; reasons: `{}`)
- **Runtime (gồm fit + DBCV + Omori + 5x stability):** `222.01`s

## 4. Đối soát Liên kết với Official Runs EXP-02 & EXP-03

- **EXP-02 Baseline Parity (`WINDOW`, `DBSCAN`, `HDBSCAN_GLOBAL`):** `all_algos_exact_match=True`
- **EXP-03 `HDBSCAN_ADAPTIVE` Cross-Check & Global Entity Resolution (`exp_run_70637f429d590e2ba8258f56b618ba85`):** `exact_match=True`, `raw_aftershocks=98795`, `resolved_unique_aftershocks=88222`, `deduplicated_aftershocks=10573`, `ambiguous_candidates=0`, `nested_mainshocks=23`.

## 5. Frozen Rule cho EXP-05 & Giới hạn Khoa học

- **Rule ID:** `exp04-frozen-reproduction-rule-v1` (khóa từ `REPRODUCTION` dataset `ds_907fe748fafdfc0f072ce4db4e1cf0aa619ed18877129e17c8440b43062c3d87`).
- **Contract Bất biến:** `window_model_version=wm_expanded_v1.0`, `scaling_config_version=scaling-global-research-90k-v1`, `feature_version=relative-4d-v1`, `mc_value=0.5`.
- **Nguyên tắc đọc kết quả:** Không chọn "mô hình thắng" chỉ dựa trên một chỉ số đơn lẻ (Window có `0%` mainshock-failure nhưng không lọc nhiễu nền; DBSCAN/HDBSCAN đánh đổi giữa lọc nhiễu, DBCV, khớp Omori và tỷ lệ mainshock bị xếp nhiễu). Kết quả chỉ là ứng viên chuỗi sau chấn hồi cứu; bước chạy khóa tham số sang giai đoạn 2018–2023 thuộc [EXP-05](../task/tasks/EXP-05.md).
