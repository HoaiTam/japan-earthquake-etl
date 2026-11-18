# EXP-04: Đánh giá Khoa học & So sánh 4 Mô hình trên Official REPRODUCTION

## 1. Thông tin Thực nghiệm và Danh tính Dataset Chính thức

- **Mã Task:** [EXP-04](../task/tasks/EXP-04.md) (Khối H - Experiment, P0 Core)
- **Report Version / Trạng thái:** `exp-04-official-v2` / `PASSED` (`OFFICIAL_RESEARCH`)
- **Frozen Rule Published:** `true`
- **Thời điểm đánh giá (UTC):** `2026-11-18T12:40:35Z`
- **Dataset Split / Status:** `REPRODUCTION` (`EXPORTED`)
- **Dataset ID:** `ds_907fe748fafdfc0f072ce4db4e1cf0aa619ed18877129e17c8440b43062c3d87`
- **Collection ID:** `col_bd10c69365da841b2cf6a62172e2350a797001eac04ce4bc6f2277a4d150ad98` (`2` shards)
- **Gold Publication ID:** `pub_832ce36ac875f546528718c3545442a77a0097f7573379f30ed92f2a43c481f4` (Snapshot: `7580315846938041321`)
- **Số lượng ứng viên đầu vào:** `156710` dòng (`194` mainshock windows, Global $M_c = 0.5$)
- **Manifest / Checksums SHA-256:** `d3ee1334bfc50005a527f7b32a8988fcb1bdd7659a7dd4e84de326a7735139ae` / `4768e500754c569bc951f23e2ac14fa9b9703ae69fbd5f78de06eacbb72f7ae9`
- **Team Handoff SHA-256:** `6cf0af7c2a3d8e8ec4c993f84ed6d86cdc417324102133dd3068eb2ebc40d297`
- **Git Commit Pin:** `c746dc6dc2db0f40cf24c3561ca3ff1c4b79a60d` (clean)
- **Random Seed / Repeats / Sigma:** `42` / `5` / `0.01`
- **Python / Packages:** Python `3.13.3`, `scikit-learn==1.6.1`, `hdbscan==0.8.40`, `scipy==1.15.1`
- **Scientific Approval:** `false`

### Nguồn upstream dùng để phát hành

- [EXP-02 chính thức đã merge #139](./EXP-02_OFFICIAL_REPRODUCTION.json) giữ nguyên.
  [EXP-02 replay cho lần đánh giá này](./EXP-02_REPRODUCTION_REPLAY_FOR_EXP04.json)
  ghim cùng dataset/config; toàn bộ case counts và các count chính của ba baseline
  khớp #139, nhưng run IDs và bytes thuộc lần chạy lại. EXP-04 xác minh physical
  bundles của **replay**, không tuyên bố đã đọc lại artifacts Windows của #139.
- [EXP-03 official của cùng lần nghiệm thu](./EXP-03_OFFICIAL_REPRODUCTION.json)
  và hai shard được kiểm trực tiếp. Lần chạy này có 130/194 adaptive windows thành
  công, so với 131/194 ở [evidence Windows lịch sử](https://github.com/HoaiTam/japan-earthquake-etl/blob/c2d00ff9d28f8acc69d551fec5f51ad55e8f9a39/docs/evidence/EXP-04_OFFICIAL_REPRODUCTION.json).
  Không trộn kết quả hoặc coi hai bộ artifacts thay thế được cho nhau.

### Cấu hình Thuật toán được Khóa (`frozen_extension_rule` cho EXP-05):
- **WINDOW:** `config/ml/models/window_baseline_v1.json` (Version: `window-baseline-v1`, File SHA: `5ba1a7aadb46a00c...`, Canonical SHA: `dfa2ee1635fc19a9...`)
- **DBSCAN:** `config/ml/models/dbscan_global_dev_v1.json` (Version: `dbscan-global-dev-v1`, File SHA: `ab5c5150eefd58c6...`, Canonical SHA: `8b5408356c8dbb71...`)
- **HDBSCAN_GLOBAL:** `config/ml/models/hdbscan_global_dev_v1.json` (Version: `hdbscan-global-dev-v1`, File SHA: `eb8c3e8cf63f1cfa...`, Canonical SHA: `0d05d137dab9702d...`)
- **HDBSCAN_ADAPTIVE:** `config/ml/models/hdbscan_adaptive_dev_v1.json` (Version: `hdbscan-adaptive-dev-v1`, File SHA: `d0f24c8732e47d09...`, Canonical SHA: `22ed180c746ff965...`)

## 2. Bảng Đối soát So sánh 4 Thuật toán (Official Reproduction Comparison Table)

| Algorithm | Total Windows | Success Windows | Failure Windows | Failure Rate | Total Aftershocks | Mean Noise Ratio | Mean DBCV | Mean Omori p | Omori Fitted Windows | Mean Jaccard Stability | Mean ARI Stability | Runtime (s) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| WINDOW | 194 | 194 | 0 | 0.0% | 135853 | 0.0% | N/A | 0.55 | 155/194 | 1.0000 | 1.0000 | 21.17 |
| DBSCAN | 194 | 128 | 66 | 34.0% | 118487 | 20.6% | 0.0912 | 0.70 | 106/194 | 0.9743 | 0.9456 | 73.93 |
| HDBSCAN_GLOBAL | 194 | 142 | 52 | 26.8% | 15118 | 16.0% | 0.3195 | 0.82 | 99/194 | 0.8718 | 0.8164 | 93.48 |
| HDBSCAN_ADAPTIVE | 194 | 130 | 64 | 33.0% | 99067 | 31.1% | 0.3112 | 0.69 | 123/194 | 0.9338 | 0.8730 | 110.35 |

### Nguyên tắc Đánh giá & Toàn vẹn Mẫu số:
- **Không dùng Accuracy / F1:** Clustering chuỗi sau chấn không có ground-truth nhãn cố định.
- **Bảo toàn mẫu số (`Denominator Integrity`):** Toàn bộ `194` windows (bao gồm window `FAILED` / mainshock noise) giữ nguyên trong mẫu số `failure_rate = failure_windows / total_windows`.
- **Honest NULL & Reason Codes:** Mọi giá trị thiếu/không áp dụng (`WINDOW` DBCV -> `METRIC_NOT_APPLICABLE`, thiếu mẫu Omori $N<5$ hoặc all-noise -> `METRIC_INSUFFICIENT_MEMBERS`, fit thất bại -> `METRIC_FIT_FAILED`) giữ `NULL`/`N/A`, tuyệt đối không ép về `0` hoặc fallback sang Silhouette.
- **DBCV Reference-Exact Memory-Safe Path:** Sử dụng `hdbscan.validity.validity_index` trực tiếp cho cụm nhỏ và đường dẫn chunked $O(N)$-memory kèm tie-breaking MST đúng hệt Moulavi (2014) / `hdbscan.validity.internal_minimum_spanning_tree` cho windows có $>50$ cụm hoặc cụm $>2000$ điểm.

## 3. Phân tích Chi tiết Từng Thuật toán (kèm Reason Codes & Sample Sizes)

### WINDOW

- **Cửa sổ:** `194` (Thành công: `194`, Thất bại: `0`, Tỷ lệ thất bại: `0.0%`)
- **Phân bổ Failure Reason:** `{}`
- **Tổng dư chấn POST per-window:** `135853` | **Noise ratio trung bình:** `0.0%`
- **DBCV trung bình:** `N/A` (tính được trên `0/194` windows; N/A reasons: `{"METRIC_NOT_APPLICABLE": 194}`)
- **Modified Omori decay:** mean `p` = `0.5475`, mean $R^2$ = `0.9565` (fit được trên `155/194` windows; N/A reasons: `{"METRIC_INSUFFICIENT_MEMBERS": 39}`)
- **Độ ổn định (5x Gaussian jitter $\sigma=0.01$):** Mean Jaccard = `1.0000`, Mean ARI = `1.0000` (`194/194` windows; reasons: `{}`)
- **Runtime (gồm fit + DBCV + Omori + 5x stability):** `21.17`s

### DBSCAN

- **Cửa sổ:** `194` (Thành công: `128`, Thất bại: `66`, Tỷ lệ thất bại: `34.0%`)
- **Phân bổ Failure Reason:** `{"EXP_MAINSHOCK_CLASSIFIED_AS_NOISE": 66}`
- **Tổng dư chấn POST per-window:** `118487` | **Noise ratio trung bình:** `20.6%`
- **DBCV trung bình:** `0.0912` (tính được trên `98/194` windows; N/A reasons: `{"METRIC_FIT_FAILED": 49, "METRIC_INSUFFICIENT_MEMBERS": 47}`)
- **Modified Omori decay:** mean `p` = `0.7013`, mean $R^2$ = `0.9744` (fit được trên `106/194` windows; N/A reasons: `{"METRIC_INSUFFICIENT_MEMBERS": 88}`)
- **Độ ổn định (5x Gaussian jitter $\sigma=0.01$):** Mean Jaccard = `0.9743`, Mean ARI = `0.9456` (`194/194` windows; reasons: `{}`)
- **Runtime (gồm fit + DBCV + Omori + 5x stability):** `73.93`s

### HDBSCAN_GLOBAL

- **Cửa sổ:** `194` (Thành công: `142`, Thất bại: `52`, Tỷ lệ thất bại: `26.8%`)
- **Phân bổ Failure Reason:** `{"EXP_MAINSHOCK_CLASSIFIED_AS_NOISE": 52}`
- **Tổng dư chấn POST per-window:** `15118` | **Noise ratio trung bình:** `16.0%`
- **DBCV trung bình:** `0.3195` (tính được trên `101/194` windows; N/A reasons: `{"METRIC_FIT_FAILED": 65, "METRIC_INSUFFICIENT_MEMBERS": 28}`)
- **Modified Omori decay:** mean `p` = `0.8220`, mean $R^2$ = `0.9540` (fit được trên `99/194` windows; N/A reasons: `{"METRIC_INSUFFICIENT_MEMBERS": 95}`)
- **Độ ổn định (5x Gaussian jitter $\sigma=0.01$):** Mean Jaccard = `0.8718`, Mean ARI = `0.8164` (`194/194` windows; reasons: `{}`)
- **Runtime (gồm fit + DBCV + Omori + 5x stability):** `93.48`s

### HDBSCAN_ADAPTIVE

- **Cửa sổ:** `194` (Thành công: `130`, Thất bại: `64`, Tỷ lệ thất bại: `33.0%`)
- **Phân bổ Failure Reason:** `{"EXP_MAINSHOCK_CLASSIFIED_AS_NOISE": 64}`
- **Tổng dư chấn POST per-window:** `99067` | **Noise ratio trung bình:** `31.1%`
- **DBCV trung bình:** `0.3112` (tính được trên `32/194` windows; N/A reasons: `{"METRIC_FIT_FAILED": 125, "METRIC_INSUFFICIENT_MEMBERS": 37}`)
- **Modified Omori decay:** mean `p` = `0.6942`, mean $R^2$ = `0.9546` (fit được trên `123/194` windows; N/A reasons: `{"METRIC_INSUFFICIENT_MEMBERS": 71}`)
- **Độ ổn định (5x Gaussian jitter $\sigma=0.01$):** Mean Jaccard = `0.9338`, Mean ARI = `0.8730` (`194/194` windows; reasons: `{}`)
- **Runtime (gồm fit + DBCV + Omori + 5x stability):** `110.35`s

## 4. Đối soát Liên kết & Physical Bundle Parity với EXP-02 & EXP-03

- **EXP-02 Official Parity (`WINDOW`, `DBSCAN`, `HDBSCAN_GLOBAL`):** `report_sha256_verified=True`, `dataset_and_pins_verified=True`, `config_pins_verified=True`, `physical_bundles_verified=True`, `physical_shards_verified=True`, `primary_membership_parity_verified=True`, `all_checks_passed=True` (report SHA: `014541a0ba58594a8254420d2fbd9e72e4abaca4ada8581bec375ccba33d2142`)
- **EXP-03 `HDBSCAN_ADAPTIVE` Cross-Check & Physical Bundle Parity (`exp_run_ade47fe60932cbcd046100b023a89e3c`):** `report_sha256_verified=True`, `dataset_and_pins_verified=True`, `config_pins_verified=True`, `physical_bundles_verified=True`, `physical_shards_verified=True`, `primary_membership_parity_verified=True`, `all_checks_passed=True` (report SHA: `2d2509d02114b375bfe437f049f2456bbad6dbb154731fe5eda8ecbfbbb51b45`, `raw_aftershocks=99067`, `resolved_unique_aftershocks=88078`, `deduplicated_aftershocks=10989`, `ambiguous_candidates=0`, `nested_mainshocks=21`).

## 5. Frozen Rule cho EXP-05 & Giới hạn Khoa học

- **Rule ID:** `exp04-frozen-reproduction-rule-v2` (khóa từ `REPRODUCTION` dataset `ds_907fe748fafdfc0f072ce4db4e1cf0aa619ed18877129e17c8440b43062c3d87`).
- **Contract Bất biến:** `window_model_version=wm_expanded_v1.0`, `scaling_config_version=scaling-global-research-90k-v1`, `feature_version=relative-4d-v1`, `mc_value=0.5`.
- **Nguyên tắc đọc kết quả:** Không chọn "mô hình thắng" chỉ dựa trên một chỉ số đơn lẻ (Window có `0%` mainshock-failure nhưng không lọc nhiễu nền; DBSCAN/HDBSCAN đánh đổi giữa lọc nhiễu, DBCV, khớp Omori và tỷ lệ mainshock bị xếp nhiễu). Kết quả chỉ là ứng viên chuỗi sau chấn hồi cứu; bước chạy khóa tham số sang giai đoạn 2018–2023 thuộc [EXP-05](../task/tasks/EXP-05.md).
