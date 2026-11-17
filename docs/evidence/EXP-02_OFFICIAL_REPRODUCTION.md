# EXP-02 — Kết quả Official Baselines trên REPRODUCTION Collection

- **Task:** [EXP-02](../task/tasks/EXP-02.md) (Khối H - Experiment, P0 Core)
- **Trạng thái:** `PASSED` (`OFFICIAL_RESEARCH`)
- **Thời điểm tạo (UTC):** `2026-11-17T13:22:59.416770+00:00`
- **Git Commit Pin:** `b8884b091fe4efad628b54631c140623fa32def8`
- **Python / HDBSCAN Runtime:** Python `3.12.10`, `scikit-learn==1.6.1`, `hdbscan==0.8.40`, `pyarrow==21.0.0`
- **Random Seed:** `42`
- **Scientific Approval:** `false`

## 1. Danh tính Dataset & Pins

- **Split:** `REPRODUCTION` (`EXPORTED`)
- **Dataset ID:** `ds_907fe748fafdfc0f072ce4db4e1cf0aa619ed18877129e17c8440b43062c3d87`
- **Collection ID:** `col_bd10c69365da841b2cf6a62172e2350a797001eac04ce4bc6f2277a4d150ad98`
- **Gold Publication ID:** `pub_832ce36ac875f546528718c3545442a77a0097f7573379f30ed92f2a43c481f4` (Snapshot: `7580315846938041321`)
- **Tổng số dòng đặc trưng:** `156710` (`194` mainshock windows, `2` shards)
- **Global Completeness $M_c$:** `0.5`
- **Window / Scaling / Feature Versions:** `wm_expanded_v1.0` / `scaling-global-research-90k-v1` / `relative-4d-v1`
- **Timestamp Reader Profile:** `prepared-spark-utc-int96-v1`
- **Manifest SHA-256:** `d3ee1334bfc50005a527f7b32a8988fcb1bdd7659a7dd4e84de326a7735139ae`
- **Checksums SHA-256:** `4768e500754c569bc951f23e2ac14fa9b9703ae69fbd5f78de06eacbb72f7ae9`
- **Team Handoff SHA-256:** `6cf0af7c2a3d8e8ec4c993f84ed6d86cdc417324102133dd3068eb2ebc40d297`

## 2. Cấu hình Thuật toán đã Khóa (Config Pins)

- **WINDOW:** `config/ml/models/window_baseline_v1.json` (`window-baseline-v1`, File SHA: `78feda8112fd4a68...`)
- **DBSCAN:** `config/ml/models/dbscan_global_dev_v1.json` (`dbscan-global-dev-v1`, File SHA: `f089d928f4ace1c6...`)
- **HDBSCAN_GLOBAL:** `config/ml/models/hdbscan_global_dev_v1.json` (`hdbscan-global-dev-v1`, File SHA: `10d2b375d66c8c95...`)

## 3. Bảng Đối soát Kết quả Official Baselines

| Thuật toán | Run ID | Tổng Ứng viên | Windows | Thành công | Mainshock Noise | Empty POST | Seq Members | Aftershocks (POST) | Noise Events | Runtime (s) |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **WINDOW** | `exp_run_b7f44708355e251ec1ff317b1524419d` | 156710 | 194 | 194 | 0 | 24 | 136047 | 135853 | 0 | 0.59 |
| **DBSCAN** | `exp_run_a6e7dc09579ffba9b232c06942c84161` | 156710 | 194 | 128 | 66 | 1 | 137840 | 118487 | 5279 | 11.70 |
| **HDBSCAN_GLOBAL** | `exp_run_2d3a6a0e31271394b42fb16ae64248c6` | 156710 | 194 | 142 | 52 | 2 | 16939 | 15118 | 42335 | 37.61 |

## 4. Per-Shard Official Bundles (`WHOLE_WINDOW_SHARD` — Hợp đồng MLI-01/MLI-03)

| Thuật toán | Shard | Run ID | Rows | Mainshocks | `_SUCCESS.json` SHA-256 (prefix) |
|---|---|---|---:|---:|---|
| `WINDOW` | `shard_00000` | `exp_run_c629c4cef664a29f08f21fc14a8d4d5b` | 85972 | 87 | `9fca318fafc3f80b...` |
| `WINDOW` | `shard_00001` | `exp_run_8bd004a0632776cffb65cb83cde74923` | 70738 | 107 | `d98a9a56f48ccf46...` |
| `DBSCAN` | `shard_00000` | `exp_run_6a193d1ff33c579c4bb9ff80a6873897` | 85972 | 87 | `3b01a71167d6e2ce...` |
| `DBSCAN` | `shard_00001` | `exp_run_12ffdcf6b575d6cbe1a1557dd24e4350` | 70738 | 107 | `2cd674a57b1fa75d...` |
| `HDBSCAN_GLOBAL` | `shard_00000` | `exp_run_3f7981c4c79cff10e78f95c7756ec297` | 85972 | 87 | `891035d039d0d60c...` |
| `HDBSCAN_GLOBAL` | `shard_00001` | `exp_run_71d2b13d58dad05691dbfaa05f6c3f32` | 70738 | 107 | `2d7d14fdbe5755ed...` |

## 5. Tuân thủ Hợp đồng & Giới hạn Khoa học

1. **Per-Mainshock Isolation:** Tất cả mô hình đều fit độc lập cho từng mainshock window trên 4-D normalized vectors `[x_scaled, y_scaled, z_scaled, time_scaled]`, không có dữ liệu đầu vào nào bị biến đổi.
2. **Mainshock Noise Policy:** Mọi cửa sổ có mainshock mang nhãn `-1` đều giữ `window_result_status = FAILED`, `failure_reason_code = EXP_MAINSHOCK_CLASSIFIED_AS_NOISE`, `0` sequence members; tuyệt đối không chọn cụm gần nhất.
3. **Null Probability Policy:** `WINDOW` và `DBSCAN` giữ `membership_probability = NULL`; `HDBSCAN_GLOBAL` ghi xác suất hữu hạn trong `[0.0, 1.0]`.
4. **Ranh giới Khoa học:** Các kết quả chỉ là **ứng viên chuỗi sau chấn (candidate sequences)** mang tính hồi cứu trên tập `REPRODUCTION`; không tuyên bố dự đoán động đất hoặc quan hệ nhân quả vật lý. Việc giải quyết đa chuỗi thích nghi thuộc [EXP-03](../task/tasks/EXP-03.md) và đánh giá khoa học DBCV/Omori/Stability thuộc [EXP-04](../task/tasks/EXP-04.md).
