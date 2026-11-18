# EXP-02 — Kết quả Official Baselines trên REPRODUCTION Collection

- **Task:** [EXP-02](../task/tasks/EXP-02.md) (Khối H - Experiment, P0 Core)
- **Report Version / Trạng thái:** `exp-02-official-v2` / `PASSED` (`OFFICIAL_RESEARCH`)
- **Thời điểm tạo (UTC):** `2026-11-18T04:19:09.959447+00:00`
- **Git Commit Pin / Code Status:** `4a324b886a543347bbaafa6fdf5a07505de1c529` (`CLEAN_VERIFIED`)
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

## 2. Cấu hình Thuật toán & Code Provenance đã Khóa

- **WINDOW:** `config/ml/models/window_baseline_v1.json` (`window-baseline-v1`, File SHA: `78feda8112fd4a687be8383cc100faa20fdc0f0ae7a9d54a312a4868d703ad40`)
- **DBSCAN:** `config/ml/models/dbscan_global_dev_v1.json` (`dbscan-global-dev-v1`, File SHA: `f089d928f4ace1c695207b1b4200f03c34e782a9fef3069f9da4518fcf5dcba1`)
- **HDBSCAN_GLOBAL:** `config/ml/models/hdbscan_global_dev_v1.json` (`hdbscan-global-dev-v1`, File SHA: `10d2b375d66c8c95a6388c8dbda068dfebc1a492bcfd27df8bd5e1c8a20829f2`)
- **Clean Verification:** `is_clean_code=True` (`git_commit=4a324b886a543347bbaafa6fdf5a07505de1c529`)
  - `notebooks/experiment_harness.py`: git_blob=`1a81e66f8c20e01cf28d5254bf603e86894aebcb`, lf_sha256=`1dfc4cbc394e9b2c5ef5ce9e403750267e0053314ccc25f32550bdb811e2c2af`
  - `notebooks/research_collection.py`: git_blob=`6326d9c1a74fc658fa3211e8ce95ff1b9bece604`, lf_sha256=`7a0f598b460d45233d8cf122c2f9e009b5d6185412a603c8d0d328555fa68b1a`
  - `notebooks/baseline_models.py`: git_blob=`174c8edfebc424d260f4a9d893109c5becd40797`, lf_sha256=`d9e30eda33f74c8eb716167d84ad97269d7ff6d0916d713e5d48cb418451e3a7`
  - `scripts/run-exp02-official.py`: git_blob=`19e8ecf21f8eb366bd447eeb19b43c9f1ac6f40e`, lf_sha256=`0639075d52940dfbcde1d34cc3a08a55f6b51b4c776e14efa76e7d32c7d15f03`
  - `config/ml/models/window_baseline_v1.json`: git_blob=`2c5dd1877c74dc8551610941df6e59d46b11f618`, lf_sha256=`5ba1a7aadb46a00c85058c5d4f0a27812f97bfee877601acfae10168dc454797`
  - `config/ml/models/dbscan_global_dev_v1.json`: git_blob=`13cfc6c04dbae9717a4530649a87504f14885c25`, lf_sha256=`ab5c5150eefd58c674937ebc885275f74f96810dc05197d936d94d70fec06c75`
  - `config/ml/models/hdbscan_global_dev_v1.json`: git_blob=`eeb8a0cdc4db7224391c858de5272e77a5e08183`, lf_sha256=`eb8c3e8cf63f1cfa30669094b0bcf3808f8fe780afdf24c4ae795bbbfee99111`

## 3. Bảng Đối soát Kết quả Official Baselines

| Thuật toán | Run ID | Tổng Ứng viên | Windows | Thành công | Mainshock Noise | Empty POST | Seq Members | Aftershocks (POST) | Noise Events | Runtime (s) |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **WINDOW** | `exp_run_b04ddf3fee79c3ed890b7045c4cf5ea4` | 156710 | 194 | 194 | 0 | 24 | 136047 | 135853 | 0 | 0.45 |
| **DBSCAN** | `exp_run_d931de9db53233d902e1b8eff431384d` | 156710 | 194 | 128 | 66 | 1 | 137840 | 118487 | 5279 | 11.65 |
| **HDBSCAN_GLOBAL** | `exp_run_269004caefa48d43e32818fcc7fe427f` | 156710 | 194 | 142 | 52 | 2 | 16939 | 15118 | 42335 | 34.58 |

## 4. Per-Shard Official Bundles (`WHOLE_WINDOW_SHARD` — Sibling Layout Hợp đồng MLI-01/MLI-03)

- Cả full-split bundle (`<algo>/`) và từng shard bundle (`<algo>_shards/<shard_id>/`) đều tách thành sibling directories không lồng nhau, tương thích trực tiếp `MlResultBundleReader` (`MLI-03`).

| Thuật toán | Shard | Relative Dir | Run ID | Rows | Mainshocks | `_SUCCESS.json` SHA-256 (prefix) |
|---|---|---|---|---:|---:|---|
| `WINDOW` | `shard_00000` | `window_shards/shard_00000` | `exp_run_c5b2f664ee7d182748f04fb13997641e` | 85972 | 87 | `a7b4f7a99bc76616...` |
| `WINDOW` | `shard_00001` | `window_shards/shard_00001` | `exp_run_a7b4a8aeffda01df996bd9809c965328` | 70738 | 107 | `5bc6c88806b08674...` |
| `DBSCAN` | `shard_00000` | `dbscan_shards/shard_00000` | `exp_run_79661e5417db8048823cdbf3ba7ca6ea` | 85972 | 87 | `ebe0a67a71e19fcf...` |
| `DBSCAN` | `shard_00001` | `dbscan_shards/shard_00001` | `exp_run_a14af7b80db5cebac3ce360fa9520342` | 70738 | 107 | `e389a58ee5d3d28d...` |
| `HDBSCAN_GLOBAL` | `shard_00000` | `hdbscan_global_shards/shard_00000` | `exp_run_7a2af19c377e065c33b802c9842729fc` | 85972 | 87 | `c51c8b1d683c4f38...` |
| `HDBSCAN_GLOBAL` | `shard_00001` | `hdbscan_global_shards/shard_00001` | `exp_run_c643e43a5f955d3efe121e7d1558b78f` | 70738 | 107 | `18f5c66ce34ae9d4...` |

## 5. Tuân thủ Hợp đồng & Giới hạn Khoa học

1. **Per-Mainshock Isolation:** Tất cả mô hình đều fit độc lập cho từng mainshock window trên 4-D normalized vectors `[x_scaled, y_scaled, z_scaled, time_scaled]`, không có dữ liệu đầu vào nào bị biến đổi.
2. **Mainshock Noise Policy:** Mọi cửa sổ có mainshock mang nhãn `-1` đều giữ `window_result_status = FAILED`, `failure_reason_code = EXP_MAINSHOCK_CLASSIFIED_AS_NOISE`, `0` sequence members; tuyệt đối không chọn cụm gần nhất.
3. **Null Probability Policy:** `WINDOW` và `DBSCAN` giữ `membership_probability = NULL`; `HDBSCAN_GLOBAL` ghi xác suất hữu hạn trong `[0.0, 1.0]`.
4. **Ranh giới Khoa học:** Các kết quả chỉ là **ứng viên chuỗi sau chấn (candidate sequences)** mang tính hồi cứu trên tập `REPRODUCTION`; không tuyên bố dự đoán động đất hoặc quan hệ nhân quả vật lý. Việc giải quyết đa chuỗi thích nghi thuộc [EXP-03](../task/tasks/EXP-03.md) và đánh giá khoa học DBCV/Omori/Stability thuộc [EXP-04](../task/tasks/EXP-04.md).
