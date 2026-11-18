# EXP-02 — Kết quả Official Baselines trên REPRODUCTION Collection

- **Task:** [EXP-02](../task/tasks/EXP-02.md) (Khối H - Experiment, P0 Core)
- **Report Version / Trạng thái:** `exp-02-official-v2` / `PASSED` (`OFFICIAL_RESEARCH`)
- **Thời điểm tạo (UTC):** `2026-11-18T03:31:56.892001+00:00`
- **Git Commit Pin / Code Status:** `df10230a2574c09fdba468ead50bf5808911dcd1` (`CLEAN_VERIFIED`)
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
- **Clean Verification:** `is_clean_code=True` (`git_commit=df10230a2574c09fdba468ead50bf5808911dcd1`)
  - `notebooks/experiment_harness.py`: git_blob=`5200eb45842250a73220b1f78c952b2288432435`, lf_sha256=`5d9f1805e5eb29d0b3ec58a56fb2308373519ef504a6a832b22d1b6ba8160476`
  - `notebooks/research_collection.py`: git_blob=`6326d9c1a74fc658fa3211e8ce95ff1b9bece604`, lf_sha256=`7a0f598b460d45233d8cf122c2f9e009b5d6185412a603c8d0d328555fa68b1a`
  - `notebooks/baseline_models.py`: git_blob=`7f0e62805ab40e4c2bbe34ed94ebe6c15a021220`, lf_sha256=`3099827f8f93c661a4efc86fe8d49edf2c7ff76de44c02c636d02d89069075f9`
  - `scripts/run-exp02-official.py`: git_blob=`f481fff07302e2fe116f676c486c434ad7fd7008`, lf_sha256=`375c3b5486e7a615f65ef8df2a0fc4d0570f82c347282731398f59eda1387175`
  - `config/ml/models/window_baseline_v1.json`: git_blob=`2c5dd1877c74dc8551610941df6e59d46b11f618`, lf_sha256=`5ba1a7aadb46a00c85058c5d4f0a27812f97bfee877601acfae10168dc454797`
  - `config/ml/models/dbscan_global_dev_v1.json`: git_blob=`13cfc6c04dbae9717a4530649a87504f14885c25`, lf_sha256=`ab5c5150eefd58c674937ebc885275f74f96810dc05197d936d94d70fec06c75`
  - `config/ml/models/hdbscan_global_dev_v1.json`: git_blob=`eeb8a0cdc4db7224391c858de5272e77a5e08183`, lf_sha256=`eb8c3e8cf63f1cfa30669094b0bcf3808f8fe780afdf24c4ae795bbbfee99111`

## 3. Bảng Đối soát Kết quả Official Baselines

| Thuật toán | Run ID | Tổng Ứng viên | Windows | Thành công | Mainshock Noise | Empty POST | Seq Members | Aftershocks (POST) | Noise Events | Runtime (s) |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **WINDOW** | `exp_run_a996231b94b1a60a7891b221da549d99` | 156710 | 194 | 194 | 0 | 24 | 136047 | 135853 | 0 | 0.47 |
| **DBSCAN** | `exp_run_625da38ae6c398504b840adeede88014` | 156710 | 194 | 128 | 66 | 1 | 137840 | 118487 | 5279 | 10.48 |
| **HDBSCAN_GLOBAL** | `exp_run_e3bbf51736ed7c88b06a530aba1cdc7a` | 156710 | 194 | 142 | 52 | 2 | 16939 | 15118 | 42335 | 33.77 |

## 4. Per-Shard Official Bundles (`WHOLE_WINDOW_SHARD` — Sibling Layout Hợp đồng MLI-01/MLI-03)

- Full-split bundle (`<algo>/`) đọc được để đánh giá và đối soát; không dùng làm request import MLI-03 cho collection.
- MLI-03 chỉ nhận `WHOLE_WINDOW_SHARD`: một request cho mỗi thư mục `<algo>_shards/<shard_id>/`. Dùng đúng Run ID và Shard bên dưới, cùng `dataset_id`, `collection_id`, thuật toán và model config version ở trên.
- Lấy `dataset_manifest_ref` và `dataset_manifest_sha256` từ receipt export trỏ đến trusted registry, không lấy Manifest SHA-256 của bundle nghiên cứu ở mục 1. Xem [runbook MLI-03](../runbooks/MLI-03_RESULT_IMPORT.md#request-tối-thiểu).
- Kiểm chứng file/shard và parity trong report này không có nghĩa đã import, commit Iceberg hoặc đạt `CANDIDATE`.

| Thuật toán | Shard | Relative Dir | Run ID | Rows | Mainshocks | `_SUCCESS.json` SHA-256 (prefix) |
|---|---|---|---|---:|---:|---|
| `WINDOW` | `shard_00000` | `window_shards/shard_00000` | `exp_run_0dfd6de7816ede922a9a87851bd3f07e` | 85972 | 87 | `5cc2f5b96929ad91...` |
| `WINDOW` | `shard_00001` | `window_shards/shard_00001` | `exp_run_1ddd02dbc57e84013508ca8c35b2c423` | 70738 | 107 | `0ecc13ea170d965a...` |
| `DBSCAN` | `shard_00000` | `dbscan_shards/shard_00000` | `exp_run_4be55b5a1de311fcb2ae78102af812ca` | 85972 | 87 | `991f3c6a3e85a6d1...` |
| `DBSCAN` | `shard_00001` | `dbscan_shards/shard_00001` | `exp_run_d18ffb0a5c4f94c6695aaae7e7bb0cf2` | 70738 | 107 | `205d73330e09fd20...` |
| `HDBSCAN_GLOBAL` | `shard_00000` | `hdbscan_global_shards/shard_00000` | `exp_run_743cbbc3454a2d36432c0126c46b2ac1` | 85972 | 87 | `ab18295f63d79773...` |
| `HDBSCAN_GLOBAL` | `shard_00001` | `hdbscan_global_shards/shard_00001` | `exp_run_4af0189724cf3cb5ef89514e523fbd18` | 70738 | 107 | `1d280c2b4f799262...` |

**Regression offline (PR #139):** `TrustedDatasetResolver → MlResultBundleReader → ResultBundleContract` nhận shard đúng từ registry fixture độc lập; thiếu shard, sai shard và full-split gắn nhầm shard đều bị từ chối. Kiểm tra này không ghi vào Iceberg và không thay đổi JSON evidence, run IDs hoặc SHA của lần chạy official v2.

## 5. Tuân thủ Hợp đồng & Giới hạn Khoa học

1. **Per-Mainshock Isolation:** Tất cả mô hình đều fit độc lập cho từng mainshock window trên 4-D normalized vectors `[x_scaled, y_scaled, z_scaled, time_scaled]`, không có dữ liệu đầu vào nào bị biến đổi.
2. **Mainshock Noise Policy:** Mọi cửa sổ có mainshock mang nhãn `-1` đều giữ `window_result_status = FAILED`, `failure_reason_code = EXP_MAINSHOCK_CLASSIFIED_AS_NOISE`, `0` sequence members; tuyệt đối không chọn cụm gần nhất.
3. **Null Probability Policy:** `WINDOW` và `DBSCAN` giữ `membership_probability = NULL`; `HDBSCAN_GLOBAL` ghi xác suất hữu hạn trong `[0.0, 1.0]`.
4. **Ranh giới Khoa học:** Các kết quả chỉ là **ứng viên chuỗi sau chấn (candidate sequences)** mang tính hồi cứu trên tập `REPRODUCTION`; không tuyên bố dự đoán động đất hoặc quan hệ nhân quả vật lý. Việc giải quyết đa chuỗi thích nghi thuộc [EXP-03](../task/tasks/EXP-03.md) và đánh giá khoa học DBCV/Omori/Stability thuộc [EXP-04](../task/tasks/EXP-04.md).
