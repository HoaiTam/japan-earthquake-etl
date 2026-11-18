# EXP-03 — Kết quả Official HDBSCAN Adaptive & Multi-Sequence trên REPRODUCTION Collection

- **Task:** [EXP-03](../task/tasks/EXP-03.md) (Khối H - Experiment, P0 Core)
- **Report Version / Trạng thái:** `exp-03-official-v2` / `PASSED` (`OFFICIAL_RESEARCH`)
- **Thời điểm tạo (UTC):** `2026-11-18T04:18:39.349332+00:00`
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

- **HDBSCAN_ADAPTIVE:** `config/ml/models/hdbscan_adaptive_dev_v1.json` (`hdbscan-adaptive-dev-v1`, File SHA: `41a4232ad6e4ae50505f14f8af2ea5cbd9353c0dc43ce6f3776d539977adbd65`, Canonical SHA: `22ed180c746ff9656dfd04027e582cd167ccdec26db332cc16946eb143df418a`)
- **Clean Verification:** `is_clean_code=True` (`git_commit=4a324b886a543347bbaafa6fdf5a07505de1c529`)
  - `notebooks/experiment_harness.py`: git_blob=`1a81e66f8c20e01cf28d5254bf603e86894aebcb`, lf_sha256=`1dfc4cbc394e9b2c5ef5ce9e403750267e0053314ccc25f32550bdb811e2c2af`
  - `notebooks/research_collection.py`: git_blob=`6326d9c1a74fc658fa3211e8ce95ff1b9bece604`, lf_sha256=`7a0f598b460d45233d8cf122c2f9e009b5d6185412a603c8d0d328555fa68b1a`
  - `notebooks/baseline_models.py`: git_blob=`174c8edfebc424d260f4a9d893109c5becd40797`, lf_sha256=`d9e30eda33f74c8eb716167d84ad97269d7ff6d0916d713e5d48cb418451e3a7`
  - `notebooks/adaptive_hdbscan.py`: git_blob=`c7a0412477c5bf5c133c05169b6694f99040aee0`, lf_sha256=`6f49663dfb080214bd4375fdf27bcdef7a58b47eff06352911531d237bf6d5e1`
  - `scripts/run-exp03-official.py`: git_blob=`2a2059347947c47bf7547db7224e71070fd6eafd`, lf_sha256=`3f5d3eed88e2af5eabca09ed388fd15a878cbf6b28400fa092588e4e5f09354f`
  - `config/ml/models/hdbscan_adaptive_dev_v1.json`: git_blob=`042f3d1359e8b038ddd68963049bcd46f9ac405c`, lf_sha256=`d0f24c8732e47d094ea42af1eb41a9413a17af4760b3fc39864bc31f8d662c73`
- **Dải tham số thích nghi tính trên `194` windows (deterministic, không manual override):** `min_cluster_size` min=`4` / median=`9.0` / max=`20`; `min_samples` min=`2` / median=`5.0` / max=`12`.

## 3. Đối soát Window Fit & Multi-Sequence Entity Resolution Toàn Cục

| Chỉ số | Giá trị |
|---|---:|
| **Run ID (Full Split)** | `exp_run_293f2720c8a765c7ab6d20243d1b797b` |
| **Tổng Ứng viên (Raw Grain)** | 156710 |
| **Windows / Thành công / Mainshock Noise / Empty POST** | 194 / 131 / 63 / 0 |
| **Raw Sequence Members (tất cả windows)** | 115636 |
| **Raw Aftershocks (POST trước khử trùng lặp)** | 98795 |
| **Ứng viên xuất hiện trong Multi-Sequences** | 9649 |
| **Resolved Unique Aftershocks (Rank 1 POST)** | 88222 |
| **Aftershocks Deduplicated (Secondary Ranks)** | 10573 |
| **Ambiguous Candidates / Ambiguous Aftershocks** | 0 / 0 |
| **Nested Mainshocks Detected** | 23 |
| **Noise Events** | 26709 |
| **Runtime (s)** | 39.61 |

## 4. Per-Shard Official Bundles (`WHOLE_WINDOW_SHARD` — Sibling Layout Hợp đồng MLI-01/MLI-03)

- Full-split bundle (`hdbscan_adaptive/`) và từng shard bundle (`hdbscan_adaptive_shards/<shard_id>/`) tách thành thư mục đồng cấp không lồng nhau, tương thích trực tiếp `MlResultBundleReader` (`MLI-03`).

| Shard | Relative Dir | Run ID | Rows | Mainshocks | `_SUCCESS.json` SHA-256 (prefix) |
|---|---|---|---:|---:|---|
| `shard_00000` | `hdbscan_adaptive_shards/shard_00000` | `exp_run_b0e755065b1d241749634e754eb5003e` | 85972 | 87 | `cc32cc67d11661eb...` |
| `shard_00001` | `hdbscan_adaptive_shards/shard_00001` | `exp_run_cf9117b7016b3f54145a5cb60c1a9815` | 70738 | 107 | `dc61c74d77d599ae...` |

## 5. Tuân thủ Hợp đồng & Giới hạn Khoa học

1. **Deterministic Adaptive Rule:** `min_cluster_size` và `min_samples` tính xác định theo `(candidate_count, mainshock_magnitude)` từ `hdbscan-adaptive-dev-v1`; không chỉnh tay theo từng sequence và không chạm dữ liệu `EXTENSION`.
2. **Global Multi-Sequence & Raw Grain Preservation:** Lưu đủ toàn bộ `156710` raw membership pairs; phân giải đa chuỗi toàn cục trước khi tách `WHOLE_WINDOW_SHARD` theo `probability -> normalized_distance -> AMBIGUOUS` (giữ `resolution_rank = NULL` cho các trường hợp hòa).
3. **Nested Mainshock Evidence:** Giữ toàn bộ `parent_mainshock_event_id` và `nested_membership_probability` trên `sequence_summary.parquet` (và gắn `AMBIGUOUS_NESTED_PARENT_TIE` nếu hòa parent).
4. **Ranh giới MLI-03 Full-Split vs Shard:** `MlResultBundleReader.read` đọc được cả full-split (`hdbscan_adaptive/`) lẫn từng shard (`hdbscan_adaptive_shards/<shard_id>/`). `ResultBundleContract.validate` kiểm tra đầy đủ trạng thái toàn cục trên full-split bundle; với các shard tách riêng có candidate giao cắt giữa nhiều shard (ví dụ `13.097` candidate dùng chung giữa `shard_00000` và `shard_00001`), việc giữ nhãn đã resolve toàn cục (`MULTIPLE`/`AMBIGUOUS`) khiến bản thân slice đơn lẻ chứa các dòng đơn có status liên-shard.
5. **Ranh giới Khoa học:** Kết quả chỉ là ứng viên chuỗi sau chấn hồi cứu; không tuyên bố adaptive tốt hơn hay dự đoán động đất trước khi hoàn tất đánh giá định lượng ở [EXP-04](../task/tasks/EXP-04.md).
