# EXP-03 — Kết quả Official HDBSCAN Adaptive & Multi-Sequence trên REPRODUCTION Collection

- **Task:** [EXP-03](../task/tasks/EXP-03.md) (Khối H - Experiment, P0 Core)
- **Trạng thái:** `PASSED` (`OFFICIAL_RESEARCH`)
- **Thời điểm tạo (UTC):** `2026-11-17T18:05:26.945916+00:00`
- **Git Commit Pin:** `a9c0d4ef07616279f8b2bf487a294258a8021e03`
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

## 2. Cấu hình Thuật toán đã Khóa (Config Pin)

- **HDBSCAN_ADAPTIVE:** `config/ml/models/hdbscan_adaptive_dev_v1.json` (`hdbscan-adaptive-dev-v1`, File SHA: `41a4232ad6e4ae50...`, Canonical SHA: `22ed180c746ff965...`)
- **Dải tham số thích nghi tính trên `194` windows (deterministic, không manual override):** `min_cluster_size` min=`4` / median=`9.0` / max=`20`; `min_samples` min=`2` / median=`5.0` / max=`12`.

## 3. Đối soát Window Fit & Multi-Sequence Entity Resolution Toàn Cục

| Chỉ số | Giá trị |
|---|---:|
| **Run ID (Full Split)** | `exp_run_70637f429d590e2ba8258f56b618ba85` |
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
| **Runtime (s)** | 62.75 |

## 4. Per-Shard Official Bundles (`WHOLE_WINDOW_SHARD` — giữ nguyên kết quả resolve toàn cục)

| Shard | Run ID | Rows | Mainshocks | `_SUCCESS.json` SHA-256 (prefix) |
|---|---|---:|---:|---|
| `shard_00000` | `exp_run_7981f385cc8dba1effcd992bf6033f61` | 85972 | 87 | `a6f8d8c7a3fe4a05...` |
| `shard_00001` | `exp_run_b3ee65e6fa5269aa5a2795c6f579d4ae` | 70738 | 107 | `c3e252bdd3d5b3c7...` |

## 5. Tuân thủ Hợp đồng & Giới hạn Khoa học

1. **Deterministic Adaptive Rule:** `min_cluster_size` và `min_samples` tính xác định theo `(candidate_count, mainshock_magnitude)` từ `hdbscan-adaptive-dev-v1`; không chỉnh tay theo từng sequence và không chạm dữ liệu `EXTENSION`.
2. **Global Multi-Sequence & Raw Grain Preservation:** Lưu đủ toàn bộ `156710` raw membership pairs; phân giải đa chuỗi toàn cục trước khi tách `WHOLE_WINDOW_SHARD` theo `probability -> normalized_distance -> AMBIGUOUS` (giữ `resolution_rank = NULL` cho các trường hợp hòa).
3. **Nested Mainshock Evidence:** Giữ toàn bộ `parent_mainshock_event_id` và `nested_membership_probability` trên `sequence_summary.parquet` (và gắn `AMBIGUOUS_NESTED_PARENT_TIE` nếu hòa parent).
4. **Ranh giới Khoa học:** Kết quả chỉ là ứng viên chuỗi sau chấn hồi cứu; không tuyên bố adaptive tốt hơn hay dự đoán động đất trước khi hoàn tất đánh giá định lượng ở [EXP-04](../task/tasks/EXP-04.md).
