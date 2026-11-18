# EXP-05 — Đánh giá Out-of-Period (2018–2023) theo Rule đã Khóa trên Tập EXTENSION

## 1. Phạm vi, Danh tính Dataset & Freeze Gate

- **Task:** [EXP-05](../task/tasks/EXP-05.md) (Tuần 7, Khối H — Out-of-period evaluation, P0 Core)
- **Report Version / Trạng thái:** `exp05-official-extension-v2` / `PASSED` (`OFFICIAL_EXTENSION_OUT_OF_PERIOD`)
- **Thời điểm chạy (UTC):** `2026-11-18T05:28:16.509672+00:00`
- **Git Commit Pin / Code Status:** `281fcce4296205ffaa5b1c9dbf1ad9b9d9edaf16` (`DIRTY_OR_UNVERIFIED`, `is_clean_code=False`)
- **Môi trường Python:** Python `3.12.10`, `scikit-learn==1.6.1`, `hdbscan==0.8.40`, `pyarrow==21.0.0`
- **Scientific Approval:** `false`
- **Frozen Rule Source:** `exp04-frozen-reproduction-rule-v2` từ [EXP-04_OFFICIAL_REPRODUCTION.json](./EXP-04_OFFICIAL_REPRODUCTION.json) (`SHA-256=2d9552724356235cbdf186a5a98687a1fdfb16e49e60caa56c55709a7a7a3ae0`, `LF SHA-256=0946298b33aa871273071ea2b64e9490a4a63bd639f515a6d0fdf9d9a7fb12b6`)
- **Đối soát lại trên main:** `74e4170e895e750a8d70f6887691435f30c598c8`. Kiểm lại kit và bundle EXTENSION đã có, rồi tính lại delta và decision table với EXP-04 mới. Metrics EXTENSION và metadata chạy model lấy từ evidence v1 đã pin `bf678c4f815baccf54e45aa45b012a3f127f26b304285641d720b48098b30fba`.
- **Parameter Tuning trên EXTENSION:** `false` (`config_freeze=True`, `eval_param_freeze=True`, `feature_contract_freeze=True`, `exp02_cross_verify=True`, `exp03_cross_verify=True`)

### Thông số `EXTENSION` Collection (`EXPORTED`)

- **Dataset Split / Status:** `EXTENSION` / `EXPORTED`
- **Dataset ID:** `ds_5259d2fe13e027245559e1ec4a1e696b5d94d0b67f44d5ec80738c9e984978be` (khác biệt hoàn toàn với REPRODUCTION `ds_907fe748fafdfc0f072ce4db4e1cf0aa619ed18877129e17c8440b43062c3d87`)
- **Collection ID:** `col_0dc6dc2fd0a60c2890f68863b8561813ecd1d5e1941a8995be1d58e192fe0906` (`4` shards)
- **Gold Publication ID / Snapshot ID:** `pub_832ce36ac875f546528718c3545442a77a0097f7573379f30ed92f2a43c481f4` / `7580315846938041321` (`gold_pin_verified=True`)
- **Khoảng thời gian split / Quan sát thực tế:** `2018-10-01T00:00:00Z` .. `2024-01-01T00:00:00Z` (`63` tháng; mainshock `2019-01-29T12:48:19.040000Z` .. `2023-09-18T19:33:04.450000Z`)
- **Khối lượng dữ liệu:** `45` mainshocks, `233664` candidate rows (`feature_version=relative-4d-v1`, `window_model_version=wm_expanded_v1.0`, `scaling_config_version=scaling-global-research-90k-v1`)
- **Manifest / Checksums / Handoff SHA-256:** `fd1908f77ffcc70e77250ef834883d0ab8bbaf38e40962958a5c296f90d4aad1` / `753506e4039130e12f88746c5f95122a66a3bac36342634d082f89acf5ad89d8` / `6cf0af7c2a3d8e8ec4c993f84ed6d86cdc417324102133dd3068eb2ebc40d297`

## 2. Biến động Dữ liệu giữa `REPRODUCTION` và `EXTENSION` (Data Drift)

| Đặc trưng phân phối | REPRODUCTION (2000–2018) | EXTENSION (2018–2023) | Chênh lệch / Tỷ lệ |
|---|---:|---:|---:|
| Mainshock Windows | 194 | 45 | -149 |
| Tổng Candidate Rows | 156710 | 233664 | +76954 (1.49x) |
| Global Completeness $M_c$ | 0.5 | 0.4 | -0.10 |
| Candidates/Window (Min / Median / Mean / Max) | 1 / 58.0 / 807.78 / 38854 | 1 / 87.0 / 5192.53 / 83504 | Mean `6.428x`, Max `2.149x` |
| Mega-Windows (`>10k` / `>50k` rows) | 3 / 0 | 5 / 2 | +2 / +2 |

## 3. Kết quả 4 Thuật toán trên `EXTENSION` và Đối chiếu Delta với `REPRODUCTION`

| Algorithm | Success/Total | Failure Rate | Aftershocks (POST) | Mean Noise | Mean DBCV (n) | Omori p / R² (n) | Jaccard / ARI | Runtime (s) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| WINDOW | 45/45 | 0.0% | 219972 | 0.0% | N/A (0) | 0.44 / 0.97 (33) | 1.0000 / 1.0000 | 55.72 |
| DBSCAN | 30/45 | 33.3% | 215240 | 20.3% | 0.0026 (27) | 0.61 / 0.97 (26) | 0.9917 / 0.9599 | 1489.11 |
| HDBSCAN_GLOBAL | 26/45 | 42.2% | 843 | 21.2% | 0.2580 (25) | 0.93 / 0.95 (20) | 0.8129 / 0.7912 | 1751.23 |
| HDBSCAN_ADAPTIVE | 30/45 | 33.3% | 128527 | 26.8% | 0.0800 (11) | 0.55 / 0.96 (28) | 0.9542 / 0.8987 | 5136.61 |

### Delta `EXTENSION` trừ `REPRODUCTION`

| Algorithm | Fail Rate Delta | Noise Ratio Delta | DBCV Delta | Omori p Delta | Jaccard Delta | ARI Delta | Runtime Ratio (Ext/Repro) |
| --- | --- | --- | --- | --- | --- | --- | --- |
| WINDOW | +0.0000 | +0.0000 | N/A | -0.11 | +0.0000 | +0.0000 | 2.63x |
| DBSCAN | -0.0069 | -0.0036 | -0.0886 | -0.09 | +0.0174 | +0.0143 | 20.14x |
| HDBSCAN_GLOBAL | +0.1542 | +0.0526 | -0.0615 | +0.10 | -0.0589 | -0.0252 | 18.73x |
| HDBSCAN_ADAPTIVE | +0.0034 | -0.0432 | -0.2312 | -0.14 | +0.0204 | +0.0257 | 46.55x |

## 4. Phân giải Đa chuỗi Toàn cục `HDBSCAN_ADAPTIVE` trên `EXTENSION`

| Chỉ số | REPRODUCTION | EXTENSION |
|---|---:|---:|
| Raw Sequence Members | 116059 | 132264 |
| Raw Aftershocks (POST trước khử trùng lặp) | 99067 | 128527 |
| Resolved Unique Aftershocks (Rank 1) | 88078 | 71012 |
| Aftershocks Deduplicated (Secondary Ranks) | 10989 | 57515 |
| Ambiguous Candidates / Aftershocks | 0 / 0 | 0 / 0 |
| Nested Mainshocks Detected | 21 | 3 |

## 5. Bảng Quyết định & Các Trường hợp Thất bại (Decision Table)

| Algorithm | Model Config | Repro Success/Total | Ext Success/Total | Failure Rate Delta | Ext Noise Ratio | Ext Mean DBCV | Ext Mean Omori p | Ext Jaccard | Ext Failure Cases | Assessment |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| WINDOW | window-baseline-v1 | 194/194 | 45/45 | +0.0000 | 0.0000 | N/A | 0.4381 | 1.0000 | 0/45 windows failed | STABLE_UNFILTERED_BASELINE |
| DBSCAN | dbscan-global-dev-v1 | 128/194 | 30/45 | -0.0069 | 0.2027 | 0.0026 | 0.6110 | 0.9917 | 15/45 (EXP_MAINSHOCK_CLASSIFIED_AS_NOISE=15) | GENERALIZES_WITH_TRADEOFFS |
| HDBSCAN_GLOBAL | hdbscan-global-dev-v1 | 142/194 | 26/45 | +0.1542 | 0.2121 | 0.2580 | 0.9268 | 0.8129 | 19/45 (EXP_MAINSHOCK_CLASSIFIED_AS_NOISE=19) | SHIFTED_ON_EXTENSION |
| HDBSCAN_ADAPTIVE | hdbscan-adaptive-dev-v1 | 130/194 | 30/45 | +0.0034 | 0.2679 | 0.0800 | 0.5536 | 0.9542 | 15/45 (EXP_MAINSHOCK_CLASSIFIED_AS_NOISE=15) | GENERALIZES_WITH_TRADEOFFS |

### Chi tiết Failure Cases & Honest NULL Reasons trên `EXTENSION`

- **WINDOW:** failures=`0`, failure_reasons=`{}`, dbcv_null_reasons=`{'METRIC_NOT_APPLICABLE': 45}`, omori_null_reasons=`{'METRIC_INSUFFICIENT_MEMBERS': 12}`, stability_null_reasons=`{}`.
- **DBSCAN:** failures=`15`, failure_reasons=`{'EXP_MAINSHOCK_CLASSIFIED_AS_NOISE': 15}`, dbcv_null_reasons=`{'METRIC_FIT_FAILED': 6, 'METRIC_INSUFFICIENT_MEMBERS': 12}`, omori_null_reasons=`{'METRIC_INSUFFICIENT_MEMBERS': 19}`, stability_null_reasons=`{}`.
- **HDBSCAN_GLOBAL:** failures=`19`, failure_reasons=`{'EXP_MAINSHOCK_CLASSIFIED_AS_NOISE': 19}`, dbcv_null_reasons=`{'METRIC_FIT_FAILED': 10, 'METRIC_INSUFFICIENT_MEMBERS': 10}`, omori_null_reasons=`{'METRIC_INSUFFICIENT_MEMBERS': 25}`, stability_null_reasons=`{}`.
- **HDBSCAN_ADAPTIVE:** failures=`15`, failure_reasons=`{'EXP_MAINSHOCK_CLASSIFIED_AS_NOISE': 15}`, dbcv_null_reasons=`{'METRIC_FIT_FAILED': 23, 'METRIC_INSUFFICIENT_MEMBERS': 11}`, omori_null_reasons=`{'METRIC_INSUFFICIENT_MEMBERS': 17}`, stability_null_reasons=`{}`.

## 6. Đối soát Bundle MLI-03 Full-Split & Per-Shard

| Algorithm | Full Run ID | Full `_SUCCESS.json` SHA-256 (prefix) | Primary Membership SHA-256 (prefix) | Shards Verified | Parity |
|---|---|---|---|---:|---|
| `WINDOW` | `exp_run_6935e84a7a2d3cd85e0123c0086ce522` | `70676285ee6df8b9...` | `5162ce003168a2e2...` | 4 (`True`) | `True` |
| `DBSCAN` | `exp_run_466813fef2f2296f60f7021a7b11e128` | `5f0255c38e2a8fca...` | `26969e94d31bd613...` | 4 (`True`) | `True` |
| `HDBSCAN_GLOBAL` | `exp_run_ba9e6bcfc1264176586077993dbd874b` | `ddbbf62e9231abbc...` | `cbbed42eb5bd19e9...` | 4 (`True`) | `True` |
| `HDBSCAN_ADAPTIVE` | `exp_run_3ba14e9eb1cbb50b3cce83ac66321db1` | `0c8a5422d1429677...` | `8188d6799a2c38d2...` | 4 (`True`) | `True` |

- **Phạm vi bảng:** Bảng ghi kiểm tra checksum, Parquet readback và primary digest phía Python. Full-split phục vụ đánh giá; MLI-03 resolve theo `WHOLE_WINDOW_SHARD`.
- **Evidence Java offline của run v1:** 12 shard baseline đạt `RESULT_READY`; 4 shard adaptive đạt reader readback. Nghiệm thu import Iceberg và contract overlap xuyên shard thuộc [MLI-03](../task/tasks/MLI-03.md).

## 7. Ranh giới Phương pháp luận & Hạn chế Khoa học

1. **Out-of-Period Temporal Generalization Check (Unsupervised):** Kiểm thử trên `EXTENSION` đo tính ổn định tổng quát hóa theo thời gian khi khóa nguyên Rule/Config từ `EXP-04`; **không phải** độ chính xác có nhãn (cấm tuyệt đối `accuracy`/`F1`).
2. **Không Tune lại Tham số:** Toàn bộ 4 cấu hình và tham số stability/Omori giữ nguyên theo `exp04-frozen-reproduction-rule-v2`; các dịch chuyển như hạ $M_c$ (`0.5 -> 0.4`) và mega-windows (`83.504` rows) phản ánh đúng thực tế vận hành ngoài mẫu.
3. **Ứng viên Chuỗi Hồi cứu & Không Phê duyệt Độc lập:** Mọi đầu ra chỉ là ứng viên chuỗi sau chấn hồi cứu phục vụ kiểm chứng khoa học ([HDBSCAN_WORKSTREAM.md](../task/HDBSCAN_WORKSTREAM.md)), **không** tuyên bố dự đoán động đất hay quan hệ nhân quả vật lý (`scientific_approval=false`).
