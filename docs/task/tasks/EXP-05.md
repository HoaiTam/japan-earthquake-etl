---
task_id: "EXP-05"
status: "Done"
week: 7
block: "H - Experiment"
workstream: "Out-of-period evaluation"
scope: "Core"
priority: "P0"
effort_hours: 5
assignee: "rosy179"
reviewer: "unassigned"
dependencies: ["EXP-04"]
---

# EXP-05 - Chạy out-of-period extension 2018-2023

## Giao việc hiện hành — 11/11/2026

- **Owner:** rosy179; **status:** Done (`scientific_approval=false`, `reviewer: unassigned`; triển khai hỗ trợ bởi ThanhTris theo phân công).
- **Làm phần gì / dùng để làm gì:** Extension 2018–2023.
- **Bắt đầu độc lập:** Chuẩn bị runner/comparison schema bằng fixture, chưa tune extension.
- **Đầu ra và điều kiện Done:** Sau EXP-04 freeze: bốn thuật toán trên EXTENSION, cùng rule đã khóa; report reproduction-vs-extension có reasons/drift/sample size.
- **Lịch:** chuẩn bị 11–15/11, nghiệm thu tuần9 16–22/11; xem
  [kế hoạch và setup](../WEEK_9_REPLAN_20261111.md). Giữ hard dependencies,
  scope/effort lịch sử; các mục tuần cũ dưới đây là lịch sử.

## Mục đích

Kiểm tra rule chọn trên reproduction period có giữ hành vi ổn định ở giai đoạn
sau hay chỉ phù hợp dữ liệu đã dùng để calibration.

## Phạm vi công việc

- Freeze window/scaling/parameter rule và `model_config_version` từ reproduction.
- Chạy Window, DBSCAN, HDBSCAN global/adaptive trên extension `2018-10-01..2023-12-31`.
- So sánh distribution metric, productive/failure rate, stability, runtime và data drift indicators.
- Ghi decision table cùng limitation; không chỉnh bằng mắt theo event extension.

## Deliverable

- Extension experiment bundle và reproduction-vs-extension comparison report.
- Evidence model config checksum/version giống rule đã khóa.
- Final method-selection recommendation có tiêu chí và failure cases.

## Tiêu chí hoàn thành

- [x] Extension dùng dataset ID riêng nhưng cùng feature/model contract đã khóa.
- [x] Không có parameter override dựa trên kết quả extension.
- [x] So sánh đủ Window/DBSCAN/HDBSCAN global/adaptive và báo sample/failure windows.
- [x] Kết luận phân biệt rõ generalization check với supervised train/test accuracy.

## Hard dependency

- [EXP-04](./EXP-04.md)

## Cách làm song song

Dataset extension có thể được `MLD` build song song từ sớm; task này chỉ chạy
sau khi reproduction rule/config đã freeze.

## Ranh giới

- Không dùng extension để quay lại tune reproduction mà không tạo experiment version mới.
- Không tuyên bố dự đoán tương lai từ out-of-period clustering.
- Không loại failure windows khỏi report.

## Theo dõi

- **Trạng thái:** Done
- **Assignee:** rosy179
- **Reviewer:** unassigned
- **Evidence / PR:**
  - Runner & kiểm chứng cổng khóa tham số: [extension_evaluation.py](../../../notebooks/extension_evaluation.py), [run-exp05-extension.py](../../../scripts/run-exp05-extension.py), [test_extension_evaluation.py](../../../tests/test_extension_evaluation.py).
  - **18/11/2026 — Official EXTENSION Out-of-Period Run (`EXP-05`):** Xác thực fail-closed rule `exp04-frozen-reproduction-rule-v2` từ [EXP-04_OFFICIAL_REPRODUCTION.json](../../evidence/EXP-04_OFFICIAL_REPRODUCTION.json) (không chỉnh tham số, `parameter_tuning_applied_on_extension=false`) trên `EXTENSION` `EXPORTED` (`ds_5259d2fe13e027245559e1ec4a1e696b5d94d0b67f44d5ec80738c9e984978be`, `45` mainshocks, `233.664` rows, `4` shards, `2018-10-01T00:00:00Z`..`2024-01-01T00:00:00Z`, $M_c=0.4$ vs `0.5` ở `REPRODUCTION`, mật độ trung bình `5192.53` rows/window gấp `6.428x`, cực đại `83.504` rows):
    - `WINDOW`: `45/45` success (`0.0%` fail, delta `+0.0000`), `219.972` POST aftershocks, noise `0.0%`, DBCV `N/A` (`METRIC_NOT_APPLICABLE: 45`), Omori `p=0.4381` / $R^2=0.9686$ (`33/45`), Jaccard `1.0000` / ARI `1.0000` (`55.72s`).
    - `DBSCAN`: `30/45` success (`15` `EXP_MAINSHOCK_CLASSIFIED_AS_NOISE`, `33.3%` fail, delta `-0.0069`), `215.240` aftershocks, noise `20.3%`, DBCV `0.0026` (`27/45`), Omori `p=0.6110` / $R^2=0.9696$ (`26/45`), Jaccard `0.9917` / ARI `0.9599` (`1489.11s`).
    - `HDBSCAN_GLOBAL`: `26/45` success (`19` `EXP_MAINSHOCK_CLASSIFIED_AS_NOISE`, `42.2%` fail, delta `+0.1542`), `843` aftershocks, noise `21.2%`, DBCV `0.2580` (`25/45`), Omori `p=0.9268` / $R^2=0.9541$ (`20/45`), Jaccard `0.8129` / ARI `0.7912` (`1751.23s`).
    - `HDBSCAN_ADAPTIVE`: `30/45` success (`15` `EXP_MAINSHOCK_CLASSIFIED_AS_NOISE`, `33.3%` fail, delta `+0.0086`), `128.527` raw POST (`132.264` raw members $\rightarrow$ `71.012` resolved unique POST aftershocks, `57.515` deduplicated, `0` ambiguous, `3` nested mainshocks), noise `26.8%`, DBCV `0.0800` (`11/45`), Omori `p=0.5536` / $R^2=0.9599$ (`28/45`), Jaccard `0.9542` / ARI `0.8987` (`5136.61s`).
    - **Đối soát bundle & ranh giới MLI-03:** Toàn bộ `4` full-split bundle + `16` sibling shard bundle đạt đối soát Python checksum/Parquet readback và `primary_membership_sha256` parity; kiểm tra Java offline (`VerifyExp05Bundles`) ghi nhận `4/4` full-split + `12/12` baseline shard (`window`, `dbscan`, `hdbscan_global`) đạt `MlResultBundleReader.read` + `ResultBundleContract.validate` (`RESULT_READY` / `exactSchema=true`), còn `4/4` `hdbscan_adaptive_shards/<shard_id>` mới qua `MlResultBundleReader.read` (`reader=OK`, `exactSchema=true`) và chưa chạy `ResultBundleContract` riêng lẻ do ứng viên dùng chung được resolve toàn cục xuyên shard. PR `EXP-05` chưa chạy import thật bundle `EXTENSION` vào Iceberg `ml.*` / Airflow / Trino (checksum/parity/`MlResultBundleReader` không thay thế nghiệm thu import thật; xem ranh giới tại [MLI-03](./MLI-03.md)). Đây là kiểm chứng tổng quát hóa hồi cứu ngoài kỳ (`is_supervised_accuracy=false`, `scientific_approval=false`), không tuyên bố dự đoán động đất. Xem [EXP-05_EXTENSION_EVALUATION.md](../../evidence/EXP-05_EXTENSION_EVALUATION.md) và [JSON](../../evidence/EXP-05_EXTENSION_EVALUATION.json); phục vụ hạ nguồn [MLI-04](./MLI-04.md) và mở khóa [MLQ-01](./MLQ-01.md).
- **Kỹ năng phù hợp:** Temporal validation, experiment analysis, HDBSCAN

## Checklist bàn giao

- [x] Deliverable và test/evidence tồn tại trong repository.
- [x] Acceptance criteria đã được kiểm tra.
- [x] Contract/docs đã cập nhật và không chứa secret/data dump.
- [ ] Reviewer độc lập được khuyến nghị cho P0; reviewer giữ `unassigned`, chưa có approval.

## Tài liệu liên quan

- [Roadmap HDBSCAN](../HDBSCAN_WORKSTREAM.md)
- [EXP-04](./EXP-04.md)
