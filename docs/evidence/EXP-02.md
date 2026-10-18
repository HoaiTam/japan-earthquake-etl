# EXP-02 — Chạy Window, DBSCAN và HDBSCAN global evidence

Ngày kiểm chứng: 2026-10-18 (Review Round 4).
Branch: `feat/exp-02-baseline-algorithms`.
Assignee: `rosy179`.
Reviewer: `unassigned`.

## 1. Mục tiêu và bối cảnh

Task EXP-02 triển khai ba thuật toán baseline trên cùng pilot feature bundle DAT-02 (đặc trưng 4-D chuẩn hóa `[x_scaled, y_scaled, z_scaled, time_scaled]` do Java/Spark MLD-04 tạo) để đánh giá:
1. Liệu density clustering (DBSCAN/HDBSCAN) có giảm background noise tốt hơn so với counting window rule hay không.
2. HDBSCAN với variable density clustering và membership probabilities có mang lại thông tin định lượng hơn DBSCAN hay không.

Theo quy trình `JUPYTER_PILOT_WORKFLOW.md`, toàn bộ phát triển và kiểm chứng kỹ thuật được thực hiện trên Jupyter local trước; chỉ khi `local_smoke_status=PASSED`, `fresh_kernel_acceptance=true` và `colab_migration_allowed=true` mới được phép chuyển sang Google Colab. Pilot outputs tuyệt đối không phát `_SUCCESS.json` hoặc tự nhận `RESULT_READY`/`CANDIDATE`.

Tại Review Round 4 (PR #84, comment-6246509089), @HoaiTam đã phản hồi về lỗ hổng đối soát cross-pilot:
- `generate_local_smoke_report()` trước đây chỉ so khớp `feature_count` (số lượng ứng viên), dẫn đến trường hợp hai pilot A và B có cùng số hàng (ví dụ: 2 hàng) nhưng khác danh tính (`pilot_id`, `mainshock_event_id`, `candidate_event_id`) thì việc chạy trên pilot A rồi truyền `bundle=B, results=results_A` vẫn được chấp thuận, cấp phép sai cho pilot B với kết quả của pilot A.

## 2. Các điểm đã khắc phục theo review của @HoaiTam

### 2.1. [Security & Lineage] Đối soát danh tính nghiêm ngặt và hạt dữ liệu (Grain Matching)
- **Vấn đề:** Nếu chỉ so khớp số lượng hàng `feature_count`, kết quả từ pilot khác có cùng số hàng có thể bị chấp nhận sai lệch.
- **Khắc phục:**
  - **Trích xuất hạt dữ liệu đầu vào:** Trích xuất `(mainshock_event_id, candidate_event_id)` kèm bội số (`collections.Counter`) từ `bundle["features_table"]`, cùng tập hợp `input_mainshocks`.
  - **Đối soát Manifest:** Bắt buộc `manifest.pilot_id == bundle.bundle_id`, `manifest.source_dataset_id == source_ds_id`, `manifest.pilot_run_id == run_id`, `manifest.algorithm_name == algo`.
  - **Đối soát cấu hình và metrics trên đĩa:** Kiểm tra `experiment_config.json` và `experiment_metrics.json` có `dataset_id == source_ds_id`, `experiment_run_id == run_id`, `algorithm_name == algo`.
  - **Đối soát hạt dữ liệu Parquet và In-memory Memberships:**
    - `Counter((m["mainshock_event_id"], m["candidate_event_id"]) for m in memberships) == input_grains`.
    - Đọc `memberships.parquet` trên đĩa và đối soát `Counter(disk_grains) == input_grains`.
    - Kiểm tra mọi bản ghi memberships (in-memory và parquet) đều mang đúng `dataset_id`, `experiment_run_id`, `algorithm_name`.
  - **Đối soát Mainshocks và Lineage của Sequence Summary:**
    - Tập mainshocks trong `summaries` và trong `sequence_summary.parquet` trên đĩa phải khớp tuyệt đối với `input_mainshocks`.
    - Kiểm tra mọi bản ghi summary mang đúng `dataset_id`, `experiment_run_id`, `algorithm_name`.
  - **Regression Test:** Thêm test `test_local_smoke_report_rejects_cross_pilot_lineage_and_grain_mismatch` tạo 2 pilot A và B độc lập (cùng 2 hàng ứng viên nhưng khác event IDs và pilot IDs); kiểm chứng bắt buộc cặp trùng khớp (A + results_A) phải PASS, còn cặp chéo (B + results_A) phải bị REJECT với lỗi lineage/grain mismatch.

### 2.2. [CI] Sửa hai test CI thất bại
- **Policy dependency dùng chung:** Đưa `notebooks/requirements-pilot.txt` ra khỏi `EXP_FILES` trong `scripts/ci-impact.py` để file dependency dùng chung này tuân thủ đúng policy fail-safe về chạy full cả 3 thành phần (`java=full, airflow=full, pilot=full`).
- **Đồng bộ nhãn summary:** Cập nhật đồng bộ assertion trong `tests/ci/test_suites.py` khớp với nhãn summary mới `DAT-02/EXP-01/EXP-02 pilot & baselines`.

### 2.3. [Gate] Thống nhất chế độ Fallback và Colab Migration Gate
- **Vấn đề:** Khi chạy inspection ở môi trường không có `nbclient`, notebook tạo `local-smoke-report.json` ghi `PASSED` và `colab_migration_allowed=true`, dẫn đến Colab gate vẫn mở.
- **Khắc phục:**
  - `scripts/inspect-exp02-notebook.py` truyền rõ ràng cờ môi trường `EXP02_RUNNER_MODE` và `EXP02_FRESH_KERNEL_ACCEPTANCE` ("false" khi chạy isolated exec).
  - `generate_local_smoke_report()` ghi nhận trung thực: trong chế độ fallback, `local_smoke_status="FALLBACK_EXEC_WARNING"`, `colab_migration_allowed=false`, `fresh_kernel_acceptance=false`.
  - `ColabPathAdapter.verify_local_smoke_gate()` bắt buộc kiểm tra `fresh_kernel_acceptance is True`, `colab_migration_allowed is True` và `local_smoke_status == "PASSED"`; từ chối mở gate nếu chạy fallback.

### 2.4. [Integrity] Bắt buộc đủ Checksum trong Manifest & Đối soát dữ liệu vật lý
- **Vấn đề:** Checksum chỉ được so sánh nếu tên file có trong manifest (nếu xóa entry manifest thì bị bỏ qua). Chưa kiểm tra dữ liệu vật lý trên đĩa ngoài bộ nhớ.
- **Khắc phục:**
  - Bắt buộc mọi artifact trong `standard_files` (`memberships.parquet`, `sequence_summary.parquet`, `experiment_metrics.json`, `experiment_config.json`) phải tồn tại entry checksum trong manifest (`MISSING_MANIFEST_CHECKSUM_ENTRY`).
  - Đọc và phân tích trực tiếp file vật lý: parse `experiment_metrics.json` và `experiment_config.json` (bắt lỗi `CORRUPT_PHYSICAL_METRICS_JSON`), đọc `memberships.parquet` và `sequence_summary.parquet` bằng PyArrow (bắt lỗi `CORRUPT_PHYSICAL_MEMBERSHIPS_PARQUET`), đối soát số hàng thực tế khớp với input candidate.

### 2.5. [Contract] Trích xuất non-null Bundle Checksum & Fingerprint
- **Vấn đề:** Report đọc `manifest.checksums["features.parquet"]` trong khi bundle DAT-02 dùng `features-part-00000.parquet`, khiến `bundle_checksum` bị `null`.
- **Khắc phục:**
  - `load_feature_bundle` trả về trường `checksums` được parse từ `checksums.sha256`.
  - `generate_local_smoke_report()` tìm nạp checksum của features parquet từ `features-part-00000.parquet` hoặc `features*.parquet`. Bắt buộc `bundle_checksum` không được null và phải là chuỗi SHA-256 64 ký tự hợp lệ.
  - Ghi nhận `bundle_fingerprint` đầy đủ (`features_sha256`, `pilot_id`, `checksums_index_sha256`) vào report.
  - `ColabPathAdapter.verify_local_smoke_gate()` từ chối nếu `bundle_checksum` thiếu hoặc null.

## 3. Checks và tests đã chạy

- **Unit and Contract Test Suite (`tests/test_baseline_algorithms.py`):**
  - Lệnh: `python -m unittest tests/test_baseline_algorithms.py`
  - Kết quả: **19 tests PASS**, 0 errors, 0 failures.
  - Bao gồm các regression tests:
    - `test_local_smoke_report_rejects_cross_pilot_lineage_and_grain_mismatch`
    - `test_local_smoke_report_rejects_missing_manifest_checksum_entry`
    - `test_local_smoke_report_rejects_corrupted_physical_json_and_parquet`
    - `test_local_smoke_report_extracts_non_null_bundle_checksum_and_fingerprint`
    - `test_fallback_mode_smoke_report_blocks_colab_gate`
    - `test_colab_adapter_rejects_null_or_invalid_bundle_checksum`

- **EXP-01 Harness Suite (`tests/test_experiment_harness.py`):**
  - Lệnh: `python -m unittest tests/test_experiment_harness.py`
  - Kết quả: **44 tests PASS**, 0 errors, 0 failures.

- **CI Test Suite (`tests/ci/`):**
  - Lệnh: `python -m unittest tests/ci/test_impact.py tests/ci/test_suites.py tests/ci/test_workflow_contract.py`
  - Kết quả: **37 tests PASS**, 0 errors, 0 failures.

- **Notebook Headless Runner (`scripts/inspect-exp02-notebook.py`):**
  - Lệnh: `python scripts/inspect-exp02-notebook.py`
  - Kết quả: 7/7 code cells thực thi thành công; lưu artifact và report tại `staging/ml-experiments/exp02-baselines/`; ghi nhận đúng trạng thái fallback khi chưa có fresh kernel engine.

- **Whitespace Check:**
  - Lệnh: `git diff --check`
  - Kết quả: 0 whitespace errors.

## 4. Giới hạn / Rủi ro / Bước tiếp theo

- **Colab pilot portability:** Việc chạy đối soát cùng code/config/bundle trên Google Colab sau local gate thuộc về giai đoạn tiếp theo của [EXP-02 - Chạy Window, DBSCAN và HDBSCAN global](docs/task/tasks/EXP-02.md).
- **Official research dataset export:** Nghiên cứu chính thức trên toàn kỳ phụ thuộc vào [MLD-05 - Validate feature snapshot và export Parquet bundle](docs/task/tasks/MLD-05.md); pilot hiện tại chỉ dùng phục vụ kiểm tra kỹ thuật (DEVELOPMENT_ONLY).
- **Kỹ thuật phân cụm nâng cao / Adaptive sequence resolver:** Thuật toán giải quyết candidate thuộc nhiều sequence thuộc về [EXP-03 - Xây HDBSCAN adaptive và giải quyết multi-sequence](docs/task/tasks/EXP-03.md).
- **Import Iceberg `ml.*`:** Việc nạp kết quả nghiên cứu vào bảng Iceberg thuộc về [MLI-03 - Validate/import kết quả và commit bảng ML Iceberg](docs/task/tasks/MLI-03.md).
- **Reviewer:** Reviewer độc lập cho task P0 này hiện là `unassigned`.
