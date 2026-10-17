# EXP-01 — Jupyter harness và chuẩn bị chuyển sang Colab evidence

Ngày kiểm chứng: 2026-10-17.
Branch: `feat/exp-01-jupyter-harness`.
Assignee: `rosy179`.
Reviewer: `HoaiTam` (phản hồi review tại [PR #83 comment-6229834764](https://github.com/HoaiTam/japan-earthquake-etl/pull/83#issuecomment-6229834764)).

## 1. Mục tiêu và bối cảnh

Task EXP-01 tạo shared Python module và thin Jupyter notebook harness tái lập được để mọi model đọc cùng feature bundle, ghi artifact máy đọc được và không phụ thuộc output hiển thị trên màn hình. Jupyter local là runtime đầu tiên; dùng lại cùng module/notebook khi chuyển sang Colab sau local gate EXP-02, không viết hai model pipelines.

## 2. Các điểm review từ Hoài Tâm và giải pháp khắc phục

1. **[P1] Loader OFFICIAL:**
   - Bắt buộc checksum set bao phủ toàn bộ files trong bundle; từ chối file `checksums.sha256` rỗng (`EXP_EMPTY_CHECKSUMS`).
   - Kiểm tra `found_files - {"checksums.sha256"} == checksum_entries.keys()`; bắt buộc index đủ `dataset_manifest.json` và `features-part-*.parquet`.
   - Bắt buộc đủ các cột ID: `dataset_id`, `mainshock_event_id`, `candidate_event_id` cùng vector columns.
   - Kiểm tra lineage: `dataset_id` trong mọi dòng Parquet phải khớp `dataset_id` trong manifest (`EXP_DATASET_LINEAGE_MISMATCH`).
   - Kiểm tra unique grain `(dataset_id, mainshock_event_id, candidate_event_id)` (`EXP_DUPLICATE_GRAIN`).
   - Kiểm tra mainshock self-event bắt buộc tồn tại (`EXP_MISSING_SELF_EVENT`) và vector của self-event phải là vector zero (`EXP_NONZERO_SELF_VECTOR`).
   - Bổ sung 6 negative unit tests chi tiết trong `tests/test_experiment_harness.py`.

2. **[P1] Official output tương thích MLI-03 importer:**
   - Trong chế độ `OFFICIAL`, tuyệt đối **không sinh file `checksums.sha256`** trên đĩa (vì `MlResultBundleReader.java` ném `IMP_UNSAFE_ARTIFACT_METADATA` cho bất kỳ file không rõ nguồn gốc ngoài required 5 files). Checksums được nhúng trực tiếp trong `_SUCCESS.json`.
   - Cập nhật schema Parquet `metric_reason_codes` có child list element tên `"element"` và `nullable=False` (`pa.list_(pa.field("element", pa.string(), nullable=False))`) khớp 100% với `MlResultBundleReader.java`.
   - Ghi trường `"runtime_environment_json"` chứa chuỗi JSON chính tắc về môi trường runtime trong `experiment_config.json`.
   - Cập nhật `MlResultBundleReader.java` và `ResultBundleContractTest.java` chấp nhận schema Parquet có trường khớp hoàn toàn (cho phép root group name khác biệt giữa Java ExampleParquetWriter `"result"` và Arrow C++ / PyArrow `"schema"`).
   - Bổ sung integration test cross-process Python writer → Java reader (`ResultBundleContractTest.pythonWriterBundleIsReadSuccessfullyByMlResultBundleReader`).

3. **[P1] Run identity & completion marker:**
   - Đưa `random_seed` vào payload sinh identity digest để các seed khác nhau luôn sinh `run_id` khác nhau.
   - Đồng bộ `effective_run_id` và `source_dataset_id` vào toàn bộ rows của memberships và summaries trước khi ghi Parquet (không bao giờ giữ lại ID tạm thời).
   - Reconciliation check: Bắt buộc `memberships` và `summaries` có số dòng > 0 và tập hợp `mainshock_event_id` phải khớp nhau 100% (`EXP_SUMMARY_WINDOW_MISMATCH`).
   - Bổ sung read-after-write verification đọc lại Parquet và kiểm tra SHA-256 các file trước khi ghi marker atomically `_SUCCESS.json`.

4. **[P2] Notebook runner fresh-kernel acceptance:**
   - Trong `scripts/inspect-exp01-notebook.py`, nếu thiếu `nbclient/ipykernel` và chỉ chạy exec fallback thì trả `notebook_status="FALLBACK_EXEC_PASSED"`, `local_smoke_status="FALLBACK_EXEC_WARNING"`, `colab_migration_allowed=false`, `fresh_kernel_acceptance=false`.
   - Chỉ khi thực sự có fresh isolated kernel runner mới trả `RESTART_RUN_ALL_PASSED` và `fresh_kernel_acceptance=true`.

5. **[P2] CI wiring & Whitespace:**
   - Nối `test_experiment_harness.py` vào target `test-pilot` trong `Makefile`.
   - Xóa trailing whitespace tại `scripts/inspect-exp01-notebook.py:27` và dòng trống thừa cuối `tests/test_experiment_harness.py`. `git diff --check` PASS 100%.

## 3. Checks và tests đã chạy

- **Python unit test suite (`tests/test_experiment_harness.py`):**
  - Chạy: `python -m unittest tests/test_experiment_harness.py`
  - Kết quả: **27 tests PASS**, 0 errors, 0 failures.

- **Java cross-process integration test (`ResultBundleContractTest`):**
  - Chạy: `./mvnw.cmd test -Dtest=ResultBundleContractTest`
  - Kết quả: **8 tests PASS**, 0 errors, 0 failures (bao gồm `pythonWriterBundleIsReadSuccessfullyByMlResultBundleReader`).

- **Java ML result import suite (`MlResultImportJobTest`):**
  - Chạy: `./mvnw.cmd test -Dtest=MlResultImportJobTest`
  - Kết quả: **7 tests PASS**, 0 errors, 0 failures.

- **Notebook headless runner (`scripts/inspect-exp01-notebook.py`):**
  - Chạy: `python scripts/inspect-exp01-notebook.py`
  - Kết quả: Clean execution, output JSON hợp lệ, notebook source hash không bị thay đổi.

- **Git whitespace check:**
  - Chạy: `git diff --check`
  - Kết quả: PASS, không có trailing whitespace hay extra blank lines ở EOF.

- **Task status check (`scripts/check-task-status.sh`):**
  - Chạy qua Git Bash: `./scripts/check-task-status.sh`
  - Kết quả: `Task status check passed: 76 task files, tracking sections and index rows agree.`

## 4. Giới hạn / Rủi ro / Bước tiếp theo

- Chưa triển khai thuật toán clustering cụ thể (Window, DBSCAN, HDBSCAN global): việc này thuộc về [EXP-02 - Chạy Window, DBSCAN và HDBSCAN global](docs/task/tasks/EXP-02.md).
- Chưa có dataset nghiên cứu chính thức được export: việc này thuộc về [MLD-05 - Validate feature snapshot và export Parquet bundle](docs/task/tasks/MLD-05.md).
- Chưa import kết quả vào Iceberg `ml.*`: việc này thuộc về [MLI-03 - Validate/import kết quả và commit bảng ML Iceberg](docs/task/tasks/MLI-03.md).
