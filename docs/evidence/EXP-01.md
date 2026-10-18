# EXP-01 — Jupyter harness và chuẩn bị chuyển sang Colab evidence

Ngày kiểm chứng: 2026-10-18.
Branch: `feat/exp-01-jupyter-harness`.
Assignee: `rosy179`.
Reviewer: `HoaiTam` (phản hồi review tại [PR #83 comment-6229834764](https://github.com/HoaiTam/japan-earthquake-etl/pull/83#issuecomment-6229834764) và [comment-6231696916](https://github.com/HoaiTam/japan-earthquake-etl/pull/83#issuecomment-6231696916)).

## 1. Mục tiêu và bối cảnh

Task EXP-01 tạo shared Python module và thin Jupyter notebook harness tái lập được để mọi model đọc cùng feature bundle, ghi artifact máy đọc được và không phụ thuộc output hiển thị trên màn hình. Jupyter local là runtime đầu tiên; dùng lại cùng module/notebook khi chuyển sang Colab sau local gate EXP-02, không viết hai model pipelines.

## 2. Các điểm review từ Hoài Tâm và giải pháp khắc phục

### Đợt 1 (PR #83 comment-6229834764)
1. **[P1] Loader OFFICIAL:** Bắt buộc checksum set bao phủ bundle; index đủ files; kiểm tra unique grain, mainshock self-event tồn tại và mang zero vector.
2. **[P1] Output tương thích MLI-03 importer:** Không sinh `checksums.sha256` lẻ trên đĩa; schema Parquet khớp `MlResultBundleReader.java`; thêm `runtime_environment_json`.
3. **[P1] Run identity & completion marker:** Đưa `random_seed` vào hash payload; đồng bộ `effective_run_id`; reconciliation check số dòng và mainshock IDs; read-after-write verification.
4. **[P2] Fresh-kernel runner:** Báo rõ `FALLBACK_EXEC_WARNING` và `fresh_kernel_acceptance=false` khi không có `nbclient/ipykernel`.
5. **[P2] CI wiring & Whitespace:** Nối `test_experiment_harness.py` vào `test-pilot` trong `Makefile`.

### Đợt 2 (PR #83 comment-6231696916)
1. **[P1] Loader OFFICIAL schema & type enforcement (`notebooks/experiment_harness.py`):**
   - Xác thực bắt buộc kiểm tra null (`col.null_count == 0` -> `EXP_NULL_REQUIRED_ID_COL`) và chuỗi rỗng (`len(val.strip()) > 0` -> `EXP_EMPTY_REQUIRED_ID_COL`) trên các cột định danh bắt buộc: `dataset_id`, `mainshock_event_id`, `candidate_event_id`.
   - Xác thực kiểu dữ liệu PyArrow: cột ID bắt buộc là string/large_string (`EXP_INVALID_ID_COL_TYPE`), vector columns (`x_scaled`, `y_scaled`, `z_scaled`, `time_scaled`) bắt buộc là floating type (`pa.types.is_floating` -> `EXP_INVALID_VECTOR_COL_TYPE`), từ chối `int64` hoặc các kiểu khác; từ chối `null` vector (`EXP_NULL_VECTOR_COL`).
   - Sắp xếp kiểm tra kiểu dữ liệu PyArrow và null/empty constraint cho toàn bộ ID và vector columns ngay tại bước schema checks trước khi đối soát count và lineage.
   - Bổ sung 5 negative unit tests trong `tests/test_experiment_harness.py`.

2. **[P1] Writer Lineage Guard (`notebooks/experiment_harness.py`):**
   - Loại bỏ hoàn toàn việc gán đè `dataset_id = source_dataset_id` trong `write_experiment_result`.
   - Xác thực fail-closed: kiểm tra toàn bộ rows của `memberships` và `summaries` phải có `dataset_id == source_dataset_id`. Nếu có bất kỳ dòng nào mang `dataset_id` khác bundle source dataset -> ném `EXP_DATASET_LINEAGE_MISMATCH`.
   - Hỗ trợ và kiểm tra đầy đủ cho cả hai kiểu input: `list[dict]` và `pa.Table`.
   - Chỉ đồng bộ `effective_run_id`, bảo toàn nguyên vẹn `dataset_id`.
   - Bổ sung 4 unit tests xác thực reject lineage mismatch cho memberships (list + table) và summaries (list + table).

3. **[P2] JUnit 5 Assumptions cho cross-process integration test (`ResultBundleContractTest.java`):**
   - Thay thế các lệnh `if (...) return;` im lặng trong `pythonWriterBundleIsReadSuccessfullyByMlResultBundleReader` bằng `Assumptions.assumeTrue(...)`.
   - Đảm bảo khi môi trường thiếu Python, PyArrow hoặc file script harness, test sẽ được JUnit 5 ghi nhận là **SKIPPED** rõ ràng thay vì báo PASS giả lập.

## 3. Checks và tests đã chạy

- **Python unit test suite (`tests/test_experiment_harness.py`):**
  - Chạy: `python -m unittest tests/test_experiment_harness.py`
  - Kết quả: **36 tests PASS**, 0 errors, 0 failures.

- **Java cross-process integration test (`ResultBundleContractTest`):**
  - Chạy: `$env:JAVA_HOME = "C:\Program Files\Java\jdk-23"; .\mvnw.cmd test -Dtest=ResultBundleContractTest`
  - Kết quả: **8 tests PASS**, 0 errors, 0 failures (bao gồm `pythonWriterBundleIsReadSuccessfullyByMlResultBundleReader`).

- **Java ML result import suite (`MlResultImportJobTest`):**
  - Chạy: `$env:JAVA_HOME = "C:\Program Files\Java\jdk-23"; .\mvnw.cmd test -Dtest=MlResultImportJobTest`
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
