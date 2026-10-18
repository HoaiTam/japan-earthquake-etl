# EXP-01 — Jupyter harness và chuẩn bị chuyển sang Colab evidence

Ngày kiểm chứng: 2026-10-18.
Branch: `feat/exp-01-jupyter-harness`.
Assignee: `rosy179`.
Reviewer: `HoaiTam` (phản hồi review tại [PR #83 comment-6229834764](https://github.com/HoaiTam/japan-earthquake-etl/pull/83#issuecomment-6229834764), [comment-6231696916](https://github.com/HoaiTam/japan-earthquake-etl/pull/83#issuecomment-6231696916), [comment-6244707869](https://github.com/HoaiTam/japan-earthquake-etl/pull/83#issuecomment-6244707869) và [comment-6244865924](https://github.com/HoaiTam/japan-earthquake-etl/pull/83#issuecomment-6244865924)).

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

### Đợt 3 (PR #83 comment-6244707869)
1. **[P1] Phân biệt lần chạy mới và retry; bảo vệ tính bất biến của completed bundle:**
   - **Lần chạy mới có Run ID riêng:** Đưa `execution_token` duy nhất (qua `execution_id` hoặc UUID token) vào payload sinh `effective_run_id`. Đảm bảo hai lần chạy mới khác nhau dù cùng dataset, algorithm, model config và random seed sẽ không bao giờ bị trùng `experiment_run_id`.
   - **Idempotent Retry & Reuse:** Khi caller thực hiện retry cho cùng run ID (`is_retry=True` hoặc `experiment_run_id == existing_run_id`) vào thư mục đã hoàn tất (`out_path` chứa `_SUCCESS.json` hoặc `pilot_result_manifest.json`), writer kiểm tra toàn diện SHA-256 các artifact files hiện có. Nếu còn nguyên vẹn, writer trả về kết quả reuse mà **không sửa đổi bất kỳ byte nào trên đĩa** (không sinh lại `timestamp_utc` mới làm lệch checksum).
   - **Pinned metadata khi retry clone:** Hỗ trợ caller truyền `timestamp_utc` và `completed_at_utc` đã pin trong `write_experiment_result` và `get_runtime_metadata`, giúp việc tái lập bundle ở thư mục mới giữ nguyên 100% SHA-256.
   - **Chống ghi đè completed bundle (`EXP_COMPLETED_BUNDLE_CONFLICT`):** Tuyệt đối không âm thầm unlink `_SUCCESS.json` và ghi đè artifact. Nếu thư mục đã có completed bundle và không phải retry hợp lệ của chính run đó, writer ném ngay `EXP_COMPLETED_BUNDLE_CONFLICT` để bảo vệ dữ liệu bất biến.
   - **Giữ nguyên guard `IMP_EXPERIMENT_RUN_REUSED` của Java validator:** Java importer (`ResultBundleValidator`) kiểm tra fingerprint đối với `previousBundleSha256`; retry giữ nguyên fingerprint nên được nhận idempotently; run khác có fingerprint khác không bị va chạm.
   - **Regression tests:** Thêm 3 regression tests trong `tests/test_experiment_harness.py` và mở rộng test tích hợp cross-process `ResultBundleContractTest.java` (kiểm tra cả retry idempotent nhận dạng khớp `bundleSha256()` và conflict rejection khi cố ý ghi đè).

### Đợt 4 (PR #83 comment-6244865924)
1. **[P1] Ràng buộc chặt chẽ Run ID, đối chiếu Config/Seed và Lineage Guard trước retry reuse:**
   - **Lineage Guard chạy đầu tiên:** Di chuyển kiểm tra lineage trên `memberships` và `summaries` lên ngay đầu hàm `write_experiment_result` trước khối kiểm tra retry reuse. Nếu dữ liệu truyền vào chứa `dataset_id` không khớp với `source_dataset_id`, ném ngay `EXP_DATASET_LINEAGE_MISMATCH`, không bao giờ bypass lineage guard dù caller đặt `is_retry=True`.
   - **Ràng buộc chính xác Run ID:** Retry chỉ cho phép tái sử dụng khi `experiment_run_id is None or experiment_run_id == existing_run_id`. Nếu caller chỉ định một run ID khác với run ID trong completed bundle, lập tức fail-closed với `EXP_COMPLETED_BUNDLE_CONFLICT`.
   - **Đối chiếu đầy đủ config artifact:** Đọc `experiment_config.json` trên đĩa và đối chiếu toàn bộ các tham số: `algorithm_name`, `algorithm_version`, `model_config_version`, `model_config` (deterministic canonical JSON), `random_seed`, và `code_version`. Mọi thay đổi về thuật toán, siêu tham số, hoặc seed đều bị từ chối tái sử dụng và fail-closed với `EXP_COMPLETED_BUNDLE_CONFLICT`.

2. **[P1] Xác thực nghiêm ngặt Marker Status, Checksums Map và File Integrity:**
   - **Marker status & schema:** Bắt buộc `status == "COMPLETED"` và `schema_version == "1.0"` đối với OFFICIAL mode; `status == "DEVELOPMENT_RUN_COMPLETED"` và `artifact_kind == "PILOT_EXPERIMENT_RESULT"` đối với DEVELOPMENT mode.
   - **Checksums map:** Bắt buộc là dictionary không rỗng và phải bao phủ đầy đủ toàn bộ 5 standard required files (`memberships.parquet`, `sequence_summary.parquet`, `experiment_metrics.json`, `experiment_config.json`, `requirements-lock.txt`). Bắt buộc giá trị SHA-256 là 64 ký tự hex thường và đường dẫn tương đối an toàn (ngăn chặn path traversal).
   - **Đọc và xác thực từng file thật trên đĩa:** Đọc bytes từng file và tính hash SHA-256 đối chiếu với checksum đã lưu. Nếu thiếu file hoặc sai lệch hash, bundle không hợp lệ và ném `EXP_COMPLETED_BUNDLE_CONFLICT`, tuyệt đối không âm thầm tái sử dụng bundle rỗng hoặc bị hư hỏng.

## 3. Checks và tests đã chạy

- **Python unit test suite (`tests/test_experiment_harness.py`):**
  - Chạy: `python -m unittest tests/test_experiment_harness.py`
  - Kết quả: **44 tests PASS**, 0 errors, 0 failures (bổ sung 5 tests chuyên biệt kiểm tra retry lineage guard, run_id matching, config matching, empty/incomplete checksums map, và invalid marker status/tampered files).

- **Java cross-process integration test (`ResultBundleContractTest`):**
  - Chạy: `$env:JAVA_HOME = "C:\Program Files\Java\jdk-23"; .\mvnw.cmd test -Dtest=ResultBundleContractTest`
  - Kết quả: **8 tests PASS**, 0 errors, 0 failures (bao gồm `pythonWriterBundleIsReadSuccessfullyByMlResultBundleReader` test cả fresh run, retry idempotent reuse và conflict rejection).

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
