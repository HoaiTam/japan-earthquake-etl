# EXP-01 — Jupyter harness và chuẩn bị chuyển sang Colab evidence

Ngày kiểm chứng: 2026-10-17.
Branch: `feat/exp-01-jupyter-harness`.
Assignee: `rosy179`.
Reviewer: `unassigned`.

## 1. Mục tiêu và bối cảnh

Task EXP-01 tạo shared Python module và thin Jupyter notebook harness tái lập được để mọi model đọc cùng feature bundle, ghi artifact máy đọc được và không phụ thuộc output hiển thị trên màn hình. Jupyter local là runtime đầu tiên; dùng lại cùng module/notebook khi chuyển sang Colab sau local gate EXP-02, không viết hai model pipelines.

## 2. Checks và tests đã chạy

- **Python unit test suite (`tests/test_experiment_harness.py`):**
  - Chạy: `python -m unittest tests/test_experiment_harness.py`
  - Kết quả: **15 tests PASS**, 0 errors, 0 failures.
  - Phạm vi bao phủ:
    1. `test_load_development_pilot_succeeds`: Nạp bundle DAT-02 pilot ở mode DEVELOPMENT thành công.
    2. `test_corrupted_checksum_fails_before_fit`: Bắt lỗi checksum sai trước khi fit model (`EXP_CHECKSUM_MISMATCH`).
    3. `test_nonfinite_feature_fails_before_fit`: Bắt lỗi NaN/Infinity trong vector trước khi fit model (`PILOT_NONFINITE_VALUE`).
    4. `test_mode_separation_development_rejects_official_dataset`: Loader DEVELOPMENT từ chối nhận official export (`DEVELOPMENT_LOADER_REJECTS_OFFICIAL_DATASET`).
    5. `test_mode_separation_official_rejects_pilot_bundle`: Loader OFFICIAL từ chối nhận pilot bundle (`OFFICIAL_LOADER_REJECTS_PILOT_BUNDLE`).
    6. `test_load_official_dataset_succeeds`: Nạp bundle MLD-05 export ở mode OFFICIAL thành công.
    7. `test_official_dataset_not_exported_is_rejected`: Bắt lỗi dataset chưa EXPORTED (`EXP_DATASET_NOT_EXPORTED`).
    8. `test_output_skeleton_conforms_to_schema`: Tạo memberships và summary skeleton đúng schema `result-bundle-v1.json`.
    9. `test_development_result_writer_never_produces_success_marker`: Development writer ghi `pilot_result_manifest.json`, **tuyệt đối không tạo `_SUCCESS.json`**.
    10. `test_official_result_writer_produces_success_marker_with_checksums`: Official writer ghi đủ artifacts và `_SUCCESS.json` chứa checksums/counts.
    11. `test_incomplete_output_never_writes_success_marker`: Quá trình ghi bị gián đoạn/lỗi thì không để lại `_SUCCESS.json`.
    12. `test_colab_adapter_gate_passes_with_passed_local_report`: Colab adapter cho phép migration khi local smoke status `PASSED`.
    13. `test_colab_adapter_gate_blocks_when_smoke_failed_or_missing`: Colab adapter chặn migration khi local smoke `FAILED` hoặc thiếu báo cáo.
    14. `test_colab_adapter_path_resolution_and_unsafe_metadata`: Adapter giải quyết đường dẫn qua parameter, từ chối credential/token trong đường dẫn.
    15. `test_notebook_restart_run_all_passes`: Tự động chạy restart kernel → run all notebook `notebooks/exp_01_harness_smoke.ipynb` từ đầu đạt `RESTART_RUN_ALL_PASSED`.

- **Notebook headless runner (`scripts/inspect-exp01-notebook.py`):**
  - Chạy: `python scripts/inspect-exp01-notebook.py`
  - Kết quả:
    - `notebook_status`: `RESTART_RUN_ALL_PASSED`
    - `local_smoke_status`: `PASSED`
    - `colab_migration_allowed`: `true`
    - `executed_code_cells`: 4
    - `source_notebook_outputs_cleared`: `true`

- **Task status check (`scripts/check-task-status.sh`):**
  - Chạy qua Git Bash: `./scripts/check-task-status.sh`
  - Kết quả: `Task status check passed: 76 task files, tracking sections and index rows agree.`

- **Baseline & CI impact check:**
  - `./scripts/check-mvp-baseline.sh`: `PLN-01 HDBSCAN MVP baseline check passed.`
  - `./scripts/check-week-5-plan.sh`: `Week 5 plan check passed: 12 Core tasks, 72h...`
  - `python -m unittest discover -s tests/ci`: 37 tests PASS.

## 3. Deliverables của task

1. `notebooks/experiment_harness.py`:
   - Phân định rõ hai chế độ: `DEVELOPMENT` (cho DAT-02 pilot) và `OFFICIAL` (cho MLD-05 dataset export).
   - Kiểm tra checksums, schema, tính hữu hạn (no NaN/Inf), và vector zero của self-event trước khi fit.
   - Hàm dựng skeleton output `build_membership_rows` và `build_sequence_summary_row` khớp 100% schema Arrow/Parquet của `result-bundle-v1.json`.
   - Kết quả development chỉ ghi `pilot_run_id` và `pilot_result_manifest.json`, không bao giờ ghi `_SUCCESS.json`.
   - Kết quả official ghi `experiment_run_id` và chỉ ghi `_SUCCESS.json` ở bước cuối sau khi tính đủ SHA-256 của toàn bộ artifacts.
   - `ColabPathAdapter` hỗ trợ đường dẫn Google Drive qua parameter và chặn di chuyển lên cloud nếu local smoke chưa đạt `PASSED`.
2. `notebooks/requirements-lock.txt`:
   - Khóa phiên bản cụ thể cho runtime: `pyarrow==21.0.0`, `numpy==2.2.2`, `scipy==1.15.1`, `scikit-learn==1.6.1`, `hdbscan==0.8.40`, `nbformat==5.10.4`, `nbclient==0.10.2`, `ipykernel==6.30.1`, `jupyterlab==4.4.7`.
3. `notebooks/exp_01_harness_smoke.ipynb`:
   - Thin Jupyter notebook kiểm tra toàn bộ luồng nạp, dựng skeleton, ghi result development và kiểm tra Colab adapter.
   - Không chứa credential, không chứa absolute path người dùng, outputs được dọn sạch trước khi commit.
4. `scripts/inspect-exp01-notebook.py`:
   - Runner kiểm tra tự động chạy fresh kernel và xác thực notebook không bị lưu output vào git.
5. `tests/test_experiment_harness.py`:
   - Bộ 15 tests tự động không phụ thuộc mạng, Docker hay staging ngoài git.
6. `Makefile`:
   - Bổ sung targets: `test-exp01`, `inspect-exp01`, `jupyter-exp01`.

## 4. Giới hạn / Rủi ro / Bước tiếp theo

- Chưa triển khai thuật toán clustering cụ thể (Window, DBSCAN, HDBSCAN global): việc này thuộc về [EXP-02 - Chạy Window, DBSCAN và HDBSCAN global](file:///c:/Users/Lenovo/UIT/Nam4/IE212/japan-earthquake-etl/docs/task/tasks/EXP-02.md).
- Chưa có dataset nghiên cứu chính thức được export: việc này thuộc về [MLD-05 - Validate feature snapshot và export Parquet bundle](file:///c:/Users/Lenovo/UIT/Nam4/IE212/japan-earthquake-etl/docs/task/tasks/MLD-05.md).
- Chưa import kết quả vào Iceberg `ml.*`: việc này thuộc về [MLI-03 - Validate/import kết quả và commit bảng ML Iceberg](file:///c:/Users/Lenovo/UIT/Nam4/IE212/japan-earthquake-etl/docs/task/tasks/MLI-03.md).
- Reviewer độc lập cho task P0 này hiện là `unassigned`.
