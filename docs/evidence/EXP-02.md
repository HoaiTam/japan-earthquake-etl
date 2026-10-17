# EXP-02 — Chạy Window, DBSCAN và HDBSCAN global evidence

Ngày kiểm chứng: 2026-10-17.
Branch: `feat/exp-02-baseline-algorithms`.
Assignee: `rosy179`.
Reviewer: `unassigned`.

## 1. Mục tiêu và bối cảnh

Task EXP-02 triển khai ba thuật toán baseline trên cùng pilot feature bundle DAT-02 (đặc trưng 4-D chuẩn hóa `[x_scaled, y_scaled, z_scaled, time_scaled]` do Java/Spark MLD-04 tạo) để đánh giá:
1. Liệu density clustering (DBSCAN/HDBSCAN) có giảm background noise tốt hơn so với counting window rule hay không.
2. HDBSCAN với variable density clustering và membership probabilities có mang lại thông tin định lượng hơn DBSCAN hay không.

Theo quy trình `JUPYTER_PILOT_WORKFLOW.md`, toàn bộ phát triển và kiểm chứng kỹ thuật được thực hiện trên Jupyter local trước; chỉ khi `local_smoke_status=PASSED` và `colab_migration_allowed=true` mới được phép chuyển sang Google Colab. Pilot outputs tuyệt đối không phát `_SUCCESS.json` hoặc tự nhận `RESULT_READY`/`CANDIDATE`.

## 2. Các quy tắc hợp đồng cốt lõi đã kiểm chứng

1. **Per-mainshock window fitting:** Cả ba thuật toán đều fit độc lập cho từng cửa sổ mainshock; không fit toàn catalog gộp.
2. **Mainshock cluster isolation:** Chỉ cụm chứa chính mainshock được coi là sequence; nếu mainshock bị gán nhãn `-1` (noise), cửa sổ ghi nhận `window_result_status="FAILED"`, `failure_reason_code="EXP_MAINSHOCK_CLASSIFIED_AS_NOISE"`, không có sequence member nào, và tuyệt đối không chọn cụm gần nhất.
3. **Probability policy:**
   - `WINDOW`: `cluster_id`, `is_noise`, `is_mainshock_cluster_member`, `membership_probability` bắt buộc là `None`.
   - `DBSCAN`: `cluster_id` và `is_noise` hợp lệ (`-1` cho noise), `membership_probability` bắt buộc là `None`.
   - `HDBSCAN_GLOBAL`: `membership_probability` bắt buộc là giá trị thực hữu hạn trong `[0.0, 1.0]`.
4. **Input immutability:** Bảng/bản ghi candidate đầu vào được bảo toàn nguyên vẹn, không bị mutate trong suốt quá trình fit và trích xuất đặc trưng.
5. **Terminology:** Toàn bộ kết quả đầu ra được gọi là sequence candidate / aftershock candidate; không tuyên bố quan hệ nhân quả vật lý hoặc dự đoán động đất.
6. **Result marker separation:** Kết quả pilot ghi `pilot_result_manifest.json`, không bao giờ ghi `_SUCCESS.json`.

## 3. Checks và tests đã chạy

- **Unit and Contract Test Suite (`tests/test_baseline_algorithms.py`):**
  - Lệnh: `python -m unittest tests/test_baseline_algorithms.py`
  - Kết quả: **8 tests PASS**, 0 errors, 0 failures (thời gian chạy: ~1.95s).
  - Chi tiết các test cases:
    1. `test_window_baseline_identifies_post_and_preserves_nulls`: Xác thực WINDOW baseline nhận diện đúng các POST candidate trong window làm aftershocks và giữ nguyên các trường cluster/probability là `None`.
    2. `test_dbscan_baseline_cluster_and_noise_separation`: Xác thực DBSCAN phân tách cụm sequence quanh mainshock và gán nhãn `-1` cho điểm nhiễu không gian xa (`FAR_NOISE`), xác nhận khả năng giảm background event so với window rule.
    3. `test_dbscan_mainshock_classified_as_noise_fails_window`: Xác thực quy tắc nghiêm ngặt: khi mainshock bị gán nhãn `-1`, toàn bộ window bị đánh dấu `EXP_MAINSHOCK_CLASSIFIED_AS_NOISE`, không nhận vơ cụm khác.
    4. `test_hdbscan_global_baseline_probabilities_and_mainshock_cluster`: Xác thực HDBSCAN global tính toán xác suất thành viên hợp lệ trong `[0.0, 1.0]` và nhận diện đúng cụm mainshock.
    5. `test_hdbscan_multiple_clusters_isolates_only_mainshock_cluster`: Xác thực khi cửa sổ có nhiều cụm động đất khác nhau, chỉ duy nhất cụm chứa mainshock được lấy làm sequence; các cụm phụ xa bị loại khỏi sequence (`NOT_MEMBER`).
    6. `test_empty_post_candidates`: Xác thực trường hợp cửa sổ không có sự kiện POST nào (chỉ có mainshock hoặc mainshock + foreshock PRE); cả ba thuật toán xử lý trơn tru với `aftershock_count=0` và `productive_sequence_detected=False`.
    7. `test_input_candidates_are_not_mutated`: Xác thực tính bất biến của dữ liệu candidate đầu vào sau khi chạy cả ba thuật toán.
    8. `test_run_all_baselines_and_generate_local_smoke_report`: Xác thực hàm điều phối `run_baseline_experiment` và xuất `local-smoke-report.json` với `local_smoke_status="PASSED"`.

- **Notebook headless runner (`scripts/inspect-exp02-notebook.py`):**
  - Lệnh: `python scripts/inspect-exp02-notebook.py`
  - Kết quả:
    - `notebook_status`: `RESTART_RUN_ALL_PASSED`
    - `local_smoke_status`: `PASSED`
    - `colab_migration_allowed`: `true`
    - `executed_code_cells`: 7
    - `source_notebook_outputs_cleared`: `true`
    - `runtime_seconds`: ~3.98s

- **EXP-01 Regression Test (`tests/test_experiment_harness.py`):**
  - Lệnh: `python -m unittest tests/test_experiment_harness.py`
  - Kết quả: **15 tests PASS**, 0 errors, 0 failures.

- **EXP-01 Notebook Inspection (`scripts/inspect-exp01-notebook.py`):**
  - Lệnh: `python scripts/inspect-exp01-notebook.py`
  - Kết quả: `RESTART_RUN_ALL_PASSED`, `local_smoke_status`: `PASSED`.

- **CI Impact Suite (`tests/ci`):**
  - Lệnh: `python -m unittest discover -s tests/ci`
  - Kết quả: **37 tests PASS**.

## 4. Deliverables của task

1. **Model Configurations:**
   - `config/ml/models/window_baseline_v1.json`: Cấu hình window baseline temporal rule.
   - `config/ml/models/dbscan_global_dev_v1.json`: Cấu hình DBSCAN (`eps=0.5, min_samples=3, metric=euclidean`).
   - `config/ml/models/hdbscan_global_dev_v1.json`: Cấu hình HDBSCAN (`min_cluster_size=3, min_samples=2, allow_single_cluster=true`).
2. **Algorithm Implementation Module:**
   - `notebooks/baseline_models.py`: Triển khai các hàm fit `fit_window_baseline`, `fit_dbscan_baseline`, `fit_hdbscan_global_baseline`, runner đa thuật toán `run_baseline_experiment`, và generator `generate_local_smoke_report`.
3. **Thin Jupyter Smoke Notebook:**
   - `notebooks/exp_02_baseline_algorithms.ipynb`: Notebook mỏng thực hiện toàn bộ quy trình tải bundle, fit 3 baseline, đối soát đếm candidate/sequence member/noise, và xuất báo cáo kiểm định. Đã dọn sạch outputs và execution counts.
4. **Notebook Inspection Script:**
   - `scripts/inspect-exp02-notebook.py`: Script tự động kiểm tra restart-run-all không sửa source notebook.
5. **Unit & Contract Test Suite:**
   - `tests/test_baseline_algorithms.py`: Bộ 8 tests tự động kiểm thử toàn diện các điều kiện biên và hợp đồng dữ liệu.
6. **Makefile Integration:**
   - Bổ sung targets: `test-exp02`, `inspect-exp02`, `jupyter-exp02`.

## 5. Giới hạn / Rủi ro / Bước tiếp theo

- **Colab pilot portability:** Việc chạy đối soát cùng code/config/bundle trên Google Colab sau local gate thuộc về giai đoạn tiếp theo của [EXP-02 - Chạy Window, DBSCAN và HDBSCAN global](docs/task/tasks/EXP-02.md).
- **Official research dataset export:** Nghiên cứu chính thức trên toàn kỳ phụ thuộc vào [MLD-05 - Validate feature snapshot và export Parquet bundle](docs/task/tasks/MLD-05.md); pilot hiện tại chỉ dùng phục vụ kiểm tra kỹ thuật (DEVELOPMENT_ONLY).
- **Kỹ thuật phân cụm nâng cao / Region tuning:** Tinh chỉnh tham số theo vùng không gian (regional / adaptive DBSCAN) thuộc về [EXP-03 - Tuning regional và adaptive DBSCAN](docs/task/tasks/EXP-03.md).
- **Import Iceberg `ml.*`:** Việc nạp kết quả nghiên cứu vào bảng Iceberg thuộc về [MLI-03 - Validate/import kết quả và commit bảng ML Iceberg](docs/task/tasks/MLI-03.md).
- **Reviewer:** Reviewer độc lập cho task P0 này hiện là `unassigned`.
