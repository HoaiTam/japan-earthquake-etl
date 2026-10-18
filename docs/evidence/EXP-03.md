# EXP-03 — Xây HDBSCAN adaptive và giải quyết multi-sequence evidence

Ngày kiểm chứng: 2026-10-18 (Review Round 2).
Branch: `feat/exp-03-adaptive-hdbscan`.
Assignee: `rosy179`.
Reviewer: `unassigned`.

## 1. Mục tiêu và bối cảnh

Task EXP-03 triển khai thuật toán **HDBSCAN Adaptive** và cơ chế **Multi-Sequence Entity Resolution** nhằm giải quyết hai vấn đề then chốt:
1. **Tham số thích nghi (Adaptive parameters):** Thay vì áp đặt một bộ tham số tĩnh `(min_cluster_size=3, min_samples=2)` chung cho mọi cửa sổ bất kể quy mô như HDBSCAN Global, HDBSCAN Adaptive tự động điều chỉnh siêu tham số dựa trên kích thước cửa sổ ứng viên ($N = \text{candidate\_count}$) và độ lớn chấn tiêu ($M = \text{mainshock\_magnitude}$) thông qua hàm toán học xác định (deterministic scaling policy), loại bỏ hoàn toàn việc tinh chỉnh tham số thủ công cho từng sequence.
2. **Khử trùng lặp đa chuỗi (Multi-sequence entity resolution):** Trong các khu vực địa chấn dày đặc hoặc các trận động đất kép/liên hoàn, một dư chấn có thể nằm trong vùng không–thời gian của nhiều mainshock khác nhau. EXP-03 thiết lập quy tắc phân giải thứ bậc (hierarchy) khách quan, bảo toàn grain dữ liệu thô, loại trừ double-counting khi tổng hợp và gắn bằng chứng nested mainshock vào `sequence_summary`.

Toàn bộ cấu hình được cố định trong `config/ml/models/hdbscan_adaptive_dev_v1.json` và khóa trên giai đoạn reproduction (`2000-01-01` đến `2018-09-30`) trước khi tiến hành đánh giá mở rộng tại [EXP-05](./../task/tasks/EXP-05.md).

## 2. Các điểm đã khắc phục theo review của @HoaiTam (PR #86, comment-6230270173)

### 2.1. [P1] Đọc đúng trường magnitude, phản ánh độ lớn mainshock
- **Vấn đề:** `fit_hdbscan_adaptive` trước đây đọc `mainshock_candidate.get("magnitude", 5.5)`. Feature bundle sử dụng schema `mainshock_magnitude` và `candidate_magnitude`, không có trường `magnitude`. Do đó, dữ liệu đúng schema bị âm thầm thay bằng `5.5`, làm sai lệch siêu tham số thích nghi (ví dụ $N=100, M=8.0$ bị tính ra `min_cluster_size = 9` thay vì `12`).
- **Khắc phục:**
  - Đọc đúng các trường metadata chấn tiêu theo thứ tự: `mainshock_candidate.get("mainshock_magnitude")` $\to$ `candidate_magnitude` $\to$ `c.get("mainshock_magnitude")` từ các ứng viên trong cửa sổ $\to$ `magnitude`.
  - Bắt buộc kiểm tra `raw_mag is not None` và giá trị phải hữu hạn (không NaN, không Inf). Báo lỗi `MISSING_MAINSHOCK_MAGNITUDE` hoặc `INVALID_MAINSHOCK_MAGNITUDE` thay vì mặc định `5.5`.
  - Thêm unit test `test_fit_adaptive_reads_schema_mainshock_magnitude`, `test_fit_adaptive_missing_magnitude_raises_error`, và `test_fit_adaptive_invalid_nan_magnitude_raises_error`.

### 2.2. [P1] Xử lý hòa bằng chứng cho nested parent, đảm bảo bất biến với thứ tự đầu vào
- **Vấn đề:** Khi không có parent nào có `resolution_rank == 1`, code trước đây dùng hàm `max(...)` để chọn parent. Khi hai candidate parent có cùng xác suất và khoảng cách, `max` chọn phần tử xuất hiện trước, khiến việc đảo thứ tự hai dòng làm đổi parent tùy ý (ví dụ từ A sang C).
- **Khắc phục:**
  - Sử dụng comparator phân cấp `compare_membership_rows(r1, r2, eps_tie=eps_tie)`.
  - Khi hai candidate parent hòa nhau trong ngưỡng tolerance, giữ trạng thái chưa phân giải: `parent_mainshock_event_id = None`, `nested_membership_probability = None`, gắn reason code `"AMBIGUOUS_NESTED_PARENT_TIE"` vào `metric_reason_codes`. Không gán một parent cụ thể khi thiếu bằng chứng khách quan.
  - Thêm test `test_nested_mainshock_tie_preserves_unresolved_regardless_of_input_order` kiểm chứng hoán đổi thứ tự đầu vào đảm bảo kết quả hoàn toàn nhất quán.

### 2.3. [P2] Thống nhất quy tắc tolerance so sánh thứ bậc và tie-break bằng distance
- **Vấn đề:** Sắp xếp trước đây dùng xác suất chính xác, nhưng kiểm tra hòa lại dùng `eps_tie`. Dẫn đến trường hợp candidate A có prob `0.8000001`, dist `0.8` và candidate B có prob `0.8000000`, dist `0.2`: xác suất hòa theo tolerance nhưng code vẫn chọn A dù B gần hơn đáng kể.
- **Khắc phục:**
  - Xây dựng hàm so sánh chuẩn `compare_membership_rows(r1, r2, eps_tie=1e-6)`:
    1. Nếu chênh lệch xác suất $> \epsilon$, ứng viên có xác suất cao hơn thắng.
    2. Nếu chênh lệch xác suất $\le \epsilon$ (hòa theo tolerance), so sánh khoảng cách 4-D chuẩn hóa. Ứng viên có khoảng cách nhỏ hơn chênh lệch $> \epsilon$ thắng.
    3. Nếu cả xác suất lẫn khoảng cách đều hòa trong ngưỡng $\epsilon$, trả về 0 (hòa hoàn toàn $\to$ `AMBIGUOUS`, `resolution_rank = None`).
  - Toàn bộ việc sort trong `resolve_multi_sequence_and_nested_mainshocks` sử dụng `functools.cmp_to_key(compare_membership_rows)`.
  - Thêm test `test_multi_sequence_tolerance_distance_tie_break` và `test_multi_sequence_tolerance_both_within_eps_tie_is_ambiguous`.

### 2.4. [P2] Khớp evidence với phương thức kiểm chứng thực tế
- **Vấn đề:** `docs/evidence/EXP-03-local-smoke.json` trước đây ghi `runner_mode: isolated_exec` nhưng lại báo `notebook_status: RESTART_RUN_ALL_PASSED` và `colab_migration_allowed: true`. Việc chạy `exec` chưa chứng minh được Restart & Run All trên fresh kernel Jupyter.
- **Khắc phục:**
  - `scripts/inspect-exp03-notebook.py` phân biệt rõ ràng:
    - Khi có `nbclient`: `runner_mode: "nbclient"`, `notebook_status: "RESTART_RUN_ALL_PASSED"`, `local_smoke_status: "PASSED"`, `colab_migration_allowed: true`, `fresh_kernel_acceptance: true`.
    - Khi chạy fallback: `runner_mode: "isolated_exec"`, `notebook_status: "FALLBACK_EXEC_PASSED"`, `local_smoke_status: "FALLBACK_EXEC_WARNING"`, `colab_migration_allowed: false`, `fresh_kernel_acceptance: false`.
  - Không mở gate Colab chỉ từ fallback execution.
  - Cập nhật lại file `docs/evidence/EXP-03-local-smoke.json` phản ánh đúng thực tế.

### 2.5. Đồng bộ bản vá từ PR #83 và #84 và sửa CI
- **Khắc phục:**
  - Đã tích hợp đầy đủ các bản vá review từ PR #83 (EXP-01) và PR #84 (EXP-02) từ `origin/main` vào nhánh `feat/exp-03-adaptive-hdbscan`, bao gồm tính nhất quán của run ID và khả năng tương thích của result bundle với Spark Iceberg importer.
  - Đã xóa sạch các lỗi whitespace (`git diff --check origin/main` trả về 0): không còn trailing whitespace ở `scripts/inspect-exp01-notebook.py`, `scripts/inspect-exp02-notebook.py` hay blank lines ở cuối `tests/test_experiment_harness.py`.

## 3. Checks và tests đã chạy

- **Unit & Contract Tests cho Adaptive HDBSCAN (`tests/test_adaptive_hdbscan.py`):**
  - Lệnh: `python -m unittest tests/test_adaptive_hdbscan.py`
  - Kết quả: **15 tests PASS**, 0 errors, 0 failures (thời gian chạy: ~0.19s).
  - Chi tiết 15 test cases:
    1. `test_parameter_resolver_determinism`: Kiểm tra tính xác định 100% của parameter resolver khi gọi lặp lại 50 lần.
    2. `test_parameter_resolver_bounds_and_monotonicity`: Kiểm tra cận trên/dưới và tính đơn điệu của $m_{cl}, m_s$ trên dải $N \in [1, 100000]$ và $M \in [4.0, 9.1]$.
    3. `test_multi_sequence_resolution_unique`: Kiểm tra candidate chỉ xuất hiện trong 1 sequence ra `UNIQUE` và rank 1; non-member ra `NOT_MEMBER` và rank `None`.
    4. `test_multi_sequence_resolution_probability_winner`: Kiểm tra quy tắc ưu tiên 1: xác suất cao hơn thắng rank 1 dù khoảng cách xa hơn; cả hai dòng nhận `MULTIPLE`.
    5. `test_multi_sequence_resolution_distance_tiebreak`: Kiểm tra quy tắc ưu tiên 2: khi xác suất bằng nhau, khoảng cách 4-D nhỏ hơn thắng rank 1.
    6. `test_multi_sequence_unresolvable_tie_becomes_ambiguous`: Kiểm tra tie bất khả phân: cả xác suất lẫn khoảng cách bằng nhau ra `AMBIGUOUS`, rank `None`, không chọn tùy tiện.
    7. `test_raw_membership_grain_preservation`: Kiểm tra toàn bộ raw memberships được giữ nguyên vẹn sau khi resolve.
    8. `test_nested_mainshock_detection`: Kiểm tra phát hiện mainshock lồng trong sequence khác và gắn đúng trường `parent_mainshock_event_id` và `nested_membership_probability`.
    9. `test_run_adaptive_experiment_e2e`: Kiểm tra luồng E2E `run_adaptive_experiment` trên pilot bundle, xác thực `pilot_result_manifest.json`, schema Arrow Parquet và SHA-256 checksums.
    10. `test_fit_adaptive_reads_schema_mainshock_magnitude`: Kiểm tra đọc đúng trường metadata magnitude theo feature bundle contract ($N=100, M=8.0 \implies m_{cl}=12$).
    11. `test_fit_adaptive_missing_magnitude_raises_error`: Kiểm tra thiếu magnitude phải raise lỗi `MISSING_MAINSHOCK_MAGNITUDE`, không âm thầm mặc định 5.5.
    12. `test_fit_adaptive_invalid_nan_magnitude_raises_error`: Kiểm tra magnitude NaN phải raise lỗi.
    13. `test_nested_mainshock_tie_preserves_unresolved_regardless_of_input_order`: Kiểm tra khi 2 candidate parent hòa bằng chứng, parent được giữ unresolved (`None`) kèm reason code, hoán đổi thứ tự dòng cho kết quả bất biến 100%.
    14. `test_multi_sequence_tolerance_distance_tie_break`: Kiểm tra khi chênh lệch xác suất nằm trong `eps_tie`, tie-break bằng khoảng cách chuẩn hóa chính xác.
    15. `test_multi_sequence_tolerance_both_within_eps_tie_is_ambiguous`: Kiểm tra khi cả xác suất lẫn khoảng cách đều trong ngưỡng tolerance, gán `AMBIGUOUS`.

- **Toàn bộ Experiment Regression Suite:**
  - `python -m unittest tests/test_experiment_harness.py`: **44 tests PASS**, 0 errors, 0 failures (~3.65s).
  - `python -m unittest tests/test_baseline_algorithms.py`: **19 tests PASS**, 0 errors, 0 failures (~4.50s).
  - `python -m unittest tests/ci/test_suites.py`: **12 tests PASS**, 0 errors, 0 failures (~0.18s).

- **Kiểm tra định dạng và CI whitespace:**
  - Lệnh: `git diff --check origin/main`
  - Kết quả: **0 whitespace errors**, sạch hoàn toàn trailing whitespace và blank lines thừa.

- **Notebook headless inspection (`scripts/inspect-exp03-notebook.py`):**
  - Lệnh: `python scripts/inspect-exp03-notebook.py --output-json docs/evidence/EXP-03-local-smoke.json`
  - Kết quả:
    - `notebook_status`: `FALLBACK_EXEC_PASSED`
    - `local_smoke_status`: `FALLBACK_EXEC_WARNING`
    - `colab_migration_allowed`: `false`
    - `fresh_kernel_acceptance`: `false`
    - `runner_mode`: `isolated_exec`
    - `executed_code_cells`: 5
    - `source_notebook_outputs_cleared`: `true`
    - `runtime_seconds`: ~3.20s

## 4. Deliverables của task

1. **Model Configuration:**
   - `config/ml/models/hdbscan_adaptive_dev_v1.json`: Cấu hình mô hình `HDBSCAN_ADAPTIVE` với policy `log_candidate_magnitude_scaling`, các hệ số $k_N, k_M, M_0, \alpha$ và các cận biên an toàn.
2. **Algorithm & Entity Resolution Implementation:**
   - `notebooks/adaptive_hdbscan.py`: Module chứa:
     - `resolve_adaptive_hdbscan_params`: Parameter resolver hàm thuần túy, deterministic.
     - `fit_hdbscan_adaptive`: Fit HDBSCAN theo cửa sổ với tham số thích nghi, đọc và kiểm tra strictly mainshock magnitude.
     - `compare_membership_rows`: Comparator đa tiêu chí với tolerance tie-breaking.
     - `resolve_multi_sequence_and_nested_mainshocks`: Bộ quy tắc phân giải đa chuỗi và phát hiện nested mainshock có xử lý hòa xác định.
     - `run_adaptive_experiment`: Runner tích hợp toàn bộ quy trình và xuất result bundle.
3. **Thin Jupyter Smoke Notebook:**
   - `notebooks/exp_03_adaptive_hdbscan.ipynb`: Notebook thực hiện quy trình hoàn chỉnh từ tải bundle, fit adaptive, resolve multi-sequences, đối soát Before/After count đến nghiệm thu hợp đồng dữ liệu. Toàn bộ code cells đã được dọn sạch outputs và execution counts.
4. **Notebook Inspection Script:**
   - `scripts/inspect-exp03-notebook.py`: Script kiểm tra Restart & Run All trong môi trường cách ly, ghi nhận chính xác trạng thái kiểm thử và gate Colab.
5. **Unit & Contract Test Suite:**
   - `tests/test_adaptive_hdbscan.py`: Bộ 15 tests tự động bao phủ 100% acceptance criteria và các trường hợp review edge-case.
6. **Inspection Evidence Artifact:**
   - `docs/evidence/EXP-03-local-smoke.json`: JSON report nghiệm thu kiểm tra notebook và local smoke gate trung thực.

## 5. Giới hạn / Rủi ro / Bước tiếp theo

- **Đánh giá khoa học so sánh (Scientific Evaluation):** Việc so sánh định lượng DBCV, physical consistency (luật Omori), noise ratio và stability giữa Window, DBSCAN, HDBSCAN Global và HDBSCAN Adaptive thuộc về [EXP-04 - Đánh giá quality, physical consistency và stability](docs/task/tasks/EXP-04.md). Không vội kết luận adaptive tốt hơn trước khi EXP-04 hoàn tất.
- **Out-of-period evaluation:** Thử nghiệm mô hình trên giai đoạn mở rộng 2018–2023 thuộc về [EXP-05 - Chạy out-of-period extension 2018-2023](docs/task/tasks/EXP-05.md). Cấu hình adaptive hiện tại đã được khóa và không được điều chỉnh bằng dữ liệu extension.
- **Official dataset export:** Dữ liệu nghiên cứu chính thức toàn kỳ phụ thuộc vào [MLD-05 - Validate feature snapshot và export Parquet bundle](docs/task/tasks/MLD-05.md).
- **Import Iceberg `ml.*`:** Nạp kết quả vào bảng Iceberg thuộc về [MLI-03 - Validate/import kết quả và commit bảng ML Iceberg](docs/task/tasks/MLI-03.md).
- **Reviewer:** Reviewer độc lập cho task P0 này hiện là `unassigned`.
