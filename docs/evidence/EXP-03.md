# EXP-03 — Xây HDBSCAN adaptive và giải quyết multi-sequence evidence

Ngày kiểm chứng: 2026-10-18 (Review Round 3).
Branch: `feat/exp-03-adaptive-hdbscan`.
Assignee: `rosy179`.
Reviewer: `unassigned`.

## 1. Mục tiêu và bối cảnh

Task EXP-03 triển khai thuật toán **HDBSCAN Adaptive** và cơ chế **Multi-Sequence Entity Resolution** nhằm giải quyết hai vấn đề then chốt:
1. **Tham số thích nghi (Adaptive parameters):** Thay vì áp đặt một bộ tham số tĩnh `(min_cluster_size=3, min_samples=2)` chung cho mọi cửa sổ bất kể quy mô như HDBSCAN Global, HDBSCAN Adaptive tự động điều chỉnh siêu tham số dựa trên kích thước cửa sổ ứng viên ($N = \text{candidate\_count}$) và độ lớn chấn tiêu ($M = \text{mainshock\_magnitude}$) thông qua hàm toán học xác định (deterministic scaling policy), loại bỏ hoàn toàn việc tinh chỉnh tham số thủ công cho từng sequence.
2. **Khử trùng lặp đa chuỗi (Multi-sequence entity resolution):** Trong các khu vực địa chấn dày đặc hoặc các trận động đất kép/liên hoàn, một dư chấn có thể nằm trong vùng không–thời gian của nhiều mainshock khác nhau. EXP-03 thiết lập quy tắc phân giải thứ bậc (hierarchy) khách quan, bảo toàn grain dữ liệu thô, loại trừ double-counting khi tổng hợp và gắn bằng chứng nested mainshock vào `sequence_summary`.

Toàn bộ cấu hình được cố định trong `config/ml/models/hdbscan_adaptive_dev_v1.json` và khóa trên giai đoạn reproduction (`2000-01-01` đến `2018-09-30`) trước khi tiến hành đánh giá mở rộng tại [EXP-05](./../task/tasks/EXP-05.md).

## 2. Các điểm đã khắc phục theo review của @HoaiTam (PR #86)

### 2.1. [P2] Order-dependent resolution khi có $\ge 3$ sequences (comment-6249299753)
- **Vấn đề:** Khi sử dụng `functools.cmp_to_key(compare_membership_rows)` với sai số `eps_tie` để sắp xếp danh sách $\ge 3$ candidate sequences, comparator theo cặp (pairwise) vi phạm tính bắc cầu (non-transitive cycle: $A > B$, $B > C$, $C > A$). Cụ thể:
  - Candidate A: prob = 0.8000000, dist = 0.1
  - Candidate B: prob = 0.80000075, dist = 0.2
  - Candidate C: prob = 0.8000015, dist = 0.3
  Với `eps_tie = 1e-6`: $|p_B - p_A| \le 10^{-6}$ (prob tied $\to$ dist $A < B \to A > B$); $|p_C - p_B| \le 10^{-6}$ (prob tied $\to$ dist $B < C \to B > C$); nhưng $|p_C - p_A| = 1.5 \times 10^{-6} > 10^{-6}$ (xét prob $C > A \to C > A$). Do đó thứ tự mảng ban đầu sẽ làm lệch kết quả phân giải.
- **Khắc phục:**
  - Thay thế toàn bộ việc sort pairwise bằng hàm phân giải toàn cục (global), bất biến với thứ tự `find_best_membership_row(rows, eps_tie=1e-6)`:
    1. Xác định xác suất lớn nhất $p_{max}$ trong tập ứng viên.
    2. Lọc nhóm ứng viên có xác suất trong $[p_{max} - \epsilon, p_{max}]$.
    3. Trong nhóm này, tìm khoảng cách nhỏ nhất $d_{min}$. Lọc nhóm ứng viên có khoảng cách trong $[d_{min}, d_{min} + \epsilon]$.
    4. Nếu chỉ có 1 ứng viên $\to$ chọn ứng viên đó làm winner (rank 1); nếu $>1$ ứng viên $\to$ unresolvable tie $\to$ gán `AMBIGUOUS`, rank `None`, không chọn tùy ý.
    5. Các rank tiếp theo $2..K$ được bóc tách tuần tự; các ứng viên hòa ở rank thấp được sắp xếp xác định theo `mainshock_event_id` để đảm bảo 100% order-invariant và không trùng rank.
  - Áp dụng chuẩn xác cùng chính sách này cho việc phân giải nested candidate parents trong `resolve_multi_sequence_and_nested_mainshocks`.
  - Thêm 6 bộ test hoán vị (`itertools.permutations`) cho $\ge 3$ sequences kiểm chứng toàn diện tính bất biến thứ tự.

### 2.2. [P2] Tích hợp adaptive tests vào CI pipeline (comment-6249299753)
- **Vấn đề:** `Makefile:test-pilot` và `scripts/ci-impact.py` chưa kích hoạt chạy `tests/test_adaptive_hdbscan.py`.
- **Khắc phục:**
  - Bổ sung `@$(PILOT_PYTHON) -m unittest tests/test_adaptive_hdbscan.py` vào target `test-pilot` trong `Makefile`.
  - Thêm các targets tiện ích `test-exp03`, `inspect-exp03`, `jupyter-exp03` trong `Makefile`.
  - Cập nhật `scripts/ci-impact.py`:
    - Đưa `notebooks/adaptive_hdbscan.py` và `notebooks/exp_03_adaptive_hdbscan.ipynb` vào `EXP_FILES`.
    - Đưa `scripts/inspect-exp03-notebook.py` vào `EXP_SCRIPTS`.
    - Đưa `config/ml/models/hdbscan_adaptive_dev_v1.json` vào `EXP_CONFIGS`.
    - Cập nhật regex selector cho `test_adaptive_hdbscan.py` và summary table.
  - Thêm test `test_adaptive_hdbscan_changes_trigger_full_pilot_python` trong `tests/ci/test_suites.py`.

### 2.3. [P2] Đồng bộ trạng thái task và chuyển sang persistent staging (comment-6249299753)
- **Vấn đề:** CI `docs-contracts` báo lỗi lệch trạng thái `EXP-02 metadata=In Progress but index=Done`. Đồng thời EXP-03 được chuyển `Done` quá sớm khi đang trong quá trình review/fix. `notebooks/exp_03_adaptive_hdbscan.ipynb` dùng `tempfile.TemporaryDirectory` làm mất artifact sau khi kernel đóng.
- **Khắc phục:**
  - Khôi phục trạng thái `EXP-02` trong `docs/task/tasks/README.md` về `In Progress` để khớp với metadata file `EXP-02.md`.
  - Giữ trạng thái `EXP-03` là `In Progress` ở cả `docs/task/tasks/EXP-03.md` (frontmatter và mục Theo dõi) và `docs/task/tasks/README.md`.
  - Loại bỏ hoàn toàn `tempfile.TemporaryDirectory` trong `notebooks/exp_03_adaptive_hdbscan.ipynb`: chuyển sang thư mục staging bền vững `staging/ml-experiments/exp03-adaptive` (cấu hình được qua biến môi trường `EXP03_OUTPUT_DIR`) và `staging/ml-pilot/exp03-fixture`.
  - Cập nhật script `scripts/inspect-exp03-notebook.py` hỗ trợ `--output-dir` và ghi nhận kết quả vào `docs/evidence/EXP-03-local-smoke.json`.

### 2.4. Các điểm khắc phục từ Round 1 (comment-6230270173)
- Đọc đúng trường `mainshock_magnitude` thay vì `magnitude` mặc định `5.5`.
- Xử lý hòa bằng chứng nested parent khách quan, ghi nhận `AMBIGUOUS_NESTED_PARENT_TIE`.
- Kiểm tra tolerance probability tie-breaking bằng distance chính xác.
- Đồng bộ các bản vá từ PR #83, #84, #85 từ `origin/main` và làm sạch whitespace.

## 3. Checks và tests đã chạy

- **Unit & Contract Tests cho Adaptive HDBSCAN (`tests/test_adaptive_hdbscan.py`):**
  - Lệnh: `python -m unittest tests/test_adaptive_hdbscan.py`
  - Kết quả: **21 tests PASS**, 0 errors, 0 failures (thời gian chạy: ~0.20s).
  - Chi tiết các test mới:
    - `test_find_best_membership_row_3_sequences_counterexample_permutations`: 6 hoán vị đều chọn B là winner.
    - `test_find_best_membership_row_3_sequences_unresolvable_tie_permutations`: 6 hoán vị đều xác định `is_tie=True` và `winner=None`.
    - `test_multi_sequence_resolution_3_sequences_all_permutations`: 6 hoán vị đều gán rank 1 cho B, rank 2 cho C, rank 3 cho A.
    - `test_multi_sequence_resolution_3_sequences_tie_all_permutations`: 6 hoán vị đều gán `AMBIGUOUS` và `resolution_rank=None`.
    - `test_nested_parent_resolution_3_sequences_all_permutations`: 6 hoán vị đều gán parent `main_B`.
    - `test_nested_parent_resolution_3_sequences_tie_all_permutations`: 6 hoán vị đều gán parent `None` kèm mã lý do `AMBIGUOUS_NESTED_PARENT_TIE`.

- **Toàn bộ Experiment Regression Suite:**
  - `python -m unittest tests/test_experiment_harness.py`: **44 tests PASS**, 0 errors, 0 failures.
  - `python -m unittest tests/test_baseline_algorithms.py`: **19 tests PASS**, 0 errors, 0 failures.
  - `python -m unittest tests/ci/test_suites.py`: **13 tests PASS**, 0 errors, 0 failures.
  - Chạy tổng hợp 97 tests: **OK**.

- **Kiểm tra đồng bộ danh mục task (76/76 tasks):**
  - Metadata frontmatter, mục Theo dõi và dòng index trong `docs/task/tasks/README.md` khớp nhau 100%.

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
    - `runtime_seconds`: ~2.11s

## 4. Deliverables của task

1. **Model Configuration:**
   - `config/ml/models/hdbscan_adaptive_dev_v1.json`: Cấu hình mô hình `HDBSCAN_ADAPTIVE` với policy `log_candidate_magnitude_scaling`, các hệ số $k_N, k_M, M_0, \alpha$ và các cận biên an toàn.
2. **Algorithm & Entity Resolution Implementation:**
   - `notebooks/adaptive_hdbscan.py`: Module chứa:
     - `resolve_adaptive_hdbscan_params`: Parameter resolver hàm thuần túy, deterministic.
     - `fit_hdbscan_adaptive`: Fit HDBSCAN theo cửa sổ với tham số thích nghi, đọc và kiểm tra strictly mainshock magnitude.
     - `find_best_membership_row`: Thuật toán phân giải toàn cục bất biến với thứ tự input.
     - `compare_membership_rows`: Comparator đa tiêu chí với tolerance tie-breaking.
     - `resolve_multi_sequence_and_nested_mainshocks`: Bộ quy tắc phân giải đa chuỗi và phát hiện nested mainshock có xử lý hòa xác định.
     - `run_adaptive_experiment`: Runner tích hợp toàn bộ quy trình và xuất result bundle.
3. **Thin Jupyter Smoke Notebook:**
   - `notebooks/exp_03_adaptive_hdbscan.ipynb`: Notebook thực hiện quy trình hoàn chỉnh từ tải bundle, fit adaptive, resolve multi-sequences, đối soát Before/After count đến nghiệm thu hợp đồng dữ liệu. Toàn bộ code cells đã được dọn sạch outputs và execution counts, sử dụng thư mục staging bền vững `staging/ml-experiments/exp03-adaptive`.
4. **Notebook Inspection Script & Tooling:**
   - `scripts/inspect-exp03-notebook.py`: Script kiểm tra Restart & Run All trong môi trường cách ly, hỗ trợ `--output-dir`.
   - `Makefile`: Bổ sung `test_adaptive_hdbscan.py` vào `test-pilot` và thêm targets `test-exp03`, `inspect-exp03`, `jupyter-exp03`.
   - `scripts/ci-impact.py`: Phân loại impact tự động cho các file của EXP-03.
5. **Unit & Contract Test Suite:**
   - `tests/test_adaptive_hdbscan.py`: Bộ 21 tests tự động bao phủ 100% acceptance criteria, các trường hợp review edge-case và toàn bộ test hoán vị $\ge 3$ sequences.
   - `tests/ci/test_suites.py`: Test chính sách CI cho các file thuộc EXP-03.
6. **Inspection Evidence Artifact:**
   - `docs/evidence/EXP-03-local-smoke.json`: JSON report nghiệm thu kiểm tra notebook và local smoke gate trung thực.

## 5. Giới hạn / Rủi ro / Bước tiếp theo

- **Đánh giá khoa học so sánh (Scientific Evaluation):** Việc so sánh định lượng DBCV, physical consistency (luật Omori), noise ratio và stability giữa Window, DBSCAN, HDBSCAN Global và HDBSCAN Adaptive thuộc về [EXP-04 - Đánh giá quality, physical consistency và stability](docs/task/tasks/EXP-04.md). Không vội kết luận adaptive tốt hơn trước khi EXP-04 hoàn tất.
- **Out-of-period evaluation:** Thử nghiệm mô hình trên giai đoạn mở rộng 2018–2023 thuộc về [EXP-05 - Chạy out-of-period extension 2018-2023](docs/task/tasks/EXP-05.md). Cấu hình adaptive hiện tại đã được khóa và không được điều chỉnh bằng dữ liệu extension.
- **Official dataset export:** Dữ liệu nghiên cứu chính thức toàn kỳ phụ thuộc vào [MLD-05 - Validate feature snapshot và export Parquet bundle](docs/task/tasks/MLD-05.md).
- **Import Iceberg `ml.*`:** Nạp kết quả vào bảng Iceberg thuộc về [MLI-03 - Validate/import kết quả và commit bảng ML Iceberg](docs/task/tasks/MLI-03.md).
- **Reviewer:** Reviewer độc lập cho task P0 này hiện là `unassigned`.
