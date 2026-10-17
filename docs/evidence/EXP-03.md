# EXP-03 — Xây HDBSCAN adaptive và giải quyết multi-sequence evidence

Ngày kiểm chứng: 2026-10-17.
Branch: `feat/exp-03-adaptive-hdbscan`.
Assignee: `rosy179`.
Reviewer: `unassigned`.

## 1. Mục tiêu và bối cảnh

Task EXP-03 triển khai thuật toán **HDBSCAN Adaptive** và cơ chế **Multi-Sequence Entity Resolution** nhằm giải quyết hai vấn đề then chốt:
1. **Tham số thích nghi (Adaptive parameters):** Thay vì áp đặt một bộ tham số tĩnh `(min_cluster_size=3, min_samples=2)` chung cho mọi cửa sổ bất kể quy mô như HDBSCAN Global, HDBSCAN Adaptive tự động điều chỉnh siêu tham số dựa trên kích thước cửa sổ ứng viên ($N = \text{candidate\_count}$) và độ lớn chấn tiêu ($M = \text{mainshock\_magnitude}$) thông qua hàm toán học xác định (deterministic scaling policy), loại bỏ hoàn toàn việc tinh chỉnh tham số thủ công cho từng sequence.
2. **Khử trùng lặp đa chuỗi (Multi-sequence entity resolution):** Trong các khu vực địa chấn dày đặc hoặc các trận động đất kép/liên hoàn, một dư chấn có thể nằm trong vùng không–thời gian của nhiều mainshock khác nhau. EXP-03 thiết lập quy tắc phân giải thứ bậc (hierarchy) khách quan, bảo toàn grain dữ liệu thô, loại trừ double-counting khi tổng hợp và gắn bằng chứng nested mainshock vào `sequence_summary`.

Toàn bộ cấu hình được cố định trong `config/ml/models/hdbscan_adaptive_dev_v1.json` và khóa trên giai đoạn reproduction (`2000-01-01` đến `2018-09-30`) trước khi tiến hành đánh giá mở rộng tại [EXP-05](./../task/tasks/EXP-05.md).

## 2. Các quy tắc cốt lõi đã kiểm chứng

1. **Deterministic Adaptive Parameter Resolver:**
   - Siêu tham số được tính toán hoàn toàn bằng giải thuật toán học đơn điệu, không ngẫu nhiên, không per-sequence manual override:
     $$m_{cl} = \mathrm{clip}\left(\left\lfloor m_{cl, \text{base}} + k_N \cdot \ln(N + 1) + k_M \cdot \max(0.0, M - M_0) \right\rfloor, m_{cl, \text{min}}, m_{cl, \text{max}}\right)$$
     $$m_s = \max\left(m_{s, \text{min}}, \min\left(m_{cl}, \left\lfloor \alpha \cdot m_{cl} \right\rfloor\right)\right)$$
   - Đảm bảo tính đơn điệu: Cửa sổ càng nhiều ứng viên hoặc mainshock càng lớn thì $m_{cl}$ càng lớn tương ứng, tránh hiện tượng vỡ cụm nhỏ lẻ (over-fragmentation); đồng thời với cửa sổ thưa thì $m_{cl}$ giữ ở cận dưới hợp lý ($m_{cl} \ge 3, m_s \ge 2$).

2. **Bảo toàn hạt dữ liệu thô (Raw Membership Grain Preservation):**
   - Mọi cặp `(experiment_run_id, mainshock_event_id, candidate_event_id)` từ tất cả các cửa sổ đều được giữ nguyên 100% trong `memberships.parquet`.
   - Tuyệt đối không xóa bất kỳ dòng thành viên nào khi candidate được resolve thuộc về sequence khác.

3. **Thứ bậc giải quyết Multi-Sequence (Hierarchy Resolution):**
   - **Xác suất thành viên (`membership_probability`):** Cửa sổ mainshock nào có xác suất thành viên cao hơn sẽ thắng (`resolution_rank = 1`, các cửa sổ khác nhận `resolution_rank = 2, 3, ...`, tất cả các dòng nhận status `MULTIPLE`).
   - **Khoảng cách 4-D chuẩn hóa (`normalized_distance`):** Nếu xác suất bằng nhau, cửa sổ có khoảng cách 4-D ngắn hơn thắng (`resolution_rank = 1`).
   - **Tie bất khả phân (`AMBIGUOUS`):** Nếu cả xác suất lẫn khoảng cách đều bằng nhau (sai số $< 10^{-6}$), không tự ý chọn bừa; toàn bộ các dòng của candidate đó nhận `multi_sequence_status = "AMBIGUOUS"`, `resolution_rank = None`.
   - **Non-member:** Sự kiện không thuộc chuỗi nhận `multi_sequence_status = "NOT_MEMBER"`, `resolution_rank = None`.
   - **Unique member:** Sự kiện chỉ thuộc 1 chuỗi duy nhất nhận `multi_sequence_status = "UNIQUE"`, `resolution_rank = 1`.

4. **Gắn bằng chứng Nested Mainshock (Nested Mainshock Evidence):**
   - Khi một mainshock $B$ xuất hiện trong cửa sổ của mainshock $A$ ($A \ne B$) và được $A$ nhận làm POST sequence member với rank 1 (hoặc xác suất cao nhất), sequence_summary của $B$ được tự động gắn:
     `is_nested_mainshock_candidate = true`, `parent_mainshock_event_id = A`, `nested_membership_probability = prob(B in A)`.
   - Ngược lại, giữ `is_nested_mainshock_candidate = false`, `parent_mainshock_event_id = null`, `nested_membership_probability = null`.

5. **Báo cáo đối soát Before/After (Reconciliation Audit):**
   - Đảm bảo cân bằng bảo toàn:
     $$\text{raw\_aftershocks} = \text{resolved\_unique\_aftershocks} + \text{ambiguous\_aftershocks} + \text{deduplicated\_aftershocks}$$
   - Khi tổng hợp số dư chấn duy nhất trong catalog bằng cách lọc `event_role_candidate == 'POST' AND resolution_rank == 1`, hiện tượng double-counting hoàn toàn bị triệt tiêu mà không làm mất thông tin raw.

6. **Giới hạn diễn giải khoa học:**
   - Kết quả phân cụm chỉ là sequence/aftershock candidate theo mô hình thống kê mật độ, không tuyên bố quan hệ nhân quả vật lý hay dự đoán động đất tiếp theo.

## 3. Checks và tests đã chạy

- **Unit & Contract Tests cho Adaptive HDBSCAN (`tests/test_adaptive_hdbscan.py`):**
  - Lệnh: `pytest tests/test_adaptive_hdbscan.py`
  - Kết quả: **9 tests PASS**, 0 errors, 0 failures (thời gian chạy: ~2.17s).
  - Chi tiết các test cases:
    1. `test_parameter_resolver_determinism`: Kiểm tra tính xác định 100% của parameter resolver khi gọi lặp lại 50 lần.
    2. `test_parameter_resolver_bounds_and_monotonicity`: Kiểm tra cận trên/dưới và tính đơn điệu của $m_{cl}, m_s$ trên dải $N \in [1, 100000]$ và $M \in [4.0, 9.1]$.
    3. `test_multi_sequence_resolution_unique`: Kiểm tra candidate chỉ xuất hiện trong 1 sequence ra `UNIQUE` và rank 1; non-member ra `NOT_MEMBER` và rank `None`.
    4. `test_multi_sequence_resolution_probability_winner`: Kiểm tra quy tắc ưu tiên 1: xác suất cao hơn thắng rank 1 dù khoảng cách xa hơn; cả hai dòng nhận `MULTIPLE`.
    5. `test_multi_sequence_resolution_distance_tiebreak`: Kiểm tra quy tắc ưu tiên 2: khi xác suất bằng nhau, khoảng cách 4-D nhỏ hơn thắng rank 1.
    6. `test_multi_sequence_unresolvable_tie_becomes_ambiguous`: Kiểm tra tie bất khả phân: cả xác suất lẫn khoảng cách bằng nhau ra `AMBIGUOUS`, rank `None`, không chọn tùy tiện.
    7. `test_raw_membership_grain_preservation`: Kiểm tra toàn bộ 20 dòng raw memberships được giữ nguyên vẹn sau khi resolve.
    8. `test_nested_mainshock_detection`: Kiểm tra phát hiện mainshock lồng trong sequence khác và gắn đúng trường `parent_mainshock_event_id` và `nested_membership_probability`.
    9. `test_run_adaptive_experiment_e2e`: Kiểm tra luồng E2E `run_adaptive_experiment` trên pilot bundle, xác thực `pilot_result_manifest.json`, schema Arrow Parquet và SHA-256 checksums.

- **Toàn bộ Experiment Regression Suite:**
  - Lệnh: `pytest tests/test_adaptive_hdbscan.py tests/test_baseline_algorithms.py tests/test_experiment_harness.py`
  - Kết quả: **32 tests PASS**, 0 errors, 0 failures.

- **Java Result Bundle Contract Tests (`spark/src/test/java/.../ResultBundleContractTest.java`):**
  - Lệnh: `$env:JAVA_HOME="C:\Program Files\Java\jdk-23"; mvn test -Dtest=ResultBundleContractTest`
  - Kết quả: **BUILD SUCCESS**, 7 tests PASS, 0 failures, 0 errors. Xác thực schema Arrow, enum `multi_sequence_status`, và logic rank 1 tương thích 100% với Spark validator.

- **Notebook headless inspection (`scripts/inspect-exp03-notebook.py`):**
  - Lệnh: `python scripts/inspect-exp03-notebook.py --output-json docs/evidence/EXP-03-local-smoke.json`
  - Kết quả:
    - `notebook_status`: `RESTART_RUN_ALL_PASSED`
    - `local_smoke_status`: `PASSED`
    - `colab_migration_allowed`: `true`
    - `executed_code_cells`: 5
    - `source_notebook_outputs_cleared`: `true`
    - `runtime_seconds`: ~2.05s

## 4. Deliverables của task

1. **Model Configuration:**
   - `config/ml/models/hdbscan_adaptive_dev_v1.json`: Cấu hình mô hình `HDBSCAN_ADAPTIVE` với policy `log_candidate_magnitude_scaling`, các hệ số $k_N, k_M, M_0, \alpha$ và các cận biên an toàn.
2. **Algorithm & Entity Resolution Implementation:**
   - `notebooks/adaptive_hdbscan.py`: Module chứa:
     - `resolve_adaptive_hdbscan_params`: Parameter resolver hàm thuần túy, deterministic.
     - `fit_hdbscan_adaptive`: Fit HDBSCAN theo cửa sổ với tham số thích nghi.
     - `resolve_multi_sequence_and_nested_mainshocks`: Bộ quy tắc phân giải đa chuỗi và phát hiện nested mainshock.
     - `run_adaptive_experiment`: Runner tích hợp toàn bộ quy trình và xuất result bundle.
3. **Thin Jupyter Smoke Notebook:**
   - `notebooks/exp_03_adaptive_hdbscan.ipynb`: Notebook thực hiện quy trình hoàn chỉnh từ tải bundle, fit adaptive, resolve multi-sequences, đối soát Before/After count đến nghiệm thu hợp đồng dữ liệu. Toàn bộ code cells đã được dọn sạch outputs và execution counts.
4. **Notebook Inspection Script:**
   - `scripts/inspect-exp03-notebook.py`: Script kiểm tra Restart & Run All trong môi trường cách ly, đảm bảo notebook không bị sửa đổi khi chạy.
5. **Unit & Contract Test Suite:**
   - `tests/test_adaptive_hdbscan.py`: Bộ 9 tests tự động bao phủ 100% acceptance criteria của task.
6. **Inspection Evidence Artifact:**
   - `docs/evidence/EXP-03-local-smoke.json`: JSON report nghiệm thu kiểm tra notebook và local smoke gate.

## 5. Giới hạn / Rủi ro / Bước tiếp theo

- **Đánh giá khoa học so sánh (Scientific Evaluation):** Việc so sánh định lượng DBCV, physical consistency (luật Omori), noise ratio và stability giữa Window, DBSCAN, HDBSCAN Global và HDBSCAN Adaptive thuộc về [EXP-04 - Đánh giá quality, physical consistency và stability](docs/task/tasks/EXP-04.md). Không vội kết luận adaptive tốt hơn trước khi EXP-04 hoàn tất.
- **Out-of-period evaluation:** Thử nghiệm mô hình trên giai đoạn mở rộng 2018–2023 thuộc về [EXP-05 - Chạy out-of-period extension 2018-2023](docs/task/tasks/EXP-05.md). Cấu hình adaptive hiện tại đã được khóa và không được điều chỉnh bằng dữ liệu extension.
- **Official dataset export:** Dữ liệu nghiên cứu chính thức toàn kỳ phụ thuộc vào [MLD-05 - Validate feature snapshot và export Parquet bundle](docs/task/tasks/MLD-05.md).
- **Import Iceberg `ml.*`:** Nạp kết quả vào bảng Iceberg thuộc về [MLI-03 - Validate/import kết quả và commit bảng ML Iceberg](docs/task/tasks/MLI-03.md).
- **Reviewer:** Reviewer độc lập cho task P0 này hiện là `unassigned`.
