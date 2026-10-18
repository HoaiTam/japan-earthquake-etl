# EXP-02 — Chạy Window, DBSCAN và HDBSCAN global evidence

Ngày kiểm chứng: 2026-10-18 (Review Round 2).
Branch: `feat/exp-02-baseline-algorithms`.
Assignee: `rosy179`.
Reviewer: `unassigned`.

## 1. Mục tiêu và bối cảnh

Task EXP-02 triển khai ba thuật toán baseline trên cùng pilot feature bundle DAT-02 (đặc trưng 4-D chuẩn hóa `[x_scaled, y_scaled, z_scaled, time_scaled]` do Java/Spark MLD-04 tạo) để đánh giá:
1. Liệu density clustering (DBSCAN/HDBSCAN) có giảm background noise tốt hơn so với counting window rule hay không.
2. HDBSCAN với variable density clustering và membership probabilities có mang lại thông tin định lượng hơn DBSCAN hay không.

Theo quy trình `JUPYTER_PILOT_WORKFLOW.md`, toàn bộ phát triển và kiểm chứng kỹ thuật được thực hiện trên Jupyter local trước; chỉ khi `local_smoke_status=PASSED` và `colab_migration_allowed=true` mới được phép chuyển sang Google Colab. Pilot outputs tuyệt đối không phát `_SUCCESS.json` hoặc tự nhận `RESULT_READY`/`CANDIDATE`.

Sau đợt review 1 (commit `68edc26`), @HoaiTam đã phản hồi tại PR #84 (comment-6229895703) về 5 điểm cải tiến (2 P1 và 3 P2). Bản cập nhật này giải quyết triệt để toàn bộ 5 điểm nêu trên.

## 2. Các điểm đã khắc phục theo review của @HoaiTam

### 2.1. [P1] Thắt chặt Local Smoke Gate và kiểm định kỹ thuật
- **Vấn đề:** `generate_local_smoke_report()` trước đây chỉ so khớp `total_candidates` rồi gán luôn `PASSED` và `colab_migration_allowed=true`, bỏ lọt trường hợp kết quả rỗng, thiếu thuật toán hoặc membership/summary không hợp lệ.
- **Khắc phục:**
  - Bắt buộc kiểm tra đủ ba thuật toán baseline: `WINDOW`, `DBSCAN`, `HDBSCAN_GLOBAL`. Từ chối ngay nếu rỗng hoặc thiếu bất kỳ thuật toán nào (`MISSING_REQUIRED_BASELINE_ALGORITHMS`).
  - Đối chiếu thực tế các artifact trên đĩa: kiểm tra tồn tại và tính hợp lệ của hash SHA-256 các file `memberships.parquet`, `sequence_summary.parquet`, `experiment_metrics.json`, `experiment_config.json`, `pilot_result_manifest.json`.
  - Kiểm tra tính hợp lệ của vai trò sự kiện: `event_role_candidate` phải thuộc `{"PRE", "MAINSHOCK", "POST"}`; mainshock tự liên kết phải mang vai trò `MAINSHOCK`.
  - Kiểm tra null probability policy: `membership_probability` bắt buộc là `None` cho `WINDOW` và `DBSCAN`; bắt buộc thuộc `[0.0, 1.0]` cho `HDBSCAN_GLOBAL`.
  - Đối soát chi tiết từng window: số lượng membership phải khớp `candidate_count`, `sequence_member_count`, `aftershock_count`, `foreshock_candidate_count`, `noise_count`.
  - Phân biệt rõ các kết quả vật lý/phân cụm hợp lệ (`EXP_MAINSHOCK_CLASSIFIED_AS_NOISE`, window không có POST) với lỗi cài đặt: ghi nhận vào `case_results` của report thay vì coi là lỗi code.
  - Cập nhật `ColabPathAdapter.verify_local_smoke_gate` để chặn các report thiếu thuật toán hoặc rỗng phần reconciliation.

### 2.2. [P1] Xử lý Multi-Sequence Overlap và Đồng bộ Run ID
- **Vấn đề:** Các hàm fit gán nhãn `UNIQUE` độc lập theo từng window; candidate nằm trong nhiều window đều bị gán `UNIQUE`/rank 1. Runner cũng gán cứng `run_id="temp_run"`.
- **Khắc phục:**
  - Giữ nguyên toàn bộ raw memberships (candidate xuất hiện ở cả hai sequence theo hợp đồng ML_DATA_MODEL.md).
  - Đối soát xuyên window: nếu candidate được nhận vào nhiều hơn 1 sequence (`is_sequence_member=True`), đánh dấu `multi_sequence_status="AMBIGUOUS"` và `resolution_rank=None` (chờ bộ giải quyết tie-break adaptive tại task EXP-03).
  - Nếu candidate chỉ thuộc 1 sequence: giữ `multi_sequence_status="UNIQUE"` và `resolution_rank=1`.
  - Đồng bộ `effective_run_id` sinh ngẫu nhiên/xác định ngay từ đầu `run_baseline_experiment` truyền xuyên suốt vào memberships, summaries, manifest và config, thay thế hoàn toàn chuỗi tạm `"temp_run"`.

### 2.3. [P2] Phân định Fresh-Kernel Run và Lưu trữ Artifacts
- **Vấn đề:** `scripts/inspect-exp02-notebook.py` fallback sang `isolated_exec` nhưng vẫn trả `RESTART_RUN_ALL_PASSED` và `colab_migration_allowed=true`. Notebook dùng `TemporaryDirectory` và xóa sạch ở cell cuối.
- **Khắc phục:**
  - Phân biệt rõ ràng: chỉ khi có `nbclient`/`ipykernel` thật sự chạy kernel mới xác nhận `RESTART_RUN_ALL_PASSED`, `fresh_kernel_acceptance=True`, `colab_migration_allowed=True`. Nếu chạy fallback `isolated_exec`, trả về `FALLBACK_EXEC_PASSED`, `fresh_kernel_acceptance=False`, `colab_migration_allowed=False`.
  - Cấu hình thư mục output `EXP02_OUTPUT_DIR` (mặc định `staging/ml-experiments/exp02-baselines`) và không xóa thư mục kết quả ở cell cuối; bảo toàn toàn bộ kết quả và báo cáo để tái kiểm chứng.

### 2.4. [P2] Điều chỉnh trạng thái Task EXP-02 về In Progress
- Chuyển trạng thái task `EXP-02` trong `EXP-02.md` và `docs/task/tasks/README.md` về `In Progress`.
- Bỏ tick các tiêu chí chờ Colab và official MLD-05 export:
  - `[ ] Colab dùng cùng code/bundle/config, đối soát invariants/tolerance; không rewrite model (chờ môi trường Colab sau local gate).`
  - `[ ] Official run pin EXPORTED dataset; pilot không phát _SUCCESS/RESULT_READY/CANDIDATE (chờ MLD-05 export).`

### 2.5. [P2] Sửa CI và Đưa Tests Mới vào Phạm vi Chạy
- Đã xóa trailing whitespace tại dòng 27 của `scripts/inspect-exp02-notebook.py`.
- Đồng bộ rebase với các bản vá harness từ nhánh `feat/exp-01-jupyter-harness`.
- Bổ sung `test_baseline_algorithms.py` vào target `test-pilot` trong `Makefile`.
- Bổ sung dependencies `numpy==2.2.2`, `scipy==1.15.1`, `scikit-learn==1.6.1` vào `tests/ci/requirements.txt`.
- Bổ sung các file/script/config EXP vào `scripts/ci-impact.py` để tự động chạy kiểm thử khi có thay đổi.

## 3. Checks và tests đã chạy

- **Unit and Contract Test Suite (`tests/test_baseline_algorithms.py`):**
  - Lệnh: `python -m unittest tests/test_baseline_algorithms.py`
  - Kết quả: **13 tests PASS**, 0 errors, 0 failures.
  - Các test cases mới bổ sung:
    1. `test_multi_sequence_overlap_marked_ambiguous_and_rank_none`: Xác thực candidate chung giữa hai mainshock window mang trạng thái `AMBIGUOUS` và `resolution_rank=None`, trong khi candidate độc quyền giữ `UNIQUE` và rank 1.
    2. `test_runner_synchronizes_run_id_across_memberships_and_manifest`: Xác thực run ID đồng bộ giữa memberships, summaries, manifest và config.
    3. `test_local_smoke_report_rejects_empty_or_missing_algorithms`: Xác thực reject khi thiếu thuật toán baseline.
    4. `test_local_smoke_report_rejects_corrupted_roles_and_count_mismatch`: Xác thực reject khi sai lệch count hoặc vai trò không hợp lệ.
    5. `test_colab_adapter_rejects_incomplete_smoke_report`: Xác thực Colab adapter reject báo cáo thiếu hoặc sai reconciliation.

- **EXP-01 Harness Suite (`tests/test_experiment_harness.py`):**
  - Lệnh: `python -m unittest tests/test_experiment_harness.py`
  - Kết quả: **44 tests PASS**, 0 errors, 0 failures.

- **Notebook headless runner (`scripts/inspect-exp02-notebook.py`):**
  - Lệnh: `python scripts/inspect-exp02-notebook.py`
  - Kết quả: Hoàn thành 7/7 code cells, lưu trữ artifacts và report tại `staging/ml-experiments/exp02-baselines/local-smoke-report.json`, thông báo chế độ fallback rõ ràng khi chưa có Jupyter kernel runner.

- **Workflow Contract & CI (`tests/ci/test_workflow_contract.py`):**
  - Lệnh: `python -m unittest tests/ci/test_workflow_contract.py`
  - Kết quả: **4 tests PASS**.

- **Task Status Consistency:**
  - Lệnh: `python scratch/check_task_status.py`
  - Kết quả: **76 task files, tracking sections and index rows agree**.

## 4. Giới hạn / Rủi ro / Bước tiếp theo

- **Colab pilot portability:** Việc chạy đối soát cùng code/config/bundle trên Google Colab sau local gate thuộc về giai đoạn tiếp theo của [EXP-02 - Chạy Window, DBSCAN và HDBSCAN global](docs/task/tasks/EXP-02.md).
- **Official research dataset export:** Nghiên cứu chính thức trên toàn kỳ phụ thuộc vào [MLD-05 - Validate feature snapshot và export Parquet bundle](docs/task/tasks/MLD-05.md); pilot hiện tại chỉ dùng phục vụ kiểm tra kỹ thuật (DEVELOPMENT_ONLY).
- **Kỹ thuật phân cụm nâng cao / Adaptive sequence resolver:** Thuật toán giải quyết candidate thuộc nhiều sequence thuộc về [EXP-03 - Xây HDBSCAN adaptive và giải quyết multi-sequence](docs/task/tasks/EXP-03.md).
- **Import Iceberg `ml.*`:** Việc nạp kết quả nghiên cứu vào bảng Iceberg thuộc về [MLI-03 - Validate/import kết quả và commit bảng ML Iceberg](docs/task/tasks/MLI-03.md).
- **Reviewer:** Reviewer độc lập cho task P0 này hiện là `unassigned`.
