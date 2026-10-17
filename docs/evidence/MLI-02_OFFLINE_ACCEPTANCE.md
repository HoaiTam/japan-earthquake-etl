# MLI-02 - Offline Evidence & Acceptance

- **Task ID:** MLI-02
- **Tên task:** Tạo Airflow DAG build ML dataset
- **Branch:** `feat/mli-02-dataset-build-dag`
- **Assignee:** HoaiTam
- **Reviewer:** unassigned
- **Trạng thái:** In Progress

---

## 1. Tóm tắt thay đổi

1. **Airflow DAG build dataset (`airflow/dags/mli_02_build_dataset.py`):**
   - Khởi tạo DAG `mli_02_build_dataset` với `schedule=None`, `catchup=False`, `is_paused_upon_creation=True`, `max_active_runs=1`, `max_active_tasks=1`, `retries=0`.
   - Kết nối tuần tự chính xác 10 task groups / tasks:
     `wait_gold_published → resolve_gold_snapshot → audit_gold_input → resolve_mc → select_mainshocks → build_candidate_windows → build_spacetime_features → validate_feature_snapshot → export_parquet → mark_dataset_exported`.
   - Leaf task duy nhất là `mark_dataset_exported.contract_gate` với quy tắc `all_success` (không dùng `all_done` để che giấu lỗi upstream).
   - Tích hợp callback thất bại `dag_failure_summary`.

2. **Control-plane runtime (`airflow/dags/ml_dataset_runtime.py`):**
   - Định nghĩa `Mli02Error` cho các mã lỗi chuẩn (`DS_GOLD_NOT_PUBLISHED`, `DS_INVALID_PERIOD`, `DS_INVALID_VERSION`, `DS_IDENTITY_CONFLICT`, `DS_EXPORT_CHECKSUM_MISMATCH`, v.v.).
   - Kiểm tra tính hợp lệ của cấu hình dataset (split `REPRODUCTION` hoặc `EXTENSION`, half-open UTC interval, baseline filters, version strings).
   - Dẫn xuất deterministic `dataset_id = "ds_" + sha256(canonical_payload)` khớp chính xác thuật toán Java trong `DatasetIdentityPlan.java`.
   - Quản lý cross-DAG staging lease thông qua `source_run_guard`.
   - Cơ chế attempt isolation: mỗi lần xuất (attempt) được ghi vào thư mục cách ly `export_attempts/attempt_<N>` trước khi promote, đảm bảo retry không bao giờ trộn lẫn các file part.
   - Fail-closed: dừng ngay lập tức nếu Gold chưa `Published` hoặc bất kỳ bước nào thất bại, không bao giờ chuyển dataset status sang `EXPORTED`.
   - Thiết lập ranh giới Colab an toàn: đánh dấu `colab_handoff` ở chế độ `MANUAL_OPERATOR_REQUIRED` với `auto_triggered=false`.

3. **Fixture kiểm thử offline (`airflow/dags/fixtures/mli_02_mock_v1.json`):**
   - Dữ liệu mock xác định cho Reproduction split, Gold publication reference, audit counts, Mc config, mainshocks và candidate windows.

4. **Bộ kiểm thử tĩnh và kiểm thử đơn vị (`airflow/tests/test_mli_02_build_dataset.py`):**
   - 11 test cases bao quát hợp đồng DAG và runtime control plane.

5. **Operator Runbook (`docs/runbooks/MLI-02_DATASET_BUILD.md`):**
   - Hướng dẫn vận hành DAG, kiểm tra checksum, quy trình handoff thủ công lên Google Drive cho Google Colab, và xử lý sự cố.

---

## 2. Kiểm tra thực tế và kết quả

### 2.1. Chạy bộ kiểm thử đơn vị & hợp đồng DAG

Lệnh:
```powershell
pytest airflow/tests/test_mli_02_build_dataset.py
```

Kết quả:
```text
============================= test session starts =============================
platform win32 -- Python 3.11.0, pytest-9.0.3, pluggy-1.6.0
collected 11 items

airflow\tests\test_mli_02_build_dataset.py ...........                   [100%]

============================= 11 passed in 1.34s ==============================
```

Chi tiết 11 test cases:
1. `test_manual_paused_single_run_and_no_retries`: Xác nhận `dag_id`, `schedule=None`, `catchup=False`, `max_active_runs=1`, `retries=0`.
2. `test_ten_task_groups_in_exact_sequence`: Xác nhận 10 task groups tồn tại và chuỗi dependency tuần tự chính xác.
3. `test_sole_final_leaf_all_success`: Xác nhận `mark_dataset_exported.contract_gate` là leaf duy nhất và dùng `trigger_rule="all_success"`.
4. `test_no_colab_external_service_triggers_in_source`: Xác nhận không có import hoặc trigger gọi dịch vụ cloud/Colab tự động.
5. `test_dag_fails_if_gold_not_published`: Xác nhận từ chối chạy khi Gold chưa `Published` (`published=false` hoặc `verify_status!="PASSED"`).
6. `test_dag_fails_if_dataset_config_invalid`: Xác nhận từ chối khi period, split hoặc version config sai quy cách.
7. `test_deterministic_dataset_id_and_identity_conflict_detection`: Xác nhận tính xác định của `dataset_id` và phát hiện xung đột payload (`DS_IDENTITY_CONFLICT`).
8. `test_retry_isolates_export_attempts_without_mixing_parts`: Xác nhận attempt isolation phân tách thư mục `attempt_1` và `attempt_2`.
9. `test_failure_stops_before_dataset_exported_and_colab_not_ready`: Xác nhận khi một bước thất bại, dataset không được đánh dấu `EXPORTED` và Colab không ready.
10. `test_readback_checksum_tampering_is_caught`: Xác nhận phát hiện sửa đổi file Parquet và từ chối xuất bundle (`DS_EXPORT_CHECKSUM_MISMATCH`).
11. `test_successful_full_run_and_manual_colab_handoff`: Xác nhận luồng E2E thành công sinh đầy đủ artifact, đăng ký manifest, và chỉ thị handoff Colab thủ công.

### 2.2. Kiểm tra tính nhất quán tài liệu và kế hoạch

Lệnh:
```powershell
& "C:\Program Files\Git\bin\sh.exe" scripts/check-task-status.sh
& "C:\Program Files\Git\bin\sh.exe" scripts/check-week-5-plan.sh
```

Kết quả:
```text
Task status check passed: 76 task files, tracking sections and index rows agree.
Week 5 plan check passed: 12 Core tasks, 72h (32/17/23); ORC-06 accepted by HoaiTam; DAT-02 Jupyter first, Colab gated, QA-04 week 6.
```

---

## 3. Đối chiếu Tiêu chí hoàn thành (Acceptance Criteria)

| Tiêu chí trong task MLI-02 | Trạng thái | Minh chứng |
|---|---|---|
| DAG không chạy nếu Gold chưa Published hoặc dataset config invalid | **Đạt** | `test_dag_fails_if_gold_not_published`, `test_dag_fails_if_dataset_config_invalid` ném `DS_GOLD_NOT_PUBLISHED` / `DS_INVALID_*` trước khi tiến hành tính toán |
| Retry dùng cùng logical dataset identity và không trộn export attempts | **Đạt** | `compute_dataset_id` dẫn xuất cố định từ identity payload; `export_attempts/attempt_<N>` cách ly hoàn toàn các file part giữa các lần retry |
| Failure dừng trước `DATASET_EXPORTED`; downstream Colab không được đánh dấu ready | **Đạt** | `test_failure_stops_before_dataset_exported_and_colab_not_ready` chứng minh status giữ nguyên `BUILDING`, không tạo file manifest trong registry, Colab `ready=false` |
| DAG không tự trigger Colab miễn phí như một job service giả định | **Đạt** | Source code kiểm tra AST không có HTTP call / cloud API; run summary trả về chỉ dẫn handoff thủ công cho operator |

---

## 4. Giới hạn / Rủi ro / Bước tiếp theo

1. **Phụ thuộc upstream MLD-05:**
   - Việc tích hợp live runner thực tế trên cụm MinIO/Trino phụ thuộc vào việc hoàn thiện mã nguồn export chính thức tại [MLD-05 - Validate feature snapshot và export Parquet bundle](docs/task/tasks/MLD-05.md).
   - Phần việc còn thiếu: Kiểm thử live E2E trên container Docker có Spark/Iceberg runner thật thay vì mock runner.
2. **Nghiệm thu toàn trình ML:**
   - Việc kiểm thử end-to-end từ Gold Published snapshot qua ML DAG đến serving Trino và báo cáo thí nghiệm thuộc phạm vi của [MLQ-01 - Chạy E2E Gold snapshot đến ML report](docs/task/tasks/MLQ-01.md).
3. **Review:**
   - Reviewer độc lập hiện chưa được chỉ định (`unassigned`).
