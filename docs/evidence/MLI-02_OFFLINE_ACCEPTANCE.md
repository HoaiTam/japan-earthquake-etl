# MLI-02 - Offline Evidence & Acceptance

- **Task ID:** MLI-02
- **Tên task:** Tạo Airflow DAG build ML dataset
- **Branch:** `feat/mli-02-dataset-build-dag`
- **Assignee:** HoaiTam
- **Reviewer:** unassigned
- **Trạng thái:** In Progress

---

## 1. Tóm tắt thay đổi & Khắc phục Review Comments

Sau review từ commit `adff6b6` ([PR #85](https://github.com/HoaiTam/japan-earthquake-etl/pull/85#issuecomment-6229976120)), toàn bộ 6 điểm yêu cầu đã được xử lý triệt để:

1. **[P1] Ranh giới runner thật vs mock test double:**
   - Production path ném lỗi fail-closed `DS_RUNNER_REQUIRED` nếu thiếu `ML_BUILD_RUNNER_COMMAND` và không có mock.
   - Tách biệt `MockPipelineAdapter` vào adapter kiểm thử riêng: mock không bao giờ ghi đè lên registry chính thức (`MLI_DATASET_REGISTRY_ROOT`) hoặc thư mục export chính thức; đánh dấu status `MOCK_COMPLETED` và `colab_handoff.ready=false`.
   - Readback Parquet bắt buộc đối soát schema, grain uniqueness, lineage (`dataset_id`), và số dòng thực tế (`feature_row_count`). Nếu Parquet không khớp (ví dụ manifest khai 142 nhưng file chỉ có 3 dòng), runtime ném ngay `DS_COUNT_RECONCILIATION_FAILED`.

2. **[P1] Khử tham chiếu hash vòng trong manifest/checksums & Handoff registry:**
   - `checksums.sha256` lập index SHA-256 cho các file part dữ liệu (`features-part-*.parquet`).
   - `export_manifest_sha256` được tính bằng hash của file `checksums.sha256`.
   - `dataset_manifest.json` được tạo đầy đủ với `export_manifest_sha256`, `export_artifact_uri`, `export_manifest_ref` và ghi một lần duy nhất, tránh mọi sửa đổi làm thay đổi checksum sau đó.
   - Checksums index được sao chép vào `registry_root / exports / <dataset_id> / checksums.sha256` và được trỏ bởi `export_manifest_ref = "exports/<dataset_id>/checksums.sha256"`, tương thích 100% với `TrustedDatasetResolver` (MLI-03).

3. **[P1] Xác minh receipt từng bước của external runner & Leaf contract gate:**
   - Xóa file report cũ trước khi gọi runner để chống đọc nhầm kết quả cũ.
   - Hàm `validate_runner_receipt` kiểm tra chặt chẽ `step`, `run_id`, `dataset_id`, `gold_snapshot_id`, `attempt`, và `identity_payload_sha256`.
   - Leaf contract gate `mark_dataset_exported_summary` đọc lại toàn bộ bundle từ đĩa: verify checksums, verify schema/rows Parquet, verify registry manifest, và kiểm tra chắc chắn lease staging đã được giải phóng (`RELEASED`), không cho phép runner vượt rào chỉ bằng `status="DATASET_EXPORTED"`.

4. **[P1] Attempt isolation & Chỉ promote file set đã verify:**
   - Từ chối tái sử dụng thư mục attempt nếu đã tồn tại file (`DS_ATTEMPT_DIRECTORY_EXISTS`).
   - Bước promote chỉ copy đúng danh sách file đã được xác minh trong `checksums.sha256`, không quét `iterdir()` thô để tránh promote part cũ/lỗi.
   - Hỗ trợ tái sử dụng idempotent bundle hợp lệ cho dataset đã được export, bảo toàn nguyên vẹn bytes đã được consumer pin.

5. **[P1] Chấp nhận DagRun run_id chuẩn của Airflow:**
   - Cập nhật pattern `SAFE_RUN_ID_PATTERN` cho phép các ký tự chuẩn của Airflow (`+`, `:`, `@`, `-`, `_`, `.`).
   - Giữ nguyên `run_id` gốc trong lease identity và metadata.
   - Tạo staging directory slug an toàn trên hệ thống tệp (đặc biệt tương thích Windows cấm dấu `:`).

6. **[P2] Sửa lỗi whitespace CI:**
   - Xóa bỏ các dòng trắng thừa ở cuối file `docs/task/tasks/MLI-02.md`, vượt qua `git diff --check`.

### 1.2. Đợt review lần 2 ([PR #85 Comment 6248018143](https://github.com/HoaiTam/japan-earthquake-etl/pull/85#issuecomment-6248018143))

5 điểm lưu ý tiếp theo từ Tâm đã được xử lý triệt để:

1. **[P1] Ngăn chặn validation FAILED phát hành DATASET_EXPORTED:**
   - Kiểm tra nghiêm ngặt `status`, `quality_gate_status == "PASSED"`, `invalid_feature_count == 0` và đối soát số dòng (`candidate_row_count == feature_row_count`) tại cả receipt runner và publish gate.
   - Thất bại validation chặn đứng quá trình export và publish; `step_10_mark_dataset_exported.json` không bao giờ được tạo; `colab_handoff` không bao giờ ở trạng thái ready.
2. **[P1] Triệt tiêu fake candidate lineage và snapshot pins giả định:**
   - Trích xuất candidate lineage trực tiếp từ các part Parquet đã xác minh; kiểm tra tính duy nhất của grain `(mainshock_event_id, candidate_event_id)`.
   - Đối soát đối chiếu tập mainshocks giữa Parquet và step 05 (`parquet_mainshocks == set(mainshock_ids)`).
   - Ghim các snapshot ID thật (`registry_snapshot_id`, `mainshock_snapshot_id`, `candidate_snapshot_id`) từ các step trước; ném lỗi fail-closed `DS_SNAPSHOT_PINS_MISSING` nếu thiếu hoặc rỗng.
3. **[P1] Cho phép retry publish an toàn sau khi đã giải phóng lease:**
   - Khi `mark_dataset_exported` được retry trên bundle đã export thành công trước đó cho run này, bỏ qua `lease("assert")` để tránh lỗi `SOURCE_LEASE_NOT_HELD`.
   - Giữ nguyên biên nhận `lease_release_receipt.json` và không bao giờ xóa hoặc cướp lease của run khác (`reused_existing_bundle=true`, `lease_released=true`).
4. **[P2] Fail-closed khi lease state bị hỏng tại leaf contract gate:**
   - Không nuốt `ValueError`/`OSError` khi đọc `owner.json` (ném lỗi `DS_LEASE_STATE_CORRUPT`).
   - Bắt buộc kiểm tra biên nhận giải phóng lease `lease_release_receipt.json` hợp lệ khớp đúng `dag_id` và `run_id`.
   - Sửa `test_leaf_gate_fails_if_lease_still_held` dùng artifact URI và export bundle thực tế để kiểm tra đúng cơ chế lease gate.
### 1.3. Đợt review lần 3 ([PR #85 Comment 6248468165](https://github.com/HoaiTam/japan-earthquake-etl/pull/85#issuecomment-6248468165))

2 điểm làm lại cần thiết đã được xử lý triệt để:

1. **[P1] Tách bạch dataset identity và execution identity, ngăn DagRun mới ghi đè bundle EXPORTED:**
   - Theo đặc tả `docs/specs/ML_DATA_MODEL.md:168`, `build_run_id` là execution context, tuyệt đối KHÔNG nằm trong dataset identity (`dataset_id`, `identity_payload_sha256`).
   - Xóa bỏ việc so khớp `build_run_id == context["run_id"]` khi kiểm tra bundle EXPORTED đã tồn tại trên đĩa (`execute_step` và `promote_and_publish_dataset`).
   - Khi DagRun B chạy lại cùng config với DagRun A đã export thành công trước đó:
     - Runtime phát hiện bundle EXPORTED hợp lệ, kiểm tra toàn vẹn checksums và Parquet readback, và REUSE bundle đó nguyên vẹn mà không ghi đè hoặc chỉnh sửa dù chỉ 1 byte của `dataset_manifest.json`, giữ nguyên SHA-256 và timestamp mà consumer (MLI-03 / Colab) đã pin.
     - Trả về `reused_existing_bundle = True`.
     - Nếu bundle đã tồn tại nhưng sai identity -> fail-closed với `DS_IDENTITY_CONFLICT`.
     - Nếu bundle đã tồn tại nhưng thiếu file, hỏng checksum hoặc schema không hợp lệ -> fail-closed với `DS_EXISTING_BUNDLE_CORRUPT`.
     - Metadata riêng của DagRun B (`run_id = "run_dagrun_B"`, `reused_existing_bundle = True`, `original_build_run_id = "run_dagrun_A"`) được ghi vào file `run_summary.json` trong staging riêng của DagRun B, không làm ảnh hưởng đến bundle đã pin.
     - Bổ sung kiểm thử `test_two_distinct_dagruns_same_dataset_reuses_existing_bundle_without_mutating_manifest`.

2. **[P1] Thắt chặt fail-closed bằng chứng quality gate (verify_validation_report):**
   - Tạo hàm chốt chặn dùng chung `verify_validation_report(report)` áp dụng đồng nhất ở TẤT CẢ các chốt chặn: `validate_runner_receipt`, `_run_external_runner_step`, `execute_step` và `promote_and_publish_dataset`.
   - `quality_gate_status`: Bắt buộc phải có trong receipt, không được null, và bắt buộc thuộc tập `{"PASSED", "SUCCESS"}`. Thiếu hoặc null ném ngay `DS_QUALITY_GATE_MISSING`; sai giá trị ném `DS_QUALITY_GATE_FAILED:<status>`.
   - `invalid_feature_count`: Bắt buộc phải có, không được null, là số nguyên hợp lệ (từ chối boolean hoặc chuỗi không parse được -> `DS_INVALID_FEATURES_INVALID_TYPE`), không âm, và bắt buộc strictly `== 0`.
   - `candidate_row_count` & `feature_row_count`: Bắt buộc phải có, kiểu số nguyên, `> 0`, và `candidate_row_count == feature_row_count`.
   - Bổ sung kiểm thử `test_validation_report_strict_fail_closed_contract` bao phủ toàn bộ các trường hợp thiếu field, field rỗng, boolean giả số nguyên, mismatch count, và xác nhận pipeline dừng trước export.

---

## 2. Kiểm tra thực tế và kết quả

### 2.1. Chạy bộ kiểm thử đơn vị & hợp đồng DAG (26/26 passed)

Lệnh:
```powershell
pytest -v airflow/tests/test_mli_02_build_dataset.py
```

Kết quả:
```text
============================= test session starts =============================
platform win32 -- Python 3.11.0, pytest-9.0.3, pluggy-1.6.0
collected 26 items

airflow/tests/test_mli_02_build_dataset.py::Mli02DagContractTest::test_manual_paused_single_run_and_no_retries PASSED [  3%]
airflow/tests/test_mli_02_build_dataset.py::Mli02DagContractTest::test_no_colab_external_service_triggers_in_source PASSED [  7%]
airflow/tests/test_mli_02_build_dataset.py::Mli02DagContractTest::test_sole_final_leaf_all_success PASSED [ 11%]
airflow/tests/test_mli_02_build_dataset.py::Mli02DagContractTest::test_ten_task_groups_in_exact_sequence PASSED [ 15%]
airflow/tests/test_mli_02_build_dataset.py::Mli02RuntimeControlPlaneTest::test_airflow_run_id_with_plus_and_colon_is_accepted PASSED [ 19%]
airflow/tests/test_mli_02_build_dataset.py::Mli02RuntimeControlPlaneTest::test_attempt_isolation_rejects_dirty_existing_directory PASSED [ 23%]
airflow/tests/test_mli_02_build_dataset.py::Mli02RuntimeControlPlaneTest::test_candidate_lineage_and_snapshot_pins_reconciliation PASSED [ 26%]
airflow/tests/test_mli_02_build_dataset.py::Mli02RuntimeControlPlaneTest::test_dag_fails_if_dataset_config_invalid PASSED [ 30%]
airflow/tests/test_mli_02_build_dataset.py::Mli02RuntimeControlPlaneTest::test_dag_fails_if_gold_not_published PASSED [ 34%]
airflow/tests/test_mli_02_build_dataset.py::Mli02RuntimeControlPlaneTest::test_deterministic_dataset_id_and_identity_conflict_detection PASSED [ 38%]
airflow/tests/test_mli_02_build_dataset.py::Mli02RuntimeControlPlaneTest::test_external_runner_receipt_validation_rejects_identity_mismatch PASSED [ 42%]
airflow/tests/test_mli_02_build_dataset.py::Mli02RuntimeControlPlaneTest::test_failure_stops_before_dataset_exported_and_colab_not_ready PASSED [ 46%]
airflow/tests/test_mli_02_build_dataset.py::Mli02RuntimeControlPlaneTest::test_leaf_gate_fails_if_lease_still_held PASSED [ 50%]
airflow/tests/test_mli_02_build_dataset.py::Mli02RuntimeControlPlaneTest::test_leaf_gate_fails_if_release_receipt_missing_or_invalid PASSED [ 53%]
airflow/tests/test_mli_02_build_dataset.py::Mli02RuntimeControlPlaneTest::test_leaf_gate_fails_on_corrupt_lease_state PASSED [ 57%]
airflow/tests/test_mli_02_build_dataset.py::Mli02RuntimeControlPlaneTest::test_mock_pipeline_isolated_cannot_publish_official_registry PASSED [ 61%]
airflow/tests/test_mli_02_build_dataset.py::Mli02RuntimeControlPlaneTest::test_production_fails_without_runner_and_without_mock PASSED [ 65%]
airflow/tests/test_mli_02_build_dataset.py::Mli02RuntimeControlPlaneTest::test_promote_excludes_stray_unindexed_attempt_parts PASSED [ 69%]
airflow/tests/test_mli_02_build_dataset.py::Mli02RuntimeControlPlaneTest::test_publish_retry_bypasses_lease_assert_and_does_not_steal_leases PASSED [ 73%]
airflow/tests/test_mli_02_build_dataset.py::Mli02RuntimeControlPlaneTest::test_readback_checksum_tampering_is_caught PASSED [ 76%]
airflow/tests/test_mli_02_build_dataset.py::Mli02RuntimeControlPlaneTest::test_readback_count_mismatch_fails_gate PASSED [ 80%]
airflow/tests/test_mli_02_build_dataset.py::Mli02RuntimeControlPlaneTest::test_retry_isolates_export_attempts_without_mixing_parts PASSED [ 84%]
airflow/tests/test_mli_02_build_dataset.py::Mli02RuntimeControlPlaneTest::test_successful_full_run_with_external_runner_and_registry_handoff PASSED [ 88%]
airflow/tests/test_mli_02_build_dataset.py::Mli02RuntimeControlPlaneTest::test_two_distinct_dagruns_same_dataset_reuses_existing_bundle_without_mutating_manifest PASSED [ 92%]
airflow/tests/test_mli_02_build_dataset.py::Mli02RuntimeControlPlaneTest::test_validation_failure_halts_export_and_leaves_colab_not_ready PASSED [ 96%]
airflow/tests/test_mli_02_build_dataset.py::Mli02RuntimeControlPlaneTest::test_validation_report_strict_fail_closed_contract PASSED [100%]

============================= 26 passed in 28.75s =============================
```

### 2.2. Kiểm tra tính tương thích resolver Java (TrustedDatasetResolverTest)

Lệnh:
```powershell
cmd /c "set JAVA_HOME=C:\Program Files\Java\jdk-23&& .\mvnw.cmd test -pl spark -Dtest=TrustedDatasetResolverTest"
```

Kết quả:
```text
[INFO] Running ie212.earthquake.spark.ml.TrustedDatasetResolverTest
[INFO] Tests run: 2, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.294 s -- in ie212.earthquake.spark.ml.TrustedDatasetResolverTest
[INFO] BUILD SUCCESS
```

### 2.3. Kiểm tra Whitespace với Git Diff

Lệnh:
```powershell
git diff --check origin/main
```

Kết quả:
Không có lỗi whitespace (`exit code 0`).

---

## 3. Đối chiếu Tiêu chí hoàn thành (Acceptance Criteria)

| Tiêu chí trong task MLI-02 | Trạng thái | Minh chứng |
|---|---|---|
| DAG không chạy nếu Gold chưa Published hoặc dataset config invalid | **Đạt** | `test_dag_fails_if_gold_not_published`, `test_dag_fails_if_dataset_config_invalid` ném `DS_GOLD_NOT_PUBLISHED` / `DS_INVALID_*` trước khi tiến hành tính toán |
| Retry dùng cùng logical dataset identity và không trộn export attempts | **Đạt** | `compute_dataset_id` dẫn xuất cố định từ identity payload; `export_attempts/attempt_<N>` cách ly hoàn toàn các file part giữa các lần retry; từ chối attempt directory có part dở; chỉ promote file set trong checksum index |
| Failure dừng trước `DATASET_EXPORTED`; downstream Colab không được đánh dấu ready | **Đạt** | Dừng trước `DATASET_EXPORTED`; khi dùng mock hoặc có lỗi, không ghi manifest vào registry chính thức, `colab_handoff.ready=false` |
| DAG không tự trigger Colab miễn phí như một job service giả định | **Đạt** | Source code kiểm tra AST không có HTTP call / cloud API; run summary trả về chỉ dẫn handoff thủ công cho operator |

---

## 4. Giới hạn / Rủi ro / Bước tiếp theo

1. **Phụ thuộc upstream MLD-05:**
   - Việc tích hợp live runner thực tế trên cụm MinIO/Trino phụ thuộc vào việc hoàn thiện mã nguồn export chính thức tại [MLD-05 - Validate feature snapshot và export Parquet bundle](docs/task/tasks/MLD-05.md).
   - Phần việc còn thiếu: Kiểm thử live E2E trên container Docker có Spark/Iceberg runner thật thay vì mock runner.
2. **Nghiệm thu toàn trình ML:**
   - Việc kiểm thử end-to-end từ Gold Published snapshot qua ML DAG đến serving Trino và báo cáo thí nghiệm thuộc phạm vi của [MLQ-01 - Chạy E2E Gold snapshot đến ML report](docs/task/tasks/MLQ-01.md).
3. **Trạng thái bàn giao:**
   - Giữ trạng thái task `In Progress` theo chỉ đạo của review Tâm cho đến khi các hard dependencies upstream hoàn tất nghiệm thu live.
   - Reviewer độc lập hiện chưa được chỉ định (`unassigned`).
