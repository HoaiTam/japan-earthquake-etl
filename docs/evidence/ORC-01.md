# ORC-01 — Evidence DAG skeleton và phase contracts

## 1. Phạm vi nghiệm thu

- Ngày `2026-10-07`; branch `feat/orc-01-etl-dag-contract`, từ
  `origin/main` `964e1d5` (merge PR #42 JMA-05).
- Assignee HoaiTam; reviewer unassigned, chưa ghi nhận approval.
- Hard dependencies CON-02/CON-03 đã Done trước khi triển khai.
- Acceptance ORC-01 là skeleton/contract/mock, không phải daily ETL Published.

Deliverables: [DAG](../../airflow/dags/orc_01_etl_pipeline.py),
[runtime boundary](../../airflow/dags/etl_pipeline_runtime.py),
[versioned fixture](../../airflow/dags/fixtures/orc_01_mock_v1.json),
[contract/runbook](../specs/ETL_ORCHESTRATION_CONTRACT.md).

## 2. Test/check thực tế

| Lệnh | Kết quả | Phạm vi |
|---|---|---|
| `make test-orchestration` | 25 tests, OK | Offline phase/context/SDK-double graph |
| `make test-airflow` | 66 tests, OK | Regression gồm 25 test ORC-01 |
| `make etl-preview` | Exit 0, exact fixture scope | Metadata plan, không ghi staging |
| `make etl-mock` | Exit 0, 6 phase, MockComplete, published=false | Không tạo Gold snapshot/publication thật |
| `env JAVA_HOME=/Users/hoaitam/Library/Java/JavaVirtualMachines/azul-21.0.11/Contents/Home make check` | Exit 0; 162 Java tests, 66 Airflow tests; contracts/config/static Compose OK | Host JDK 21 compile release 17, không chạy nghiệp vụ ETL thật |
| Airflow 3.3.2 DagBag import trong container | Exit 0, 8 tasks/6 groups, no import errors, all_success | SDK thật, không trigger scheduler run |
| `git diff --check` | Exit 0 | Không lỗi whitespace |

Không cộng test chạy lặp trong `make check` thành số test độc lập. Maven
enforcer/verify đạt; warning shade/resource overlap có sẵn không làm build
thất bại. Không gọi USGS/JMA, xóa Bronze/warehouse/volume, sửa `.env` hoặc
catalog DAT-01 trong task này.

## 3. Acceptance mapping

| Tiêu chí | Evidence |
|---|---|
| Failure dừng đúng tầng | Inject failure mỗi phase, assert adapter sau không được gọi; rejected Bronze, incomplete Silver, uncommitted Gold, failed Trino và mock Published đều bị chặn |
| Task-group I/O rõ | Contract v1.0/fixture; context SHA/upstream SHA, pinned manifest/raw SHA, partition set và per-table snapshot bundle |
| DAG structure không cần mạng | SDK double import chính file DAG, assert 8 nodes/edges/6 groups/strict final leaf; không cần Airflow/Docker |

Cases khác: UTC Z/half-open/future guard, explicit real scope, secret/payload
field bị reject, unsafe URI, zero count estimate, fixture rerun immutable,
revision checksum phải resolve context mới, safe timeout/nonzero/malformed/
oversized/duplicate-key JSON. Real-mode protocol unit test có receipt
`published=true` từ **subprocess mock**, không có Iceberg/Trino publication.

## 4. Airflow import thực tế

Lần thành công `2026-10-07T10:38:36Z` dùng
`airflow.dag_processing.dagbag.DagBag`, `PYTHONPATH=/opt/airflow/dags`, file
`/opt/airflow/dags/orc_01_etl_pipeline.py`. Không có import error.

| Task | Upstream task IDs | Trigger rule |
|---|---|---|
| resolve_run_context | — | all_success |
| source_readiness.execute_adapter | resolve_run_context | all_success |
| bronze.execute_adapter | resolve_run_context, source_readiness.execute_adapter | all_success |
| silver.execute_adapter | bronze.execute_adapter, resolve_run_context | all_success |
| gold.execute_adapter | resolve_run_context, silver.execute_adapter | all_success |
| verification.execute_adapter | gold.execute_adapter, resolve_run_context | all_success |
| publication.execute_adapter | resolve_run_context, verification.execute_adapter | all_success |
| publication.contract_gate | publication.execute_adapter, resolve_run_context | all_success |

Leaf duy nhất `publication.contract_gate`. Lệnh lặp lại ở
[contract §6](../specs/ETL_ORCHESTRATION_CONTRACT.md#6-cách-test-hiện-tại).
Diagnostic được điều chỉnh theo API đang cài: không dùng `include_examples`,
thêm helper path và đọc `TriggerRule.value`; không bỏ failure gate.

## 5. Giới hạn và bàn giao

ORC-01 đủ acceptance skeleton nên chuyển Done. Chưa commit/push/mở PR tự
động; reviewer độc lập vẫn được khuyến nghị cho P0. Không nhận mock như hệ
thống thật hoặc chuyển Done các downstream sau:

- [ORC-02 - Cấu hình lịch và readiness cho hai nguồn](../task/tasks/ORC-02.md):
  source adapters/planner/lịch/readiness daily thật; DAG hiện manual.
- [ORC-03 - Implement backfill và reprocessing](../task/tasks/ORC-03.md):
  real preview, Bronze reuse/current state và affected-scope recovery.
- [ORC-04 - Chuẩn hóa logging và run summary](../task/tasks/ORC-04.md):
  persistent success/failure summary, source/layer counts và durations.
- [ORC-05 - Chốt recovery, concurrency và tài nguyên](../task/tasks/ORC-05.md):
  retry/idempotency/resource/cross-DAG concurrency thật, chưa benchmark.
- [SLV-09 - Tích hợp và kiểm thử Silver đa nguồn](../task/tasks/SLV-09.md):
  adapter/bundle thật gồm link/membership, readback/reconciliation.
- [GLD-01 - Xây canonical event, dimensions và bands](../task/tasks/GLD-01.md),
  [GLD-03 - Ghi Gold Iceberg và commit snapshot](../task/tasks/GLD-03.md),
  [GLD-04 - Tạo Trino views và verification SQL](../task/tasks/GLD-04.md):
  business output, committed bundle, SQL reports/publication thật.
- [QA-01 - Chạy E2E daily đa nguồn](../task/tasks/QA-01.md): scheduler run
  đến Gold Published với evidence xuyên tầng.

Nhóm JMA-04/05/ORC-01 bàn giao Bronze thật và skeleton phase I/O. ML/Colab
ngoài DAG này; build/import riêng do
[MLI-02 - Tạo Airflow DAG build ML dataset](../task/tasks/MLI-02.md) và
[MLI-03 - Validate/import kết quả và commit bảng ML Iceberg](../task/tasks/MLI-03.md).
