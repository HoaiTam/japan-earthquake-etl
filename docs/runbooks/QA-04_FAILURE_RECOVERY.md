# QA-04 — Failure và recovery theo tầng

QA-04 dùng ma trận bounded trong
`airflow/dags/fixtures/qa_04_failure_recovery_matrix.json`. Offline runner ghi
state/report vào thư mục được chỉ định, không gọi nguồn thật và không ghi
lakehouse. Runtime chỉ được bật trong namespace QA sau khi ORC-07 và MLI-03
đã nghiệm thu.

## Offline checks

```bash
python3 -m unittest discover -s airflow/tests -p 'test_qa_04_failure_recovery.py'
make test-qa-04
```

Tạo report chuẩn bị trước khi runtime:

```bash
python3 airflow/dags/qa_04_failure_recovery.py prepare \
  --json docs/evidence/QA-04-prepared.json \
  --markdown docs/evidence/QA-04-prepared.md
```

Chạy một case offline để kiểm tra state/recovery bằng fixture:

```bash
python3 airflow/dags/qa_04_failure_recovery.py offline \
  --case gold_commit_partial --root staging/qa-04
```

Output phải có `status=PASS`, `outside_scope_unchanged=true`, fault reason
đúng matrix và recovery dùng đúng scope. Report không chứa payload, secret,
credential hoặc exception text không được kiểm soát.

## Runtime gate

Runtime request phải pin `task_id=QA-04`, các `case_ids` trong matrix, namespace
dạng `iceberg.qa04_<id>` và đường dẫn report chính xác. Runner executable chỉ
được lấy từ `QA_04_RUNTIME_COMMAND`, không lấy từ request.

```bash
make smoke-qa-04 \
  QA_04_REQUEST=/opt/pipeline/staging/qa-04/request.json \
  QA_04_RUNTIME_COMMAND='python3 /opt/pipeline/qa04_runner.py'
```

Lệnh này chỉ preflight dịch vụ; nó không build, pull, start, init hoặc thay đổi
credential. Set `ORC_07_STATUS=Done` và `MLI_03_STATUS=Done` trong môi trường
đã có evidence tương ứng. Nếu dependency còn thiếu, lệnh dừng với
`QA04_RUNTIME_DEPENDENCY_PENDING`.

## Recovery rules

- Giữ nguyên `run_id`, operation identity, UTC window, input checksum và scope
  khi retry; không chọn `latest` hoặc wildcard.
- Fetch/archive/Silver lỗi thì dừng downstream. Reuse exact verified Bronze và
  chạy lại cùng scope sau khi nguyên nhân đã được xử lý.
- Gold partial commit hoặc mất receipt phải reconcile operation/fingerprint;
  chỉ ghi bảng còn thiếu hoặc receipt bị mất. Không replay commit mù.
- Trino verify lỗi chỉ retry query trên cùng snapshot bundle; publication vẫn
  bị chặn cho tới khi readback đạt.
- ML bundle lỗi ghi `REJECTED`; không sửa rồi tái sử dụng experiment run ID.
  Partial import cùng fingerprint chỉ được resume sau khi schema/checksum và
  exact snapshot readback đạt.
- Timeout/interruption giữ whole-run lease. Chỉ release bằng exact owner sau
  khi chứng minh writer/query đã quiescent; không xóa lease, bucket, volume,
  warehouse hoặc metadata để làm recovery xanh.

## Runtime evidence

Mỗi case phải ghi fault stage/reason, downstream calls, lease state, snapshots,
counts trước/sau, logical keys ngoài scope và report/query readback. Evidence
fixture chỉ chứng minh control-plane/component behavior. QA-04 chỉ chuyển Done
sau khi ORC-07 và MLI-03 cung cấp evidence thật từ Bronze → Silver → Gold →
Trino/Published và ML import.
