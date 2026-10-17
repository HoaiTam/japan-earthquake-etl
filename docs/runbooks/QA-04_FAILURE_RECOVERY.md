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

Runtime request dùng contract `qa-04-runtime-v1`, pin SHA-256 của matrix,
`run_id`/`operation_id`, namespace `iceberg.qa04_<id>`, scope canonical theo
case, exact input/artifact checksums, baseline snapshot receipts và report path
mới trong `/opt/pipeline/staging/qa-04`. Tháng ETL phải khớp window UTC tháng
đầy đủ; case ML phải pin experiment run ID. Snapshot chưa tồn tại được ghi rõ
`state=ABSENT`. URI wildcard hoặc có credential bị từ chối.

Runner executable chỉ lấy từ `QA_04_RUNTIME_COMMAND`; request phải pin đúng một
artifact `qa04-runtime-runner` bằng đường dẫn tuyệt đối và SHA-256. Controller
đối chiếu command với đường dẫn này và xác minh bytes runner trước khi gọi.
Controller pin bytes của request trước khi gọi runner và kiểm tra request/matrix
không đổi sau execution. Report và từng evidence phải mang runner SHA-256 đã
pin. Runner trả contract `qa-04-report-v1`; mỗi case phải gắn evidence
JSON đã stage, checksum, fault/recovery identity, snapshots/counts và readback.
Report offline (`mode=offline_control`), receipt thiếu hoặc report ghi sẵn
không thể thành runtime PASS. Gold cases cần sáu snapshot và GLD-04 Trino
publication receipt; ML cases cần MLI-03 journal/publication evidence.

```bash
make smoke-qa-04 \
  QA_04_REQUEST=/opt/pipeline/staging/qa-04/request.json \
  QA_04_RUNTIME_COMMAND='/opt/pipeline/bin/qa04-runtime-runner'
```

Lệnh preflight `ml` kiểm MinIO/Catalog/Trino, image Spark QA, JAR và mounts;
sau đó gọi service `qa-04-failure-recovery`. Nó không build, pull, start, init
hoặc đổi credential. Runtime dừng với `QA04_RUNTIME_DEPENDENCY_PENDING` khi
ORC-07 hoặc MLI-03 chưa nghiệm thu xong. PR #79 đã merge nhưng MLI-03 vẫn
`In Progress` do còn thiếu acceptance MinIO/Catalog/Trino.

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
- Timeout/interruption giữ whole-run lease và persisted state. Contender tiếp
  tục bị chặn cho tới khi writer/query được xác nhận đã quiescent; controller
  assert exact owner và chỉ ghi `lease_released=true` sau receipt `RELEASED`.
  Không xóa lease, bucket, volume, warehouse hoặc metadata để làm recovery xanh.

## Runtime evidence

Mỗi case phải ghi fault stage/reason, downstream calls, lease state, snapshots,
counts trước/sau, logical keys ngoài scope và report/query readback. Evidence
fixture chỉ chứng minh control-plane/component behavior. QA-04 chỉ chuyển Done
sau khi ORC-07 và MLI-03 cung cấp evidence thật từ Bronze → Silver → Gold →
Trino/Published và ML import.
