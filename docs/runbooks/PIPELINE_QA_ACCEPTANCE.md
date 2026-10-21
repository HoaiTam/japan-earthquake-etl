# QA-02 / QA-03 — Nghiệm thu trên storage thật

## Mục đích và ranh giới

Runner cố định `pipeline_qa_acceptance.py --task QA-02|QA-03` gọi Java/Spark qua
production Bronze writers → ORC-07 sáu phase → Gold Iceberg → Trino Published.
Python chỉ quản lý request, lease, checksum và kiểm tra receipt; xử lý record,
đọc/ghi Parquet và bảng Iceberg đều nằm trong Java.

QA-02 dùng input **tổng hợp, nhỏ và xác định được**, không gọi USGS/JMA qua mạng.
MinIO, REST Catalog, Iceberg và Trino là service thật. Đây là kiểm thử hành vi
revision/rerun, không phải bằng chứng đã tải đầy đủ catalog lịch sử.

Bản này hỗ trợ thêm QA-03 bằng archive public đã tải/pin ở bước chuẩn bị riêng;
chi tiết tại [QA-03 runbook](./QA-03_JMA_HISTORICAL_BACKFILL.md).
[QA-04 — Failure/recovery](../task/tasks/QA-04.md) còn cần nghiệm thu các lỗi
ETL/ML và phục hồi, chưa được hỗ trợ bởi fixed runner này.
Không dùng kết quả của một task để đánh dấu task khác Done.

## Chuẩn bị riêng

Chạy từ thư mục gốc repository, với `.env` hợp lệ và Docker Desktop đang chạy:

```bash
make docker-build
make up-ml WAIT_TIMEOUT=600
```

`docker-build` chạy Maven verify bằng Java 17, chuẩn bị JAR/image có fingerprint.
Chỉ cần build lại khi source/build inputs thay đổi. `up-ml` chuẩn bị MinIO,
Catalog, Trino và init cần thiết; không xóa volume/bucket. Không chạy các bước
này bên trong smoke.

Nếu image/JAR đã đúng fingerprint và hai init `minio-init`/`airflow-init` đã
hoàn tất với cùng cấu hình, QA-02 chỉ cần MinIO, Catalog và Trino; không cần
bật Airflow hoặc Spark standalone. Lượt nghiệm thu cuối dùng cách này:

```bash
docker compose --profile ml up -d --no-deps --no-build --pull never --wait --wait-timeout 180 minio iceberg-rest trino
```

Lệnh trên là bước chuẩn bị tường minh, không thay init/build. Smoke vẫn kiểm
tra health, init receipts, image fingerprint và JAR; thiếu/stale thì dừng,
không tự sửa hoặc bỏ qua guard.

## Kiểm tra offline và nghiệm thu

```bash
make test-qa-02
make test-qa-acceptance
make smoke-qa-acceptance PIPELINE_QA_TASK=QA-02
```

Hai lệnh đầu không cần Docker hoặc gọi nguồn. Java safety test chỉ kiểm tra
argument/namespace/fixture columns, không khởi tạo Spark. Full Java 17
regression nằm trong bước build.

Smoke chỉ preflight read-only rồi chạy one-off client với
`--no-deps --pull never`; không build/pull/start/init và không trigger/clear
Airflow DagRun. Mỗi lần tạo namespace `<catalog>.qa02_<32 hex>` riêng,
request/matrix checksum và operation identity riêng. QA-03 dùng namespace
`<catalog>.qa03_<32 hex>`; không ghi Gold production hoặc namespace ML.

## QA-02: tám tình huống và điều kiện đạt

| Case | Kiểm tra trên output thật |
|---|---|
| `rerun_same_input` | Chạy lại cùng operation qua đủ sáu phase; logical keys/counts và sáu snapshot IDs không đổi. |
| `duplicate_same_source` | Input giống nhau từ Bronze attempt khác vẫn chỉ một current observation; canonical không tăng. |
| `usgs_late_revision` | Updated mới thắng, revision cũ vẫn audit được từ source history/Bronze. |
| `jma_release_replacement` | Release mới thay đúng slot; archive release cũ còn nguyên bytes và đọc được. |
| `overlap_usgs_jma` | Một sự kiện hai nguồn giữ bridge/lineage, không double count canonical. |
| `event_moves_month` | Resolver suy cả tháng cũ/mới; current cũ biến mất và tháng mới có đúng một record. |
| `jma_record_withdrawn` | Release mới rút record; current cũ không còn nhưng snapshot cũ vẫn đọc được. |
| `retention_outside_scope` | Tháng 08/2023 có dữ liệu dương giữ nguyên toàn bộ nội dung; snapshot cũ còn đọc được. |

Mỗi publication phải có receipt đúng chuỗi sáu phase, balanced Silver,
exact six-table bundle, commit SHA/publication ID khớp và Trino checks đầy đủ:
count, identity, uniqueness, required fields, provenance, UTC/JST, dimensions,
source counts và outside scope. Blocker phải có actual=expected=0;
không chấp nhận cờ PASS đơn lẻ, report PREPARED hay snapshot `latest`.

## QA-02: nhận biết thành công và đọc evidence

Terminal cuối phải có `"task": "QA-02"`, `"status": "PASS"`,
summary 8 passed / 0 failed / 0 pending và lease_release.status=RELEASED.
Event bắt đầu in đúng thư mục evidence của lần chạy.

Trong volume staging:
`/opt/pipeline/staging/qa-02/qa02-runtime-<uuid>/` có:

- `request.json`, `matrix.json`: identity, namespace và checksum đã pin.
- `runtime-report.json`: tám case theo contract `qa-02-report-v2`.
- `<case>-evidence.json`: before/after, exact snapshots và reconciliation.
- `<operation>/`: artifact từng phase và publication acceptance.
- `execution.json`: Java class, exit code và request SHA.
- `lease-release.json`: xác nhận release đúng owner sau quiescence.
- `acceptance.json`: nghiệm thu cuối; chỉ được ghi sau validation và release.

Raw stdout/stderr riêng có mode 0600; không commit credential/private log.
Evidence tổng hợp có thể version control nhưng không đưa data dump vào Git.
Xem [báo cáo nghiệm thu QA-02](../evidence/QA-02_RUNTIME_ACCEPTANCE.md).

## Khi thất bại

Java timeout/nonzero giữ lease vì JVM thoát không chứng minh remote query đã
dừng. Không xóa lease, bucket, warehouse, snapshot hoặc registry để chạy lại xanh.
Xác minh đúng owner, container/JVM và các query Trino; sau khi xác nhận
quiescence, người vận hành mới release đúng owner theo
[ORC-05 — Recovery/concurrency](../task/tasks/ORC-05.md).
Failed evidence phải giữ để điều tra. Không có automatic stale takeover.

Fixed suite runner hiện không có chế độ resume giữa các process. QA-03 kiểm tra
retry cùng operation sau một IOException có kiểm soát, không giả làm crash JVM.
Không chạy lại Java bằng request
cũ trên namespace đã ghi dở: baseline và Bronze timestamp có thể đã thay đổi.
Nếu hạ tầng bị dừng, chỉ sau khi xác nhận writer/query đã quiescent và đúng
owner, người vận hành mới ghi nhận abort, release đúng lease rồi chạy smoke
để tạo namespace QA mới. Giữ nguyên namespace cũ cùng mọi partial commit,
publication và failed evidence; không dùng run bị ngắt làm bằng chứng PASS.

QA-02 không thay [QA-01 — Daily ETL E2E](../task/tasks/QA-01.md):
scheduled daily run có bằng chứng riêng, không gán fixed Java QA thành
scheduled Airflow acceptance.
