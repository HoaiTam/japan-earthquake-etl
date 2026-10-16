# ORC-06 — Vận hành resolver state đủ tháng

## 1. Chuẩn bị đúng một lần

Đọc [contract](../specs/FULL_MONTH_STATE_CONTRACT.md). Cần JDK17/21, Python3,
Maven wrapper và curl (nếu tải public capture). Kiểm tra JAVA_HOME của máy:

```bash
java -version
make prepare-full-month
```

`prepare-full-month` package JAR/classpath host và fingerprint dependency,
không start/build Docker. Bước chuẩn bị skip test có chủ đích, **không phải
nghiệm thu**; chạy tests riêng bên dưới. Khi Java code/dependency đổi phải
prepare lại. Lệnh test/smoke không tự build, pull, start, init hoặc đổi .env.

## 2. Test offline theo task

```bash
make test-full-month
make test-ci-workflow
make check-task-status check-week-5-plan test-week-5-plan
```

Thành công: unittest `OK`, Java `BUILD SUCCESS`, 0 failure/error/skip. Các ca
bootstrap, baseline+delta, moved event, JMA snapshot replacement, empty month,
gap/missing source, stale baseline, source pin conflict, cross-month closure,
partial-write retry và GoldInputReader được kiểm tra bằng fixture nhỏ.
Native record fixtures là synthetic, không được ghi là live coverage.

CI có check `Java / orc-06-full-month`; local chạy tương đương:

```bash
CI_COMPONENT=java CI_SUITE=orc-06-full-month make ci-suite
```

Nếu PR plan chọn none cho nhóm này, dùng `--force-full` khi chủ động test
ngoài scope. `ci-suite` mặc định không PR context chạy đầy đủ **nhóm đó**,
không phải toàn repository. Không chạy hai Maven commands cùng checkout.

## 3. Nghiệm thu dữ liệu thật có phạm vi, không cần Docker

```bash
make qa-full-month-live FULL_MONTH_ROOT=staging/orc06-full-month
```

Target `qa-*` là QA host, không thuộc `smoke-*` container/preflight Docker.
Opt-in này tải USGS tháng 09/2023 + halo 16s (không magnitude/depth filter),
JMA h2023.zip nguyên bản (<=32MiB/file), rồi dùng production Bronze writers,
resolver/Silver publisher và Spark GoldInputReader trên **file-backed QA**.
JMA được quét toàn member, parse đầy đủ tháng/halo, không first-N hay random
sample. Không tải 40 năm, không MinIO/Airflow/Gold commits hoặc publication.
USGS query chạm limit, thiếu source proof hoặc parser reject thì fail.

Thành công terminal:

```text
event=full_month_state_passed action=live evidence_root=...
```

Kiểm tra kết quả:

```bash
jq '{task, backend, rerun_identical, gold_handoff_verified, gold_published,
     receipt: (.receipt | {status,scope_completeness,affected_months,reconciliation})}' \
  staging/orc06-full-month/qa/acceptance.json
```

Phải thấy backend `file-backed-public-capture-QA`, status `FullMonthReady`,
scope `COMPLETE_MONTHS`, rerun/handoff true, balanced true, gold_published false.
Chạy lại cùng lệnh dùng capture/request đã có, kiểm tra reuse; có thể khác máy
về root URI/SHA bundle, không so SHA giữa hai root khác nhau.

```text
staging/orc06-full-month/
  captures/usgs-2023-09.geojson   # raw API response, không commit Git
  captures/jma-2023.zip          # full raw archive, không commit Git
  java-run.log                  # scoped Java diagnostics, không đưa cả log vào PR
  qa/request.json               # exact public capture pins + timestamps cố định
  qa/acceptance.json            # receipt, readback/rerun + Gold transform counts
  qa/bronze/...                 # original bytes + manifests của production writers
  qa/silver/bundles/<run>/...    # four datasets + manifests + marker
  qa/silver/full-month/<run>/... # identity/registry/receipt/marker
  qa/gold-readback/...           # local staged Parquet, không phải Gold warehouse
```

TLS/download fail: không tắt certificate verification. Curl exit code được
in, raw stderr không echo. Giữ `.partial` để kiểm tra; muốn thử lại an toàn
dùng `FULL_MONTH_ROOT` mới hoặc tự xử lý đúng file partial, không xóa staging
toàn bộ. Root không được là staging chung hoặc symlink. Capture hiện có
không được tự tải đè. Nếu root cũ thuộc implementation/config khác, tạo
root/request version mới; không sửa marker/SHA để ép reuse.

## 4. Replay request local riêng

```bash
make full-month-local \
  FULL_MONTH_REQUEST=staging/orc06-full-month/qa/request.json \
  FULL_MONTH_ROOT=staging/orc06-full-month/qa
```

Lệnh chỉ đọc exact file-backed Bronze inputs, ghi immutable Silver state
trong root này. Không download/compile/start. Local mode chỉ nhận Gold
baselines zero, không chứng nhận stale snapshot của catalog thật. Main
resolver có actual snapshot probe ở production và test Iceberg local riêng.

## 5. Handoff MinIO cho ORC-07

Production entrypoint đã có: `ie212.earthquake.spark.FullMonthStateJob <request.json>`.
Dùng Spark runtime đã chuẩn bị với JAR, Iceberg runtime và catalog/config
QRY-01 hiện tại. Không lấy `qa/request.json` file-backed để gửi thẳng vào
MinIO: phải resolve exact **S3** manifests và đúng namespace/baseline snapshots.
Template request là [config example](../../config/orchestration/orc06-request.example.json),
zero SHA/replace-* là placeholder, không chạy nguyên trạng.

Trước gọi production job, adapter ORC-07 phải giữ exact shared dag/run lease.
Job chỉ assert owner, không tự acquire/release/takeover. Không dùng arbitrary
command từ dag_run.conf. Catalog probe chỉ so expected current snapshots,
không cập nhật request sang latest. Nếu thiếu owner/config/input thì fail closed.

Sau khi production resolver chạy, kiểm tra trong MinIO **Silver prefix**:

- `bundles/<run>/manifest.json`, `_SUCCESS`, bốn dataset manifests/Parquet.
- `full-month/<run>/identity.sha256`, `registry.json`, `receipt.json`, `_SUCCESS`.
- SHA marker khớp bytes receipt; registry SHA và Silver bundle SHA khớp receipt.
- affected months có cả cũ/mới; coverage đủ từng nguồn + halo; empty tháng
  có canonical count 0 explicit, không chỉ thiếu file.
- Receipt không có Published/approved flag; `gold_request` pin exact bundle
  và đủ sáu baseline snapshots cho GLD-03.

Nếu đã có Gold: pin thêm commit journal/SHA trỏ cùng Silver registry/bundle
và cùng namespace/snapshot IDs. After Gold commit thành công, ORC-07 lưu
liên kết registry → commit; không chỉnh registry cũ để gắn snapshot mới.
Receipt/marker chỉ là byte consistency, không thay fresh resolver/probe/lease.

**Chưa có make target/DAG production toàn chuỗi trong ORC-06.**
[ORC-07 — Kết nối real ETL adapters đến Gold Published](../task/tasks/ORC-07.md)
sở hữu wiring/run context/lease và sáu commits → Trino → publish;
[QA-01 — Chạy E2E daily đa nguồn](../task/tasks/QA-01.md) nghiệm thu MinIO/Airflow
toàn chuỗi. Không đưa lệnh placeholder giả đã chạy cho hai task chưa hoàn tất.

Một tháng QA không chứng minh full research coverage; historical rollout vẫn
thuộc [QA-03](../task/tasks/QA-03.md). Không ghi nhãn nghiên cứu hoặc ML EXPORTED
từ receipt FullMonthReady.
