# QA-04 — Failure và recovery theo tầng

## Làm gì và dùng để làm gì

Fixed runner kiểm tra 10 case của qa_04_failure_recovery_matrix.json trên
MinIO/REST Catalog/Iceberg/Trino thật. Input nhỏ, tổng hợp; Java thực hiện
ETL/Parquet/ML import, Python chỉ quản lý process, metadata, receipts và lease.
Không thay nghiệm thu scheduled Airflow của QA-01 hoặc official ML E2E của MLQ-01.

Hard dependencies ORC-05, ORC-07 và MLI-03 đã Done.
Runner tạo namespace mới <catalog>.qa04_<32hex> cho ETL và
<catalog>.qa04_ml_<32hex> cho ML; không ghi production gold/ml.
EXPORTED trong ML QA là synthetic component fixture, không phải DAT-02 pilot
được promote hoặc official dataset do MLD-05 export.

## Chuẩn bị và chạy

Từ repo root, .env hợp lệ, Docker Desktop đang chạy:

```bash
make docker-build
make up-ml WAIT_TIMEOUT=600
```

Đây là bước chuẩn bị riêng khi image/JAR/config chưa đúng fingerprint.
Không chạy build/start/init trong smoke. Nếu artifact đã được chuẩn bị và
init đã hoàn tất cùng cấu hình, chỉ cần MinIO/Catalog/Trino healthy theo
[runbook chung](./PIPELINE_QA_ACCEPTANCE.md); không cần Airflow/Spark standalone.

Dùng Java 17 cho các test Java/Spark, không coi lỗi host JDK mới là lỗi pipeline:

```bash
make test-qa-04
make smoke-qa-acceptance PIPELINE_QA_TASK=QA-04
```

test-qa-04 gồm fixed-runner guards, matrix/recovery control tests và Java
component gates. Smoke chỉ preflight rồi chạy one-off --no-deps --pull never;
không build/pull/start/init, không clear/mark-success/trigger DagRun.
PipelineQaSupport giữ whole-run exact-owner source lease qua cả ETL và ML.

Thành công phải có task=QA-04, status=PASS, summary 10 passed / 0 failed /
0 pending và lease_release.status=RELEASED. Chỉ publication rời hoặc
JAVA_PASSED_LEASE_PENDING chưa đủ; acceptance.json được ghi cuối sau validation
và release đúng owner. Không fallback sang fixture/external command khi lỗi.

## Mười tình huống và recovery boundary

| Case | Fault được thực thi | Recovery và bằng chứng |
|---|---|---|
| fetch_timeout | UsgsHttpClient gọi HTTP loopback có delay, timeout thật; không giả API public bị outage | Cùng request/window nhận 200 và cùng fixture bytes; sau đó exact Bronze → Published. |
| archive_invalid | JmaBronzeWriter từ chối SHA sai, attempt 1 không Ready | Cùng run/release, attempt 2 đúng checksum được verify; xử lý đúng 1997-09 và support archive halo. |
| silver_failure | SilverBundlePublisher chặn quality_passed=false trước publish | Reuse verified Bronze; chạy production resolver/quality cùng scope rồi Published. |
| gold_commit_partial | QA seam gây lỗi khi đã có bảng đầu commit và còn bảng chưa commit | Reconcile operation fingerprint; các snapshot đã commit giữ nguyên, chỉ hoàn tất bảng thiếu. |
| gold_commit_receipt_lost | Sáu bảng đã commit; QA store gây lỗi trước khi ghi durable commit receipt | Khôi phục receipt bằng snapshot identity/readback; không xóa receipt hoặc reappend sáu bảng. |
| trino_verify_failure | QA SQL wrapper gây lỗi trước query verify | Retry verify trên cùng committed bundle bằng Trino thật; không commit lại. |
| publication_metadata_failure | QA SQL wrapper lỗi trước INSERT publication status | Retry cùng bundle; không có row Published cho operation lỗi, views tạm vẫn gated. Không mô phỏng mất ACK sau INSERT. |
| ml_bundle_invalid | Hai lỗi tách biệt: checksum sai và _SUCCESS.schema_version sai; IDs còn đúng | Canonical journals REJECTED, không CANDIDATE/result writes; bundle hợp lệ dùng experiment ID mới. Physical Parquet schema có component tests riêng. |
| ml_import_partial | Writer test seam lỗi sau membership commit | IMPORT_VALIDATING, không CANDIDATE; retry cùng fingerprint reuse membership snapshot, Trino verify rồi CANDIDATE 2/1/1. |
| lease_timeout_contender | Python child no-I/O còn sống khi wait timeout; contender thật bị global lease chặn, foreign release NOT_OWNER | Chỉ terminate child do chính QA tạo, xác nhận quiescence; giữ owner đến final validation/release. Không kill JVM/service hoặc giả đã quiesce remote writer. |

Mọi ETL case đều gọi DatasetManifestWriter thật trên publication chưa có:
phải DS_GOLD_NOT_PUBLISHED và chưa tạo dataset_manifest. Đây là kiểm tra gate,
không chạy official build/export. Baseline đã Published vẫn được phục vụ;
published_before_recovery=false nói về operation đang lỗi, không nói cả namespace.

## Retention, snapshots và counts

Baseline Gold tháng 08/2023 có dữ liệu dương. Recovery của mỗi case đối chiếu
toàn nội dung partition ngoài scope (giữ cả multiplicity), đọc lại snapshots
cũ và chạy Trino outside/current/bridge checks của production publication.

ML có baseline exp_outside đã CANDIDATE 2 memberships / 1 summary / 1 registry
row trong cùng namespace. So sánh toàn bộ nội dung ba bảng cho experiment đó
trước/sau, giữ duplicate multiplicity, và đọc lại pinned serving bundle.
Các lỗi checksum/schema không đổi snapshot; partial import có raw rows nhưng
consumer chỉ đọc CANDIDATE registry + exact result pins, không raw latest.

Python hậu kiểm đủ tám Gold publication (baseline + bảy recovery), sáu phase
receipts, artifact SHA, exact six-table bundles, balanced Silver, Trino gates,
fault/recovery context, partial snapshots và ML journals/receipt metadata.
ML faults phải giữ nguyên Gold baselines. Một cờ PASS hoặc report offline không đủ.

GoldPublisher dùng SHA của commit bytes có thứ tự khóa gốc; phase artifact
được canonicalize và final report có thể sort keys. Hậu kiểm phải giữ
<operation>/acceptance.json nguyên bản Java, đối chiếu commit với phase
artifact đã pin receipt và final report rồi mới tính publication SHA.
Không sort/rewrite evidence đó để làm khớp SHA.

## Evidence và hai giao diện

Event bắt đầu in root /opt/pipeline/staging/qa-04/qa04-runtime-<uuid>/:

- request.json, matrix.json: namespace/operation identity và checksum.
- <operation>/: sáu phase artifacts và publication acceptance.
- <case>-fault.json: stage/reason, context, baselines, partial pins, dataset gate.
- ml/: synthetic bundles, trusted registry pins, production import journals và
  ml-evidence.json. Không đưa Parquet/raw bundles hoặc private log vào Git.
- java-report.json: evidence Java nguyên bản, chờ lease case.
- lease-timeout-evidence.json: contender/quiescence của child do QA tạo.
- runtime-report.json: qa04-storage-acceptance-v1, đủ 10 case.
- execution.json, lease-release.json, acceptance.json: JVM exit/capacity,
  exact-owner release và final acceptance. Raw logs mode 0600.

Fixed request dùng qa04-fixed-request-v1. Nó khác **external runner API** cũ
qa-04-runtime-v1 / qa-04-report-v1 của qa_04_failure_recovery.py và
make smoke-qa-04. External API vẫn giữ nguyên runner executable SHA pin,
request SHA, snapshot/readback/evidence guards; không hạ validator để nhận
fixed storage report. External deployment mặc định fail closed, operator phải
đặt ORC_07_STATUS=Done và MLI_03_STATUS=Done khi đã nghiệm thu thật.
Dùng smoke-qa-acceptance PIPELINE_QA_TASK=QA-04 cho implementation ở đây.

make test-qa-04 có thể tái tạo PREPARED evidence và offline_control PASS.
Các trạng thái đó không phải storage runtime acceptance.

[Nghiệm thu runtime 21/10/2026](../evidence/QA-04_RUNTIME_ACCEPTANCE.md):
10/10 cases và Trino readback độc lập; capsule không chứa raw data/secret.

## Khi có lỗi thật

Nonzero/timeout Java giữ lease: JVM exit không chứng minh query remote đã dừng.
Không auto stale takeover, reset namespace, xóa bucket/volume/warehouse/metadata.
Operator phải kiểm tra exact owner, container/JVM và query Trino đã quiescent,
reconcile partial outputs rồi lưu abort/release evidence; release bằng API lease
đúng identity, không rm owner.json hoặc release owner của run khác.
Chạy suite mới trên namespace mới sau abort; không replay toàn suite bằng request
cũ có baseline đã đổi. Retry trong cùng case/operation đã được fixed runner pin.

## Giới hạn / handoff

Controlled exceptions/hooks không thay full chaos testing hoặc sự cố mạng thật.
Không dùng capacity fixture nhỏ để bảo đảm toàn catalog 40 năm.
[QA-05 — Profile dữ liệu lịch sử và tài nguyên local](../task/tasks/QA-05.md)
còn profiling mở rộng; full production chaos/outage acceptance chưa có task riêng.
[MLD-05 — Validate feature snapshot/export](../task/tasks/MLD-05.md),
[MLI-02 — Build ML dataset DAG](../task/tasks/MLI-02.md) và
[MLQ-01 — E2E Gold snapshot đến ML report](../task/tasks/MLQ-01.md)
còn official export/whole-flow acceptance; QA-04 không tự hoàn tất các task đó.
[SEC-01 — Review secret và bề mặt truy cập](../task/tasks/SEC-01.md)
còn findings/coverage riêng. Reviewer độc lập vẫn được khuyến nghị, không tự ghi approval.
