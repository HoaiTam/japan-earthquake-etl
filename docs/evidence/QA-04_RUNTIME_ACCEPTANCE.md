# QA-04 — Failure/recovery runtime acceptance

Trạng thái: **PASS — 10/10 cases**, Java hoàn tất `2026-10-21T07:50:50.024Z`
(14:50 ngày 21/10/2026, UTC+07); acceptance được ghi sau validation và
exact-owner lease release. Không dùng PREPARED/offline PASS thay runtime.

Branch `test/qa-04-runtime-acceptance` từ main `b383452` sau QA-03 (#92).
Assignee giữ ThanhTris; reviewer unassigned, chưa có approval độc lập.
ORC-05, ORC-07 và MLI-03 đều Done trước lượt nghiệm thu này.

## Phạm vi và implementation

Input nhỏ, tổng hợp từ fixtures; MinIO, REST Catalog, Iceberg và Trino thật.
Java/Spark thực hiện Bronze → Silver → Gold → Trino và ML Parquet/import.
Python chỉ quản lý request/process, whole-run lease và kiểm tra metadata/receipts.
Không thêm fault flags vào production CLI, không đổi contract Gold/ML.

Fixed storage runner dùng qa04-fixed-request-v1 / qa04-storage-acceptance-v1.
External runner API qa-04-runtime-v1 / qa-04-report-v1 được giữ riêng và vẫn
fail closed nếu chưa pin executable/dependencies; không thay validator cũ
bằng cờ PASS. Cách chuẩn bị/chạy và từng fault seam ở
[runbook](../runbooks/QA-04_FAILURE_RECOVERY.md).

Run: `qa04-runtime-9504f2f71b884df0abb78ce50d4e7490`.
Gold namespace: `iceberg.qa04_848e9b920dbe4e98962e872c898cb5d1`.
ML namespace: `iceberg.qa04_ml_848e9b920dbe4e98962e872c898cb5d1`.
Root: `/opt/pipeline/staging/qa-04/qa04-runtime-9504f2f71b884df0abb78ce50d4e7490/`.
Report SHA-256:
`83b1ac4b90d599b40c6d9906ff6b3139914eb3100b7f62783ff7aa4525e00c1c`.
Java exit 0; summary 10 passed / 0 failed / 0 pending, RELEASED đúng owner;
global lease trống khi hậu kiểm/export.

[Capsule JSON](./QA-04-runtime.json) giữ identities, exact 64-bit snapshot IDs
dạng chuỗi, checksum, fault/recovery pins, tám publication và ML readback.
Raw logs, Parquet, full receipts/journals vẫn ở staging/MinIO; không đưa vào Git.

## Recovery matrix đã quan sát

| Case | Fault và kết quả thực tế |
|---|---|
| fetch_timeout | UsgsHttpClient timeout thật khi HTTP loopback delay; request thứ hai cùng window trả 200/cùng bytes, recovery đến Published. Không giả public USGS outage. |
| archive_invalid | JmaBronzeWriter từ chối expected checksum sai ở attempt 1, chưa Ready. Cùng run/release, attempt 2 verify đúng; xử lý 1997-09 cùng support archive halo rồi Published. |
| silver_failure | SilverBundlePublisher chặn quality_passed=false trước output publish. Gold baselines không đổi; retry production resolver/quality từ verified Bronze đến Published. |
| gold_commit_partial | Controlled lease seam dừng sau bảng đầu commit. Có cả bảng đổi/chưa đổi; recovery giữ nguyên pin đã commit và hoàn tất bảng thiếu. |
| gold_commit_receipt_lost | Controlled store exception trước durable receipt, sau đủ sáu bảng commit. Reconcile/khôi phục receipt, không xóa dữ liệu và không commit lại các bảng. |
| trino_verify_failure | SQL wrapper lỗi trước verify query. Retry Trino thật trên cùng six-table bundle, giữ nguyên sáu pins rồi Published. |
| publication_metadata_failure | SQL wrapper lỗi trước INSERT publication_status. Chưa có row Published của operation lỗi; retry cùng bundle/pins rồi Published. Không claim lost-ACK sau INSERT. |
| ml_bundle_invalid | Checksum mismatch và _SUCCESS.schema_version mismatch tách biệt; experiment IDs đúng. Canonical journals REJECTED, snapshots không đổi, không CANDIDATE. Bundle hợp lệ dùng ID mới exp_recovered. |
| ml_import_partial | Exception sau membership commit; durable IMPORT_VALIDATING, CANDIDATE=0. Retry cùng fingerprint reuse đúng membership pin, chỉ một operation snapshot, Trino verify rồi CANDIDATE 2/1/1. |
| lease_timeout_contender | Child no-I/O do QA tạo còn sống khi wait timeout. Contender thật bị chặn; foreign release NOT_OWNER. Chỉ terminate child đó, xác nhận quiescence, giữ owner qua validation rồi mới release. |

Mỗi fault ETL có publication_rows_before_recovery=0 và gọi
DatasetManifestWriter thật: phải DS_GOLD_NOT_PUBLISHED, không tạo
dataset_manifest. Baseline đã Published vẫn phục vụ; không diễn giải thành
toàn namespace mất publication. Tám publication gồm baseline + bảy recovery,
mỗi publication có đủ sáu phase receipts, balanced Silver, six-table pins và
36 Trino checks đạt. Các snapshot trước recovery vẫn đọc được.

## Hậu kiểm Trino độc lập và retention

- publication_status: **8 rows / 8 distinct publication IDs / 8 PASSED**.
- Final pinned event_current: **3 rows** — 1997-09: 1, 2023-08: 1, 2023-09: 1.
  Bridge: **4 rows**; stable serving view: **3 rows**. Đây là canonical/lineage
  của fixture, không phải count catalog Nhật Bản thật.
- Baseline ngoài scope 2023-08 có 1 event; hash toàn nội dung trước/sau cùng
  `f17777137d6b3de988f9594022733b04cfa9a8f9551cddd1b587b6db056edd0b`.
- ML registry cuối có đúng exp_outside, exp_recovered, exp_partial, mỗi ID
  một CANDIDATE. Hai ID bị từ chối không có registry row được publish.
- ML physical totals: membership **6**, summary **3**, experiment **3**.
  Truy vấn từng experiment bằng exact result pins: **2 / 1 / 1** cho mỗi ID.
  Không dùng raw latest để phục vụ consumer.
- exp_outside giữ nguyên toàn bộ nội dung ba bảng (kể cả multiplicity),
  pinned serving snapshots cũ đọc được; ML faults không đổi sáu Gold baselines.

ML input là synthetic EXPORTED component fixture với candidate/mainshock/
registry snapshots Iceberg thật và Trino readback 2/1/1. Nó không phải official
MLD-05 export hoặc DAT-02 pilot được promote, không scientific approval.

## Kiểm thử và provenance

- Java 17 offline clean verify: **60 tests đạt, không skip**, compile/package
  runner JAR thành công. Bao gồm fixture identity/schema, multiplicity,
  namespace guard, Bronze writers, Silver quality, publication và ML import.
- Java 17 component bổ sung: **15 discovered, 14 đạt, 1 skip** do thiếu
  notebook/PyArrow harness trong build container. ResultBundleContract notebook
  probe từng đạt trên host; không skip publication gate. Tổng Java scoped
  75 discovered / 74 đạt / 1 skip; không công bố full Java regression PASS.
- `TMPDIR=/private/tmp make ci-airflow`: **402 tests đạt**, gồm fixed runner
  46 tests, QA-04 storage guards 20, external API 20 và recovery tests 11.
- `make ci-docs-base`: docs/contracts/task sync/config/Compose/lifecycle/CI
  guards đạt. Smoke không build/pull/start/init; preparation được chạy riêng.
- Host chỉ có JDK 21/25/26, không có JDK17. Lượt make test-qa-04 trên JDK26
  không được tính PASS (Hadoop getSubject và sandbox socket). Các component
  Java tương ứng đã được kiểm tra lại trong Java17 container như số liệu trên.
- Image MinIO/client/Gold QA đã build thật với source fingerprint trong capsule;
  JAR SHA-256 được đọc trực tiếp từ mount khi export.
- Hậu kiểm bắt lỗi rehash commit từ JSON đã sort keys. Guard mới đọc commit
  có thứ tự gốc trong <operation>/acceptance.json, đối chiếu nội dung với
  phase artifact đã pin receipt và final report rồi kiểm tra publication SHA.
  Có regression sorted report / missing original / changed original; toàn
  report đã lưu đạt guard mới và các truy vấn Trino độc lập ở trên.
  Evidence QA-02 và QA-03 đã lưu cũng được validate lại read-only, đều đạt
  với guard publication dùng chung; không chạy lại hoặc thay đổi dữ liệu cũ.
  controller_sha256_at_export là checksum lúc hậu kiểm, không claim source
  attestation của process đã nạp trước bản sửa guard này.
- Source/history security audit còn **7 findings cũ, 3 pending coverage**;
  không có finding mới trong file QA-04. Không coi scan này là SEC-01 Done.

## Lượt thất bại được giữ nguyên

Run `qa04-runtime-0f253c8d9175412fab3a6dd67df91547` đạt bảy ETL recovery,
nhưng dừng vì fixture schema dùng experiment ID cũ, importer trả lineage
mismatch thay schema mismatch. Không dùng lượt này làm acceptance PASS.

Giữ nguyên namespace Gold qa04_1d616ee85bfd4c908b7f02ebf127110c, ML
qa04_ml_1d616ee85bfd4c908b7f02ebf127110c và toàn bộ receipts/journals/logs.
Sau khi xác nhận không còn JVM/container/query liên quan, reconcile đủ tám
publication và chỉ exp_outside CANDIDATE, operator lưu abort/release evidence
rồi release đúng owner. Không xóa lease của người khác, không reset namespace.
Sửa fixture giữ đúng IDs, thêm unit test và chạy namespace mới như trên.

## Capacity và giới hạn

Bootstrap + Java child: **381,89 giây**, peak child RSS **1.396,91 MiB**;
local[1], driver heap 768 MiB, container limit 2 GiB. Không phải peak toàn stack
hay thời gian bao gồm hậu kiểm Python/Trino/export; fixture nhỏ không chứng
minh capacity catalog 40 năm.

- [QA-05 — Profile dữ liệu lịch sử và tài nguyên local](../task/tasks/QA-05.md):
  còn profiling mở rộng. Production chaos/outage toàn hạ tầng chưa có task riêng.
- [QA-01 — Chạy E2E daily đa nguồn](../task/tasks/QA-01.md): nghiệm thu scheduled
  deployment riêng; fixed QA-04 không trigger/clear DagRun hay thay task này.
- [MLD-05 — Validate feature snapshot và export Parquet bundle](../task/tasks/MLD-05.md),
  [MLI-02 — Tạo Airflow DAG build ML dataset](../task/tasks/MLI-02.md),
  [MLQ-01 — Chạy E2E Gold snapshot đến ML report](../task/tasks/MLQ-01.md):
  còn official export/whole-flow acceptance, không nhận Done từ synthetic ML QA.
- [SEC-01 — Review secret và bề mặt truy cập](../task/tasks/SEC-01.md): findings
  và coverage còn theo dõi riêng. Reviewer độc lập vẫn được khuyến nghị.
