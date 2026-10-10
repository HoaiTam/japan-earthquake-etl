# SLV-09 — Verified Bronze → Silver bundle → Gold handoff

## Dùng để làm gì

Xác nhận dữ liệu hai nguồn qua parser/lineage/quality/dedup/link rồi bàn giao
bốn dataset đã đọc ngược cho `GoldEventTransformer`. Đây là SilverReady của
**mẫu được pin**, không phải Gold Published hoặc ingest toàn lịch sử.

## Các thành phần

- `SilverMultiSourceIntegrationRunner`: đối soát parsed = valid + rejected,
  valid = current + duplicate + superseded; canonical theo matched/single-source;
  memberships theo current. History không được cộng vào canonical count.
- `SilverBundlePublisher`: ghi immutable bundle theo run, manifests/checksums
  của cả `source_observation`, `reject_record`, `source_link`, `canonical_membership`.
- `SilverBronzeIntegrationJob`: Java đọc exact Bronze qua MinIO SDK, chạy
  `BronzeReuseVerifier`, parse mẫu thật, publish, đọc lại Parquet và chạy Spark
  `GoldEventTransformer` từ chính bytes đã lưu.
- `silver_integration_qa.py`: chỉ control plane — lease, preflight tài nguyên,
  submit hai lần với cùng request và so sánh receipt; không parse business data.

## Input và coverage thật

Exact manifest URI, manifest SHA và raw SHA nằm trong
[`slv_09_bronze_inputs.json`](../../airflow/dags/fixtures/slv_09_bronze_inputs.json).
USGS dùng raw DAT-01 16-event trong `[2023-01-01, 2023-01-04)` UTC.
JMA dùng **2000** để tái lập handoff JMA-05, thêm **2023** làm coverage mở rộng.
Toàn manifest/raw/ZIP CRC/member/count phải được verify trước; chỉ lấy 256 dòng
vật lý đầu của từng archive (`first-n-physical-lines-v1`), không chọn theo kết quả
match và không tạo JMA giả. Native-year có thể khác UTC partition ở biên JST.

Giới hạn mỗi raw 32 MiB, USGS 10.000 records, JMA selection tối đa 512/archive.
Report tách full Bronze count, selected, parser-considered, ignored, rejects.
Không suy ra full-year Silver quality hoặc chất lượng toàn giai đoạn nghiên cứu.
Bronze chỉ được đọc; không gọi USGS/JMA để tải lại. Nếu pin chưa tồn tại ở môi
trường khác, dừng fail-closed và đối chiếu evidence nguồn; không dùng latest/wildcard.

`run_id` của bundle là run xử lý Silver; `ingest_run_id` trên observation/reject
vẫn là run đã tạo Bronze, theo CON-03. Java tạo whitelist `manifest_id → ingest
run_id` từ các manifest vừa verify, validate từng nhóm theo whitelist rồi tổng
hợp vào report Silver. Không lấy whitelist từ record, không sửa lineage để ép
bằng run Silver. Manifest chưa resolve hoặc ingest ID không khớp bị chặn.

## Publish và recovery

Layout dưới `SILVER_PREFIX` (mặc định `silver`):

```text
bundles/<run_id>/identity.sha256
bundles/<run_id>/source_observation/event_year_utc=.../event_month_utc=.../source_system=.../part-00000.parquet
bundles/<run_id>/<dataset>/manifest.json
bundles/<run_id>/reject_record/part-00000.parquet
bundles/<run_id>/source_link/part-00000.parquet
bundles/<run_id>/canonical_membership/part-00000.parquet
bundles/<run_id>/manifest.json
bundles/<run_id>/_SUCCESS
```

Dataset rỗng vẫn có Parquet đúng schema và manifest count 0; không bỏ qua
reject/link/membership rỗng. Observation rỗng dùng một file trực tiếp dưới
`source_observation` không tạo partition giả.

Quality/reconciliation phải đạt **trước mọi storage write**. Reservation
`identity.sha256` pin toàn context, exact input, selection, thời gian xử lý,
cấu hình linking, schema và file SHA/count. Ghi file + dataset manifests + bundle
manifest; readback **final objects** (bytes/SHA/schema/footer row count); cuối cùng
mới ghi `_SUCCESS` chứa SHA bundle manifest. Không có atomic rename đa object
trên S3: marker cuối là visibility gate, được bảo vệ bởi whole-run lease.

Consumer phải gọi `SilverBundlePublisher.verify(runId)`: marker hiện diện không
đủ. Hàm kiểm tra lại reservation, bundle SHA, cả bốn manifests, exact file keys,
schema SHA, bytes SHA/count trước handoff. Không scan month prefix để tìm latest.

Rerun cùng request dùng lại bundle sau fresh verification và **không ghi object**.

Serialization policy `slv-09-parquet-stable-footer-v1` được pin trong bundle.
Parquet 1.13.1 có các encoding sets dùng enum identity hash; thứ tự list trong
footer có thể đổi giữa JVM dù records không đổi. Serializer sort các list
metadata không có ý nghĩa thứ tự (encodings, encoding stats, key-value metadata)
theo mã/key trước khi chốt bytes. Không đổi pages, schema, row/column order,
offsets hoặc values. Regression chạy sáu JVM độc lập với identity-hash allocation
khác nhau; checksum phải giống nhau. Không dùng row count làm thay checksum.
Bundle QA cũ giữ nguyên, vẫn verify bytes gốc; code/policy mới cần run mới nếu
fingerprint khác. Không sửa manifest/marker để hợp thức hóa checksum khác.

Cùng run khác dữ liệu/config bị từ chối, không sửa bundle đã commit. Lỗi trước
marker để lại partial objects nhưng không Ready; cùng identity có thể resume sau
khi đã chắc writer cũ dừng. Không overwrite/xóa month partitions hoặc bundle khác.
`SilverParquetWriter` cũ được vá invalidation marker và final readback, nhưng
integration mới **không dùng mutable overwrite API đó**.

## Chạy test

```bash
make test-silver-integration
make smoke-silver-integration
```

Offline suite dùng fixture/mock/temp filesystem. In-memory S3 URI tests không
được gọi là live MinIO. Smoke cần `make up WAIT_TIMEOUT=600` đã healthy và ba
Bronze pins tồn tại; target build image mới, không restart các service đang chạy.
Smoke ghi bundle QA riêng `slv09-live-<uuid>`, không chạm Gold/Iceberg.

Nếu image runtime Spark Java17 đã có và muốn tránh tải lại dependencies khi
build Docker, package JAR mới bằng `make package-java` rồi chạy
`make smoke-silver-integration SILVER_RUNNER_JAR="$(pwd)/spark/target/japan-earthquake-etl-runner.jar"`.
Option yêu cầu đường dẫn tuyệt đối/file tồn tại, mount JAR read-only vào one-off
client; không thay JAR trong các foundation containers đang chạy. Dùng đúng
JAR runner shaded (không phải thin JAR). Wrapper vẫn kiểm tra Java17 thật,
lease, exact input, dataset readback và rerun như mode build mặc định.

Driver/executor 512 MiB, 1 core, shuffle 1; driver container 1 CPU/1 GiB. Wrapper
giới hạn 600 giây/submit; QA tắt AQE/whole-stage codegen như fixture integration
để tránh chi phí lập kế hoạch/JIT trên một core. Vẫn chạy distributed actions,
không bỏ validation/count/readback hoặc chuyển Gold logic sang Python.
Wrapper giữ source lease từ preflight đến hết cả hai submit/readback/receipt. Contender
dừng trước staging; timeout không tự release nếu cluster còn app hoặc không kiểm
tra được. Khi bị giữ lease, dùng runbook recovery ORC-05 để xác minh app đã dừng,
không xóa owner/volume/bucket nhằm bỏ qua khóa.

Scoped shared staging có request, hai receipts, report tổng và Parquet readback
cho executor; chỉ metadata nhỏ được đưa vào Git. Private submit logs không được
echo/commit. `make` in runtime metadata, không credential/payload.
Failure metadata chỉ ghi error type, scope, exit code và stdout size trong
`failure.json`/`submit_failure.json`; không ghi exception message, argv hay env.

## Handoff và giới hạn

Receipt có run/scope/input/context, bốn exact manifest URI/SHA/count, bundle SHA,
reconciliation, application ID, Java/Spark version, Gold current/canonical/bridge
counts và `gold_published=false`. Đây là integration receipt riêng của SLV-09,
**chưa phải adapter command/receipt của DAG ORC-01/02/03**.

- [GLD-03 — Ghi Gold Iceberg và commit snapshot](../task/tasks/GLD-03.md): chưa
  tạo/commit Iceberg snapshot; Gold trong smoke chỉ là transformation.
- [GLD-04 — Tạo Trino views và verification SQL](../task/tasks/GLD-04.md): chưa
  verify snapshot qua Trino, không được đổi thành Published.
- [QA-01 — Chạy E2E daily đa nguồn](../task/tasks/QA-01.md): còn ghép adapter
  run context/receipt với orchestration và kiểm thử flow đủ tới Gold Published.

Mẫu không nhất thiết có accepted link; empty link là kết quả hợp lệ. Accepted,
ambiguous, duplicate và revised cases được kiểm tra riêng bằng fixture có nhãn.
