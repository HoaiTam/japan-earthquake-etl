# GLD-03 — Gold Iceberg commit

Historical v1 giữ nguyên `COMPLETE_MONTHS`. Extension operational dùng
[component current-month riêng](../runbooks/CURRENT_MONTH_GOLD_PUBLICATION.md),
reserved namespace và persisted policy; legacy writer từ chối namespace đó.
Không dùng overload operational để gọi provisional input là full-month research.

## Input và phạm vi

`GoldCommitJob` đọc exact Silver run và bundle SHA qua `SilverBundlePublisher.verify`.
`GoldInputReader` đọc bốn manifest đã verify, stage đúng Parquet bytes cho Spark;
current/membership/link được chuyển vào GLD-01, không chạy matching lần hai.

Request cần `scope_completeness=COMPLETE_MONTHS`: caller bàn giao **toàn bộ current
state của những tháng UTC đã resolve**, kể cả tháng rỗng. Không dùng cửa sổ overlap
hoặc sample vài dòng làm replacement một tháng sản xuất. Nếu event revision chuyển
tháng, cần cả tháng cũ và mới. Upstream chưa cung cấp full-month state thì dừng;
resolver thuộc [ORC-06](../task/tasks/ORC-06.md), adapter whole daily/backfill
thuộc [ORC-07](../task/tasks/ORC-07.md), QA-01 chỉ nghiệm thu; không suy
full-month từ mẫu SLV-09. Resolver/proof ORC-06 hiện có trong
[full-month contract](./FULL_MONTH_STATE_CONTRACT.md); consume exact receipt,
không tự gắn COMPLETE_MONTHS từ overlap hoặc file-backed QA.

`affected_months` explicit, `baseline_snapshots` đủ sáu bảng (0 = chưa có snapshot).
Không infer latest khi retry. Request pin `run_context`, Silver SHA, code/config
version. Cùng operation khác data/schema/context/baseline bị reject.

## Tables và visibility

- `gold.event_current`: partition transform `months(event_time_utc)`.
- `gold.event_source_bridge`: thêm physical `event_month_utc` để overwrite đúng tháng,
  không thay logical grain; các field CON-03 vẫn giữ nguyên.
- `gold.dim_date`, `gold.dim_region`, `gold.dim_magnitude_band`, `gold.dim_depth_band`:
  union theo key; label đã tồn tại không được âm thầm đổi.
- Format version 2, Parquet, REST Catalog/MinIO dùng cùng QRY-01.
- UTC timestamps lưu Iceberg timestamptz; `event_time_jst` cast Spark timestamp_ntz
  để giữ JST wall time theo logical `timestamp_local`, không gắn UTC timezone cho JST.

Mỗi bảng có commit Iceberg atomic riêng. **Sáu bảng không atomic cùng nhau**.
Consumer nhận bundle exact table/snapshot từ `commit.json`; serving vẫn chờ GLD-04.
Writer luôn trả `COMMITTED`, `published=false`; không tạo publication PASSED/view.
Consumer không dùng raw current/latest làm Published. GLD-04 phải verify đủ bundle
rồi dùng snapshot-pinned serving views/publication.

Overwrite filter là nguyên tháng UTC, không wildcard hoặc overwrite toàn fact.
Dimension tables không có monthly scope; union giữ các key cũ ngoài scope.
Gold quality/count/unique và input ngoài scope bị chặn trước storage writes.

## Recovery

Giữ whole-run `source_run_guard` lease ORC-05 qua commit/readback. Gold job kiểm tra
exact dag/run owner trước write từng bảng. Không gọi trực tiếp ngoài control plane.

Journal nằm `SILVER_PREFIX/gold-commits/<run_id>/identity.json`, `commit.json`.
Identity hash gồm input/schema/row hashes đã sort/context/baseline, không phụ thuộc
row order. Snapshot summary pin operation/identity/baseline/expected count.

Nếu mất receipt/commit timeout: đối chiếu snapshot summary của **đúng operation**.
Bảng đã commit được reuse; bảng chưa commit chỉ ghi khi baseline còn khớp. Không
replay toàn bundle, rollback/delete metadata hoặc tự overwrite baseline mới.
`commit.json` chỉ ghi sau readback mọi snapshot và kiểm tra count/unique.
Journal readback kiểm tra bytes; rerun có receipt vẫn đọc lại snapshots đã pin.

## Build và chạy

`./mvnw --batch-mode --no-transfer-progress -pl spark -am verify`.
Package copy hai JAR Iceberg/AWS 1.10.1 vào `spark/target/iceberg/` và Docker image;
giữ ngoài shaded SDK runner. Spark load bằng `--jars`, không đổi classloader.

Entry point: `ie212.earthquake.spark.GoldCommitJob`, một argument request JSON.
Run context dùng snake_case trong `run_context`; cần `dag_id`, `namespace`,
`silver_run_id`, `silver_bundle_sha256`, `processed_at_utc`, `code_version`,
`affected_months`, `baseline_snapshots`, `scope_completeness`. Không đưa secret
vào request; catalog/warehouse/MinIO lấy environment theo QRY-01.

Smoke opt-in qua `spark-gold-qa` profile `gold`, sau khi foundation MinIO/Catalog
healthy và staging volume được init bởi Airflow. Chuẩn bị trước rồi chạy:

    make docker-build
    make docker-up WAIT_TIMEOUT=600
    make smoke-gold-storage

`make test-gold-storage` chạy offline writer/control tests; `make smoke-gold-storage`
preflight image/host JAR đã verify và chạy service trên foundation đã khởi tạo;
không build/start/init lại. Xem [Docker runtime workflow](../DOCKER_RUNTIME_WORKFLOW.md).
Runtime evidence nằm [GLD-03](../evidence/GLD-03.md).

QA chỉ tạo namespace `gold_qa_<uuid>`, capture USGS public đã có trong fixture repo,
ghi Bronze/Silver bằng production writers/readback rồi commit sáu bảng. Capture SHA
được pin trong request/receipt; đây là **pinned public capture**, không gọi nguồn
live hoặc thay identity DAT-01 gốc. Chỉ 16 USGS events, không claim JMA/full month.
Rerun cùng operation phải giữ exact snapshots, counts và commit receipt.

Local QA dùng Spark `local[1]`/Java17, 512 MiB heap; đây không phải benchmark standalone
hoặc full historical. Private stderr nằm scoped staging, không echo/commit.
Timeout giữ lease, cần xác nhận JVM đã dừng trước recovery; không auto-delete owner.

## Kiểm thử và giới hạn

`GoldIcebergWriterTest` ghi/readback snapshot Iceberg thật trên local filesystem:
retry không tạo snapshot mới, partial commit resume, stale baseline/config conflict,
revision, empty month, outside-month retention, old-snapshot time travel, quality/scope
blocked trước write. Windows dùng test-only NIO mkdir/permission adapter cho NTFS;
Linux và production S3 không đổi permission behavior. Không tải winutils binary.

- GLD-04 — Tạo Trino views và verification SQL: Trino blockers/publication còn cần task này.
- MLD-01 — Pin Gold snapshot và tạo dataset manifest: cần Published từ GLD-04.
- [ORC-06 — Full-month Silver state](../task/tasks/ORC-06.md): resolver và coverage proof đủ tháng.
- [ORC-07 — Real ETL adapters](../task/tasks/ORC-07.md): orchestration receipt/lease adapter daily/backfill.
- QA-01 — Chạy E2E daily đa nguồn: nghiệm thu hai task trên, không coi bounded capture là toàn lịch sử.

API write/time travel đối chiếu [Iceberg Spark writes 1.10.1](https://iceberg.apache.org/docs/1.10.1/spark-writes/)
và [Spark queries 1.10.1](https://iceberg.apache.org/docs/1.10.1/spark-queries/).
