# GLD-03 — Gold Iceberg commit

## Input và phạm vi

`GoldCommitJob` đọc exact Silver run và bundle SHA qua `SilverBundlePublisher.verify`.
`GoldInputReader` đọc bốn manifest đã verify, stage đúng Parquet bytes cho Spark;
current/membership/link được chuyển vào GLD-01, không chạy matching lần hai.

Request cần `scope_completeness=COMPLETE_MONTHS`: caller bàn giao **toàn bộ current
state của những tháng UTC đã resolve**, kể cả tháng rỗng. Không dùng cửa sổ overlap
hoặc sample vài dòng làm replacement một tháng sản xuất. Nếu event revision chuyển
tháng, cần cả tháng cũ và mới. Upstream chưa cung cấp full-month state thì dừng;
adapter whole daily/backfill thuộc QA-01, không suy full-month từ mẫu SLV-09.

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
healthy và staging volume được init bởi Airflow. Build JAR trước rồi:

    docker compose --env-file .env build spark-gold-qa
    docker compose --env-file .env run --rm --no-deps spark-gold-qa

`make test-gold-storage` chạy offline writer/control tests; `make smoke-gold-storage`
build/verify JAR, build QA image và chạy service trên foundation đã khởi tạo.
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
- QA-01 — Chạy E2E daily đa nguồn: full-month resolver và orchestration receipt/lease adapter
  cho daily/backfill sản xuất, không coi bounded capture là toàn lịch sử.

API write/time travel đối chiếu [Iceberg Spark writes 1.10.1](https://iceberg.apache.org/docs/1.10.1/spark-writes/)
và [Spark queries 1.10.1](https://iceberg.apache.org/docs/1.10.1/spark-queries/).
