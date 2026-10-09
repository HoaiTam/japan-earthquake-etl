# Spark Java module

Module Maven cho các job Spark Java của pipeline. `SPK-01` đã chốt Spark
`3.5.9`, Scala `2.12`, Java `17`, package
`ie212.earthquake.spark` và JAR
`spark/target/japan-earthquake-etl.jar`.

```text
spark/
├── pom.xml
└── src/
    ├── main/java/              # Java production source
    └── test/
        ├── java/               # Unit tests
        └── resources/fixtures/ # Fixture riêng cho Spark tests
```

Build và chạy static gate từ project root:

```bash
./mvnw --batch-mode --no-transfer-progress clean verify
./scripts/check-spark.sh
```

Chạy acceptance trên Spark standalone bằng Docker:

```bash
./scripts/smoke-spark.sh
```

`HelloWorldJob` tạo range `[0, 10)`, xác nhận `record_count=10` và `id_sum=45`.
Runtime smoke chỉ thành công khi worker ở trạng thái `ALIVE`, executor chạy trên
worker và `spark-submit` trả exit code `0`.

`USG-01` đặt request config/planner dưới package
`ie212.earthquake.spark.usgs`. Planner chỉ tạo URI và các cửa sổ UTC
cho daily/backfill, không gọi mạng; contract chi tiết nằm ở
[USGS request contract](../docs/specs/USGS_REQUEST_CONTRACT.md). Unit test có thể
chạy độc lập bằng `./mvnw --batch-mode --no-transfer-progress -pl spark -am test`.
HTTP timeout/retry, size guard và pagination nằm trong [USGS HTTP client contract](../docs/specs/USGS_HTTP_CLIENT_CONTRACT.md).
GeoJSON validation, immutable raw write và manifest nằm trong [USGS Bronze writer contract](../docs/specs/USGS_BRONZE_WRITER_CONTRACT.md).

`USG-06` thêm `UsgsIngestRunner` và `MinioBronzeObjectStore`. Maven Shade tạo
executable `spark/target/japan-earthquake-etl-runner.jar`; custom Airflow image
đặt JAR này sau `/opt/pipeline/bin/usgs-ingest-runner`. Runner xử lý bốn phase
`fetch/validate/upload/verify`, đóng MinIO client sau mỗi phase và chỉ xuất
metadata JSON. Static/live commands cùng evidence nằm trong
[USGS live Bronze runbook](../docs/specs/USGS_LIVE_BRONZE_RUNBOOK.md).

`JMA-02` thêm package `ie212.earthquake.spark.jma` để đọc inventory CSV,
preflight HEAD, tải có giới hạn đồng thời, resume qua HTTP Range, lưu
metadata/SHA-256 và giữ release cũ khi archive đổi. ZIP/member validation
thuộc `JMA-03`.

`JMA-03` thêm `JmaArchiveValidator` và `JmaBronzeWriter`: kiểm tra ZIP/member,
CRC/size, record 96 byte; lưu ZIP nguyên bản và manifest CON-02 bất biến sau
readback SHA-256. File hỏng đi vào quarantine, không `BronzeReady`; không
normalize/filter event tại Bronze. Chạy riêng bằng `make test-jma-bronze`.
Thành phần, metadata, retry/revision và handoff JMA-02 → SLV-01 nằm trong
[JMA Bronze writer contract](../docs/specs/JMA_BRONZE_WRITER_CONTRACT.md).

`JMA-04` thêm `JmaYearIngestRunner` vào cùng shaded JAR, gọi qua
`/opt/pipeline/bin/jma-ingest-runner --context-file <path>`. Một process ingest
một exact year/segment bằng downloader/writer, giữ metadata GET thật và
verify publication trước trả Ready. Reuse pointer theo SHA kiểm tra lại raw
và manifest, không nhân bản raw cho run mới khi cache còn hợp lệ. Chạy riêng
`make test-jma-backfill`; protocol và cache/recovery boundary ở
[JMA year backfill](../docs/specs/JMA_YEAR_BACKFILL.md).

`JMA-05` thêm `JmaBronzeQaVerifier` trong shaded runner JAR: read-only exact
MinIO raw/manifest, ZIP/count/SHA/release/interval/provenance, DAT-01 và
baseline trước rerun. Report chỉ metadata, không sửa object/catalog. Offline
`make test-jma-qa`, live `make smoke-jma-live`; tham số CLI/report và scope ở
[JMA Bronze QA](../docs/specs/JMA_BRONZE_QA.md).

`SLV-01` thêm package `ie212.earthquake.spark.silver` để resolve đúng một
manifest BronzeReady theo run/source, kiểm tra validation flags, raw object
length/SHA-256 và stage deterministic. Resolver không quét wildcard hoặc chọn
object latest mơ hồ; adapter hỗ trợ local path và BronzeObjectStore MinIO/S3.

`SLV-02` triển khai `UsgsGeoJsonParser`, `SilverObservation`, `SilverRejectRecord`,
`SilverSchemas`, `UsgsParseContext` và `UsgsParseResult` dưới package
`ie212.earthquake.spark.silver`. Parser ánh xạ GeoJSON FeatureCollection sang
schema Silver `1.0` (CON-03), chuẩn hóa UTC/JST (`Asia/Tokyo`), envelope kỹ thuật
`[20.0, 50.0]` x `[120.0, 155.0]`, coordinates, depth, magnitude, alert/tsunami và
source updated timestamp. Parser giữ nguyên chính sách null (không ép null thành 0),
phân loại reject record với reason code rõ ràng (`MISSING_SOURCE_KEY`, `INVALID_EVENT_TIME`,
`INVALID_LATITUDE`, `INVALID_LONGITUDE`, `INVALID_NUMBER`), và giữ negative depth kèm cờ
cảnh báo `NEGATIVE_DEPTH`.

`SLV-04` thêm `SourceKeyGenerator` và `SilverLineage` để version hóa thuật toán
tạo khóa và liên kết lineage đa nguồn. USGS dùng feature `id`; JMA dùng thuật toán
version hóa `jma_k1` từ các cột hypocenter chính thức (agency, origin time JST, tọa độ
độ/phút), bảo đảm khóa giữ nguyên khi rerun hoặc qua revision cập nhật depth/magnitude.
`source_revision_key` phản ánh catalog release/update timestamp và raw hash;
`source_observation_id` tuân thủ công thức `obs_` + SHA-256. `canonical_event_id`
tuân thủ nghiêm ngặt nguyên tắc tạo ID opaque (`evt_` + SHA-256) từ stable source identity,
tuyệt đối không dùng time/tọa độ. `SilverLineage` cung cấp cơ chế audit và kiểm chứng
ngược về raw record locator/hash và raw object SHA-256 từ Bronze.

`SLV-03` thêm `JmaFixedWidthParser`, `JmaParseContext`, `JmaParseResult`,
`JmaCodeMapping` và `JmaNativeFields`: parse đúng cột 96 **byte**, JST → UTC,
degree/minute, depth, hai magnitude/type, intensity/tsunami/region/agency/flags
và catalog release. Dùng model/schema/key chung, giữ nullable values,
duplicate/revision và native audit fields; không tự filter/dedup/link.
`make test-jma-parser` chạy riêng tests offline, gồm Bronze/resolver → parser
→ quality → Parquet readback. API, biên cột, code mapping và giới hạn nằm trong
[JMA Silver parser](../docs/specs/JMA_SILVER_PARSER.md).

`SLV-05` dùng trực tiếp model chung của SLV-02 để validate, đối soát
valid/rejected/parsed và xuất quality summary đầy đủ Bronze lineage. Overload
nhận `UsgsParseResult` hoặc `JmaParseResult` bao gồm parser rejects trong run gate; overload
`SilverParquetWriter.write(request, quality)` chặn storage writes khi gate fail
hoặc request không khớp run/datasets đã validate. Cách dùng, JMA flag mapping,
giới hạn của API persistence thấp tầng và regression tests nằm trong
[Silver quality validation](../docs/specs/SILVER_QUALITY_VALIDATION.md).

`SLV-08` triển khai `SilverPartitionKey`, `SilverStorageLayout`, `SilverPartitionManifest`,
`SilverObjectStore`, `FileSilverObjectStore`, `MinioSilverObjectStore`, `SilverParquetSerializer`,
`SilverParquetWriter`, `SilverWriteRequest` và `SilverWriteResult`. Module tổ chức partition theo
đúng thứ bậc `source_observation/event_year_utc=YYYY/event_month_utc=MM/source_system=<source_system>`
dựa trên `event_time_utc` (UTC). Áp dụng quy trình staging nguyên tử (`_staging/<run_id>/...`),
đối soát checksum SHA-256 và row count trước khi promote vào partition chính thức; đảm bảo retry/rerun
ghi đè sạch sẽ không append duplicate record. Tự động xuất marker `_SUCCESS` và `manifest.json`
ghi nhận metadata partition, danh sách file, SHA-256, số dòng và summary chất lượng dữ liệu.

`GLD-01` thêm Spark DataFrame transformation trong package `gold`: current event,
source bridge, natural/ROI view và dimensions/bands. API dùng canonical membership
đã resolve từ Silver; không tạo canonical ID lại. Input/test/handoff nằm tại
[Gold transformation](../docs/specs/GOLD_TRANSFORMATION.md).

Không commit `target/`, JAR hoặc local metastore. Kiến trúc service, dependency,
version matrix, marker output và cách mở rộng được mô tả trong
[Spark standalone contract](../docs/specs/SPARK_STANDALONE.md).
