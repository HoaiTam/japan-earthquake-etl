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

Không commit `target/`, JAR hoặc local metastore. Kiến trúc service, dependency,
version matrix, marker output và cách mở rộng được mô tả trong
[Spark standalone contract](../docs/specs/SPARK_STANDALONE.md).
