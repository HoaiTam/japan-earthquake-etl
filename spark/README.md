# Spark Java module

Module Maven cho các job Spark Java của pipeline. `SPK-01` đã chốt Spark
`3.5.9`, Scala `2.12`, Java `17`, package
`vn.edu.uit.ie212.earthquake.spark` và JAR
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

Không commit `target/`, JAR hoặc local metastore. Kiến trúc service, dependency,
version matrix, marker output và cách mở rộng được mô tả trong
[Spark standalone contract](../docs/specs/SPARK_STANDALONE.md).
