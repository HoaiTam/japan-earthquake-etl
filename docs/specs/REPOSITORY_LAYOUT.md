# Repository layout và mount contract

| Thuộc tính | Giá trị |
|---|---|
| Task | `REP-01` |
| Trạng thái | Implemented |
| Phạm vi | Cấu trúc module, ownership, build contract và mount path |

## 1. Mục đích

Tài liệu này là contract cho các task thêm Airflow, Spark Java, Trino và Docker
Compose. Mỗi task downstream mở rộng đúng module được giao và không tự tạo một
cấu trúc hoặc mount path cạnh tranh.

Scaffold hiện đã được mở rộng bằng MinIO, Airflow, Spark và query runtime. Maven
Wrapper, root/module `pom.xml`, Java package và Spark smoke do `SPK-01` cung
cấp; Iceberg REST Catalog, Trino catalog properties và query smoke do `QRY-01`
cung cấp. `FND-01` gom các contract trên thành static checklist và full-stack
runtime smoke.

## 2. Cấu trúc chuẩn

```text
project-root/
├── README.md
├── AGENTS.md
├── .env.example
├── .mvn/wrapper/
├── compose.yaml
├── mvnw
├── mvnw.cmd
├── pom.xml
├── config/
│   └── jma/
│       └── hypocenter_archives_v1.csv
├── airflow/
│   ├── README.md
│   ├── dags/
│   │   └── README.md
│   └── tests/
│       └── README.md
├── compose/
│   ├── README.md
│   ├── airflow/
│   │   └── smoke.sh
│   ├── minio/
│   │   ├── Dockerfile
│   │   ├── init.sh
│   │   └── smoke.sh
│   ├── spark/
│   │   ├── Dockerfile
│   │   └── smoke.sh
│   └── trino/
│       └── smoke.sh
├── docs/
│   └── specs/
│       ├── COMPOSE_FOUNDATION.md
│       ├── CONFIGURATION_AND_SECRETS.md
│       ├── MINIO_STORAGE.md
│       ├── SPARK_STANDALONE.md
│       ├── JMA_ARCHIVE_INVENTORY.md
│       ├── ICEBERG_TRINO.md
│       ├── FOUNDATION_SMOKE.md
│       └── REPOSITORY_LAYOUT.md
├── scripts/
│   ├── README.md
│   ├── check-airflow.sh
│   ├── check-compose.sh
│   ├── check-config.sh
│   ├── check-foundation.sh
│   ├── check-jma-inventory.sh
│   ├── check-minio.sh
│   ├── check-query.sh
│   ├── check-repository-layout.sh
│   ├── check-spark.sh
│   ├── smoke-airflow.sh
│   ├── smoke-foundation.sh
│   ├── smoke-minio.sh
│   ├── smoke-query.sh
│   └── smoke-spark.sh
├── spark/
│   ├── README.md
│   ├── pom.xml
│   └── src/
│       ├── main/java/vn/edu/uit/ie212/earthquake/spark/
│       └── test/
│           ├── java/vn/edu/uit/ie212/earthquake/spark/
│           └── resources/fixtures/
├── tests/
│   ├── README.md
│   ├── fixtures/
│   │   └── README.md
│   └── integration/
│       └── README.md
└── trino/
    ├── README.md
    └── catalog/
        ├── README.md
        └── iceberg.properties
```

Package gốc đã được `SPK-01` chốt là
`vn.edu.uit.ie212.earthquake.spark`. Job downstream đặt dưới namespace này và
không tạo Maven module cạnh tranh.

## 3. Ownership theo module

| Đường dẫn | Nội dung được phép | Không đặt tại đây |
|---|---|---|
| `airflow/dags/` | DAG và helper chỉ phục vụ DAG | Secret, log, database Airflow |
| `airflow/tests/` | Unit/import test cho DAG | Test Spark hoặc test E2E Compose |
| `config/jma/` | Inventory source machine-readable đã review | Credential, raw ZIP hoặc metadata runtime |
| `spark/` | Maven module, Java source và unit fixture | JAR/`target/` đã build |
| `trino/catalog/` | Catalog properties không chứa secret | Password hoặc access key thật |
| `compose/` | Script init/healthcheck và asset cho service | `compose.yaml`; file này đặt tại root |
| `tests/fixtures/` | Fixture nhỏ dùng chung, có nguồn và mục đích rõ | Data dump hoặc dữ liệu runtime |
| `tests/integration/` | Test xuyên service/module | Unit test riêng của Spark/Airflow |
| `scripts/` | Script lặp lại được cho dev/CI/operations | Credential hoặc thao tác xóa rộng mặc định |

## 4. Mount contract

`CMP-01` dành trước các named volume dưới đây. Task downstream phải dùng đúng
đường dẫn hoặc cập nhật contract này trong cùng PR nếu có lý do kỹ thuật đã
được review.

| Service/consumer | Source | Container target | Mode | Ghi chú |
|---|---|---|---|---|
| Airflow API/scheduler/DAG processor | `./airflow/dags` | `/opt/airflow/dags` | Bind, read-only | DAG source duy nhất |
| Trino coordinator | `./trino/catalog` | `/etc/trino/catalog` | Bind, read-only | Chỉ catalog properties |
| Airflow task và Spark runtime | Named volume `pipeline_staging` | `/opt/pipeline/staging` | Read-write | Dữ liệu tạm trao đổi; không commit |
| Airflow components | Named volume `airflow_logs` | `/opt/airflow/logs` | Read-write | Log runtime tách khỏi DAG source |
| Airflow metadata DB | Named volume `airflow_db_data` | `/var/lib/postgresql/data` | Read-write | Chỉ metadata Airflow |
| MinIO | Named volume `minio_data` | `/data` | Read-write | Bronze, Silver và Gold warehouse |
| Iceberg Catalog | Named volume `iceberg_catalog_data` | `/home/iceberg` | Read-write | SQLite catalog state tách khỏi data lake |

Spark source là build context, không bind toàn bộ repository vào container.
`SPK-01` chốt tên JAR; image/runtime đặt artifact tại
`/opt/spark/jobs/japan-earthquake-etl.jar`. Iceberg Catalog ghi `catalog.db`
dưới `/home/iceberg`; named volume này không được dùng lại làm warehouse.
Iceberg metadata và Parquet data vẫn nằm trong warehouse MinIO.

Quy tắc chống xung đột:

1. Mọi bind source dùng đường dẫn tương đối từ project root.
2. Source code và cấu hình chỉ đọc phải mount `read-only`.
3. Không bind mount project root, `data/`, `staging/` hoặc `warehouse/` từ host.
4. Dữ liệu bền vững và staging dùng named volume khác nhau.
5. Một service không được khai báo hai mount có cùng container target.
6. Secret chỉ đi qua environment/secret mechanism; không nằm trong catalog,
   DAG hoặc source tree.

## 5. Build contract

Chạy scaffold, Maven và Spark contract check:

```bash
./scripts/check-repository-layout.sh
./scripts/check-spark.sh
```

Script layout thất bại nếu thiếu module, Maven source/test layout, tài liệu hoặc
bind source. Spark checker chạy Maven verify và xác nhận JAR
`spark/target/japan-earthquake-etl.jar`, manifest, dependency scope và Compose
runtime contract. Compose validation thuộc `CMP-01`; MinIO/Airflow/Spark/query
đều có static và runtime smoke riêng. `FND-01` chạy lại các contract riêng và
kiểm chứng chúng trong cùng một project mà không xóa service/volume.

## 6. Handoff cho task downstream

| Task | Trách nhiệm tiếp theo |
|---|---|
| `CFG-01` | Đã thêm `.env.example`, config contract và secret hygiene check |
| `CMP-01` | Đã thêm Compose network, named volumes và extension baseline |
| `MIO-01` | Đã thêm MinIO, bucket/prefix bootstrap, pipeline policy và smoke test |
| `SPK-01` | Đã thêm Maven Wrapper, POM, Java package, Hello World, standalone cluster và smoke test |
| `QRY-01` | Đã thêm Trino/Iceberg REST Catalog config, catalog state volume và query smoke |
| `FND-01` | Đã thêm full-stack health, init, network, volume/mount và startup-log checklist |
