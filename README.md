# Japan Earthquake ETL

Nền tảng ETL local-first để thu thập dữ liệu cập nhật hằng ngày từ USGS và
lịch sử 40 năm từ JMA, xử lý bằng Spark Java theo mô hình Bronze–Silver–Gold,
công bố bảng Gold qua Iceberg/Trino, rồi tạo dataset và so sánh
Window/DBSCAN/HDBSCAN theo từng mainshock để nhập kết quả đã kiểm tra vào
Iceberg `ml.*`. Static report là output Core; Power BI là phần Stretch tùy chọn.

Phạm vi, KPI và Definition of Done hiện hành nằm tại
[baseline PLN-01](./docs/specs/MVP_SCOPE_KPI_AND_DOD.md).

> Trạng thái hiện tại: **foundation đã có full-stack smoke checklist**.
> MinIO bucket bootstrap, Airflow local runtime, Spark Java build/runtime,
> Iceberg REST Catalog và Trino đã được triển khai và kiểm tra cùng nhau.
> ORC-01 đã có khung DAG ETL/phase gates và tests mock; chưa có daily ETL thật
> đến Gold Published (GLD-03/04 và QA-01 chưa hoàn tất).
> SLV-09 đã nghiệm thu Bronze thật USGS/JMA → immutable Silver → Gold
> transformation/handoff và rerun trên mẫu bounded, chưa commit Gold.
> Cách chạy và phạm vi ở [Silver integration](./docs/specs/SILVER_INTEGRATION.md).

Source coverage USGS/JMA, vùng nghiên cứu, timezone và overlap được chốt tại
[CON-01 source coverage contract](./docs/specs/SOURCE_COVERAGE.md). Kiểm tra
contract không cần mạng bằng:

```bash
./scripts/check-source-coverage.sh
```

Request daily/backfill USGS, bounding box, seed `2023-01-01`, revision overlap
và quy tắc chia chunk được chốt tại [USGS request contract](./docs/specs/USGS_REQUEST_CONTRACT.md).
Builder Java của `USG-01` chỉ lập kế hoạch request, không gọi mạng; `USG-02`
tiếp nhận plan để thực hiện HTTP client, retry và pagination theo [USGS HTTP
client contract](./docs/specs/USGS_HTTP_CLIENT_CONTRACT.md).

USG-03 kiểm tra envelope GeoJSON, giữ raw bytes bất biến, đọc lại checksum và
tạo manifest `BronzeReady`; payload lỗi được lưu ở `_quarantine` theo [USGS
Bronze writer contract](./docs/specs/USGS_BRONZE_WRITER_CONTRACT.md).

USG-04 thêm DAG `usg_04_usgs_ingest` với task group resolve/fetch/validate/upload/
verify, publish gate và run summary. DAG truyền cùng logical window qua retry và
chỉ mở đường cho Silver sau khi Bronze đã verify theo [USGS Airflow ingest
contract](./docs/specs/USGS_AIRFLOW_INGEST_CONTRACT.md).

ORC-01 thêm DAG manual `orc_01_etl_pipeline`: sáu task groups, exact
manifest/SHA/partition/snapshot scope và cổng Trino trước publication. Team
test offline bằng `make test-orchestration`, `make etl-preview`, `make etl-mock`;
mock chỉ trả `MockComplete`, không publish Gold. Thành phần và handoff adapter ở
[ETL orchestration contract](./docs/specs/ETL_ORCHESTRATION_CONTRACT.md).

ORC-03 thêm DAG `orc_03_backfill` để preview USGS UTC chunks/JMA year-segment,
reuse exact Bronze release và bàn giao affected-scope reprocessing. Chạy
`make backfill-preview`, `make test-backfill`; real Silver/Gold vẫn fail closed
khi chưa có scoped adapter. Xem [backfill/reprocessing runbook](./docs/specs/BACKFILL_AND_REPROCESSING.md).

ORC-05 giới hạn local concurrency/heap, chốt retry boundary và recovery từng
tầng. `make test-recovery`, `make smoke-recovery`, `make smoke-resource-pilot`
kiểm tra policy/failure/readback/actual Spark probe mà không ghi lake hoặc
download lại nguồn. Xem [profile và operator notes](./docs/specs/RECOVERY_AND_RESOURCES.md).

USG-05 dùng fixture và mock HTTP để kiểm thử success/empty/invalid, timeout,
`429/5xx`, checksum mismatch và đối soát manifest/count trước khi mở gate cho
Silver. Ma trận nằm tại [USGS Bronze QA contract](./docs/specs/USGS_BRONZE_QA_CONTRACT.md).

Bronze object path, manifest, checksum, retry và trạng thái `BronzeReady` được
chốt tại [CON-02 Bronze storage contract](./docs/specs/BRONZE_STORAGE_CONTRACT.md).
Kiểm tra contract không cần mạng bằng:

```bash
./scripts/check-bronze-contract.sh
```

Tên trường, kiểu dữ liệu, null policy, canonical event, magnitude/depth bands
và KPI Silver/Gold được chốt tại [CON-03 logical data model](./docs/specs/SILVER_GOLD_DATA_MODEL.md).
Grain, lineage, bundle mapping và lifecycle dataset/experiment HDBSCAN được
khóa riêng tại [ML logical data model](./docs/specs/ML_DATA_MODEL.md).
Kiểm tra contract không cần mạng bằng:

```bash
./scripts/check-data-model-contract.sh
```

Fixture USGS/JMA dùng chung cho parser, quality, dedup và Gold nằm tại
[`tests/fixtures`](./tests/fixtures/README.md). Bộ fixture là dữ liệu synthetic,
không phụ thuộc mạng và có ma trận expected output/reason code. Kiểm tra bằng:

```bash
./scripts/check-shared-fixtures.sh
```

## Chuẩn bị trên máy local

### Lệnh ngắn qua Makefile

Chạy từ thư mục gốc repository; `make` mặc định chỉ hiện hướng dẫn:

```bash
make help
make test                # Contract + unit test Java/Airflow; không start Docker.
make env-init            # Chỉ tạo .env nếu chưa có, không ghi đè.
# Thay toàn bộ change-me-* trong .env trước khi chạy các lệnh dưới.
make check-config-local
make up                  # Build/start toàn bộ foundation, chờ healthy.
make smoke               # Full foundation runtime smoke.
make verify-samples      # Readback USGS/JMA DAT-01 đã tồn tại trên MinIO.
make logs-follow SERVICE=airflow-scheduler
make stop                # Giữ container và volume dữ liệu.
```

Có target riêng cho từng component (`up-airflow`, `up-spark`, `up-query`,
`smoke-minio`, ...), `smoke-usgs-live` để gọi USGS thật và `down` để gỡ
container/network nhưng giữ named volume. `make test` không gọi nguồn thật;
Maven có thể tải dependency lần đầu. Hướng dẫn đầy đủ về phạm vi, biến
`ENV_FILE`/`CHECK_ENV_FILE` và an toàn dữ liệu nằm tại
[Makefile command guide](./docs/MAKEFILE_COMMANDS.md).

### Các entrypoint script gốc

Static scaffold check chỉ cần Git và shell POSIX. Spark build local cần JDK 17,
`curl` hoặc `wget`, `unzip`; runtime smoke cần Docker Engine/Desktop và Docker
Compose plugin. Từ thư mục gốc repository, chạy:

```bash
./scripts/check-repository-layout.sh
```

Kết quả thành công:

```text
REP-01 repository scaffold check passed.
```

Lệnh này kiểm tra các module, thư mục source/test, tài liệu module và các nguồn
bind mount đã được dành trước. Nó là smoke check cho scaffold, không thay thế
Maven test hay `docker compose config` sau khi các task tương ứng được triển
khai.

Tạo cấu hình local, thay tất cả placeholder `change-me-*`, rồi kiểm tra:

```bash
cp .env.example .env
./scripts/check-config.sh --require-local
```

Không commit hoặc chia sẻ file `.env`. Contract đầy đủ nằm trong
[Configuration and secret contract](./docs/specs/CONFIGURATION_AND_SECRETS.md).

Kiểm tra network, named volumes và Compose extensions (không pull image hoặc
khởi động service):

```bash
./scripts/check-compose.sh
```

Kiểm tra contract MinIO tĩnh, sau đó chạy smoke test ghi/đọc thật bằng `.env`
local:

```bash
./scripts/check-minio.sh
./scripts/smoke-minio.sh
```

Smoke test giữ MinIO chạy, giữ volume dữ liệu và chỉ xóa đúng object kiểm thử.
Chi tiết bucket, prefix, quyền truy cập và cơ chế init nằm tại
[MinIO storage contract](./docs/specs/MINIO_STORAGE.md).

Kiểm tra contract Airflow tĩnh, sau đó khởi động runtime và trigger DAG smoke:

```bash
./scripts/check-airflow.sh
./scripts/smoke-airflow.sh
```

Smoke test giữ Airflow/PostgreSQL chạy và giữ metadata/log volume. UI/API mặc
định ở `http://127.0.0.1:8080`. Kiến trúc service, init, healthcheck và cách
debug nằm tại [Airflow local contract](./docs/specs/AIRFLOW_LOCAL.md).

Build Spark JAR, kiểm tra static contract và chạy acceptance trên standalone
cluster:

```bash
./mvnw --batch-mode --no-transfer-progress clean verify
./scripts/check-spark.sh
./scripts/smoke-spark.sh
```

Smoke test chờ master/worker healthy, yêu cầu worker `ALIVE`, chạy
`HelloWorldJob` bằng `spark-submit` và chỉ thành công khi exit code bằng `0`.
Spark master UI mặc định ở `http://127.0.0.1:8082`. Chi tiết version matrix,
JAR, tài nguyên và luồng kiểm thử nằm tại
[Spark standalone contract](./docs/specs/SPARK_STANDALONE.md).

Kiểm tra Iceberg REST Catalog/Trino tĩnh, sau đó chạy một vòng tạo–ghi–đọc
Iceberg table thật:

```bash
./scripts/check-query.sh
./scripts/smoke-query.sh
```

Smoke test chờ Catalog và Trino healthy, tạo schema nếu cần, ghi/đọc một row
qua MinIO rồi chỉ xóa table kiểm thử. SQL endpoint mặc định ở
`http://127.0.0.1:8081`; Catalog chỉ ở trong Compose network. Kiến trúc, secret
injection, volume và troubleshooting nằm tại
[Iceberg REST Catalog và Trino contract](./docs/specs/ICEBERG_TRINO.md).

Kiểm tra toàn bộ foundation stack bằng một entrypoint:

```bash
./scripts/check-foundation.sh
./scripts/smoke-foundation.sh
```

Full smoke yêu cầu `.env` hợp lệ, kiểm tra 9 service healthy, 2 init service
thoát `0`, network, 5 volume/mount, startup log và bốn smoke hành vi. Script giữ
service/volume sau khi chạy để debug. Checklist và troubleshooting nằm tại
[Foundation environment smoke contract](./docs/specs/FOUNDATION_SMOKE.md).

## Cấu trúc repository

```text
.
├── .env.example              # Mẫu cấu hình không chứa secret thật
├── .mvn/wrapper/             # Maven Wrapper config đã pin version/checksum
├── compose.yaml              # Network, volumes và Compose baseline
├── Makefile                  # Lệnh ngắn cho test/build/start/smoke/log/stop
├── mvnw / mvnw.cmd           # Maven Wrapper entrypoint
├── pom.xml                   # Maven reactor và version management
├── airflow/                  # DAG và test orchestration
│   ├── dags/
│   └── tests/
├── compose/                  # Asset init/smoke cho service Compose
│   └── trino/                # Query smoke runner
├── docs/                     # Kiến trúc, đặc tả, flow và runbook
├── scripts/                  # Script phát triển/vận hành dùng chung
├── spark/                    # Module Spark Java/Maven
│   └── src/
│       ├── main/java/
│       └── test/
│           ├── java/
│           └── resources/fixtures/
├── tests/                    # Fixture và test tích hợp xuyên module
└── trino/                    # Cấu hình Trino
    └── catalog/
        └── iceberg.properties
```

Chi tiết ownership, mount path và quy tắc mở rộng nằm trong
[Repository layout và mount contract](./docs/specs/REPOSITORY_LAYOUT.md).

## Trạng thái lệnh local

| Lệnh | Trạng thái | Task cung cấp |
|---|---|---|
| `./scripts/check-repository-layout.sh` | Chạy được | `REP-01` |
| `cp .env.example .env` | Chạy được | `CFG-01` |
| `./scripts/check-config.sh --require-local` | Chạy được | `CFG-01` |
| `./scripts/check-compose.sh` | Chạy được | `CMP-01` |
| `docker compose config` | Chạy được | `CMP-01` |
| `./scripts/check-minio.sh` | Chạy được, không cần start service | `MIO-01` |
| `./scripts/smoke-minio.sh` | Chạy được khi có `.env` và Docker daemon | `MIO-01` |
| `./scripts/check-airflow.sh` | Chạy được, không cần start service | `AFL-01` |
| `./scripts/smoke-airflow.sh` | Chạy được khi có `.env` và Docker daemon | `AFL-01` |
| `./mvnw clean verify` | Chạy được với JDK 17 và network lần đầu | `SPK-01` |
| `./scripts/check-spark.sh` | Chạy được, không cần start service | `SPK-01` |
| `./scripts/smoke-spark.sh` | Chạy được khi có `.env` và Docker daemon | `SPK-01` |
| `./scripts/check-query.sh` | Chạy được, không cần start service | `QRY-01` |
| `./scripts/smoke-query.sh` | Chạy được khi có `.env` và Docker daemon | `QRY-01` |
| `./scripts/check-foundation.sh` | Chạy toàn bộ static foundation checks | `FND-01` |
| `./scripts/smoke-foundation.sh` | Chạy full-stack smoke khi có `.env` và Docker daemon | `FND-01` |
| `./mvnw --batch-mode --no-transfer-progress -pl spark -am test` | Chạy unit test request planner offline | `USG-01` |

Hướng dẫn vận hành đầy đủ được duy trì trong
[Local operations runbook](./docs/LOCAL_OPERATIONS_RUNBOOK.md).

## Làm việc với repository

GitHub Actions được cấu hình ở [ci.yml](./.github/workflows/ci.yml): PR vào
`main` và push `main` chạy ba checks docs/contracts, Java17/Spark và Airflow
control-plane. Không dùng secret hoặc dữ liệu lakehouse thật. Lệnh local,
required-check setup và giới hạn ở [CI guide](./docs/conventions_and_workflow/CONTINUOUS_INTEGRATION.md).

- Đọc [AGENTS.md](./AGENTS.md) trước khi bắt đầu task.
- Tạo branch mới từ `main` và chỉ giải quyết một task trong mỗi branch.
- Tuân theo [Git workflow](./docs/conventions_and_workflow/GIT_WORKFLOW.md) và
  [commit convention](./docs/conventions_and_workflow/COMMIT_CONVENTION.md).
- Không commit secret, dữ liệu runtime, Maven `target/` hoặc volume local.
