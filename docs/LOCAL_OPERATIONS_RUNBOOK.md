# Runbook cài đặt và vận hành local

| Thuộc tính | Giá trị |
|---|---|
| Trạng thái | Implemented từng phần — MinIO, Airflow, Spark và query layer có smoke test |
| Đối tượng | Thành viên phát triển/vận hành demo |
| Môi trường | Docker Compose trên máy local |

## 1. Mục đích

Runbook mô tả thứ tự chuẩn để chuẩn bị, khởi động, kiểm tra, chạy pipeline và
xử lý lỗi. Repository đã có scaffold, configuration contract, Compose
foundation, MinIO runtime, Airflow local runtime và Spark standalone. DAG ETL
chưa có; Iceberg REST Catalog và Trino đã có static/runtime acceptance riêng.

## 2. Yêu cầu máy

| Thành phần | Khuyến nghị |
|---|---|
| CPU | Từ 4 core |
| RAM | 16 GiB trở lên; 8 GiB chỉ thử nghiệm và chạy tuần tự |
| Dung lượng trống | Từ 50 GiB |
| Phần mềm | Git, Docker Engine/Desktop, Docker Compose plugin, JDK 17, `curl` hoặc `wget`, `unzip` |
| Power BI | Power BI Desktop trên Windows và ODBC driver tương thích Trino |
| Mạng | Truy cập được USGS API khi extract |

Kiểm tra công cụ:

```bash
git --version
docker --version
docker compose version
java -version
```

## 3. Cấu trúc repository hiện tại

```text
project-root/
├── .env.example
├── .mvn/wrapper/
├── compose.yaml
├── mvnw
├── pom.xml
├── airflow/
│   ├── dags/
│   └── tests/
├── compose/
│   ├── airflow/
│   ├── minio/
│   ├── spark/
│   └── trino/
├── scripts/
├── spark/
│   ├── pom.xml
│   └── src/
│       ├── main/java/
│       └── test/
├── tests/
│   ├── fixtures/
│   └── integration/
├── trino/
│   └── catalog/
│       └── iceberg.properties
└── docs/
```

Kiểm tra scaffold từ project root:

```bash
./scripts/check-repository-layout.sh
```

Mount source/target và ownership module được chốt tại
[Repository layout và mount contract](./specs/REPOSITORY_LAYOUT.md). Hiện đã có
MinIO, Airflow, Spark và query runtime.
Network, volume lifecycle và dependency policy nằm tại
[Compose foundation contract](./specs/COMPOSE_FOUNDATION.md). Contract
bucket/prefix, credential và bootstrap nằm tại
[MinIO storage contract](./specs/MINIO_STORAGE.md).
Airflow service, init, DAG smoke và troubleshooting nằm tại
[Airflow local contract](./specs/AIRFLOW_LOCAL.md).
Spark version matrix, Java build, service và smoke flow nằm tại
[Spark standalone contract](./specs/SPARK_STANDALONE.md).
Iceberg REST Catalog, Trino, secret injection và query smoke nằm tại
[Iceberg/Trino contract](./specs/ICEBERG_TRINO.md).

## 4. Nhóm biến môi trường

Không ghi giá trị thật trong tài liệu. Contract đầy đủ và phân loại biến nhạy
cảm nằm tại
[Configuration and secret contract](./specs/CONFIGURATION_AND_SECRETS.md).
Các nhóm cấu hình hiện có:

| Nhóm | Ví dụ tên biến | Ghi chú |
|---|---|---|
| MinIO | `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY` | Secret không commit |
| Bucket | `DATA_BUCKET`, `WAREHOUSE_PATH` | Tên thống nhất giữa Spark/Trino |
| Airflow | admin user/password, executor/db connection | Chỉ publish UI cần thiết |
| Spark | master URL, memory/cores | Giới hạn phù hợp máy local |
| Iceberg | catalog URI, warehouse | Dùng cùng namespace |
| Trino | host/port/catalog/schema | Port host chỉ mở khi cần |
| Pipeline | timezone, schedule, overlap days, study area | Không hard-code trong job |

Khởi tạo `.env`:

```bash
cp .env.example .env
```

Sau đó thay toàn bộ placeholder `change-me-*` bằng credential local mạnh rồi
chạy:

```bash
./scripts/check-config.sh --require-local
```

Checker không in giá trị secret. Không khởi động service nếu kiểm tra thất bại.

## 5. Khởi động lần đầu

### 5.1. Preflight

- Docker daemon đang chạy.
- Các port host dự kiến chưa bị chiếm.
- `.env` tồn tại và không được Git track.
- `./scripts/check-config.sh --require-local` thành công.
- `./scripts/check-compose.sh` thành công.
- `./scripts/check-minio.sh` thành công.
- `./scripts/check-airflow.sh` thành công.
- `./scripts/check-spark.sh` thành công.
- `./scripts/check-query.sh` thành công.
- Máy còn đủ disk/RAM.
- Thư mục/volume mount có quyền phù hợp.

### 5.2. Validate Compose foundation

Các lệnh sau chạy được ngay và không khởi động service:

```bash
./scripts/check-compose.sh
docker compose --env-file .env config --quiet
docker compose --env-file .env --profile validation config --quiet
```

Profile `validation` chỉ giữ health/dependency/resource reference trong resolved
config; không cần chạy hoặc pull image của các contract service.

### 5.3. Khởi tạo MinIO runtime

Các lệnh này đã được triển khai và có thể chạy sau khi `.env` hợp lệ:

```bash
docker compose --env-file .env up -d minio minio-init
docker compose --env-file .env ps -a minio minio-init
./scripts/smoke-minio.sh
```

Kết quả đúng là `minio` healthy, `minio-init` thoát `0` và smoke test báo
`passed`. Script smoke để MinIO tiếp tục chạy và không xóa volume. Ba prefix
logic được tạo bằng marker `.keep`; marker không phải dữ liệu pipeline.

Các query service có thể được khởi động bằng quy trình ở mục 5.6; không cần
publish MinIO S3 API hoặc Catalog port ra host.

### 5.4. Khởi tạo Airflow local

Sau khi `.env` hợp lệ:

```bash
./scripts/smoke-airflow.sh
docker compose --env-file .env ps -a \
  airflow-postgres airflow-init airflow-api-server \
  airflow-scheduler airflow-dag-processor
```

Kết quả đúng là PostgreSQL/API/scheduler/DAG processor healthy,
`airflow-init` thoát `0` và DAG `afl_01_smoke` kết thúc `success`. Script giữ
service cùng metadata/log volume để tiếp tục debug hoặc phát triển DAG.

Airflow UI/API mặc định: `http://127.0.0.1:8080`.

### 5.5. Khởi tạo Spark standalone

Build JAR/static contract và chạy runtime acceptance:

```bash
./scripts/check-spark.sh
./scripts/smoke-spark.sh
docker compose --env-file .env ps -a spark-master spark-worker
```

Kết quả đúng là master/worker healthy, master báo ít nhất một worker `ALIVE`,
`HelloWorldJob` in marker có `record_count=10 id_sum=45` và `spark-submit` trả
exit code `0`. Script giữ master/worker và `pipeline_staging` để debug.

Spark master UI mặc định: `http://127.0.0.1:8082`. RPC `7077` và worker UI
không publish ra host.

### 5.6. Khởi tạo Iceberg REST Catalog và Trino

Chạy static contract và acceptance tạo–ghi–đọc một Iceberg table:

```bash
./scripts/check-query.sh
./scripts/smoke-query.sh
docker compose --env-file .env ps -a minio minio-init iceberg-rest trino
```

Kết quả đúng là Catalog và Trino healthy, catalog `iceberg` cùng schema
`earthquakes` nhìn thấy được, và smoke báo `row_count=1`. Runner xóa đúng table
`qry_01_smoke`; schema, `minio_data` và `iceberg_catalog_data` được giữ lại.

Trino SQL endpoint mặc định: `http://127.0.0.1:8081`. Catalog `8181` và MinIO
S3 API `9000` chỉ truy cập được trong Compose network. Baseline local chưa bật
Trino authentication, vì vậy không đổi loopback binding thành public binding.

### 5.7. Kiểm tra service

```bash
docker compose --env-file .env ps -a minio minio-init
docker compose --env-file .env logs --tail=100 minio
docker compose --env-file .env logs minio-init
docker compose --env-file .env logs --tail=100 spark-master spark-worker
docker compose --env-file .env logs --tail=100 iceberg-rest trino
```

Các kiểm tra logic:

- Airflow UI truy cập được và scheduler heartbeat bình thường.
- MinIO bucket/prefix đã được bootstrap và smoke ghi/đọc thành công.
- Spark master thấy worker.
- Iceberg Catalog health/readiness đạt.
- Trino hoàn tất startup và thấy catalog Iceberg.
- Airflow metadata DB healthy.

Catalog state nằm trong `iceberg_catalog_data:/home/iceberg`, còn Iceberg
metadata/Parquet nằm trong warehouse MinIO. Khi backup hoặc chẩn đoán mất
table, phải kiểm tra cả hai volume.

## 6. Build Spark job

Maven Wrapper đã pin Maven `3.9.16`; module compile bằng Java `17` và tạo JAR
`spark/target/japan-earthquake-etl.jar`:

```bash
./mvnw --batch-mode --no-transfer-progress clean verify
```

Không thay wrapper bằng Maven global để né version/checksum contract. Không bỏ
qua unit test trước khi submit JAR mới. Image runtime chứa JAR tại
`/opt/spark/jobs/japan-earthquake-etl.jar`.

## 7. Chạy pipeline

### Chạy theo lịch

Airflow scheduler tạo daily run theo timezone cấu hình. Máy phải đang bật, không sleep và Docker phải hoạt động.

### Trigger thủ công

Trong Airflow UI:

1. Mở DAG chính.
2. Kiểm tra logical date/data interval.
3. Truyền config chỉ khi schema config đã được tài liệu hóa.
4. Trigger và theo dõi Graph/Grid view.
5. Không trigger run thứ hai cho cùng interval khi run đầu còn đang sửa dữ liệu.

Khi DAG ID và CLI contract được tạo, bổ sung lệnh cụ thể vào runbook thay vì dùng placeholder.

## 8. Kiểm tra sau lần chạy

- DAG run ở trạng thái success.
- Bronze có GeoJSON và ingest metadata của run.
- Silver có partition bị ảnh hưởng và quality summary đạt.
- Gold có snapshot mới.
- Trino query verify chạy được.
- Count/metric có thể đối soát.
- Power BI chỉ refresh sau khi pipeline `Published`.

Checklist nhanh:

```text
[ ] run_id và data interval đúng
[ ] Bronze object + checksum
[ ] Silver valid/rejected/duplicate counts
[ ] Gold snapshot ID
[ ] Trino verification passed
[ ] Power BI last refresh updated (nếu có refresh)
```

## 9. Dừng hệ thống

Dừng container nhưng giữ volume:

```bash
docker compose stop
```

Gỡ container/network nhưng theo mặc định vẫn giữ named volume:

```bash
docker compose down
```

Không dùng `docker compose down -v` trong quy trình thường ngày vì tùy cấu hình có thể xóa volume chứa dữ liệu. Chỉ xóa volume khi người thực hiện đã xác nhận rõ phạm vi, backup và chấp nhận mất dữ liệu local.

## 10. Backup và restore

### Cần backup

- MinIO data/warehouse volume.
- Trạng thái Catalog nếu không hoàn toàn nằm trong warehouse.
- Airflow metadata nếu cần giữ lịch sử run.
- File Power BI và cấu hình nguồn không chứa secret.
- Commit/tag mã nguồn tương ứng với snapshot/demo.

### Nguyên tắc

- Backup phải nhất quán; tránh copy volume khi đang có writer hoạt động.
- Thử restore trên môi trường tách biệt trước khi tin vào backup.
- Ghi ngày, code version, config version và snapshot ID trong manifest backup.
- Không lưu credential thật vào gói backup chia sẻ.

Lệnh backup/restore cụ thể sẽ được bổ sung sau khi loại volume và catalog backend được chốt.

## 11. Troubleshooting

### Container không healthy

1. Chạy `docker compose ps`.
2. Đọc log service và dependency ngay trước nó.
3. Kiểm tra port, env, volume permission và disk.
4. Restart đúng service; không xóa volume để thử ngẫu nhiên.

### Airflow DAG không xuất hiện

- Kiểm tra DAG file được mount đúng.
- Kiểm tra import error trong Airflow UI/log scheduler.
- Kiểm tra clock/timezone của host.
- Kiểm tra scheduler đang hoạt động.

### Spark job lỗi/worker mất kết nối

- Chạy `./scripts/check-spark.sh`, rồi xem
  `docker compose --env-file .env logs spark-master spark-worker`.
- Mở `http://127.0.0.1:${SPARK_MASTER_UI_HOST_PORT}` và xác nhận worker
  `ALIVE`.
- Nếu job không nhận resource, đưa core/memory về default hoặc tăng worker
  limit có chủ đích; executor mặc định cần `2g`.
- Xác nhận JAR/dependency tương thích Spark `3.5.9`, Scala `2.12` và Java `17`.
- Client phải ở network `pipeline`, hostname `spark-client`; không dùng
  `localhost` làm `spark.driver.host`.

### Không truy cập được MinIO

- S3 API nội bộ là `http://minio:9000`; host chỉ truy cập Console qua
  `http://127.0.0.1:9001` mặc định.
- Kiểm tra credential và bucket policy.
- Kiểm tra service name/DNS trong Compose network.
- Chạy `./scripts/check-minio.sh`, sau đó chạy lại
  `docker compose --env-file .env run --rm minio-init`.
- Không in secret vào log khi debug.

### Trino không thấy bảng/snapshot

- Kiểm tra Catalog URI và warehouse path giữa Spark/Trino.
- Kiểm tra object storage credentials.
- Xác nhận Gold commit thực sự thành công.
- Xác nhận query dùng đúng catalog/schema/table.

### Power BI/ODBC không kết nối

- Kiểm tra Trino query được từ client khác trước.
- Kiểm tra DSN/driver architecture và network route.
- Xác nhận catalog/schema mặc định hoặc ghi đầy đủ tên bảng.
- Kiểm tra timeout với query nhỏ trước khi refresh dataset.

### Disk gần đầy

- Dừng backfill/job mới.
- Xác định volume/service sử dụng dung lượng.
- Áp dụng retention/snapshot cleanup chỉ bằng procedure đã kiểm thử.
- Không xóa thủ công file trong Iceberg warehouse.

## 12. Bảng endpoint

| Dịch vụ | URL từ host | Chỉ local? | Xác thực | Trạng thái |
|---|---|---:|---|---|
| Airflow UI/API | `http://127.0.0.1:8080` mặc định | Có, bind loopback | FAB local admin | Implemented |
| MinIO Console | `http://127.0.0.1:9001` mặc định | Có, bind loopback | Root credential local | Implemented |
| Spark master UI | `http://127.0.0.1:8082` mặc định | Có, bind loopback | Không — chỉ local | Implemented |
| Trino | TBD | Có | TBD | Draft |

MinIO S3 API là `http://minio:9000` trong Compose network và không publish ra
host. Spark RPC/worker UI, Catalog và metadata database cũng không public.

## 13. Việc phải cập nhật khi có mã nguồn

- Tên/DAG ID/service/port thực tế của các component chưa triển khai.
- Câu lệnh init/build/trigger/backfill chính xác.
- Health-check URL/expected response của service chưa triển khai; MinIO dùng
  `/minio/health/live`, Airflow API dùng `/api/v2/monitor/health`, scheduler và
  DAG processor dùng job heartbeat CLI.
- Bucket/catalog/schema/table naming.
- Backup/restore command đã kiểm thử.
- Known issues và resource profile đo được.
