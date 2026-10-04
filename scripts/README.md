# Shared scripts

Thư mục này chứa script dùng chung cho phát triển, CI và vận hành local. Script
phải chạy từ bất kỳ working directory nào, fail fast và không xóa data/volume
theo mặc định.

Hiện có:

- `check-mvp-baseline.sh`: kiểm tra baseline `PLN-01` đã chốt kế hoạch 8 tuần,
  HDBSCAN Core, Power BI Stretch, research split/feature/grain, artifact gate và
  trạng thái đồng bộ giữa task index với roadmap; không gọi mạng.
- `check-repository-layout.sh`: smoke check cho scaffold `REP-01`.
- `check-config.sh`: kiểm tra `.env.example`, `.env` và secret hygiene cho
  `CFG-01`; dùng `--require-local` trước runtime và không in giá trị cấu hình.
- `check-compose.sh`: validate Compose schema, network, named volumes và
  lifecycle labels cho `CMP-01` mà không khởi động service.
- `check-minio.sh`: validate service, image pin, healthcheck, mount, dependency
  và host exposure của `MIO-01` mà không khởi động container.
- `smoke-minio.sh`: dùng `.env` local để bootstrap MinIO và kiểm tra ghi/đọc
  object bằng pipeline credential; không xóa bucket hoặc volume.
- `check-airflow.sh`: chạy unit test DAG và validate service, image, executor,
  healthcheck, dependency, mount và host exposure của `AFL-01`/`USG-04`.
- `smoke-airflow.sh`: khởi động Airflow local, chờ các component healthy và
  trigger `afl_01_smoke`; không xóa metadata/log volume.
- `check-spark.sh`: chạy Maven verify và kiểm tra JAR, dependency, image,
  healthcheck, mount, exposure cùng Compose contract của `SPK-01`.
- `smoke-spark.sh`: build image một lần, chờ master/worker healthy và chạy
  `HelloWorldJob` trên worker; giữ cluster và staging volume để debug.
- `check-query.sh`: validate catalog properties, digest-pinned images,
  dependency, healthcheck, mount, host exposure và smoke service của `QRY-01`.
- `smoke-query.sh`: bootstrap MinIO, chờ REST Catalog/Trino healthy rồi chạy
  acceptance tạo–ghi–đọc Iceberg table; giữ service và durable volumes.
- `check-foundation.sh`: gom toàn bộ static contract từ repository/config đến
  MinIO, Airflow, Spark và query layer; dùng `--require-local` trước full smoke.
- `smoke-foundation.sh`: build/start toàn bộ foundation stack, chạy bốn smoke
  hành vi và xác nhận health, init exit code, network, volume/mount cùng startup
  log; không dừng service hoặc xóa volume.
- `check-source-coverage.sh`: kiểm tra các quyết định bắt buộc của source
  coverage contract `CON-01`, range JMA 40 năm, ROI, USGS seed/overlap và link
  theo dõi task; không gọi mạng hoặc tải dữ liệu.
- `mvnw --batch-mode --no-transfer-progress -pl spark -am test`: chạy unit test
  offline cho `USG-01` request planner, `USG-02` HTTP client, `USG-03` Bronze
  writer và `USG-05` fixture acceptance; test
  không gọi USGS API thật.
- `python3 -m unittest discover -s airflow/tests -p 'test_*.py'`: kiểm tra DAG
  USGS, interval UTC, retry context, runner boundary và Bronze publish gate mà
  không gọi mạng.
- `check-bronze-contract.sh`: kiểm tra layout object, manifest, lifecycle
  `BronzeReady`/`Rejected` và retry rule của `CON-02`; không gọi mạng hoặc
  truy cập MinIO.
- `check-data-model-contract.sh`: kiểm tra dataset, field, lineage, null policy,
  bands và KPI Silver/Gold cùng grain, lifecycle, enum, reason code và publish
  gate ML bắt buộc của contract `CON-03`; không gọi mạng.
- `build-shared-fixtures.sh`: tái tạo JMA fixed-width/ZIP và checksum xác định
  của `CON-04` sau khi maintainer chủ động đổi fixture.
- `check-shared-fixtures.sh`: kiểm tra coverage test matrix, GeoJSON, JMA record
  96 byte, ZIP content, checksum và secret hygiene của fixture `CON-04`.
- `check-week-3-plan.sh`: kiểm tra `USG-06`/`DAT-01`, tổng task/effort, cổng
  data-readiness 12 giờ, ba luồng implementation 41 giờ và trạng thái Ready
  cần thiết cho kế hoạch tuần 3; không gọi mạng hoặc tải raw data.

Full checklist và hướng xử lý lỗi:
[Foundation environment smoke contract](../docs/specs/FOUNDATION_SMOKE.md).
