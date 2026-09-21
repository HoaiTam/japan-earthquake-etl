# Shared scripts

Thư mục này chứa script dùng chung cho phát triển, CI và vận hành local. Script
phải chạy từ bất kỳ working directory nào, fail fast và không xóa data/volume
theo mặc định.

Hiện có:

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
  healthcheck, dependency, mount và host exposure của `AFL-01`.
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
