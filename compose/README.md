# Compose assets

`compose.yaml` nằm tại project root. Thư mục này chỉ dành cho asset hỗ trợ
service như script init hoặc healthcheck không thuộc module khác.

Asset hiện có:

- `airflow/Dockerfile`: build runner JAR, ghép Java 17 vào Airflow 3.3.2.
- `airflow/usgs-runner.sh`: executable bridge cho phase protocol của DAG USGS.
- `airflow/usgs-live-smoke.sh`: fixed-window live acceptance và immutable rerun.
- `airflow/smoke.sh`: kiểm tra DAG import, trigger `afl_01_smoke` và chờ trạng
  thái terminal.
- `minio/Dockerfile`: build security release đã pin từ source upstream.
- `minio/init.sh`: bootstrap bucket, prefix marker, pipeline user và policy.
- `minio/smoke.sh`: xác nhận marker và ghi/đọc/xóa đúng object smoke bằng
  pipeline credential.
- `spark/Dockerfile`: build Maven multi-stage và tạo image Spark `3.5.9`/Java
  `17` chứa JAR đã verify.
- `spark/smoke.sh`: yêu cầu worker `ALIVE`, chạy `spark-submit` và kiểm tra
  marker Hello World cùng exit code.
- `trino/smoke.sh`: xác nhận catalog/schema, tạo Iceberg table Parquet, ghi/đọc
  một row qua REST Catalog + MinIO và chỉ xóa table kiểm thử.

Không đặt secret, data volume, warehouse hoặc file runtime trong thư mục này.
Mọi mount phải tuân theo
[repository mount contract](../docs/specs/REPOSITORY_LAYOUT.md#4-mount-contract).

Kiểm tra Compose foundation:

```bash
./scripts/check-compose.sh
./scripts/check-airflow.sh
./scripts/check-minio.sh
./scripts/check-spark.sh
./scripts/check-query.sh
./scripts/check-foundation.sh
```

Chi tiết network, volume lifecycle và extension baseline nằm tại
[Compose foundation contract](../docs/specs/COMPOSE_FOUNDATION.md).
Hành vi Airflow, metadata DB và DAG smoke nằm tại
[Airflow local contract](../docs/specs/AIRFLOW_LOCAL.md).
Luồng USGS thật đến MinIO, command và evidence nằm tại
[USGS live Bronze runbook](../docs/specs/USGS_LIVE_BRONZE_RUNBOOK.md).
Hành vi MinIO, quyền truy cập và lệnh runtime nằm tại
[MinIO storage contract](../docs/specs/MINIO_STORAGE.md).
Hành vi Spark master/worker/client, build và smoke nằm tại
[Spark standalone contract](../docs/specs/SPARK_STANDALONE.md).
Hành vi Iceberg REST Catalog, Trino và query smoke nằm tại
[Iceberg/Trino contract](../docs/specs/ICEBERG_TRINO.md).
Checklist kết hợp health, network, volume, log và mọi component smoke nằm tại
[Foundation environment smoke contract](../docs/specs/FOUNDATION_SMOKE.md).
