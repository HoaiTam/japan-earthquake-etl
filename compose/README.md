# Compose assets

`compose.yaml` nằm tại project root. Thư mục này chỉ dành cho asset hỗ trợ
service như script init hoặc healthcheck không thuộc module khác.

Asset hiện có:

- `airflow/smoke.sh`: kiểm tra DAG import, trigger `afl_01_smoke` và chờ trạng
  thái terminal.
- `minio/Dockerfile`: build security release đã pin từ source upstream.
- `minio/init.sh`: bootstrap bucket, prefix marker, pipeline user và policy.
- `minio/smoke.sh`: xác nhận marker và ghi/đọc/xóa đúng object smoke bằng
  pipeline credential.

Không đặt secret, data volume, warehouse hoặc file runtime trong thư mục này.
Mọi mount phải tuân theo
[repository mount contract](../docs/specs/REPOSITORY_LAYOUT.md#4-mount-contract).

Kiểm tra Compose foundation:

```bash
./scripts/check-compose.sh
./scripts/check-airflow.sh
./scripts/check-minio.sh
```

Chi tiết network, volume lifecycle và extension baseline nằm tại
[Compose foundation contract](../docs/specs/COMPOSE_FOUNDATION.md).
Hành vi Airflow, metadata DB và DAG smoke nằm tại
[Airflow local contract](../docs/specs/AIRFLOW_LOCAL.md).
Hành vi MinIO, quyền truy cập và lệnh runtime nằm tại
[MinIO storage contract](../docs/specs/MINIO_STORAGE.md).
