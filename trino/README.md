# Trino module

Module này chứa cấu hình phục vụ SQL của Trino.

- `catalog/iceberg.properties`: đăng ký Iceberg REST Catalog và native S3
  client cho MinIO, chỉ tham chiếu secret qua `${ENV:...}`.

`QRY-01` pin Trino `483`, bind `catalog/` tới `/etc/trino/catalog` ở chế độ
read-only và publish SQL endpoint mặc định tại `127.0.0.1:8081`. REST Catalog
không publish ra host. Kiểm tra bằng:

```bash
./scripts/check-query.sh
./scripts/smoke-query.sh
```

Chi tiết version, dependency flow, volume và troubleshooting:
[Iceberg/Trino contract](../docs/specs/ICEBERG_TRINO.md).
