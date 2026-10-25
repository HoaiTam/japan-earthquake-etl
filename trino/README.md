# Trino module

Module này chứa cấu hình phục vụ SQL của Trino.

- `catalog/iceberg.properties`: đăng ký Iceberg REST Catalog và native S3
  client cho MinIO, chỉ tham chiếu secret qua `${ENV:...}`.
- `log.properties`: SEC-01 đặt `io.trino.bootstrap.catalog=WARN` để catalog
  bootstrap không in `s3.aws-access-key` ở INFO; giữ warning/error và log runtime.
  Bind read-only tại `/etc/trino/log.properties`. Code fix không xóa log đã lưu
  và chưa có hiệu lực trên container chưa recreate; xem
  [SEC-01 checklist](../docs/security/SEC-01-CHECKLIST.md).
- `jvm.config` và `config.properties`: DAT-03 giới hạn heap1536MiB,
  direct512MiB, code cache256MiB trong container3GiB; query user memory512MB,
  total768MB và heap headroom512MB. Không dùng mặc định heap80% của image483:
  một query verify đã làm container OOM-kill exit137. Ngân sách query+headroom
  phải nhỏ hơn heap, total phải lớn hơn user memory theo
  [tài liệu đúng tag483](https://github.com/trinodb/trino/blob/483/docs/src/main/sphinx/admin/properties-resource-management.md).
  Giữ headroom cho native/non-heap; đây là baseline local, không cam kết capacity
  full catalog. Query vượt budget phải fail, không bỏ blocker để ép pass.
  Hai file bind read-only; chỉ có hiệu lực sau recreate Trino được owner cho phép.
  Trước recreate phải lưu stdout/stderr và `/data/trino/var/log` ra staging riêng
  vì container hiện không có persistent log volume. Không commit log/heap dump.

`QRY-01` pin Trino `483`, bind `catalog/` tới `/etc/trino/catalog` ở chế độ
read-only và publish SQL endpoint mặc định tại `127.0.0.1:8081`. REST Catalog
không publish ra host. Kiểm tra bằng:

```bash
./scripts/check-query.sh
./scripts/smoke-query.sh
```

Chi tiết version, dependency flow, volume và troubleshooting:
[Iceberg/Trino contract](../docs/specs/ICEBERG_TRINO.md).
