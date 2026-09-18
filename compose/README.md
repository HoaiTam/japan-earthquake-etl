# Compose assets

`compose.yaml` nằm tại project root. Thư mục này chỉ dành cho asset hỗ trợ
service như script init hoặc healthcheck không thuộc module khác.

Không đặt secret, data volume, warehouse hoặc file runtime trong thư mục này.
Mọi mount phải tuân theo
[repository mount contract](../docs/specs/REPOSITORY_LAYOUT.md#4-mount-contract).

Kiểm tra Compose foundation:

```bash
./scripts/check-compose.sh
```

Chi tiết network, volume lifecycle và extension baseline nằm tại
[Compose foundation contract](../docs/specs/COMPOSE_FOUNDATION.md).
