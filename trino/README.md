# Trino module

Module này chứa cấu hình phục vụ SQL của Trino.

- `catalog/`: catalog properties không chứa secret.

`QRY-01` chịu trách nhiệm chốt cấu hình Iceberg Catalog, compatibility và cách
inject credential. Compose bind `catalog/` tới `/etc/trino/catalog` ở chế độ
read-only.

