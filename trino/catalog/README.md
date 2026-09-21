# Trino catalogs

`iceberg.properties` tạo Trino catalog tên `iceberg`, dùng Iceberg REST
Catalog cho namespace/table registration và native S3 file system cho MinIO.

Credential được tham chiếu bằng cú pháp `${ENV:VARIABLE}` của Trino rồi Compose
inject từ `.env`; không ghi access key/password thật hoặc placeholder secret
vào file này. Catalog directory được mount read-only tại
`/etc/trino/catalog`.

Khi đổi catalog name phải đổi đồng bộ tên file, `ICEBERG_CATALOG_NAME`,
`TRINO_CATALOG`, static checker và tài liệu. Contract đầy đủ nằm tại
[Iceberg/Trino contract](../../docs/specs/ICEBERG_TRINO.md).
