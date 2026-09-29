# Configuration and secret contract

| Thuộc tính | Giá trị |
|---|---|
| Task | `CFG-01` |
| Trạng thái | Implemented |
| File mẫu | `.env.example` |
| File local | `.env` — bị Git ignore |

## 1. Nguyên tắc

- Cấu hình thay đổi theo môi trường nằm ngoài source code.
- `.env.example` chỉ chứa default local không nhạy cảm và placeholder
  `change-me-*`; không chứa credential dùng được.
- `.env` được tạo trên từng máy, không commit, không gửi qua chat và không đưa
  vào ảnh/log demo.
- Code và Compose đọc biến môi trường; không tự động `source .env` trong script
  kiểm tra.
- Tên biến là contract giữa Compose, Airflow, Spark, Iceberg và Trino. PR đổi
  tên biến phải cập nhật tất cả consumer và tài liệu liên quan.

## 2. Khởi tạo local

Từ project root:

```bash
cp .env.example .env
```

Thay toàn bộ giá trị bắt đầu bằng `change-me-`. Có thể tạo giá trị ngẫu nhiên
bằng công cụ local, ví dụ:

```bash
openssl rand -hex 24
python3 -c 'import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())'
```

Dùng lệnh Python cho `AIRFLOW_FERNET_KEY`; Fernet yêu cầu đúng 32 byte được mã
hóa URL-safe Base64. Các password/secret khác có thể dùng chuỗi hex từ OpenSSL.

Không đặt kết quả của các lệnh trên vào `.env.example`. Sau khi hoàn tất:

```bash
./scripts/check-config.sh --require-local
```

Checker chỉ báo tên key/file lỗi và không in giá trị cấu hình. Chế độ không có
`--require-local` dành cho CI/repository check khi `.env` cố ý không tồn tại.

## 3. Contract biến môi trường

### Pipeline và data lake

| Biến | Bắt buộc | Nhạy cảm | Consumer / ý nghĩa |
|---|---|---|---|
| `CONFIG_VERSION` | Có | Không | Phiên bản config ghi vào run context |
| `PIPELINE_TIMEZONE` | Có | Không | Timezone điều phối; baseline là `Asia/Ho_Chi_Minh` |
| `PIPELINE_SCHEDULE_CRON` | Có | Không | Lịch daily; default local là 07:15 |
| `PIPELINE_OVERLAP_DAYS` | Có | Không | Số ngày đọc chồng để nhận late update |
| `USGS_API_BASE_URL` | Có | Không | Endpoint extract HTTPS |
| `STRONG_MAGNITUDE_THRESHOLD` | Có | Không | Ngưỡng KPI strong earthquake |
| `DATA_BUCKET` | Có | Không | Bucket chung của data lake |
| `BRONZE_PREFIX` | Có | Không | Prefix lưu response nguồn |
| `SILVER_PREFIX` | Có | Không | Prefix lưu Silver Parquet |
| `WAREHOUSE_PATH` | Có | Không | URI `s3://` dùng chung cho Gold/Iceberg |

`WAREHOUSE_PATH` phải nằm trong `DATA_BUCKET`. Tọa độ/bounding box Nhật Bản
thuộc contract của `CON-01` và `USG-01`; CFG-01 không đặt giá trị giả để tránh trở thành
default ngoài ý muốn.

### MinIO

| Biến | Bắt buộc | Nhạy cảm | Consumer / ý nghĩa |
|---|---|---|---|
| `MINIO_ENDPOINT` | Có | Không | Endpoint nội bộ trong Compose network |
| `MINIO_ROOT_USER` | Có | Có | Tài khoản bootstrap/admin local |
| `MINIO_ROOT_PASSWORD` | Có | Có | Mật khẩu bootstrap/admin local |
| `MINIO_ACCESS_KEY` | Có | Có | Credential riêng cho pipeline |
| `MINIO_SECRET_KEY` | Có | Có | Secret riêng cho pipeline |
| `MINIO_CONSOLE_HOST_PORT` | Có | Không | Port host của giao diện vận hành |
| `S3_REGION` | Có | Không | Region cố định cho MinIO/S3 client local |

MinIO API không có biến host port vì runtime giữ API trong Compose network.
`MIO-01` tạo pipeline user và policy giới hạn trong `DATA_BUCKET`: Bronze không
có quyền delete, còn Silver/warehouse có quyền phục vụ rerun và Iceberg. Root
credential chỉ dùng cho bootstrap/admin local. Chi tiết tại
[MinIO storage contract](./MINIO_STORAGE.md).

### Airflow và metadata database

| Biến | Bắt buộc | Nhạy cảm | Consumer / ý nghĩa |
|---|---|---|---|
| `AIRFLOW_UID` | Có | Không | UID ghi file/volume trên local host |
| `AIRFLOW_ADMIN_USERNAME` | Có | Không | Username quản trị local |
| `AIRFLOW_ADMIN_PASSWORD` | Có | Có | Mật khẩu quản trị local |
| `AIRFLOW_FERNET_KEY` | Có | Có | Mã hóa connection/variable nhạy cảm |
| `AIRFLOW_API_JWT_SECRET` | Có | Có | Ký JWT cho API/task execution |
| `AIRFLOW_WEB_HOST_PORT` | Có | Không | Port host của Airflow UI |
| `AIRFLOW_DB_USER` | Có | Không | User metadata database |
| `AIRFLOW_DB_PASSWORD` | Có | Có | Mật khẩu metadata database |
| `AIRFLOW_DB_NAME` | Có | Không | Tên metadata database |

`AFL-01` dùng Airflow `3.3.2` với FAB auth manager và API server thay cho lệnh
webserver cũ. `AIRFLOW_DB_PASSWORD` phải dùng ký tự URI-safe vì được truyền vào
SQLAlchemy connection URI nội bộ. Admin password, database password và API JWT
secret phải khác nhau. Chi tiết tại
[Airflow local contract](./AIRFLOW_LOCAL.md).

### Spark, Iceberg và Trino

| Biến | Bắt buộc | Nhạy cảm | Consumer / ý nghĩa |
|---|---|---|---|
| `SPARK_MASTER_URL` | Có | Không | Spark master nội bộ |
| `SPARK_MASTER_UI_HOST_PORT` | Có | Không | Port host loopback của master UI |
| `SPARK_WORKER_CORES` | Có | Không | Số core worker quảng bá cho master |
| `SPARK_DRIVER_MEMORY` | Có | Không | Memory mặc định cho driver local |
| `SPARK_EXECUTOR_MEMORY` | Có | Không | Memory mặc định cho executor local |
| `ICEBERG_CATALOG_URI` | Có | Không | REST Catalog endpoint nội bộ |
| `ICEBERG_CATALOG_NAME` | Có | Không | Tên catalog logic dùng chung |
| `TRINO_HOST` | Có | Không | Service name nội bộ |
| `TRINO_INTERNAL_PORT` | Có | Không | Port trong Compose network |
| `TRINO_HOST_PORT` | Có | Không | Port host dành cho Power BI/ODBC |
| `TRINO_CATALOG` | Có | Không | Catalog mặc định khi query |
| `TRINO_SCHEMA` | Có | Không | Schema mặc định khi query |

Credential MinIO được inject riêng vào Spark, Catalog và Trino từ environment;
không ghi trực tiếp vào `pom.xml`, DAG hoặc catalog properties.

`SPK-01` yêu cầu `SPARK_MASTER_URL=spark://spark-master:7077`, memory theo dạng
số nguyên dương kèm `m` hoặc `g`, và worker core từ `1` đến `64`. Spark master
UI bind loopback; Spark RPC và worker UI không publish ra host. Chi tiết tại
[Spark standalone contract](./SPARK_STANDALONE.md).

`QRY-01` yêu cầu `S3_REGION=us-east-1`,
`ICEBERG_CATALOG_URI=http://iceberg-rest:8181`, `TRINO_HOST=trino` và
`TRINO_INTERNAL_PORT=8080`. `ICEBERG_CATALOG_NAME` cùng `TRINO_CATALOG` phải là
`iceberg` vì tên catalog lấy từ `trino/catalog/iceberg.properties`.
`TRINO_SCHEMA` và các tên catalog/schema phải là SQL identifier chữ thường.
Catalog port chỉ nội bộ; `TRINO_HOST_PORT` được bind loopback. Chi tiết tại
[Iceberg/Trino contract](./ICEBERG_TRINO.md).

## 4. Secret hygiene gate

`scripts/check-config.sh` kiểm tra:

1. `.env` bị ignore và không được Git track; `.env.example` không bị ignore.
2. Key bắt buộc không thiếu, không trùng, không rỗng.
3. Secret trong file mẫu chỉ là `change-me-*`; file local không còn placeholder.
4. Secret local đủ dài và khác nhau theo vai trò, Fernet key đúng định dạng;
   port/numeric/URI hợp lệ và các host port không trùng nhau.
5. `WAREHOUSE_PATH` khớp `DATA_BUCKET`.
6. File chuẩn bị commit không chứa private-key header hoặc token pattern phổ
   biến; checker chỉ in tên file nghi vấn.
7. Bucket/prefix đúng cú pháp, không trùng nhau; MinIO endpoint là service nội
   bộ và credential pipeline khác root credential.
8. Catalog/Trino endpoint, S3 region, SQL identifier và tên catalog khớp
   baseline QRY-01.

Checker là guardrail, không thay thế review hoặc secret scanner của CI. Nếu
secret từng được commit, chỉ xóa file là chưa đủ: phải revoke/rotate secret và
xử lý Git history theo quy trình được nhóm duyệt.
