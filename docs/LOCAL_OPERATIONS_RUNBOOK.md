# Runbook cài đặt và vận hành local

| Thuộc tính | Giá trị |
|---|---|
| Trạng thái | Implemented từng phần — MinIO đã có lệnh kiểm chứng |
| Đối tượng | Thành viên phát triển/vận hành demo |
| Môi trường | Docker Compose trên máy local |

## 1. Mục đích

Runbook mô tả thứ tự chuẩn để chuẩn bị, khởi động, kiểm tra, chạy pipeline và
xử lý lỗi. Repository đã có scaffold, configuration contract, Compose
foundation và MinIO runtime. Airflow, Spark, Catalog, Trino, DAG và JAR chưa có;
placeholder của các thành phần đó không nên copy chạy.

## 2. Yêu cầu máy

| Thành phần | Khuyến nghị |
|---|---|
| CPU | Từ 4 core |
| RAM | 16 GiB trở lên; 8 GiB chỉ thử nghiệm và chạy tuần tự |
| Dung lượng trống | Từ 50 GiB |
| Phần mềm | Git, Docker Engine/Desktop, Docker Compose plugin |
| Power BI | Power BI Desktop trên Windows và ODBC driver tương thích Trino |
| Mạng | Truy cập được USGS API khi extract |

Kiểm tra công cụ:

```bash
git --version
docker --version
docker compose version
```

## 3. Cấu trúc repository hiện tại

```text
project-root/
├── .env.example
├── compose.yaml
├── airflow/
│   ├── dags/
│   └── tests/
├── compose/
│   └── minio/
├── scripts/
├── spark/
│   └── src/
│       ├── main/java/
│       └── test/
├── tests/
│   ├── fixtures/
│   └── integration/
├── trino/
│   └── catalog/
└── docs/
```

Kiểm tra scaffold từ project root:

```bash
./scripts/check-repository-layout.sh
```

Mount source/target, ownership module và các artifact chưa được tạo được chốt
tại [Repository layout và mount contract](./specs/REPOSITORY_LAYOUT.md). Hiện đã
có MinIO runtime nhưng chưa có Airflow/Spark/query runtime, Maven Wrapper hoặc
`pom.xml`; các artifact đó thuộc task tiếp theo. Network, volume lifecycle và
dependency policy nằm tại
[Compose foundation contract](./specs/COMPOSE_FOUNDATION.md). Contract
bucket/prefix, credential và bootstrap nằm tại
[MinIO storage contract](./specs/MINIO_STORAGE.md).

## 4. Nhóm biến môi trường

Không ghi giá trị thật trong tài liệu. Contract đầy đủ và phân loại biến nhạy
cảm nằm tại
[Configuration and secret contract](./specs/CONFIGURATION_AND_SECRETS.md).
Các nhóm cấu hình hiện có:

| Nhóm | Ví dụ tên biến | Ghi chú |
|---|---|---|
| MinIO | `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY` | Secret không commit |
| Bucket | `DATA_BUCKET`, `WAREHOUSE_PATH` | Tên thống nhất giữa Spark/Trino |
| Airflow | admin user/password, executor/db connection | Chỉ publish UI cần thiết |
| Spark | master URL, memory/cores | Giới hạn phù hợp máy local |
| Iceberg | catalog URI, warehouse | Dùng cùng namespace |
| Trino | host/port/catalog/schema | Port host chỉ mở khi cần |
| Pipeline | timezone, schedule, overlap days, study area | Không hard-code trong job |

Khởi tạo `.env`:

```bash
cp .env.example .env
```

Sau đó thay toàn bộ placeholder `change-me-*` bằng credential local mạnh rồi
chạy:

```bash
./scripts/check-config.sh --require-local
```

Checker không in giá trị secret. Không khởi động service nếu kiểm tra thất bại.

## 5. Khởi động lần đầu

### 5.1. Preflight

- Docker daemon đang chạy.
- Các port host dự kiến chưa bị chiếm.
- `.env` tồn tại và không được Git track.
- `./scripts/check-config.sh --require-local` thành công.
- `./scripts/check-compose.sh` thành công.
- `./scripts/check-minio.sh` thành công.
- Máy còn đủ disk/RAM.
- Thư mục/volume mount có quyền phù hợp.

### 5.2. Validate Compose foundation

Các lệnh sau chạy được ngay và không khởi động service:

```bash
./scripts/check-compose.sh
docker compose --env-file .env config --quiet
docker compose --env-file .env --profile validation config --quiet
```

Profile `validation` chỉ giữ health/dependency/resource reference trong resolved
config; không cần chạy hoặc pull image của các contract service.

### 5.3. Khởi tạo MinIO runtime

Các lệnh này đã được triển khai và có thể chạy sau khi `.env` hợp lệ:

```bash
docker compose --env-file .env up -d minio minio-init
docker compose --env-file .env ps -a minio minio-init
./scripts/smoke-minio.sh
```

Kết quả đúng là `minio` healthy, `minio-init` thoát `0` và smoke test báo
`passed`. Script smoke để MinIO tiếp tục chạy và không xóa volume. Ba prefix
logic được tạo bằng marker `.keep`; marker không phải dữ liệu pipeline.

Khi các task runtime còn lại hoàn tất, bổ sung chúng vào lệnh startup chung;
không tự thay placeholder bằng service name chưa được merge.

### 5.4. Kiểm tra service

```bash
docker compose --env-file .env ps -a minio minio-init
docker compose --env-file .env logs --tail=100 minio
docker compose --env-file .env logs minio-init
```

Các kiểm tra logic:

- Airflow UI truy cập được và scheduler heartbeat bình thường.
- MinIO bucket/prefix đã được bootstrap và smoke ghi/đọc thành công.
- Spark master thấy worker.
- Iceberg Catalog health/readiness đạt.
- Trino hoàn tất startup và thấy catalog Iceberg.
- Airflow metadata DB healthy.

## 6. Build Spark job

Khi module Maven tồn tại:

```bash
./mvnw clean test package
```

Nếu project không cung cấp Maven Wrapper thì dùng `mvn`. Artifact path và tên JAR phải được ghi lại ở đây sau khi chốt. Không bỏ qua unit test trước khi submit JAR mới.

## 7. Chạy pipeline

### Chạy theo lịch

Airflow scheduler tạo daily run theo timezone cấu hình. Máy phải đang bật, không sleep và Docker phải hoạt động.

### Trigger thủ công

Trong Airflow UI:

1. Mở DAG chính.
2. Kiểm tra logical date/data interval.
3. Truyền config chỉ khi schema config đã được tài liệu hóa.
4. Trigger và theo dõi Graph/Grid view.
5. Không trigger run thứ hai cho cùng interval khi run đầu còn đang sửa dữ liệu.

Khi DAG ID và CLI contract được tạo, bổ sung lệnh cụ thể vào runbook thay vì dùng placeholder.

## 8. Kiểm tra sau lần chạy

- DAG run ở trạng thái success.
- Bronze có GeoJSON và ingest metadata của run.
- Silver có partition bị ảnh hưởng và quality summary đạt.
- Gold có snapshot mới.
- Trino query verify chạy được.
- Count/metric có thể đối soát.
- Power BI chỉ refresh sau khi pipeline `Published`.

Checklist nhanh:

```text
[ ] run_id và data interval đúng
[ ] Bronze object + checksum
[ ] Silver valid/rejected/duplicate counts
[ ] Gold snapshot ID
[ ] Trino verification passed
[ ] Power BI last refresh updated (nếu có refresh)
```

## 9. Dừng hệ thống

Dừng container nhưng giữ volume:

```bash
docker compose stop
```

Gỡ container/network nhưng theo mặc định vẫn giữ named volume:

```bash
docker compose down
```

Không dùng `docker compose down -v` trong quy trình thường ngày vì tùy cấu hình có thể xóa volume chứa dữ liệu. Chỉ xóa volume khi người thực hiện đã xác nhận rõ phạm vi, backup và chấp nhận mất dữ liệu local.

## 10. Backup và restore

### Cần backup

- MinIO data/warehouse volume.
- Trạng thái Catalog nếu không hoàn toàn nằm trong warehouse.
- Airflow metadata nếu cần giữ lịch sử run.
- File Power BI và cấu hình nguồn không chứa secret.
- Commit/tag mã nguồn tương ứng với snapshot/demo.

### Nguyên tắc

- Backup phải nhất quán; tránh copy volume khi đang có writer hoạt động.
- Thử restore trên môi trường tách biệt trước khi tin vào backup.
- Ghi ngày, code version, config version và snapshot ID trong manifest backup.
- Không lưu credential thật vào gói backup chia sẻ.

Lệnh backup/restore cụ thể sẽ được bổ sung sau khi loại volume và catalog backend được chốt.

## 11. Troubleshooting

### Container không healthy

1. Chạy `docker compose ps`.
2. Đọc log service và dependency ngay trước nó.
3. Kiểm tra port, env, volume permission và disk.
4. Restart đúng service; không xóa volume để thử ngẫu nhiên.

### Airflow DAG không xuất hiện

- Kiểm tra DAG file được mount đúng.
- Kiểm tra import error trong Airflow UI/log scheduler.
- Kiểm tra clock/timezone của host.
- Kiểm tra scheduler đang hoạt động.

### Spark job lỗi/worker mất kết nối

- Kiểm tra Spark master/worker và resource allocation.
- Giảm concurrency/memory nếu host bị pressure.
- Xác nhận JAR/dependency tương thích với Spark/Iceberg.
- Dùng đúng run context và input URI.

### Không truy cập được MinIO

- S3 API nội bộ là `http://minio:9000`; host chỉ truy cập Console qua
  `http://127.0.0.1:9001` mặc định.
- Kiểm tra credential và bucket policy.
- Kiểm tra service name/DNS trong Compose network.
- Chạy `./scripts/check-minio.sh`, sau đó chạy lại
  `docker compose --env-file .env run --rm minio-init`.
- Không in secret vào log khi debug.

### Trino không thấy bảng/snapshot

- Kiểm tra Catalog URI và warehouse path giữa Spark/Trino.
- Kiểm tra object storage credentials.
- Xác nhận Gold commit thực sự thành công.
- Xác nhận query dùng đúng catalog/schema/table.

### Power BI/ODBC không kết nối

- Kiểm tra Trino query được từ client khác trước.
- Kiểm tra DSN/driver architecture và network route.
- Xác nhận catalog/schema mặc định hoặc ghi đầy đủ tên bảng.
- Kiểm tra timeout với query nhỏ trước khi refresh dataset.

### Disk gần đầy

- Dừng backfill/job mới.
- Xác định volume/service sử dụng dung lượng.
- Áp dụng retention/snapshot cleanup chỉ bằng procedure đã kiểm thử.
- Không xóa thủ công file trong Iceberg warehouse.

## 12. Bảng endpoint

| Dịch vụ | URL từ host | Chỉ local? | Xác thực | Trạng thái |
|---|---|---:|---|---|
| Airflow UI | TBD | Có | TBD | Draft |
| MinIO Console | `http://127.0.0.1:9001` mặc định | Có, bind loopback | Root credential local | Implemented |
| Spark UI | TBD | Có | TBD | Draft |
| Trino | TBD | Có | TBD | Draft |

MinIO S3 API là `http://minio:9000` trong Compose network và không publish ra
host. Catalog và metadata database cũng không cần public nếu consumer không sử
dụng trực tiếp.

## 13. Việc phải cập nhật khi có mã nguồn

- Tên/DAG ID/service/port thực tế.
- Câu lệnh init/build/trigger/backfill chính xác.
- Health-check URL/expected response của service chưa triển khai; MinIO đã chốt
  `/minio/health/live`.
- Artifact path của Spark JAR.
- Bucket/catalog/schema/table naming.
- Backup/restore command đã kiểm thử.
- Known issues và resource profile đo được.
