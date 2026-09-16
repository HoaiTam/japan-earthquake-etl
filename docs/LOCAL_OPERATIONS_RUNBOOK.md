# Runbook cài đặt và vận hành local

| Thuộc tính | Giá trị |
|---|---|
| Trạng thái | Draft — cập nhật lệnh chính xác sau khi có mã nguồn |
| Đối tượng | Thành viên phát triển/vận hành demo |
| Môi trường | Docker Compose trên máy local |

## 1. Mục đích

Runbook mô tả thứ tự chuẩn để chuẩn bị, khởi động, kiểm tra, chạy pipeline và xử lý lỗi. Vì project chưa có `compose.yaml`, DAG hay JAR, các lệnh có placeholder được ghi rõ; không nên copy chạy cho tới khi đã thay bằng tên thực tế.

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

## 3. File/cấu hình dự kiến

```text
project-root/
├── compose.yaml
├── .env.example
├── .env                    # không commit
├── airflow/
│   └── dags/
├── spark/
│   ├── pom.xml
│   └── src/
├── trino/
│   └── catalog/
└── docs/
```

Cấu trúc cuối cùng phải được cập nhật tại đây ngay khi scaffold project.

## 4. Nhóm biến môi trường

Không ghi giá trị thật trong tài liệu. `.env.example` cần mô tả tối thiểu:

| Nhóm | Ví dụ tên biến | Ghi chú |
|---|---|---|
| MinIO | `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY` | Secret không commit |
| Bucket | `DATA_BUCKET`, `WAREHOUSE_PATH` | Tên thống nhất giữa Spark/Trino |
| Airflow | admin user/password, executor/db connection | Chỉ publish UI cần thiết |
| Spark | master URL, memory/cores | Giới hạn phù hợp máy local |
| Iceberg | catalog URI, warehouse | Dùng cùng namespace |
| Trino | host/port/catalog/schema | Port host chỉ mở khi cần |
| Pipeline | timezone, schedule, overlap days, study area | Không hard-code trong job |

Khởi tạo `.env` sau khi file mẫu tồn tại:

```bash
cp .env.example .env
```

Sau đó thay placeholder bằng credential local mạnh và cấu hình cần thiết.

## 5. Khởi động lần đầu

### 5.1. Preflight

- Docker daemon đang chạy.
- Các port host dự kiến chưa bị chiếm.
- `.env` tồn tại và không được Git track.
- Máy còn đủ disk/RAM.
- Thư mục/volume mount có quyền phù hợp.

### 5.2. Build và khởi tạo

Sau khi `compose.yaml` tồn tại, chuỗi lệnh chuẩn dự kiến:

```bash
docker compose config
docker compose build
docker compose up -d <initialization-services>
docker compose up -d
```

`<initialization-services>` phải được thay bằng service thật, ví dụ khởi tạo bucket/Airflow. Không chạy placeholder nguyên văn.

### 5.3. Kiểm tra service

```bash
docker compose ps
docker compose logs --tail=100 <service-name>
```

Các kiểm tra logic:

- Airflow UI truy cập được và scheduler heartbeat bình thường.
- MinIO bucket/warehouse đã được tạo.
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

- Phân biệt endpoint bên trong Compose với endpoint từ host.
- Kiểm tra credential và bucket policy.
- Kiểm tra service name/DNS trong Compose network.
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

Điền bảng này sau khi `compose.yaml` được tạo:

| Dịch vụ | URL từ host | Chỉ local? | Xác thực | Trạng thái |
|---|---|---:|---|---|
| Airflow UI | TBD | Có | TBD | Draft |
| MinIO Console | TBD | Có | TBD | Draft |
| Spark UI | TBD | Có | TBD | Draft |
| Trino | TBD | Có | TBD | Draft |

Catalog, MinIO API nội bộ và metadata database không cần public lên host nếu consumer không sử dụng trực tiếp.

## 13. Việc phải cập nhật khi có mã nguồn

- Tên/DAG ID/service/port thực tế.
- Câu lệnh init/build/trigger/backfill chính xác.
- Health-check URL và expected response.
- Artifact path của Spark JAR.
- Bucket/catalog/schema/table naming.
- Backup/restore command đã kiểm thử.
- Known issues và resource profile đo được.

