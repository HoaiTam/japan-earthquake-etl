# Kịch bản demo Japan Earthquake ETL

| Thuộc tính | Giá trị |
|---|---|
| Trạng thái | Draft — dùng sau khi các thành phần được triển khai |
| Thời lượng mục tiêu | 15–20 phút |
| Mục tiêu | Chứng minh pipeline chạy, dữ liệu đúng và dashboard truy vết được |

## 1. Thông điệp chính

1. Hệ thống bảo toàn dữ liệu nguồn ở Bronze.
2. Spark làm sạch và loại trùng để tạo Silver.
3. Gold chỉ được công bố bằng snapshot Iceberg hoàn chỉnh.
4. Trino cung cấp SQL cho Power BI mà không cần copy Gold sang database serving khác.
5. Pipeline chạy lại an toàn và có số liệu đối soát.

## 2. Chuẩn bị trước demo

### Môi trường

- [ ] Docker đang chạy, các service healthy.
- [ ] Airflow, MinIO Console, Spark UI và Trino client mở sẵn.
- [ ] ODBC DSN và Power BI dataset hoạt động.
- [ ] Máy không chạy job nặng ngoài demo và đã tắt sleep tự động.
- [ ] Mạng truy cập được USGS; có fixture/ Bronze dự phòng nếu mạng lỗi.

### Dữ liệu

- [ ] Có một cửa sổ dữ liệu nhỏ, ổn định để trigger trong demo.
- [ ] Có sẵn một daily run thành công gần nhất.
- [ ] Có fixture chứa duplicate/late update để minh họa idempotency nếu cần.
- [ ] Ghi lại count kỳ vọng và snapshot ID trước demo.
- [ ] Power BI có last refresh rõ ràng.

### Cửa sổ trình bày

- [ ] Zoom/font đủ lớn.
- [ ] Ẩn `.env`, password, access key và thông tin cá nhân.
- [ ] Đóng terminal/tab không liên quan.
- [ ] Chuẩn bị link tài liệu kiến trúc và quality rules.

## 3. Thứ tự demo tổng thể

| Bước | Nội dung | Thời lượng |
|---:|---|---:|
| 1 | Bài toán, phạm vi và kiến trúc | 2 phút |
| 2 | Trigger/quan sát một DAG run | 3 phút |
| 3 | Bronze → Silver → Gold và data quality | 4 phút |
| 4 | Trino query và Iceberg snapshot | 2 phút |
| 5 | Power BI dashboard | 5 phút |
| 6 | Idempotency/recovery và kết luận | 2–4 phút |

## 4. Luồng 1 — Giới thiệu bài toán và kiến trúc

### Thao tác

1. Mở [Giới thiệu đề tài](./GIOI_THIEU_DE_TAI_DONG_DAT_NHAT_BAN.md).
2. Nêu nguồn USGS, chu kỳ batch hằng ngày và mục tiêu phân tích.
3. Mở [Kiến trúc hệ thống](./SYSTEM_ARCHITECTURE.md), đi theo luồng Airflow → MinIO → Spark → Iceberg → Trino → Power BI.

### Điểm cần nói

- Airflow điều phối, không làm thay Spark.
- MinIO lưu cả raw và lakehouse data.
- Power BI không đọc file Parquet trực tiếp.
- Đây là hệ thống phân tích mô tả, không phải cảnh báo/dự báo động đất.

### Thành công khi

Người xem hiểu được input, các tầng dữ liệu, output và ranh giới ngoài phạm vi.

## 5. Luồng 2 — Một lần chạy ETL

### Thao tác

1. Mở Airflow Grid/Graph của DAG chính.
2. Chỉ ra logical date, UTC data interval và run ID.
3. Trigger một run nhỏ hoặc mở run đã chuẩn bị.
4. Theo dõi thứ tự Extract → Bronze → Silver → Gold → Verify.
5. Mở log summary của từng bước thay vì cuộn toàn bộ log.

### Bằng chứng

- Request window và feature count.
- Bronze URI/checksum.
- Silver valid/rejected/duplicate counts.
- Gold snapshot ID.
- Trino verification result.

### Thành công khi

Các task chạy đúng dependency và run chỉ success sau verify.

## 6. Luồng 3 — Bronze, Silver và chất lượng dữ liệu

### Thao tác

1. Trong MinIO, mở đúng prefix theo ingest date/run ID.
2. Cho thấy GeoJSON và ingest metadata ở Bronze.
3. Mở Silver partitions bị ảnh hưởng, không tải/hiển thị secret.
4. Đối chiếu quality summary theo [tài liệu chất lượng](./DATA_QUALITY_AND_OBSERVABILITY.md).

### Điểm cần nói

- Bronze bất biến theo lần ingest nên có thể tái xử lý.
- Silver chuẩn hóa timestamp/type và giữ phiên bản `updated` mới nhất theo `id`.
- Record lỗi không vào Silver; reason count được log, raw vẫn còn ở Bronze.

### Thành công khi

Giải thích được chênh lệch giữa source, valid, rejected, duplicate và output.

## 7. Luồng 4 — Gold snapshot và Trino

### Thao tác

1. Hiển thị snapshot ID vừa publish.
2. Chạy query tổng count, average/max magnitude và tsunami count.
3. Chạy một query group theo tháng/khu vực.
4. Chỉ ra Trino đang đọc current Iceberg snapshot.

Query mẫu cần thay tên placeholder bằng tên thật:

```sql
SELECT
    COUNT(DISTINCT earthquake_id) AS total_events,
    AVG(magnitude) AS avg_magnitude,
    MAX(magnitude) AS max_magnitude
FROM <catalog>.<schema>.<earthquake_fact_or_view>;
```

### Thành công khi

Trino trả kết quả từ Gold và các số chính khớp run summary.

## 8. Luồng 5 — Power BI dashboard

### Trang Tổng quan

- Dùng filter ngày và khu vực.
- Đối chiếu Total/Avg/Max với query Trino.
- Drill thời gian year → month → day.
- Mở sự kiện đáng chú ý.

### Trang Không gian

- Hiển thị map tâm chấn.
- Lọc magnitude/depth.
- Chỉ ra Offshore/Unknown vẫn được giữ.

### Trang Độ sâu và độ lớn

- Xem histogram và scatter.
- Giải thích đây là tương quan mô tả, không phải dự đoán.

### Thành công khi

Filter hoạt động, số liệu khớp SQL và last refresh/snapshot được hiển thị rõ.

## 9. Luồng 6 — Idempotency và recovery

Không nên rerun job dài trực tiếp nếu thời lượng demo hạn chế. Dùng một fixture nhỏ hoặc hai run đã chuẩn bị.

### Idempotency

1. Hiển thị hai run cùng input hoặc cùng interval.
2. So sánh distinct event count và KPI.
3. Chứng minh duplicate không tăng sau rerun.

### Recovery

1. Mở một run từng fail hoặc mô tả failure fixture.
2. Chỉ ra Bronze đã thành công nên retry từ Silver.
3. Nêu nguyên tắc không refresh BI cho đến khi snapshot verify đạt.

### Thành công khi

Người xem thấy pipeline có thể chạy lại theo phạm vi, không phải xóa dữ liệu làm lại từ đầu.

## 10. Fallback khi demo trực tiếp gặp lỗi

| Sự cố | Fallback |
|---|---|
| USGS/mạng lỗi | Dùng Bronze fixture có checksum và nói rõ đang replay |
| Spark job quá lâu | Mở run thành công đã chuẩn bị, chỉ trigger job nhỏ |
| Power BI refresh lỗi | Dùng dataset đã refresh và đối chiếu query Trino live |
| ODBC lỗi | Chạy SQL trực tiếp trên Trino, mở ảnh/video refresh đã chuẩn bị |
| Máy thiếu RAM | Dừng refresh/job không cần thiết, trình bày tuần tự |
| Map không tải nền | Vẫn trình bày tọa độ/bar chart, không che giấu hạn chế mạng |

Fallback phải được nói rõ; không trình bày output lưu sẵn như kết quả của run đang chạy.

## 11. Nội dung chưa nên tuyên bố hoàn thiện

- Dự đoán động đất hoặc cảnh báo thời gian thực.
- High availability/production readiness.
- Enrichment prefecture nếu tỷ lệ match chưa được kiểm chứng.
- Driver ODBC “ổn định hoàn toàn” nếu mới thử một máy.
- Backfill quy mô lớn nếu chỉ mới test fixture nhỏ.
- Data quality threshold động nếu chưa có baseline.

## 12. Bằng chứng bàn giao sau demo

- Ảnh Airflow DAG run thành công.
- Run summary với record counts.
- Snapshot ID và query verification.
- Ảnh ba trang Power BI cùng last refresh.
- Git commit/tag của phiên bản demo.
- Known issues và các phần `Deferred`.

## 13. Checklist kết thúc

- [ ] Không lộ secret trong ảnh/video/log.
- [ ] Số liệu nói trong demo có query/bằng chứng.
- [ ] Nêu rõ dữ liệu mô tả và giới hạn sử dụng.
- [ ] Dừng job thử không cần thiết.
- [ ] Backup file Power BI và ghi lại commit/snapshot demo.

