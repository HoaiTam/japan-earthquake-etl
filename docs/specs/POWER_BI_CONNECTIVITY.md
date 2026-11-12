# BI-01 — Power BI & Trino ODBC Connectivity

Tài liệu hướng dẫn kết nối Power BI Desktop với Trino Lakehouse phục vụ xây dựng báo cáo phân tích động đất Nhật Bản theo hợp đồng [CON-03](./SILVER_GOLD_DATA_MODEL.md) và nhiệm vụ [BI-01](../task/tasks/BI-01.md).

## 1. Kiến trúc kết nối

```
+-------------------------------------------------------+
|                 Power BI Desktop                      |
|  - Chế độ: Import (VertiPaq In-Memory Engine)        |
|  - Bộ mã: Power Query M scripts (powerbi/power_query) |
+-------------------------------------------------------+
                           |
                     (ODBC 64-bit)
                           v
+-------------------------------------------------------+
|           Trino ODBC Driver (Starburst / Trino)       |
|  - DSN Name: TrinoJapanEarthquake                    |
|  - Host: 127.0.0.1 (Loopback)                         |
|  - Port: 8080 (hoặc TRINO_HOST_PORT từ Compose)       |
|  - Authentication: No Authentication (Local Dev)      |
+-------------------------------------------------------+
                           |
                      (HTTP / REST)
                           v
+-------------------------------------------------------+
|             Trino Coordinator :8080                   |
|  - Catalog: iceberg                                   |
|  - Schemas: gold, ml                                  |
|  - Serving Views: v_event_daily, v_event_monthly,     |
|                   event_current, dim_*                |
+-------------------------------------------------------+
                           |
          (Apache Iceberg REST Catalog + MinIO)
```

## 2. Các nguyên tắc bắt buộc (Core Invariants)

1. **Tuyệt đối không đọc trực tiếp Parquet (No Direct Parquet Reads):**
   - Power BI tuyệt đối **không** kết nối trực tiếp qua S3/MinIO API để đọc các file object Parquet thô.
   - Toàn bộ truy vấn phải đi qua cổng SQL của Trino (`iceberg.gold.*`). Điều này bảo đảm:
     - Tính cô lập snapshot của bảng Iceberg (chỉ đọc snapshot đã commit `Published`).
     - Bảo đảm lọc đúng các sự kiện động đất tự nhiên trong khu vực nghiên cứu (`is_earthquake = true AND is_in_study_area = true`).
     - Tránh double-count từ bảng cầu nối nguồn `event_source_bridge`.
2. **Không lưu trữ Secret / Credential (Zero Credential Leak):**
   - Các file script M (`.m`) và file `.pbix` không chứa mật khẩu, private key hay access token.
   - Kết nối sử dụng DSN hệ thống (`TrinoJapanEarthquake`) hoặc tham số môi trường (`pTrinoHost`, `pTrinoPort`) trỏ tới loopback local.
3. **Chế độ Import (VertiPaq In-Memory):**
   - Sử dụng chế độ **Import** thay vì DirectQuery. Chế độ Import lưu trữ dữ liệu nén trong VertiPaq engine, giúp các phép đo DAX (tính trung bình, phân nhóm theo chấn độ, lọc không gian) phản hồi tức thì với độ trễ < 100ms mà không gây tải liên tục lên Trino coordinator.
4. **Định kiểu dữ liệu nghiêm ngặt (Strict Explicit Typing):**
   - Mọi cột trong bảng Fact và Dimension đều được định kiểu tường minh (`Int64.Type`, `type number`, `type date`, `type datetimezone`, `type logical`, `type text`), tránh trường hợp Power BI tự suy diễn kiểu `any` gây sai lệch DAX.

## 3. Hướng dẫn thiết lập ODBC DSN trên Windows (64-bit)

### Bước 1: Cài đặt Driver
- Tải và cài đặt **Starburst ODBC Driver for Trino** (hoặc Trino ODBC Driver 64-bit).
- Đảm bảo phiên bản driver tương thích với Trino 483 (ODBC driver 64-bit phù hợp với bản cài đặt Power BI Desktop 64-bit).

### Bước 2: Cấu hình System DSN
1. Mở **ODBC Data Source Administrator (64-bit)** trên Windows (`odbcad32.exe`).
2. Chọn tab **System DSN**, nhấn **Add...** và chọn **Starburst ODBC Driver**.
3. Điền các tham số cấu hình:
   - **Data Source Name (DSN):** `TrinoJapanEarthquake`
   - **Host:** `127.0.0.1` (hoặc `localhost`)
   - **Port:** `8080` (hoặc port đã map trong `.env`: `TRINO_HOST_PORT`)
   - **Catalog:** `iceberg`
   - **Schema:** `gold`
   - **Authentication:** `No Authentication` (cho môi trường local Docker)
   - **User:** `trino` (hoặc tên người dùng của bạn)
4. Nhấn **Test** để xác nhận kết nối thành công, sau đó nhấn **OK**.

## 4. Danh mục Query Power Query M (`powerbi/power_query/`)

| Script M | Bảng / View Trino nguồn | Mục đích | Hạt dữ liệu |
|---|---|---|---|
| [Parameters.m](../../powerbi/power_query/Parameters.m) | Tham số môi trường | Quản lý DSN, Host, Port tập trung | Tham số |
| [FactEarthquakeEvents.m](../../powerbi/power_query/FactEarthquakeEvents.m) | `iceberg.gold.event_current` | Dữ liệu từng trận động đất chi tiết (bản đồ, độ sâu) | 1 dòng / canonical event |
| [FactEventDaily.m](../../powerbi/power_query/FactEventDaily.m) | `iceberg.gold.v_event_daily` | Tổng hợp nhanh theo ngày, vùng, band magnitude/depth | 1 dòng / ngày JST + dimension |
| [FactEventMonthly.m](../../powerbi/power_query/FactEventMonthly.m) | `iceberg.gold.v_event_monthly` | Tổng hợp xu hướng dài hạn theo tháng | 1 dòng / tháng JST + dimension |
| [DimDate.m](../../powerbi/power_query/DimDate.m) | `iceberg.gold.dim_date` | Chiều thời gian (năm, tháng, ngày, thứ, cuối tuần) | 1 dòng / ngày |
| [DimRegion.m](../../powerbi/power_query/DimRegion.m) | `iceberg.gold.dim_region` | Chiều địa lý vùng Nhật Bản (giữ Offshore/Unknown) | 1 dòng / vùng |
| [DimMagnitudeBand.m](../../powerbi/power_query/DimMagnitudeBand.m) | `iceberg.gold.dim_magnitude_band` | Chiều phân nhóm độ lớn (LT_3, M3-M7+) | 1 dòng / band độ lớn |
| [DimDepthBand.m](../../powerbi/power_query/DimDepthBand.m) | `iceberg.gold.dim_depth_band` | Chiều phân nhóm độ sâu (Negative, Shallow, Deep) | 1 dòng / band độ sâu |
| [MetadataFreshness.m](../../powerbi/power_query/MetadataFreshness.m) | Metadata aggregation | Thẻ đo lường độ tươi dữ liệu và thời điểm refresh | 1 dòng tổng hợp |

## 5. Bàn giao Mock Schema cho nhiệm vụ BI-02

Để hỗ trợ bạn **rosy179 (Trang)** triển khai nhiệm vụ [BI-02](../task/tasks/BI-02.md) (Xây dựng mô hình Semantic, quan hệ giữa các bảng và các chỉ số DAX) độc lập mà không cần chờ khởi động toàn bộ hạ tầng Docker/Trino/Iceberg, tệp [sample_schema.sql](../../powerbi/sample_schema.sql) đã được tạo sẵn:
- Chạy được trực tiếp trên SQLite hoặc bất kỳ SQL database nào.
- Chứa đầy đủ DDL của fact và dimensions theo đúng hợp đồng [CON-03](./SILVER_GOLD_DATA_MODEL.md).
- Có sẵn 9 sự kiện động đất thực tế mẫu phản ánh đủ các tình huống:
  - Động đất trong đất liền và ngoài khơi (`OFFSHORE`).
  - Đa nguồn (`BOTH_USGS_AND_JMA`), đơn nguồn (`USGS_ONLY`, `JMA_ONLY`).
  - Cảnh báo sóng thần (`tsunami_flag = 1`) và cảnh báo mức độ (`alert_level = 'RED'/'YELLOW'`).
  - Dữ liệu thiếu hợp lệ (`depth_km` âm, `magnitude` NULL) để kiểm tra tính toàn vẹn của Honest NULL.
  - Sự kiện phi tự nhiên và ngoài vùng nghiên cứu để kiểm tra logic lọc view.
