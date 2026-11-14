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
|  - Port: 8081 (TRINO_HOST_PORT từ .env.example)       |
|  - Authentication: No Authentication (Local Dev)      |
+-------------------------------------------------------+
                           |
                      (HTTP / REST)
                           v
+-------------------------------------------------------+
|             Trino Coordinator (Container :8080)       |
|  - Catalog: iceberg                                   |
|  - Schemas: gold, ml                                  |
|  - Serving: Immutable Versioned Views                 |
|            (v_<bundle_suffix>_earthquake_event_current|
|             v_<bundle_suffix>_dim_*,                  |
|             publication_status)                       |
+-------------------------------------------------------+
                           |
          (Apache Iceberg REST Catalog + MinIO)
```

## 2. Các nguyên tắc bắt buộc (Core Invariants)

1. **Tuyệt đối không đọc trực tiếp Parquet (No Direct Parquet Reads):**
   - Power BI tuyệt đối **không** kết nối trực tiếp qua S3/MinIO API để đọc các file object Parquet thô.
   - Toàn bộ truy vấn phải đi qua cổng SQL của Trino (`iceberg.gold.*`). Điều này bảo đảm:
     - Tính cô lập snapshot của bảng Iceberg (chỉ đọc snapshot đã được `GoldPublisher` xác minh và ghi nhận `verify_status = 'PASSED'`).
     - Tự động lọc đúng các sự kiện động đất tự nhiên trong khu vực nghiên cứu (`is_natural_earthquake = true AND is_in_study_area = true`) qua view chuẩn `earthquake_event_current`.
     - Tránh double-count từ bảng cầu nối nguồn `event_source_bridge`.
2. **Không lưu trữ Secret / Credential (Zero Credential Leak):**
   - Các file script M (`.m`) và file `.pbix` không chứa mật khẩu, private key hay access token.
   - Kết nối sử dụng DSN hệ thống (`TrinoJapanEarthquake`) và record cấu hình chung `Parameters.m` trỏ tới loopback local.
3. **Chế độ Import (VertiPaq In-Memory):**
   - Sử dụng chế độ **Import** thay vì DirectQuery. Chế độ Import lưu trữ dữ liệu nén trong VertiPaq engine, giúp các phép đo DAX (tính trung bình, phân nhóm theo chấn độ, lọc không gian) phản hồi tức thì với độ trễ < 100ms mà không gây tải liên tục lên Trino coordinator.
4. **Định kiểu dữ liệu nghiêm ngặt (Strict Explicit Typing):**
   - Mọi cột trong bảng Fact và Dimension đều được định kiểu tường minh (`Int64.Type`, `type number`, `type date`, `type datetimezone`, `type logical`, `type text`), bám sát 100% schema Gold hiện hành của `GoldEventTransformer.java`.

5. **Cô lập Snapshot bất biến qua Pinned Publication (Fail-closed Snapshot Pin Invariant):**
   - Mọi bảng Fact, Dimension và Aggregate đều bắt buộc phải được ghim (pinned) vào một publication ID cụ thể đã được xác minh thành công (`verify_status = 'PASSED'`) thông qua tham số `Parameters[PinnedPublicationId]`.
   - **Cơ chế Fail-Closed:** `ActiveBundle.m` bắt buộc `PinnedPublicationId` không được rỗng. Nếu để rỗng hoặc whitespace, query sẽ báo lỗi ngay lập tức, tuyệt đối không tự động fallback sang `LIMIT 1` (latest). Điều này ngăn chặn triệt để race condition khi Power Query VertiPaq engine đánh giá các bảng độc lập trên nhiều thread/container, loại bỏ hoàn toàn nguy cơ split-brain (Fact đọc snapshot cũ, Dimension/Freshness đọc snapshot mới).
   - **Quy trình 2 bước cho phiên Refresh:**
     - **Bước 1 (Discovery):** Chạy query `DiscoveryLatestPublication.m` (hoặc truy vấn Trino) để tra cứu `publication_id` mới nhất có `verify_status = 'PASSED'`.
     - **Bước 2 (Pin & Refresh):** Điền `publication_id` vào `Parameters[PinnedPublicationId]`, sau đó nhấn Refresh toàn bộ model.

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
   - **Port:** `8081` (Port mặc định mapping từ `TRINO_HOST_PORT` trong `.env.example`/Compose ra máy Host; container lắng nghe nội bộ tại `8080`)
   - **Catalog:** `iceberg`
   - **Schema:** `gold`
   - **Authentication:** `No Authentication` (cho môi trường local Docker)
   - **User:** `trino` (hoặc tên người dùng của bạn)
4. Nhấn **Test** để xác nhận kết nối thành công, sau đó nhấn **OK**.

## 4. Danh mục Query Power Query M (`powerbi/power_query/`)

| Script M | Bảng / View Trino nguồn | Mục đích | Hạt dữ liệu |
|---|---|---|---|
| [Parameters.m](../../powerbi/power_query/Parameters.m) | Record cấu hình tập trung | Quản lý DSN, Catalog, Schema, PinnedPublicationId tập trung | Record cấu hình |
| [DiscoveryLatestPublication.m](../../powerbi/power_query/DiscoveryLatestPublication.m) | `iceberg.gold.publication_status` | Tra cứu `publication_id` mới nhất có verify_status = 'PASSED' phục vụ ghim ID trước refresh | 1 dòng metadata |
| [ActiveBundle.m](../../powerbi/power_query/ActiveBundle.m) | `iceberg.gold.publication_status` | Xác định `bundle_suffix` (24 ký tự) tương ứng với PinnedPublicationId; fail ngay nếu để rỗng | 1 dòng metadata |
| [FactEarthquakeEvents.m](../../powerbi/power_query/FactEarthquakeEvents.m) | `iceberg.gold.v_<bundle_suffix>_earthquake_event_current` | Dữ liệu từng trận động đất chi tiết (bản đồ, độ sâu) | 1 dòng / canonical event |
| [FactEventDaily.m](../../powerbi/power_query/FactEventDaily.m) | `iceberg.gold.v_<bundle_suffix>_earthquake_event_current` | Tổng hợp nhanh theo ngày, vùng, band magnitude/depth từ cùng pinned snapshot | 1 dòng / ngày JST + dimension |
| [FactEventMonthly.m](../../powerbi/power_query/FactEventMonthly.m) | `iceberg.gold.v_<bundle_suffix>_earthquake_event_current` | Tổng hợp xu hướng dài hạn theo tháng từ cùng pinned snapshot | 1 dòng / tháng JST + dimension |
| [DimDate.m](../../powerbi/power_query/DimDate.m) | `iceberg.gold.v_<bundle_suffix>_dim_date` | Chiều thời gian (`calendar_date`, `day_of_month`, `year`, `month`, ...) | 1 dòng / ngày |
| [DimRegion.m](../../powerbi/power_query/DimRegion.m) | `iceberg.gold.v_<bundle_suffix>_dim_region` | Chiều địa lý vùng Nhật Bản (`region_key`, `region_name`, `region_category`) | 1 dòng / vùng |
| [DimMagnitudeBand.m](../../powerbi/power_query/DimMagnitudeBand.m) | `iceberg.gold.v_<bundle_suffix>_dim_magnitude_band` | Chiều phân nhóm độ lớn (`band_code`, `band_label`, `sort_order`) | 1 dòng / band độ lớn |
| [DimDepthBand.m](../../powerbi/power_query/DimDepthBand.m) | `iceberg.gold.v_<bundle_suffix>_dim_depth_band` | Chiều phân nhóm độ sâu (`band_code`, `band_label`, `sort_order`) | 1 dòng / band độ sâu |
| [MetadataFreshness.m](../../powerbi/power_query/MetadataFreshness.m) | `iceberg.gold.publication_status` | Đo lường độ tươi dữ liệu (`published_at_utc`, `max_event_time_utc`) tương ứng chính xác với `ActiveBundle[publication_id]` | 1 dòng tổng hợp |

## 5. Bàn giao Mock Schema cho nhiệm vụ BI-02

Để hỗ trợ bạn **rosy179 (Trang)** triển khai nhiệm vụ [BI-02](../task/tasks/BI-02.md) (Xây dựng mô hình Semantic, quan hệ giữa các bảng và các chỉ số DAX) độc lập mà không cần chờ khởi động toàn bộ hạ tầng Docker/Trino/Iceberg, tệp [sample_schema.sql](../../powerbi/sample_schema.sql) đã được chuẩn hóa:
- Chạy được trực tiếp trên SQLite hoặc bất kỳ SQL database nào.
- Chứa đầy đủ DDL của fact và dimensions bám sát 100% schema Gold của `GoldEventTransformer.java` và `GoldPublisher.java`.
- Có sẵn 9 sự kiện động đất thực tế mẫu phản ánh đủ các tình huống:
  - Phân vùng địa lý chuẩn: `PREFECTURE` (đất liền các tỉnh), `OFFSHORE` (ngoài khơi), `UNKNOWN` (chưa phân vùng).
  - Phủ nguồn chuẩn: `USGS_JMA` (đa nguồn), `USGS_ONLY`, `JMA_ONLY`.
  - Phân định ngày JST chuẩn xác tại mốc 15:00 UTC (+9h chuyển ngày kế tiếp):
    - `can-006` (18:45 UTC ngày 15/01/2023) $\rightarrow$ ngày JST 16/01/2023.
    - `can-009` (22:15 UTC ngày 05/02/2023) $\rightarrow$ ngày JST 06/02/2023.
  - Cảnh báo sóng thần (`tsunami_flag = 1`), cảnh báo mức độ (`alert_level = 'RED'/'YELLOW'`), và cấp chấn độ Shindo (`max_intensity_code = '5+'/'4'`).
  - Dữ liệu thiếu hợp lệ (`depth_km` âm, `magnitude` NULL) để kiểm tra tính toàn vẹn của Honest NULL.
  - Bảng `publication_status` chứa snapshot metadata phục vụ đo lường data freshness.
