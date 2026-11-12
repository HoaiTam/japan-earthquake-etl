# GLD-02 — Gold Dashboard Aggregates & Serving Views

Tài liệu đặc tả kiến trúc biến đổi aggregate và serving views phục vụ dashboard phân tích động đất Nhật Bản theo hợp đồng [CON-03](./SILVER_GOLD_DATA_MODEL.md) và nhiệm vụ [GLD-02](../task/tasks/GLD-02.md).

## 1. Mục đích và phạm vi

- **Mục đích:** Cung cấp bảng/view aggregate theo ngày (`gold.event_daily`) và theo tháng (`gold.event_monthly`) phục vụ trực quan hóa trên BI/Dashboard (Trino, Power BI Stretch) với độ trễ thấp mà không cần quét toàn bộ bảng fact `gold.event_current`.
- **Ranh giới:**
  - GLD-02 là nhiệm vụ **Stretch (P2)**, thuộc Block E (Gold & Serving).
  - Không làm chậm hoặc trở thành hard dependency của đường găng ML (`MLD-*`, `EXP-*`, `MLI-*`).
  - Phục vụ dashboard tùy chọn sau khi Gold Core snapshot và ML pipeline ổn định.

## 2. Hạt dữ liệu (Granularity) và Schema

### 2.1. Hạt ngày (`gold.event_daily` & `v_event_daily`)

- **Hạt chính (Grain):** `(event_date_key_jst, region_key, magnitude_band_code, depth_band_code, source_coverage_code)`.
- **Timezone:** Chuẩn JST (`Asia/Tokyo`, UTC+9). Các sự kiện từ 15:00 UTC của ngày hôm trước được phân định chính xác sang ngày JST kế tiếp.
- **Phạm vi lọc:** Chỉ tổng hợp các sự kiện động đất tự nhiên trong khu vực nghiên cứu (`is_earthquake = true AND is_in_study_area = true`), lấy từ `gold.earthquake_event_current`.
- **Các trường đo lường (Metrics):**
  - `event_count`: Số lượng động đất hợp lệ (canonical events).
  - `magnitude_non_null_count`, `magnitude_sum`, `magnitude_avg`, `magnitude_max`: Thống kê độ lớn với chính sách Honest NULL (không ép NULL thành 0.0).
  - `depth_non_null_count`, `depth_sum`, `depth_avg`: Thống kê độ sâu (km).
  - `strong_earthquake_count`: Số trận động đất mạnh ($M \ge 6.0$).
  - `tsunami_flagged_count`: Số trận có cảnh báo sóng thần (`tsunami_flag = true`).
  - `alerted_event_count`: Số trận có cảnh báo khẩn cấp từ USGS/JMA.
  - `intensity_reported_count`: Số trận có báo cáo cấp chấn độ Shindo từ JMA.
  - `offshore_or_unknown_count`: Số trận ngoài khơi (`OFFSHORE`) hoặc chưa định danh vùng đất liền (`UNKNOWN`).

### 2.2. Hạt tháng (`gold.event_monthly` & `v_event_monthly`)

- **Hạt chính (Grain):** `(year_month_jst, region_key, magnitude_band_code, depth_band_code, source_coverage_code)`.
- **Tính cộng dồn chính xác (Additive Rollup Parity):**
  - `event_count`, `magnitude_sum`, `depth_sum`, các counts khác được cộng dồn trực tiếp từ hạt ngày:
    $$\sum_{\text{day} \in \text{month}} \text{event\_count}_{\text{day}} = \text{event\_count}_{\text{month}}$$
  - `magnitude_avg` và `depth_avg` được tính theo bình quân trọng số:
    $$\text{magnitude\_avg}_{\text{month}} = \frac{\sum \text{magnitude\_sum}}{\sum \text{magnitude\_non\_null\_count}}$$
    Tuyệt đối không tính trung bình của các trung bình (average of averages).

## 3. Các bất biến chất lượng (Quality Invariants)

1. **Tuyệt đối không double count (Zero Double-Counting):**
   - Không được join bảng fact `gold.event_current` với `event_source_bridge` để tính count. Một trận động đất được liên kết từ 2 nguồn (USGS và JMA) chỉ được tính là 1 sự kiện trong aggregate.
2. **Chính sách Honest NULL:**
   - Khi `magnitude_non_null_count = 0`, `magnitude_avg` và `magnitude_max` phải là `NULL`, không được gán bằng 0.0.
   - Khi `depth_non_null_count = 0`, `depth_avg` phải là `NULL`.
3. **Bảo toàn phân vùng địa lý:**
   - Không loại bỏ sự kiện ngoài khơi hoặc chưa xác định vùng; gán nhãn `region_category = 'OFFSHORE'` hoặc `'UNKNOWN'`.
4. **Fail-Closed Validation trong Spark Java:**
   - Transformer trong Spark Java (`GoldAggregateTransformer`) tự động kiểm tra đối soát sau khi tổng hợp:
     - Tổng `event_count` trong output phải khớp tuyệt đối với số dòng input.
     - Số lượng khóa nhóm trùng lặp phải bằng 0. Nếu vi phạm, hàm ném `IllegalStateException` ngay lập tức.

## 4. Serving Views trên Trino (`sql/gold/`)

- `sql/gold/v_event_daily.sql`: Trino DDL view tổng hợp theo ngày từ `iceberg.gold.event_current`.
- `sql/gold/v_event_monthly.sql`: Trino DDL view cộng dồn theo tháng từ `iceberg.gold.v_event_daily`.
- `sql/gold/gold_aggregate_verification_queries.sql`: Bộ 5 câu truy vấn đối soát độc lập:
  - **Q1:** Đối soát số lượng giữa Fact và Daily Aggregate (`fact_event_count - daily_agg_event_count = 0`).
  - **Q2:** Kiểm toán chống nhân đôi dòng do liên kết đa nguồn trong `event_source_bridge`.
  - **Q3:** Kiểm toán tính cộng dồn đồng nhất giữa Daily và Monthly aggregate.
  - **Q4:** Kiểm toán Honest NULL và điều kiện chia cho 0.
  - **Q5:** Kiểm toán tính duy nhất của tổ hợp khóa nhóm (0 duplicate grains).

## 5. Kiểm thử và xác thực

- **Spark Java Unit Test:** `spark/src/test/java/ie212/earthquake/spark/gold/GoldAggregateTransformerTest.java` (6 bài test kiểm tra biên JST, đối soát không double count, bảo toàn NULL, lọc sự kiện phi tự nhiên, cộng dồn tháng).
- **Offline SQLite Test:** `tests/test_gld02_aggregates.py` (kiểm tra cú pháp view Trino thích ứng, mock data đa nguồn, thực thi trọn vẹn 5 query kiểm toán).
