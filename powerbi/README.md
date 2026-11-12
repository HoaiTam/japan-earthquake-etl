# Power BI Assets & Connectivity Kit (BI-01)

Thư mục chứa các tài nguyên kết nối Power BI Desktop với Trino Lakehouse phục vụ xây dựng báo cáo phân tích động đất Nhật Bản theo hợp đồng [CON-03](../docs/specs/SILVER_GOLD_DATA_MODEL.md).

## Cấu trúc thư mục

```
powerbi/
├── power_query/               # Bộ mã nguồn Power Query M
│   ├── Parameters.m           # Tham số cấu hình kết nối ODBC/Trino tập trung
│   ├── FactEarthquakeEvents.m # Bảng Fact sự kiện động đất chi tiết (gold.event_current)
│   ├── FactEventDaily.m       # Bảng Fact tổng hợp theo ngày (gold.v_event_daily)
│   ├── FactEventMonthly.m     # Bảng Fact tổng hợp theo tháng (gold.v_event_monthly)
│   ├── DimDate.m              # Bảng Dimension ngày tháng (gold.dim_date)
│   ├── DimRegion.m            # Bảng Dimension khu vực (gold.dim_region)
│   ├── DimMagnitudeBand.m     # Bảng Dimension phân nhóm độ lớn (gold.dim_magnitude_band)
│   ├── DimDepthBand.m         # Bảng Dimension phân nhóm độ sâu (gold.dim_depth_band)
│   └── MetadataFreshness.m    # Bảng chỉ số độ tươi và đối soát dòng dữ liệu
├── sample_schema.sql          # Schema mẫu và fixture SQL ANSI/SQLite phục vụ bàn giao sớm cho BI-02
├── sample_data.json           # Dữ liệu JSON mẫu
└── README.md                  # Hướng dẫn tổng quan
```

## Cách sử dụng trong Power BI Desktop

1. Cài đặt **Starburst / Trino ODBC Driver** (64-bit).
2. Tạo **System DSN** tên `TrinoJapanEarthquake` trỏ về `127.0.0.1:8080`, catalog `iceberg`, schema `gold`.
3. Trong Power BI Desktop:
   - Chọn **Get Data** -> **Blank Query**.
   - Mở **Advanced Editor** và dán nội dung từ file `.m` tương ứng trong thư mục `power_query/`.
   - Chọn chế độ **Import** khi tải dữ liệu vào mô hình.

Chi tiết xem tại tài liệu đặc tả [POWER_BI_CONNECTIVITY.md](../docs/specs/POWER_BI_CONNECTIVITY.md).
