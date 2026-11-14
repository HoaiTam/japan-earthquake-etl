# Power BI Assets & Connectivity Kit (BI-01)

Thư mục chứa các tài nguyên kết nối Power BI Desktop với Trino Lakehouse phục vụ xây dựng báo cáo phân tích động đất Nhật Bản theo hợp đồng [CON-03](../docs/specs/SILVER_GOLD_DATA_MODEL.md).

## Cấu trúc thư mục

```
powerbi/
├── power_query/               # Bộ mã nguồn Power Query M
│   ├── Parameters.m           # Tham số cấu hình kết nối ODBC/Trino tập trung (HostPort = 8081, zero credential)
│   ├── ActiveBundle.m         # Truy vấn resolve và pin publication bundle (snapshot consistency gate)
│   ├── FactEarthquakeEvents.m # Bảng Fact sự kiện động đất chi tiết (v_<suffix>_earthquake_event_current)
│   ├── FactEventDaily.m       # Bảng Fact tổng hợp theo ngày tính từ fact đã pin
│   ├── FactEventMonthly.m     # Bảng Fact tổng hợp theo tháng tính từ fact đã pin
│   ├── DimDate.m              # Bảng Dimension ngày tháng (v_<suffix>_dim_date)
│   ├── DimRegion.m            # Bảng Dimension khu vực (v_<suffix>_dim_region)
│   ├── DimMagnitudeBand.m     # Bảng Dimension phân nhóm độ lớn (v_<suffix>_dim_magnitude_band)
│   ├── DimDepthBand.m         # Bảng Dimension phân nhóm độ sâu (v_<suffix>_dim_depth_band)
│   └── MetadataFreshness.m    # Bảng chỉ số độ tươi và đối soát dòng dữ liệu bám sát publication đã pin
├── sample_schema.sql          # Schema mẫu và fixture SQL ANSI/SQLite phục vụ bàn giao sớm cho BI-02
├── sample_data.json           # Dữ liệu JSON mẫu
└── README.md                  # Hướng dẫn tổng quan
```

## Cách sử dụng trong Power BI Desktop

1. Cài đặt **Starburst / Trino ODBC Driver** (64-bit).
2. Tạo **System DSN** tên `TrinoJapanEarthquake` trỏ về Host `127.0.0.1` và Port `8081` (cổng host mặc định ánh xạ từ container port 8080 qua biến `TRINO_HOST_PORT=8081` trong compose), catalog `iceberg`, schema `gold`.
3. Trong Power BI Desktop:
   - Chọn **Get Data** -> **Blank Query**.
   - Tạo query `Parameters` từ `Parameters.m`, query `ActiveBundle` từ `ActiveBundle.m`.
   - Tạo tiếp các query Fact và Dim từ các file `.m` tương ứng trong thư mục `power_query/`.
   - Chọn chế độ **Import** khi tải dữ liệu vào mô hình.

Chi tiết xem tại tài liệu đặc tả [POWER_BI_CONNECTIVITY.md](../docs/specs/POWER_BI_CONNECTIVITY.md).
