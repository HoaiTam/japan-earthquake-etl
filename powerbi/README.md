# Power BI Assets & Connectivity Kit (BI-01)

Thư mục chứa các tài nguyên kết nối Power BI Desktop với Trino Lakehouse phục vụ xây dựng báo cáo phân tích động đất Nhật Bản theo hợp đồng [CON-03](../docs/specs/SILVER_GOLD_DATA_MODEL.md).

## Cấu trúc thư mục

```
powerbi/
├── power_query/               # Bộ mã nguồn Power Query M
│   ├── Parameters.m           # Tham số cấu hình kết nối ODBC/Trino tập trung (HostPort = 8081, PinnedPublicationId bắt buộc)
│   ├── DiscoveryLatestPublication.m # Tra cứu publication mới nhất có verify_status = 'PASSED' trước khi refresh
│   ├── ActiveBundle.m         # Truy vấn resolve và pin publication bundle (fail-closed snapshot consistency gate)
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
2. Tạo **System DSN** tên `TrinoJapanEarthquake` trỏ về Host `127.0.0.1` và Port `8081` (hoặc `8082` nếu dùng proxy), catalog `iceberg`, schema `gold`.
   - *Lưu ý:* Nếu dùng Starburst ODBC Driver với Trino mã nguồn mở bị lỗi gói tin JSON, khởi chạy proxy: `python scripts/trino_proxy.py --listen-port 8082 --upstream http://127.0.0.1:8081` và cấu hình DSN trỏ tới cổng `8082`.
3. Quy trình 2 bước để nạp / refresh dữ liệu (Fail-closed Snapshot Pinning):
   - **Bước 1 (Discovery):** Chạy query `DiscoveryLatestPublication` để xem danh sách / lấy `publication_id` mới nhất đã kiểm định đạt (`verify_status = 'PASSED'`).
   - **Bước 2 (Pin & Import):** Điền `publication_id` vào tham số `Parameters[PinnedPublicationId]`. `ActiveBundle` bắt buộc tham số này không được rỗng để bảo đảm toàn bộ query Fact, Dimensions, Aggregates và Freshness cùng đọc chính xác một snapshot bất biến.
   - Chọn chế độ **Import** khi tải dữ liệu vào mô hình.

Chi tiết xem tại tài liệu đặc tả [POWER_BI_CONNECTIVITY.md](../docs/specs/POWER_BI_CONNECTIVITY.md).
