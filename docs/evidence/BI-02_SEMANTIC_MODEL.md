# BI-02 — Semantic model và đối soát KPI

## Mô hình

[File Power BI](../../powerbi/japan_earthquake_model.pbix) sử dụng KPI từ một dòng
mỗi `canonical_event_id` trong serving view của Gold. Quan hệ nhiều-một lọc một
chiều từ các dimension sang fact theo ngày JST, vùng, band độ lớn và band độ
sâu. Bộ lọc nguồn dùng `source_coverage_code` ở fact, với ba nhóm loại trừ nhau.
Xu hướng và KPI dùng cùng fact để chia sẻ bộ lọc. Freshness đến từ
`MetadataFreshness` của publication, độc lập với bộ lọc sự kiện.

[DAX có thể review](../../powerbi/kpi_measures.dax) và
[SQL đối chiếu](../../powerbi/kpi_reconciliation.sql) là nguồn tham chiếu cho các
phép đo. Count rỗng là 0. Average/max trên tập rỗng hoặc toàn giá trị thiếu
trả BLANK. Chấn độ bỏ mã null, rỗng và `none`.

## Chạy lại

1. Dùng DSN theo [BI-01](../specs/POWER_BI_CONNECTIVITY.md). Ghim publication
   đạt `PASSED` bằng `PinnedPublicationId` rồi chọn Refresh. Lưu file sau
   khi refresh thành công.
2. Chạy file DAX trong DAX Query View cho kết quả tổng. Để kiểm tra các
   measures đã lưu trong model, chạy riêng phần từ `EVALUATE` đến cuối file.
   Với kịch bản có lọc, bọc biểu thức `ROW(...)` sau `EVALUATE` bằng
   `CALCULATETABLE` và thêm điều kiện trên các cột bên dưới.
3. Resolve cùng publication trong Trino, dùng đúng serving view và điền cùng
   giá trị vào CTE `filters` của file SQL. So count chính xác và các giá trị
   số thực tới 6 chữ số thập phân. SQL NULL phải tương ứng DAX BLANK.

## Kết quả QA

Power BI Desktop đã refresh qua ODBC tới Trino 483 local. Trino xác nhận
publication `pub-202302-001` có `verify_status=PASSED` với suffix
`bndl20230201sha256test01`. Đây là dữ liệu QA của BI-01.

| Filter context | Count | Avg M | Max M | Avg depth km | Strong | Tsunami | Intensity | USGS / JMA / cả hai |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| Toàn publication | 16 | 4.487500 | 5.3 | 92.362813 | 3 | 0 | 0 | 16 / 0 / 0 |
| JST tháng 01/2023, UNKNOWN, M4_TO_LT5, SHALLOW, USGS_ONLY | 9 | 4.355556 | 4.5 | 35.034778 | 0 | 0 | 0 | 9 / 0 / 0 |
| JST tháng 02/2023 | 0 | BLANK / NULL | BLANK / NULL | BLANK / NULL | 0 | 0 | 0 | 0 / 0 / 0 |

Kịch bản lọc dùng `DimDate[date_key]`, `DimRegion[region_key]`,
`DimMagnitudeBand[band_code]`, `DimDepthBand[band_code]` và
`FactEarthquakeEvents[source_coverage_code]`. Power BI và Trino cho kết quả
khớp cả ba kịch bản. Publication QA live chỉ chứa USGS; regression SQL
dùng thêm fixture SQLite có 9 canonical events gồm 2 USGS_ONLY, 1 JMA_ONLY,
6 USGS_JMA. Tổng ba nhóm là 9 và lọc USGS_JMA cho count 6.

Các bước kiểm tra refresh bị từ chối và đối soát báo cáo hoàn chỉnh thuộc
[BI-05 — Thiết lập refresh và đối soát BI](../task/tasks/BI-05.md).
