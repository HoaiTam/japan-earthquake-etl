# BI-03 — Trang Tổng quan

Dựng trên model đã merge từ [BI-02 (main `8c66e8b`)](../evidence/BI-02_SEMANTIC_MODEL.md) tại [japan_earthquake_model.pbix](../../powerbi/japan_earthquake_model.pbix), dùng [kpi_measures.dax](../../powerbi/kpi_measures.dax) và [kpi_reconciliation.sql](../../powerbi/kpi_reconciliation.sql). Trang Tổng quan trong PBIX dùng model Import đã ghim publication. Kết quả kiểm thử tại [evidence BI-03](../evidence/BI-03_OVERVIEW.md).

## Bố cục và trường dữ liệu

| Vị trí | Visual | Trường hoặc measure |
|---|---|---|
| Đầu trang | Năm KPI cards | Total Earthquakes, Average Magnitude, Maximum Magnitude, Average Depth (km), Strong Earthquakes |
| Đầu trang, dưới KPI | Slicer ngày, vùng, nguồn, band độ lớn và band độ sâu | DimDate[calendar_date], DimRegion[region_name], FactEarthquakeEvents[source_coverage_code], DimMagnitudeBand[band_label], DimDepthBand[band_label] |
| Giữa trang | Xu hướng ngày/tháng JST | Lịch DimDate theo năm → tháng → ngày, Total Earthquakes |
| Giữa trang | Phân bố độ lớn và top 5 vùng | Band độ lớn theo thứ tự model, tên vùng và Total Earthquakes; giữ Offshore và Unknown nếu có |
| Cuối trang | Sự kiện đáng chú ý | ID canonical, thời gian JST, vùng, độ lớn, độ sâu km, độ phủ nguồn; filter M ≥ 5 và sắp theo độ lớn giảm dần, thời gian giảm dần |
| Cuối trang | Độ phủ nguồn và độ mới | USGS Only, JMA Only, USGS and JMA, Gold Publication và Data Freshness (UTC); hiển thị tách riêng Report Refreshed (UTC) nếu đưa thời điểm import |

Dùng một fact canonical để tất cả biểu đồ sự kiện cùng nhận bộ lọc. Publication ID và freshness lấy từ MetadataFreshness và mô tả toàn bundle; tách riêng `Report Refreshed (UTC)` khỏi `Data Freshness (UTC)`. Hiển thị rõ nguồn (USGS / JMA / cả hai), múi giờ JST trên trục sự kiện, UTC ở freshness và km trên độ sâu.

## Kiểm thử trang

Dùng lại điều kiện và query [kpi_reconciliation.sql](../../powerbi/kpi_reconciliation.sql) của BI-02 trên cùng publication. Các giá trị dưới đây dùng publication QA `pub-202302-001`. Mở báo cáo với bộ lọc mặc định và kiểm tra publication trước khi đối soát.

| Ca | Thao tác | Kỳ vọng |
|---|---|---|
| Toàn bộ | Xóa slicer | 16 sự kiện, Avg M 4.487500, Max M 5.3, Avg depth 92.362813 km (92.3628125 trước làm tròn), 3 trận M ≥ 5, USGS/JMA/cả hai = 16/0/0. Tổng trend, band và độ phủ nguồn khớp tổng KPI; `Gold Publication = pub-202302-001`. |
| Lọc kết hợp | Tháng 01/2023 JST, `UNKNOWN`, `USGS_ONLY`, `M4_TO_LT5`, `SHALLOW` | 9 sự kiện, Avg M 4.355556, Max M 4.5, Avg depth 35.034778 km, bảng sự kiện M ≥ 5 rỗng. `Gold Publication` và `Data Freshness (UTC)` giữ pin `pub-202302-001`. |
| Kỳ rỗng | Đặt bộ lọc trang DimDate[date_key] từ 20230201 đến 20230228 JST trên bản sao QA | Count 0, Avg/Max và Avg depth trả BLANK (SQL NULL). `Gold Publication` và `Data Freshness (UTC)` vẫn giữ pin `pub-202302-001`. |
| Khôi phục | Xóa slicer, lưu, mở lại báo cáo | Số liệu trở về baseline, nguồn/đơn vị/timezone đọc được. |

Xem [đối soát Power BI Desktop và Trino](../evidence/BI-03_OVERVIEW.md). Không lưu bộ lọc dùng riêng cho QA vào PBIX bàn giao.
