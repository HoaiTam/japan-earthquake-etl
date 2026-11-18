# BI-03 — Trang Tổng quan

Bản chuẩn bị dựa trên model [BI-02, PR #145](https://github.com/HoaiTam/japan-earthquake-etl/pull/145) tại `3700a79`. Dựng và nghiệm thu trang trên Power BI Desktop sau khi BI-02 vào main.

## Bố cục và trường dữ liệu

| Vị trí | Visual | Trường hoặc measure |
|---|---|---|
| Đầu trang | Năm KPI cards | Total Earthquakes, Average Magnitude, Maximum Magnitude, Average Depth (km), Strong Earthquakes |
| Đầu trang, dưới KPI | Slicer ngày, vùng, nguồn, band độ lớn và band độ sâu | DimDate[calendar_date], DimRegion[region_name], FactEarthquakeEvents[source_coverage_code], DimMagnitudeBand[band_label], DimDepthBand[band_label] |
| Giữa trang | Xu hướng ngày/tháng JST | Lịch DimDate theo năm → tháng → ngày, Total Earthquakes |
| Giữa trang | Phân bố độ lớn và top 5 vùng | Band độ lớn theo thứ tự model, tên vùng và Total Earthquakes; giữ Offshore và Unknown nếu có |
| Cuối trang | Sự kiện đáng chú ý | ID canonical, thời gian JST, vùng, độ lớn, độ sâu km, độ phủ nguồn; filter M ≥ 5 và sắp theo độ lớn giảm dần, thời gian giảm dần |
| Cuối trang | Độ phủ nguồn và độ mới | USGS Only, JMA Only, USGS and JMA, Gold Publication và Data Freshness (UTC) |

Dùng một fact canonical để tất cả biểu đồ sự kiện cùng nhận bộ lọc. Publication ID và freshness lấy từ MetadataFreshness và mô tả toàn bundle. Hiển thị JST trên trục sự kiện, UTC ở freshness và km trên độ sâu.

## Kiểm thử trang

Dùng lại điều kiện và query của BI-02 trên cùng publication. Các giá trị dưới đây là baseline QA từ BI-02 cho `pub-202302-001`, sẽ kiểm lại trước khi chụp evidence.

| Ca | Thao tác | Kỳ vọng |
|---|---|---|
| Toàn bộ | Xóa slicer | 16 sự kiện, Avg M 4.4875, Max M 5.3, Avg depth 92.3628125, 3 trận M ≥ 5. Tổng trend, band và độ phủ nguồn khớp tổng KPI. |
| Lọc kết hợp | Tháng 01/2023 JST, Unknown, USGS_ONLY, M4_TO_LT5, SHALLOW | 9 sự kiện, Max M 4.5, bảng sự kiện M ≥ 5 rỗng. Publication và freshness giữ pin. |
| Kỳ rỗng | Chọn tháng 02/2023 JST | Count 0, Avg/Max và Avg depth trống. |
| Khôi phục | Xóa slicer, lưu, mở lại báo cáo | Số liệu trở về baseline, nhãn và đơn vị đọc được. |

Evidence cần có ảnh các trạng thái, publication ID, giá trị slicer và kết quả SQL độc lập.
