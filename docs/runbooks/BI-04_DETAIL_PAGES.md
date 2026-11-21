# BI-04 — Phân tích không gian và độ sâu–độ lớn

Chuẩn bị trường dữ liệu và measures từ model đã merge của [BI-02 (main `8c66e8b`)](../evidence/BI-02_SEMANTIC_MODEL.md) tại [japan_earthquake_model.pbix](../../powerbi/japan_earthquake_model.pbix), dùng [kpi_measures.dax](../../powerbi/kpi_measures.dax). Báo cáo dùng trang Tổng quan đã vào main trong [BI-03](../task/tasks/BI-03.md). Năm slicer đồng bộ qua Tổng quan, Không gian và Độ sâu & Độ lớn. Trang Chi tiết sự kiện ẩn khỏi chế độ xem thông thường và mở qua Truy xuất nguồn gốc.

## Bố cục và trường dữ liệu

- **Không gian.** Bản đồ dùng Trung bình của `latitude` và `longitude` theo `canonical_event_id` ở trường Vị trí. Tooltip gồm thời gian JST, vùng, độ lớn, `depth_km` và `source_coverage_code`. Bảng sự kiện và breakdown vùng/nguồn dùng measure `Total Earthquakes` trên cùng fact. Bộ lọc điểm có tọa độ chỉ áp dụng cho visual bản đồ để các tổng `Offshore`/`Unknown` không bị mất.
- **Độ sâu–Độ lớn.** Scatter dùng `magnitude` và `depth_km`, với một điểm cho mỗi `canonical_event_id` đủ hai giá trị. Ma trận hàng là band độ sâu (`DimDepthBand[band_label]` sắp theo `sort_order`), cột là band độ lớn (`DimMagnitudeBand[band_label]` sắp theo `sort_order`), giá trị là `Total Earthquakes`. Giữ band `Unknown` và `Negative` theo Gold. Thêm số sự kiện có báo cáo chấn độ và cờ sóng thần bằng measures `Intensity Reported` và `Tsunami Flagged` của BI-02.
- **Chi tiết sự kiện.** Drill-through bằng `canonical_event_id`, giữ filter nguồn và thời gian. Hiển thị độ lớn, `depth_km`, timestamp JST, tọa độ, hai cờ nguồn, `max_intensity_code` và `tsunami_flag`. Có nút quay lại. Đồng bộ năm slicer của Tổng quan (`DimDate[calendar_date]`, `DimRegion[region_name]`, `FactEarthquakeEvents[source_coverage_code]`, `DimMagnitudeBand[band_label]`, `DimDepthBand[band_label]`) trên hai trang phân tích.

## Ma trận kiểm thử

| Ca | Input | Kỳ vọng |
|---|---|---|
| Tọa độ thực *(Power BI Desktop)* | Publication QA với canonical event có latitude và longitude | Điểm theo đúng tọa độ trên Power BI Desktop. Chọn điểm và mở chi tiết trả đúng ID. |
| Offshore và Unknown *(Fixture SQLite)* | Fixture SQLite [sample_schema.sql](../../powerbi/sample_schema.sql) có `can-003` ở Offshore và `can-008` ở Unknown | Hai nhóm xuất hiện trong breakdown. Magnitude thiếu của `can-008` vẫn hiện trống trong chi tiết (`BLANK`). |
| Thiếu tọa độ *(Fixture / projection)* | Projection thử visual với một latitude NULL, giả lập lỗi đầu vào của visual ngoài contract Gold | Dòng vẫn được tính trong tổng canonical và bảng chi tiết, số điểm trên bản đồ giảm đúng một. |
| Filter chung *(Power BI Desktop)* | Tháng JST, vùng, nguồn, band độ lớn và độ sâu | Chuyển qua lại các trang vẫn giữ filter; tổng ma trận và breakdown khớp KPI cho cùng filter. |
| Một sự kiện có hai nguồn *(Fixture SQLite + Desktop)* | Fixture `can-003` với `USGS_JMA` | Count 1, tooltip ghi nguồn USGS và JMA. |

Phân tách rõ ca fixture SQLite với ca chụp từ Power BI Desktop thật trên model [BI-02](../evidence/BI-02_SEMANTIC_MODEL.md) và trang Tổng quan [BI-03](../task/tasks/BI-03.md). Nếu visual bản đồ chưa dùng được trên môi trường nghiệm thu, ghi rõ case còn thiếu.

## Đối soát đã chạy

Dùng phần Trino QA hoặc SQLite Fixture trong [bi04_reconciliation.sql](../../powerbi/bi04_reconciliation.sql) trên đúng nguồn. Xác nhận publication trước khi đọc serving view. Chọn một mã trong bảng rồi mở Truy xuất nguồn gốc → Chi tiết sự kiện. Trong chế độ chỉnh sửa, chọn nút Quay lại rồi Ctrl+Enter để quay về trang nguồn.

## Nghiệm thu bản đồ Desktop

1. Mở PBIX trên máy Power BI Desktop được phép dùng visual bản đồ. Xác nhận publication pub-202302-001 và vào trang Không gian.
2. Chụp bản đồ với slicer mặc định rồi chọn Shallow và chụp lại. Đối chiếu số bản ghi đủ tọa độ và số cặp khác nhau với truy vấn coordinate_groups trong SQL. Điểm gần nhau có thể chồng trên màn hình tùy mức zoom.
3. Rê chuột lên một điểm để đọc mã và tooltip. Chuột phải ngay trên điểm rồi mở Chi tiết sự kiện. Dùng canonical_event_id từ trang chi tiết để lọc truy vấn chi tiết của Trino và so tọa độ, giờ JST, vùng, độ lớn, độ sâu và nguồn.
4. Lưu từng ảnh và query ID vào [bằng chứng Desktop và Trino](../evidence/BI-04_DETAIL_PAGES.md). Nếu visual hiện thông báo bị tắt, ghi rõ tình trạng và giữ nghiệm thu bản đồ đang chờ.
