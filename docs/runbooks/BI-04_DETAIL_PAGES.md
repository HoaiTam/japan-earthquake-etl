# BI-04 — Phân tích không gian và độ sâu–độ lớn

Bản chuẩn bị dùng trường dữ liệu của [BI-02, PR #145](https://github.com/HoaiTam/japan-earthquake-etl/pull/145) tại `3700a79`. Dựng hai trang trên bản Power BI đã có trang Tổng quan của BI-03 để cùng giữ slicer và điều hướng.

## Bố cục và trường dữ liệu

- **Không gian.** Bản đồ dùng latitude/longitude và grain `canonical_event_id`, tooltip gồm thời gian JST, vùng, độ lớn, độ sâu km và độ phủ nguồn. Bảng sự kiện và breakdown vùng/nguồn dùng measure Total Earthquakes trên cùng fact. Bộ lọc điểm có tọa độ chỉ áp dụng cho visual bản đồ.
- **Độ sâu–Độ lớn.** Scatter dùng độ lớn và depth_km, với một điểm cho mỗi canonical event đủ hai giá trị. Ma trận hàng là band độ sâu, cột là band độ lớn, giá trị là Total Earthquakes. Giữ band Unknown và Negative theo Gold. Thêm số sự kiện có báo cáo chấn độ và cờ sóng thần bằng measures BI-02.
- **Chi tiết sự kiện.** Drill-through bằng canonical_event_id, giữ filter nguồn và thời gian. Hiển thị độ lớn, depth_km, timestamp JST, tọa độ, hai cờ nguồn, chấn độ và sóng thần. Có nút quay lại. Đồng bộ năm slicer của Tổng quan trên hai trang phân tích.

## Ma trận kiểm thử

| Ca | Input | Kỳ vọng |
|---|---|---|
| Tọa độ thực | Publication QA với canonical event có latitude và longitude | Điểm theo đúng tọa độ. Chọn điểm và mở chi tiết trả đúng ID. |
| Offshore và Unknown | Fixture SQLite `powerbi/sample_schema.sql` có `can-003` ở Offshore và `can-008` ở Unknown | Hai nhóm xuất hiện trong breakdown. Magnitude thiếu của can-008 vẫn hiện trống trong chi tiết. |
| Thiếu tọa độ | Projection thử visual với một latitude NULL, giả lập lỗi đầu vào của visual ngoài contract Gold | Dòng vẫn được tính trong tổng canonical và bảng chi tiết, số điểm trên bản đồ giảm đúng một. |
| Filter chung | Tháng JST, vùng, nguồn, band độ lớn và độ sâu | Chuyển qua lại các trang vẫn giữ filter; tổng ma trận và breakdown khớp KPI cho cùng filter. |
| Một sự kiện có hai nguồn | Fixture `can-003` với USGS_JMA | Count 1, tooltip ghi nguồn USGS và JMA. |

Dùng dữ liệu mẫu để kiểm edge cases, rồi lưu riêng ảnh bản đồ và drill-through chạy thật trên Power BI Desktop. Nếu visual bản đồ chưa dùng được trên môi trường nghiệm thu, ghi rõ case còn thiếu.
