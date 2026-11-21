# BI-05 — Refresh và đối soát Power BI

Checklist dùng [kết nối BI-01](../specs/POWER_BI_CONNECTIVITY.md), model [BI-02](../evidence/BI-02_SEMANTIC_MODEL.md) và [kpi_reconciliation.sql](../../powerbi/kpi_reconciliation.sql). Dùng [bản báo cáo Power BI](../../powerbi/japan_earthquake_model.pbix) từ `main` tại `5d61efd` có cả các trang [BI-03](../task/tasks/BI-03.md) và [BI-04](../task/tasks/BI-04.md) để chạy nghiệm thu Desktop.

## Refresh

1. Xác nhận Trino và ODBC sẵn sàng. Dùng Power BI Desktop với DSN `TrinoJapanEarthquake`.
2. Chạy [SQL kiểm publication](../../sql/gold/bi05_publication_preflight.sql) bằng Trino. Tiếp tục khi `published_qa` có `matching_publication_rows = 1`, `eligible_publications = 1` và `absent_control_id` có cả hai count bằng `0`. Nếu có `existing_non_passed_control`, ghi lại `pinned_id` để thử trên bản sao Desktop. Ca này cần có ít nhất một dòng khớp ID và không có dòng đủ điều kiện.
3. Ghim `Parameters[PinnedPublicationId]` theo publication đã xác minh. Ghi publication ID, checksum bundle, serving view, thời điểm publish và event count.
4. Refresh bản QA. So sánh `Gold Publication` và `Data Freshness (UTC)` (`max_event_time_utc`) trong Power BI với SQL; ghi tách riêng `Report Refreshed (UTC)`. Chạy [kpi_reconciliation.sql](../../powerbi/kpi_reconciliation.sql) của BI-02 với cùng serving view và giá trị slicer. Lưu kết quả SQL và giá trị hiển thị.
5. Thực hiện các ca lỗi trên bản sao `.pbix` rồi khôi phục pin hợp lệ và chạy lại refresh (không tính cache hiển thị sau lỗi là refresh thành công). Dùng bản đã khôi phục hoặc bản gốc khi bàn giao.

## Ca nghiệm thu

| Ca | Kỳ vọng và bằng chứng |
|---|---|
| Pin QA `pub-202302-001` | Refresh đạt trên báo cáo đủ trang BI-03/BI-04, metadata cùng pin, total 16 nếu fixture QA giữ nguyên. |
| Pin rỗng hoặc whitespace | Power BI Desktop (`ActiveBundle`) báo lỗi bắt buộc `PinnedPublicationId`, không chọn publication gần nhất. Ghi lỗi thực tế. |
| Pin ID vắng khỏi catalog | `bi05_publication_preflight.sql` (`absent_control_id`) xác minh `matching_publication_rows = 0`, `eligible_publications = 0`. Desktop từ chối refresh và không chuyển sang bundle khác. |
| Publication tồn tại nhưng chưa `PASSED` | Với ID từ `existing_non_passed_control`, SQL phải trả `eligible_publications = 0` và Desktop phải từ chối refresh. Khi `catalog_non_passed_publications = 0`, ghi ca này chưa chạy live. Fixture [BI-05](../../tests/test_bi05_preflight.py) kiểm bản ghi `PENDING`/`FAILED`; fixture [BI-01](../../tests/test_bi01_connectivity.py) kiểm `commit-before-publish`. |
| Khôi phục pin | Refresh lại thành công, publication, count và freshness về đúng baseline. Cache hiển thị sau ca lỗi không được ghi là refresh thành công. |
| SQL parity | So sánh tổng canonical events, avg/max magnitude, avg depth, độ phủ nguồn và `publication_id`/`max_event_time_utc` ở tổng, filter kết hợp và kỳ rỗng JST. Count khớp tuyệt đối; sai số tuyệt đối `0.000001` cho số thực; DAX `BLANK` tương ứng SQL `NULL`; độ phân giải Date/Time tối đa ~`3.33 ms`. |

Báo cáo kết quả ghi từng bộ lọc, hai giá trị đối chiếu và nguyên nhân nếu lệch. Nếu pin hoặc schema thay đổi, resolve lại publication và chạy lại cả hai phía trước khi đánh dấu đạt.

## Preflight Trino ngày 21/11/2026

Chạy hai tệp SQL trên Trino local ở chế độ chỉ đọc. Publication `pub-202302-001` khớp một dòng `PASSED`, có 16 sự kiện và freshness `2023-01-03T09:51:59.188Z`. ID `qa-bi05-absent-control` khớp 0 dòng và có 0 dòng đủ điều kiện. Catalog QA có 0 bản ghi chưa `PASSED`.

| Bộ lọc theo [BI-03](../evidence/BI-03_OVERVIEW.md) | Tổng | Avg M | Max M | Avg độ sâu (km) | USGS_ONLY / JMA_ONLY / USGS_JMA |
|---|---:|---:|---:|---:|---:|
| Toàn bộ | 16 | 4.487500 | 5.3 | 92.362813 | 16 / 0 / 0 |
| Kết hợp | 9 | 4.355556 | 4.5 | 35.034778 | 9 / 0 / 0 |
| Kỳ rỗng tháng 02/2023 JST | 0 | NULL | NULL | NULL | 0 / 0 / 0 |

Cả ba truy vấn trả về cùng publication và freshness. Kết quả này là mốc SQL cho lần nghiệm thu Desktop gồm từ chối các pin lỗi và refresh lại với pin hợp lệ trên báo cáo đã tích hợp BI-04.
