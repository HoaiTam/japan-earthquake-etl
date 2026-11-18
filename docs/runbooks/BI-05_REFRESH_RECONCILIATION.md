# BI-05 — Refresh và đối soát Power BI

Checklist chuẩn bị theo [kết nối BI-01](../specs/POWER_BI_CONNECTIVITY.md) và [model BI-02, PR #145](https://github.com/HoaiTam/japan-earthquake-etl/pull/145) tại `3700a79`. Chạy trên báo cáo đầy đủ sau khi các trang BI-03 và BI-04 có trên main.

## Refresh

1. Xác nhận Trino và ODBC sẵn sàng. Dùng Power BI Desktop với DSN `TrinoJapanEarthquake`.
2. Chạy [SQL kiểm publication](../../sql/gold/bi05_publication_preflight.sql) bằng Trino. Tiếp tục khi ca published có đúng 1 publication đủ điều kiện và ca đối chứng rỗng có 0.
3. Ghim `Parameters[PinnedPublicationId]` theo publication đã xác minh. Ghi publication ID, checksum bundle, view, thời điểm publish và event count.
4. Refresh bản QA. So sánh publication ID và freshness trong Power BI với SQL. Chạy SQL KPI của BI-02 với cùng serving view và giá trị slicer. Lưu kết quả SQL và giá trị hiển thị.
5. Thực hiện ca lỗi trên bản sao .pbix rồi khôi phục pin hợp lệ và chạy lại refresh. Dùng bản đã khôi phục hoặc bản gốc khi bàn giao.

## Ca nghiệm thu

| Ca | Kỳ vọng và bằng chứng |
|---|---|
| Pin QA `pub-202302-001` | Refresh đạt, metadata cùng pin, total 16 nếu fixture giữ nguyên. |
| Pin rỗng hoặc whitespace | Desktop báo thiếu pin, không chọn publication gần nhất. Ghi lỗi thực tế. |
| Pin chưa có publication PASSED | SQL xác minh không có dòng đủ điều kiện. Desktop từ chối refresh và không chuyển sang bundle khác. Nếu môi trường chỉ cung cấp ID vắng, ghi rõ phạm vi đó và chạy thêm regression commit-before-publish của BI-01. |
| Khôi phục pin | Refresh lại thành công, publication và count về đúng baseline. Cache hiển thị sau ca lỗi không được ghi là refresh thành công. |
| SQL parity | So sánh tổng canonical events, avg/max magnitude, avg depth và độ phủ nguồn ở tổng, filter kết hợp và kỳ rỗng JST. Count phải khớp tuyệt đối. Dùng sai số tuyệt đối 0.000001 cho số thực; DAX BLANK tương ứng SQL NULL. |

Báo cáo kết quả ghi từng bộ lọc, hai giá trị đối chiếu và nguyên nhân nếu lệch. Nếu pin hoặc schema thay đổi, resolve lại publication và chạy lại cả hai phía trước khi đánh dấu đạt.
