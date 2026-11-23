# BI-03 — Đối soát trang Tổng quan

Kiểm tra ngày 20/11/2026 trên Power BI Desktop 2.159.1231.0 với [tệp PBIX](../../powerbi/japan_earthquake_model.pbix) và Trino 483 trên máy dev. Model Import của [BI-02](BI-02_SEMANTIC_MODEL.md) ghim publication `pub-202302-001`, có `verify_status=PASSED` và serving view `iceberg.gold.v_bndl20230201sha256test01_earthquake_event_current`.

## Kết quả Power BI Desktop và Trino

Dùng DAX truy vấn model của trang Tổng quan qua engine cục bộ Power BI Desktop. Đối chiếu độc lập bằng [kpi_reconciliation.sql](../../powerbi/kpi_reconciliation.sql), với cùng publication và bộ lọc. Số đếm và ID khớp chính xác. Số đo thực khớp trong sai số tuyệt đối `1e-6`.

| Ca | Bộ lọc | Số trận | TB M | Max M | TB sâu (km) | M ≥ 5 | USGS / JMA / cả hai |
|---|---|---:|---:|---:|---:|---:|---|
| Baseline | Tất cả | 16 | 4.487500 | 5.3 | 92.362813 | 3 | 16 / 0 / 0 |
| Kết hợp | 01/2023 JST, `UNKNOWN`, `USGS_ONLY`, `M4_TO_LT5`, `SHALLOW` | 9 | 4.355556 | 4.5 | 35.034778 | 0 | 9 / 0 / 0 |
| Kỳ rỗng | 02/2023 JST | 0 | BLANK / NULL | BLANK / NULL | BLANK / NULL | 0 | 0 / 0 / 0 |

Cả ba ca giữ nguyên publication và freshness. Trino trả sự kiện mới nhất `2023-01-03T09:51:59.188000Z`. Model Import trả `2023-01-03T09:51:59.186667Z`, chênh dưới 2 ms do độ phân giải timestamp, cùng hiển thị `2023-01-03 09:51:59` ở độ chính xác giây. Thẻ lần làm mới báo cáo hiển thị riêng `2026-11-15 17:25:51 UTC`.

## Ảnh Power BI Desktop

- [Baseline](BI-03_OVERVIEW_BASELINE.jpg) — 16 sự kiện, 3 sự kiện M ≥ 5.
- [Lọc kết hợp](BI-03_OVERVIEW_COMBINED.jpg) — 9 sự kiện và bảng M ≥ 5 rỗng.
- [Kỳ JST rỗng](BI-03_OVERVIEW_EMPTY.jpg) — 0 sự kiện. Ảnh hiển thị bộ lọc nâng cao với cả hai cận tháng.

Hai ca lọc dùng bộ lọc trang trên bản sao QA để ghim chính xác giá trị và khoảng tháng. Mở lại PBIX bàn giao sau khi lưu cho thấy trang khôi phục 16 sự kiện và 3 sự kiện M ≥ 5. Thử trực tiếp slicer độ lớn từ 4 đến dưới 5 trên PBIX cho 13 sự kiện, ba biểu đồ cập nhật theo bộ lọc và bảng M ≥ 5 rỗng. Bỏ chọn khôi phục trang về 16 sự kiện.

## Tái kiểm tra

1. Mở PBIX với Power BI Desktop. Kiểm tra thẻ publication, tổng số trận, bảng sự kiện M ≥ 5 và đơn vị M/km. Nguồn USGS/JMA và múi giờ JST/UTC xuất hiện trên trang.
2. Chọn tháng 01/2023 JST, vùng Unknown, nguồn USGS_ONLY, dải M từ 4 đến dưới 5 và dải độ sâu Shallow. Dùng bộ lọc trang tương đương nếu cần đặt đủ khoảng tháng trong bản sao QA.
3. Đặt bộ lọc trang `DimDate[date_key]` từ `20230201` đến `20230228` để thử kỳ rỗng. Các ngày có trong DimDate của publication QA chỉ từ 01 đến 03/01 JST.
4. Trên Trino, chạy truy vấn đối soát với các tham số dưới đây theo thứ tự khai báo trong CTE `filters`. Giá trị kết thúc là cận loại trừ.

| Ca | Start JST | End JST | Region | Source | Magnitude | Depth |
|---|---:|---:|---|---|---|---|
| Baseline | NULL | NULL | NULL | NULL | NULL | NULL |
| Kết hợp | 20230101 | 20230201 | UNKNOWN | USGS_ONLY | M4_TO_LT5 | SHALLOW |
| Kỳ rỗng | 20230201 | 20230301 | NULL | NULL | NULL | NULL |

Bản PBIX bàn giao giữ bộ lọc mặc định. Đây là đối soát trên publication QA 16 trận. Nghiệm thu refresh và đối soát trên dashboard hoàn chỉnh thuộc [BI-05 — Thiết lập refresh và đối soát BI](../task/tasks/BI-05.md).
