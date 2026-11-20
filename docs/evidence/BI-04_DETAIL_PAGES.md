# BI-04 — Đối soát trang chi tiết

Kiểm tra trên Power BI Desktop 2.159.1231.0 và Trino 483. [PBIX](../../powerbi/japan_earthquake_model.pbix) mở mặc định ở Tổng quan. Ba trang phân tích dùng năm nhóm slicer và mô hình Import của BI-02/BI-03.

## Publication QA thật

Trino xác nhận `pub-202302-001` có trạng thái `PASSED` và serving view `iceberg.gold.v_bndl20230201sha256test01_earthquake_event_current`. Truy vấn DAX độc lập trong Power BI Desktop và [SQL đối soát](../../powerbi/bi04_reconciliation.sql) cùng trả:

| Ca | Tổng | M ≥ 5 | Độ lớn trung bình (M) | Độ sâu trung bình (km) |
|---|---:|---:|---:|---:|
| Baseline | 16 | 3 | 4.487500 | 92.362813 |
| Shallow | 11 | 2 | 4.500000 | 32.755727 |

Baseline có 16 USGS_ONLY, 0 JMA_ONLY, 0 USGS_JMA, 0 sự kiện có báo cáo chấn độ và 0 sự kiện có cờ sóng thần. Cả 16 sự kiện thuộc Unknown. DAX và Trino khớp từng ô có dữ liệu trong ma trận:

| Độ sâu | M từ 4 đến dưới 5 | M từ 5 đến dưới 6 |
|---|---:|---:|
| Shallow | 9 | 2 |
| Intermediate | 2 | 1 |
| Deep | 2 | 0 |

- Chọn Shallow từ Tổng quan. [Không gian](bi-04/khong-gian-shallow.jpg) và [Độ sâu & Độ lớn](bi-04/do-sau-do-lon-shallow.jpg) cùng giữ lựa chọn và trả 11 sự kiện.
- Từ bảng ở Độ sâu & Độ lớn, drill-through mở một hàng `evt_164d93d37553bb8e00b4d5cd0a437731`. Trino trả JST `2023-01-03 03:47:26.446`, M 4.4, độ sâu 98.487 km, vĩ độ 36.009, kinh độ 139.9001 và USGS_ONLY. Nút Quay lại (chọn nút rồi Ctrl+Enter trong chế độ chỉnh sửa) đưa về trang nguồn. [Ảnh chi tiết](bi-04/drill-through.jpg)
- Trino xác nhận 16 cặp tọa độ. Projection chỉ phục vụ kiểm thử giảm số điểm đủ tọa độ xuống 15 và giữ 16 canonical events.
- Cài đặt bảo mật Power BI đang chặn visual bản đồ trên máy thử. Còn thiếu ảnh và kiểm tra điểm được vẽ từ visual bản đồ thật. Trạng thái BI-04 là In Progress.

Query IDs của Trino cho publication, KPI, vùng, ma trận, sự kiện, projection và lọc Shallow lần lượt là `20261120_162653_00001_b9fu9`, `20261120_162654_00002_b9fu9`, `20261120_162709_00003_b9fu9`, `20261120_162715_00004_b9fu9`, `20261120_162732_00007_b9fu9`, `20261120_162726_00006_b9fu9` và `20261120_162737_00008_b9fu9`.

## SQLite fixture

[Fixture SQLite](../../powerbi/sample_schema.sql) là cơ sở dữ liệu riêng 9 sự kiện. `can-003` thuộc Offshore, có hai nguồn, cờ sóng thần và được tính một sự kiện. `can-008` thuộc Unknown, thiếu độ lớn và nằm trong dải độ sâu Negative. Projection thiếu một tọa độ giữ tổng 9 sự kiện và giảm xuống 8 điểm đủ tọa độ.

## Tệp bàn giao

Kiểm tra ZIP của PBIX đạt. Tệp có bốn trang, đúng năm nhóm slicer trên ba trang phân tích, drill-through dùng canonical_event_id và bộ lọc mặc định. Desktop đóng gói lại DataModel khi lưu. Các tệp định nghĩa TMDL giữ nguyên theo checksum và kết quả DAX khớp publication đã ghim.

Kiểm tra cấu trúc PBIR offline trả 0 lỗi và 1 cảnh báo visual Bing Maps legacy. Bản thử Azure Maps yêu cầu đăng nhập, nên PBIX bàn giao dùng visual Map như phạm vi task.
