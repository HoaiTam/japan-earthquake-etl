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

- Chọn Shallow từ Tổng quan. [Không gian](bi-04/map-shallow.jpg) và [Độ sâu & Độ lớn](bi-04/do-sau-do-lon-shallow.jpg) cùng giữ lựa chọn và trả 11 sự kiện.
- Từ bảng ở Độ sâu & Độ lớn, drill-through mở một hàng `evt_164d93d37553bb8e00b4d5cd0a437731`. Trino trả JST `2023-01-03 03:47:26.446`, M 4.4, độ sâu 98.487 km, vĩ độ 36.009, kinh độ 139.9001 và USGS_ONLY. Nút Quay lại (chọn nút rồi Ctrl+Enter trong chế độ chỉnh sửa) đưa về trang nguồn. [Ảnh chi tiết](bi-04/drill-through.jpg)
- Trino xác nhận 16 cặp tọa độ. Projection chỉ phục vụ kiểm thử giảm số điểm đủ tọa độ xuống 15 và giữ 16 canonical events.

Query IDs của Trino cho publication, KPI, vùng, ma trận, sự kiện, projection và lọc Shallow lần lượt là `20261120_162653_00001_b9fu9`, `20261120_162654_00002_b9fu9`, `20261120_162709_00003_b9fu9`, `20261120_162715_00004_b9fu9`, `20261120_162732_00007_b9fu9`, `20261120_162726_00006_b9fu9` và `20261120_162737_00008_b9fu9`.

## Nghiệm thu bản đồ Desktop ngày 21/11

Truy vấn Trino 20261121_081632_00002_tm2uf trên pub-202302-001 so sánh số bản ghi và số cặp tọa độ:

| Ca | Canonical events | Đủ vĩ độ và kinh độ | Cặp tọa độ khác nhau | Số sự kiện tối đa mỗi cặp |
|---|---:|---:|---:|---:|
| Baseline | 16 | 16 | 16 | 1 |
| Shallow | 11 | 11 | 11 | 1 |

Sau khi máy thử cho phép visual bản đồ, Power BI yêu cầu dùng tổng hợp Trung bình cho vĩ độ và kinh độ khi Vị trí giữ mã sự kiện. Đã sửa hai trường này trong PBIX. Mỗi nhóm mã sự kiện ứng với một canonical event. Giữ bộ lọc tọa độ thiếu ở cấp visual và trang Tổng quan làm trang mở mặc định. So sánh cấu trúc PBIR với commit 463de77 xác nhận chỉ định nghĩa visual bản đồ thay đổi.

- [Baseline Desktop](bi-04/map-baseline.jpg) hiển thị bản đồ và tổng 16 sự kiện. [Shallow Desktop](bi-04/map-shallow.jpg) hiển thị bản đồ và tổng 11 sự kiện. Một số vòng tròn gần nhau chồng một phần theo mức zoom. Số liệu trong bảng lấy từ bản ghi và cặp tọa độ trên Trino.
- Chọn một điểm ở ca Shallow. Tooltip của Power BI cho mã evt_7499246dc0a50a7d8266ce1377ae87a5, vĩ độ 31.8534, kinh độ 131.5447, M 4.2, độ sâu 47.99 km, giờ JST 03/01/2023 00:37:15, vùng Unknown và nguồn USGS_ONLY. Trino query 20261121_084128_00001_y2brh trả đúng mã và tọa độ, độ sâu gốc 47.985 km và giờ JST 2023-01-03 00:37:15.905. Desktop hiển thị độ sâu đến hai chữ số và giờ đến giây.
- Chuột phải trực tiếp trên điểm → Truy xuất nguồn gốc → Chi tiết sự kiện mở đúng một dòng với mã trên. [Ảnh drill-through từ bản đồ](bi-04/map-drill-through.jpg). Nút Quay lại đưa về Không gian và giữ Shallow.

## SQLite fixture

[Fixture SQLite](../../powerbi/sample_schema.sql) là cơ sở dữ liệu riêng 9 sự kiện. `can-003` thuộc Offshore, có hai nguồn, cờ sóng thần và được tính một sự kiện. `can-008` thuộc Unknown, thiếu độ lớn và nằm trong dải độ sâu Negative. Projection thiếu một tọa độ giữ tổng 9 sự kiện và giảm xuống 8 điểm đủ tọa độ.

## Tệp bàn giao

Kiểm tra ZIP của PBIX đạt. Tệp có bốn trang, đúng năm nhóm slicer trên ba trang phân tích, drill-through dùng canonical_event_id và bộ lọc mặc định. Desktop đóng gói lại DataModel khi lưu. Các tệp định nghĩa TMDL giữ nguyên theo checksum và kết quả DAX khớp publication đã ghim.

Kiểm tra cấu trúc PBIR offline trả 0 lỗi và 1 cảnh báo visual Bing Maps legacy. Bản thử Azure Maps yêu cầu đăng nhập, nên PBIX bàn giao dùng visual Map như phạm vi task.
