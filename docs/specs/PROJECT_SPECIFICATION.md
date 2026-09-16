# Đặc tả dự án Japan Earthquake ETL

| Thuộc tính | Giá trị |
|---|---|
| Trạng thái | Draft |
| Phạm vi | Phiên bản local-first |
| Ngôn ngữ xử lý chính | Java |
| Cập nhật dữ liệu dự kiến | Một lần mỗi ngày |

## 1. Bối cảnh

Dữ liệu động đất công khai có thể được truy vấn theo thời gian, vị trí, độ lớn và độ sâu. Tuy nhiên, dữ liệu API thô chưa phù hợp để phân tích trực tiếp vì có thể được cập nhật muộn, có bản ghi lặp theo mã sự kiện, dùng múi giờ khác với người xem và chưa có các nhóm phân tích thống nhất.

Project xây dựng một pipeline dữ liệu có thể chạy lại, lưu được dữ liệu gốc, chuẩn hóa dữ liệu bằng Spark và cung cấp dữ liệu phân tích cho Power BI thông qua Trino.

## 2. Mục tiêu sản phẩm

1. Thu thập được dữ liệu động đất trong vùng nghiên cứu quanh Nhật Bản theo lịch.
2. Lưu được dữ liệu gốc để truy vết và tái xử lý.
3. Tạo được dữ liệu sạch, không trùng theo mã sự kiện và giữ phiên bản cập nhật mới nhất.
4. Xuất bản dữ liệu phân tích ở tầng Gold dưới dạng bảng Iceberg.
5. Cho phép Trino truy vấn Gold và Power BI import kết quả.
6. Quan sát được trạng thái từng lần chạy, số lượng bản ghi và nguyên nhân thất bại.
7. Chạy được toàn bộ backend trên máy local bằng Docker Compose.

## 3. Đối tượng sử dụng

| Đối tượng | Nhu cầu |
|---|---|
| Thành viên phát triển | Cài đặt, chạy, kiểm thử và sửa pipeline |
| Người phân tích dữ liệu | Truy vấn dữ liệu Gold, định nghĩa KPI và xây dựng báo cáo |
| Người xem dashboard | Khám phá xu hướng theo thời gian, vị trí, độ lớn và độ sâu |
| Giảng viên/người đánh giá | Kiểm tra kiến trúc, tính đúng đắn, khả năng chạy lại và kết quả trực quan |

Hệ thống không có nghiệp vụ đăng ký người dùng hoặc phân quyền ứng dụng trong phiên bản đầu tiên. Việc bảo vệ các giao diện quản trị được xử lý ở mức cấu hình môi trường local.

## 4. Phạm vi

### 4.1. Trong phạm vi

- USGS Earthquake Catalog API là nguồn sự kiện chính.
- Dữ liệu địa giới Nhật Bản là nguồn enrichment tùy theo khả năng hoàn thành.
- Lưu trữ Bronze, Silver và Gold trên MinIO.
- Spark job viết bằng Java để làm sạch, chuẩn hóa, loại trùng và tổng hợp.
- Airflow điều phối lịch chạy, dependency, retry và kiểm tra sau xử lý.
- Gold được quản lý bằng Apache Iceberg và truy vấn bằng Trino.
- Power BI dùng ODBC ở chế độ Import.
- Môi trường chạy chính là Docker Compose trên máy local.

### 4.2. Ngoài phạm vi phiên bản đầu tiên

- Dự đoán chính xác thời gian hoặc vị trí động đất tiếp theo.
- Hệ thống cảnh báo thiên tai thời gian thực.
- Streaming hoặc near-real-time ingestion.
- High availability, multi-node production hoặc autoscaling.
- Ứng dụng web/mobile riêng cho người dùng cuối.
- Data Science hoặc Generative AI.
- Thiết kế database/DDL chi tiết và chia task triển khai.

## 5. Giả định và ràng buộc

- Máy chạy pipeline có kết nối Internet khi extract dữ liệu.
- Nguồn USGS có thể cập nhật lại sự kiện đã công bố; pipeline phải đọc chồng một khoảng thời gian gần nhất.
- Airflow và Spark không chạy nhiều job nặng đồng thời trên máy ít RAM.
- Power BI Desktop chạy trên Windows; backend có thể chạy trên cùng máy hoặc máy khác trong mạng local.
- Múi giờ lưu chuẩn là UTC; JST (`Asia/Tokyo`) được bổ sung để phân tích và hiển thị.
- Credentials chỉ đi qua biến môi trường hoặc secret mechanism, không commit vào Git.

## 6. Yêu cầu chức năng

### FR-01 — Thu thập sự kiện

Hệ thống phải truy vấn USGS theo cửa sổ thời gian và vùng nghiên cứu cấu hình được. Kết quả nguyên bản phải được lưu trước khi biến đổi.

**Chấp nhận khi:** có thể xác định lần chạy, khoảng thời gian truy vấn, object Bronze và số bản ghi nguồn.

### FR-02 — Bảo toàn Bronze

Hệ thống phải lưu phản hồi nguồn theo lần ingest và không sửa nội dung object đã ghi thành công.

**Chấp nhận khi:** một lần chạy Silver có thể truy ngược về đúng input Bronze.

### FR-03 — Chuẩn hóa Silver

Spark phải parse schema, chuẩn hóa timestamp, kiểu số, tọa độ, cờ boolean và tên trường cần thiết cho downstream.

**Chấp nhận khi:** dữ liệu Silver chỉ chứa bản ghi đáp ứng các quy tắc bắt buộc trong tài liệu chất lượng dữ liệu.

### FR-04 — Loại trùng và xử lý cập nhật muộn

Với cùng `id`, hệ thống phải giữ bản ghi có `updated` mới nhất. Chạy lại cùng cửa sổ dữ liệu không được làm tăng số bản ghi logic nếu nguồn không thay đổi.

**Chấp nhận khi:** kiểm thử rerun cho kết quả cùng tập khóa và cùng phiên bản dữ liệu.

### FR-05 — Enrichment không gian

Khi dữ liệu địa giới sẵn sàng, hệ thống gán tỉnh/khu vực gần nhất hoặc phân loại sự kiện ngoài khơi. Thiếu enrichment không được làm mất sự kiện hợp lệ.

**Chấp nhận khi:** sự kiện không match vẫn tồn tại với nhãn `Unknown`/`Offshore` theo quy ước được triển khai.

### FR-06 — Xuất bản Gold

Hệ thống phải tạo các bảng hoặc view phục vụ KPI, biểu đồ theo thời gian, vị trí, độ lớn và độ sâu. Việc xuất bản chỉ hoàn tất sau khi Iceberg commit snapshot thành công.

**Chấp nhận khi:** Trino nhìn thấy snapshot mới và truy vấn kiểm tra sau commit đạt yêu cầu.

### FR-07 — Điều phối pipeline

Airflow phải đảm bảo thứ tự Extract → Bronze → Silver → Gold → Verify; hỗ trợ retry cho lỗi tạm thời và ngăn các bước downstream chạy khi upstream thất bại.

**Chấp nhận khi:** trạng thái và log của mỗi bước hiển thị trong một DAG run.

### FR-08 — Backfill và chạy lại

Người vận hành phải có thể chọn một khoảng ngày để thu thập/xử lý lại mà không tạo bản ghi trùng hoặc công bố Gold dở dang.

**Chấp nhận khi:** quy trình trong tài liệu backfill chạy được trên dữ liệu mẫu.

### FR-09 — Truy vấn phân tích

Trino phải cho phép truy vấn Gold bằng SQL; Power BI phải import được dataset cần thiết thông qua kết nối ODBC.

**Chấp nhận khi:** một truy vấn kiểm chứng và một lần refresh Power BI hoàn tất.

### FR-10 — Ghi nhận vận hành

Mỗi lần chạy phải ghi tối thiểu: run ID, cửa sổ dữ liệu, thời gian bắt đầu/kết thúc, số input, số hợp lệ, số bị loại, số insert/update logic, snapshot Gold và trạng thái.

**Chấp nhận khi:** có thể đối soát số lượng giữa các bước từ log và truy vấn kiểm tra.

## 7. Yêu cầu phi chức năng

| ID | Thuộc tính | Yêu cầu |
|---|---|---|
| NFR-01 | Tính đúng đắn | Không công bố Gold nếu kiểm tra bắt buộc thất bại |
| NFR-02 | Idempotency | Rerun cùng input tạo cùng kết quả logic |
| NFR-03 | Truy vết | Mỗi output xác định được input, run ID và thời điểm xử lý |
| NFR-04 | Khả năng phục hồi | Retry lỗi tạm thời; backfill thủ công cho khoảng ngày xác định |
| NFR-05 | Bảo mật | Không hard-code secret; không public MinIO, Catalog, Spark và PostgreSQL |
| NFR-06 | Khả năng bảo trì | Tách extract, Silver, Gold và verify; cấu hình ngoài mã nguồn |
| NFR-07 | Tài nguyên | Có thể chạy tuần tự trên máy 16 GiB RAM; 8 GiB là chế độ thử nghiệm có giới hạn |
| NFR-08 | Hiệu năng | Hoàn tất trước cửa sổ refresh Power BI được cấu hình; ngưỡng chính xác sẽ đo sau khi có dữ liệu mẫu |
| NFR-09 | Tính quan sát | Có log có cấu trúc và metric số lượng bản ghi cho từng bước |

## 8. Tiêu chí hoàn thành phiên bản đầu tiên

- Một DAG run lấy được dữ liệu của ngày UTC trước đó và ghi Bronze.
- Spark tạo Silver hợp lệ, loại trùng và giữ bản cập nhật mới nhất.
- Spark commit Gold Iceberg; Trino truy vấn được snapshot mới.
- Các data quality gate bắt buộc đều đạt.
- Power BI refresh được và hiển thị ba trang dashboard đã đặc tả.
- Rerun cùng ngày không tạo duplicate logic.
- Backfill một khoảng ngày nhỏ hoàn tất và không làm hỏng dữ liệu đã có.
- Một thành viên mới có thể làm theo runbook để khởi động và chạy thử hệ thống.

## 9. Rủi ro chính

| Rủi ro | Ảnh hưởng | Hướng xử lý |
|---|---|---|
| USGS timeout/rate limit | Thiếu dữ liệu lần chạy | Retry có backoff, giới hạn cửa sổ query, backfill |
| Driver ODBC không tương thích đầy đủ | Power BI không refresh | Kiểm thử sớm, chuẩn bị driver tương thích khác |
| Thiếu RAM khi chạy nhiều service | Container bị kill/job chậm | Giới hạn concurrency, chạy Spark và BI refresh lệch giờ |
| Spatial join phức tạp | Trễ phạm vi chính | Giữ tọa độ; enrichment là bước có thể tắt |
| Nguồn cập nhật sự kiện cũ | Số liệu thay đổi sau công bố | Overlap window và chọn `updated` mới nhất |
| Lỗi giữa ghi file và commit | Dữ liệu mồ côi | Chỉ coi Gold thành công sau commit/verify; dọn file theo quy trình bảo trì |

## 10. Thuật ngữ

| Thuật ngữ | Ý nghĩa trong project |
|---|---|
| Bronze | Dữ liệu nguồn nguyên bản, bất biến theo lần ingest |
| Silver | Dữ liệu đã parse, chuẩn hóa, kiểm tra và loại trùng |
| Gold | Dữ liệu sẵn sàng cho truy vấn phân tích |
| Snapshot | Phiên bản bảng Iceberg được commit nguyên tử |
| Run ID | Mã duy nhất của một lần thực thi pipeline |
| Data interval | Khoảng thời gian dữ liệu mà một DAG run chịu trách nhiệm |
| Backfill | Chạy pipeline cho một hoặc nhiều khoảng ngày trong quá khứ |
| Late update | Sự kiện đã tồn tại nhưng nguồn cập nhật lại sau đó |

