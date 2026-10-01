# Luồng ETL hằng ngày

| Thuộc tính | Giá trị |
|---|---|
| Trạng thái | Draft |
| Trigger | Theo lịch hoặc thủ công |
| Chu kỳ dự kiến | Mỗi ngày |
| Múi giờ điều phối | `Asia/Ho_Chi_Minh` |
| Múi giờ cửa sổ dữ liệu | UTC |

## 1. Mục đích

Tài liệu này mô tả happy path của một lần chạy pipeline. Các trường hợp chạy bù, retry có chọn lọc và khôi phục sau lỗi được mô tả tại [Backfill và phục hồi](./BACKFILL_AND_RECOVERY.md). Phạm vi nguồn, ROI, timezone và overlap tuân theo [source coverage contract](../specs/SOURCE_COVERAGE.md); layout raw và manifest tuân theo [Bronze storage contract](../specs/BRONZE_STORAGE_CONTRACT.md); schema và KPI tuân theo [Silver/Gold logical data model](../specs/SILVER_GOLD_DATA_MODEL.md).

## 2. Cửa sổ dữ liệu

Lịch tham khảo là **07:15 giờ Việt Nam**, sau khi ngày UTC trước đó đã kết thúc.

Với ngày chạy logic `D`:

- Cửa sổ bắt buộc: toàn bộ ngày UTC `D - 1`.
- Cửa sổ revision mặc định: đọc ba ngày UTC hoàn chỉnh gần nhất để nhận sự kiện được USGS sửa muộn; cách tính và seed clipping nằm trong [USGS request contract](../specs/USGS_REQUEST_CONTRACT.md).
- Mọi timestamp truyền cho API phải có timezone rõ ràng.
- Giá trị phải cấu hình được; thay default ba ngày là contract change và phải có evidence trước/sau.

Ví dụ, DAG chạy lúc `2026-09-16 07:15 Asia/Ho_Chi_Minh` có ngày UTC bắt buộc là `2026-09-15` và có thể đọc chồng từ `2026-09-13T00:00:00Z` đến trước `2026-09-16T00:00:00Z`.

JMA không phải nguồn daily event-level. Catalog index/update metadata được kiểm
tra riêng; chỉ year/release mới hoặc đổi checksum mới đi qua JMA ingest. Daily
Gold có thể dùng observation JMA đã publish cùng USGS mới.

## 3. Luồng tổng thể

```mermaid
flowchart TD
    START["DAG run bắt đầu"] --> RESOLVE["Xác định data interval và run ID"]
    RESOLVE --> CHECK_SOURCE["Kiểm tra tham số nguồn"]
    CHECK_SOURCE --> EXTRACT["Gọi USGS API"]
    EXTRACT --> VALIDATE_RESPONSE{"Response hợp lệ?"}
    VALIDATE_RESPONSE -- Không --> RETRY["Retry với backoff"]
    RETRY --> EXTRACT
    VALIDATE_RESPONSE -- Có --> WRITE_BRONZE["Ghi GeoJSON và ingest metadata vào Bronze"]
    WRITE_BRONZE --> VERIFY_BRONZE["Kiểm tra object và record count"]
    VERIFY_BRONZE --> STAGE_BRONZE["Stage đúng input theo run ID"]
    STAGE_BRONZE --> BUILD_SILVER["Spark Java build_silver"]
    BUILD_SILVER --> CHECK_SILVER{"Silver quality gate đạt?"}
    CHECK_SILVER -- Không --> FAIL["DAG failed — dừng downstream"]
    CHECK_SILVER -- Có --> PUBLISH_SILVER["Upload Silver Parquet"]
    PUBLISH_SILVER --> STAGE_SILVER["Stage partition Silver cần xử lý"]
    STAGE_SILVER --> BUILD_GOLD["Spark Java build_gold"]
    BUILD_GOLD --> COMMIT["Commit Iceberg snapshot"]
    COMMIT --> VERIFY_GOLD{"Trino verification đạt?"}
    VERIFY_GOLD -- Không --> FAIL
    VERIFY_GOLD -- Có --> READY["Đánh dấu run Published"]
    READY --> REFRESH["Power BI refresh sau cửa sổ pipeline"]
```

## 4. Chi tiết từng bước

### P01 — Khởi tạo lần chạy

**Input:** logical date, DAG data interval, cấu hình vùng nghiên cứu.

**Xử lý:**

- Sinh/nhận `run_id` duy nhất từ Airflow.
- Chuẩn hóa `window_start` và `window_end` sang UTC.
- Xác nhận `window_start < window_end` và không vượt quá thời điểm hiện tại.
- Ghi các tham số vào log có cấu trúc.

**Output:** run context dùng chung cho các task.

### P02 — Extract USGS

**Input:** run context.

**Xử lý:**

- Dùng request plan của `USG-01` để tạo các chunk half-open UTC, endpoint
  GeoJSON, bounding box Nhật Bản và `eventtype=earthquake`.
- Truyền thời gian, bounding box và các bộ lọc đã cấu hình; `USGS endtime` được
  gửi là thời điểm kết thúc độc quyền trừ 1 ms.
- Dùng `USG-02` HTTP client để đặt timeout, retry lỗi mạng/HTTP `429`/`5xx`,
  guard kích thước response và pagination khi count vượt `limit`.
- Không retry vô hạn lỗi tham số hoặc response không đúng hợp đồng.

**Output:** response nguyên bản cùng metadata request/response không chứa secret.

### P03 — Ghi Bronze

**Input:** response GeoJSON hợp lệ ở mức cú pháp từ `USG-02`; lifecycle chi
tiết nằm trong [USGS Bronze writer contract](../specs/USGS_BRONZE_WRITER_CONTRACT.md).

**Xử lý:**

- Ghi object theo đường dẫn gắn ngày ingest và run ID.
- Ghi metadata tối thiểu: source, request window, fetch time, HTTP status, feature count, checksum và run ID.
- Xác nhận object có thể đọc lại trước khi hoàn tất task.

**Output dự kiến:**

```text
bronze/usgs/ingest_date=YYYY-MM-DD/run_id=<run-id>/attempt=<nn>/response.geojson
bronze/usgs/ingest_date=YYYY-MM-DD/run_id=<run-id>/attempt=<nn>/manifest.json
```

Tên cuối cùng sẽ được xác nhận khi mã nguồn được tạo.

### P04 — Build Silver

**Input:** danh sách Bronze object được khóa theo run context.

**Xử lý:**

1. Parse `features` và ánh xạ các trường cần thiết.
2. Chuẩn hóa timestamp về UTC và tạo timestamp JST.
3. Ép kiểu magnitude, depth, latitude, longitude và flags.
4. Kiểm tra range, required field và tính hợp lệ.
5. Loại bản ghi không hợp lệ; ghi count và lý do tổng hợp vào log.
6. Deduplicate revision trong từng nguồn (`id`/`updated` cho USGS; source key/catalog release cho JMA).
7. Liên kết observation xuyên nguồn theo contract; candidate mơ hồ không được auto-merge.
8. Ghi Parquet tạm, chạy quality gate rồi mới upload Silver.

**Output dự kiến:**

```text
silver/source_observation/event_year_utc=YYYY/event_month_utc=MM/source_system=<source>/*.parquet
```

### P05 — Build Gold

**Input:** Silver partitions bị ảnh hưởng bởi cửa sổ chạy.

**Xử lý:**

- Enrich khu vực nếu boundary dataset sẵn sàng; giữ `UNKNOWN`/`OFFSHORE` khi
  không map được.
- Tạo `event_current`, source bridge và các magnitude/depth band theo contract.
- Tính các aggregate cần thiết cho dashboard nếu có lợi cho hiệu năng.
- Ghi vào bảng Iceberg theo chiến lược merge/overwrite partition đã thống nhất.
- Commit snapshot nguyên tử; lưu snapshot ID trong log/XCom phù hợp.

Thiếu enrichment không được làm mất event. Record không match phải giữ tọa độ gốc và nhận giá trị khu vực quy ước.

### P06 — Verify và publish

**Input:** snapshot ID vừa commit.

**Xử lý:**

- Truy vấn Trino để kiểm tra bảng có thể đọc.
- Đối soát số lượng, uniqueness và range bắt buộc.
- Xác nhận snapshot hiện tại đúng với snapshot vừa ghi.
- Ghi metric cuối cùng và đánh dấu run thành công.

**Output:** trạng thái `Published`, cho phép Power BI refresh.

## 5. Sequence diagram

```mermaid
sequenceDiagram
    autonumber
    participant AF as Airflow
    participant API as USGS API
    participant M as MinIO
    participant S as Spark
    participant C as Iceberg Catalog
    participant T as Trino
    participant BI as Power BI

    AF->>API: Query UTC window
    API-->>AF: GeoJSON
    AF->>M: Put Bronze + metadata
    AF->>M: Get/stage Bronze
    AF->>S: spark-submit build_silver(run context)
    S-->>AF: Silver output + metrics
    AF->>M: Put Silver Parquet
    AF->>M: Get/stage affected Silver
    AF->>S: spark-submit build_gold(run context)
    S->>C: Commit Gold snapshot
    S->>M: Write Iceberg metadata/data files
    C-->>S: Snapshot committed
    S-->>AF: Snapshot ID + metrics
    AF->>T: Verification SQL
    T->>C: Resolve current snapshot
    T->>M: Read required files
    T-->>AF: Verification result
    AF-->>BI: Run ready for scheduled refresh
    BI->>T: Import query via ODBC
```

## 6. Hợp đồng run context

Mỗi job nên nhận cùng một nhóm tham số logic, bất kể cơ chế truyền cụ thể:

| Tham số | Bắt buộc | Ý nghĩa |
|---|---|---|
| `run_id` | Có | Mã truy vết end-to-end |
| `window_start_utc` | Có | Đầu cửa sổ, inclusive |
| `window_end_utc` | Có | Cuối cửa sổ, exclusive |
| `processing_date` | Có | Ngày logic của run |
| `input_uri` | Có | Input đã được resolve, không dùng wildcard mơ hồ |
| `output_uri`/table | Có | Đích của bước xử lý |
| `is_backfill` | Có | Phân biệt run lịch thường và run chạy bù |
| `config_version` | Nên có | Phiên bản cấu hình/commit phục vụ truy vết |

## 7. Logging và metric bắt buộc

Mỗi bước phải log:

- `run_id`, task, attempt, window start/end.
- Input/output URI hoặc table logic, không log credential.
- Thời gian chạy và trạng thái.
- Số bản ghi input, parsed, valid, rejected, duplicate, inserted/updated logic và output.
- Lỗi kèm nguyên nhân gốc và đủ context để chạy lại.
- Snapshot ID sau khi Gold commit.

## 8. Điều kiện dừng

Pipeline phải dừng trước downstream nếu:

- Response không parse được hoặc thiếu cấu trúc nguồn bắt buộc.
- Bronze không ghi/đọc lại được.
- Silver vi phạm quality gate bắt buộc.
- Spark job trả exit code khác 0.
- Iceberg commit thất bại.
- Trino không đọc được snapshot mới hoặc Gold vi phạm kiểm tra bắt buộc.

Việc không có sự kiện trong một cửa sổ nhỏ không mặc định là lỗi; phải phân biệt response rỗng hợp lệ với extract thất bại.

## 9. Điều kiện hoàn tất

Một daily run chỉ hoàn tất khi:

1. Bronze raw object và manifest `BronzeReady` tồn tại, readback/checksum đã được xác nhận.
2. Silver của các partition liên quan đã publish.
3. Gold snapshot đã commit.
4. Trino verification đạt.
5. Metric đối soát đã ghi đầy đủ.
6. Run được đánh dấu thành công và có thể refresh Power BI.
