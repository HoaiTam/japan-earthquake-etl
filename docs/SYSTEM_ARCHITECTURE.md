# Kiến trúc hệ thống

| Thuộc tính | Giá trị |
|---|---|
| Trạng thái | Foundation runtime đã được kiểm chứng full-stack |
| Kiểu triển khai chính | Local-first, Docker Compose |
| Mẫu kiến trúc dữ liệu | Batch Lakehouse, Bronze–Silver–Gold |

## 1. Mục tiêu kiến trúc

- Tách rõ orchestration, compute, storage, catalog, query và visualization.
- Bảo toàn dữ liệu nguồn và cho phép xử lý lại.
- Chạy lại an toàn theo ngày hoặc khoảng ngày.
- Chỉ công bố dữ liệu Gold khi một snapshot hoàn chỉnh đã commit.
- Có thể chạy trên một máy local nhưng vẫn thể hiện đúng luồng của một hệ thống dữ liệu.

## 2. Sơ đồ bối cảnh

```mermaid
flowchart LR
    USGS["USGS Earthquake Catalog API"]
    JMA["JMA Seismological Bulletin archives"]
    GEO["Nguồn địa giới Nhật Bản"]
    TEAM["Nhóm phát triển / vận hành"]
    VIEWER["Người xem báo cáo"]
    SYS["Japan Earthquake ETL Platform"]
    PBI["Power BI Desktop"]

    USGS -->|"GeoJSON qua HTTPS"| SYS
    JMA -->|"Annual ZIP qua HTTPS"| SYS
    GEO -->|"Boundary dataset"| SYS
    TEAM -->|"Cấu hình, trigger, theo dõi"| SYS
    SYS -->|"SQL qua ODBC"| PBI
    PBI -->|"Dashboard"| VIEWER
```

## 3. Sơ đồ thành phần

```mermaid
flowchart LR
    subgraph Sources["Nguồn dữ liệu"]
        USGS_API["USGS API"]
        JMA_ARCHIVE["JMA annual archives"]
        BOUNDARY["Japan Boundaries"]
    end

    subgraph Orchestration["Điều phối"]
        AIRFLOW["Apache Airflow"]
    end

    subgraph Compute["Xử lý"]
        EXTRACT["Source ingest tasks"]
        SILVER_JOB["Spark Java — Build Silver"]
        GOLD_JOB["Spark Java — Build Gold"]
        VERIFY["Quality & publish checks"]
    end

    subgraph Storage["Lưu trữ"]
        MINIO_B["MinIO — Bronze"]
        STAGE["Shared staging volume"]
        MINIO_S["MinIO — Silver Parquet"]
        ICEBERG["MinIO — Gold Iceberg"]
        CATALOG["Iceberg REST Catalog"]
    end

    subgraph Serving["Truy vấn và hiển thị"]
        TRINO["Trino"]
        POWERBI["Power BI — Import"]
    end

    USGS_API --> EXTRACT
    JMA_ARCHIVE --> EXTRACT
    BOUNDARY --> EXTRACT
    AIRFLOW -.-> EXTRACT
    AIRFLOW -.-> SILVER_JOB
    AIRFLOW -.-> GOLD_JOB
    AIRFLOW -.-> VERIFY
    EXTRACT --> MINIO_B
    MINIO_B --> STAGE
    STAGE --> SILVER_JOB
    SILVER_JOB --> MINIO_S
    MINIO_S --> STAGE
    STAGE --> GOLD_JOB
    GOLD_JOB --> ICEBERG
    GOLD_JOB <--> CATALOG
    VERIFY --> TRINO
    CATALOG --> TRINO
    ICEBERG --> TRINO
    TRINO --> POWERBI
```

Nét liền biểu diễn data flow; nét đứt biểu diễn quyền điều phối.

## 4. Trách nhiệm từng thành phần

| Thành phần | Chịu trách nhiệm | Không chịu trách nhiệm |
|---|---|---|
| Airflow | Lịch chạy, dependency, retry, backfill, log trạng thái | Xử lý dữ liệu phân tán thay Spark |
| Source ingest tasks | Lấy USGS GeoJSON/JMA ZIP, xác nhận payload, ghi Bronze và metadata/version | Làm sạch nghiệp vụ hoặc trộn hai nguồn |
| MinIO | Lưu object Bronze/Silver và warehouse Gold | Hiểu bảng logic hoặc thực thi SQL |
| Shared volume | Staging tạm giữa Airflow và Spark | Lưu dữ liệu dài hạn |
| Spark Java | Parse, validate, deduplicate, enrich, aggregate | Điều phối lịch chạy hoặc cung cấp BI endpoint |
| Iceberg | Schema, partition, metadata, snapshot và commit bảng Gold | Thực thi query cho Power BI |
| REST Catalog | Ánh xạ bảng logic tới metadata/snapshot hiện hành | Lưu toàn bộ data file |
| Trino | Thực thi SQL trên bảng Gold | Tạo bản sao serving bắt buộc |
| Power BI | Semantic model, measure, filter và visualization | Đọc từng object Parquet trực tiếp |
| PostgreSQL của Airflow | Metadata nội bộ cho Airflow nếu cấu hình dùng | Lưu fact/dimension phục vụ BI |

## 5. Ranh giới dữ liệu

### Bronze

- Chứa phản hồi nguồn nguyên bản và metadata của lần ingest.
- Append-only theo `run_id`/`ingest_date`.
- Là điểm bắt đầu cho reprocessing, không dùng trực tiếp cho dashboard.

### Silver

- Chứa observation USGS/JMA đã chuẩn hóa kiểu dữ liệu, timestamp, tọa độ và lineage.
- Deduplicate revision trong từng nguồn, sau đó lưu source-link/canonical selection để tránh double count.
- Định dạng Parquet, phân vùng theo thời gian sự kiện.

### Gold

- Chứa dữ liệu đã sẵn sàng cho KPI và dashboard.
- Được quản lý như bảng Iceberg; consumer chỉ đọc snapshot đã commit.
- Thiết kế bảng vật lý chi tiết được trì hoãn cho đến khi có mẫu dữ liệu và truy vấn thực tế.

## 6. Biên giao tiếp

| Từ | Đến | Giao tiếp | Dữ liệu |
|---|---|---|---|
| Airflow USGS ingest | USGS | HTTPS GET | Query params, GeoJSON response |
| Airflow JMA ingest | JMA | HTTPS GET | Annual ZIP, source metadata và checksum |
| Airflow/Spark | MinIO | S3-compatible API | Object Bronze/Silver/Gold |
| Airflow | Spark | `spark-submit` trong môi trường Compose | JAR, tham số run và đường dẫn staging |
| Spark/Trino | Catalog | Iceberg REST | Namespace, table metadata, snapshot |
| Trino | MinIO | S3-compatible API | Iceberg metadata và Parquet data files |
| Power BI | Trino | ODBC/SQL | Result set phục vụ semantic model |

Tên port, bucket, catalog và service phải nằm trong cấu hình. MinIO hiện dùng
service name `minio`, S3 API nội bộ `http://minio:9000`, bucket/prefix từ `.env`
và volume `minio_data`; xem
[MinIO storage contract](./specs/MINIO_STORAGE.md). Spark dùng master
`spark://spark-master:7077`, một worker và client `spark-submit`; xem
[Spark standalone contract](./specs/SPARK_STANDALONE.md).
[Iceberg/Trino contract](./specs/ICEBERG_TRINO.md) chốt Catalog nội bộ tại
`http://iceberg-rest:8181`, Trino nội bộ `trino:8080`, endpoint host loopback
và warehouse dùng chung với MinIO.
[Source coverage contract](./specs/SOURCE_COVERAGE.md) chốt vai trò USGS/JMA,
ROI, range 1984–2023 của JMA, USGS daily window và chính sách overlap.

## 7. Tính nhất quán và công bố dữ liệu

```mermaid
stateDiagram-v2
    [*] --> Extracting
    Extracting --> BronzeReady: object và metadata hợp lệ
    BronzeReady --> BuildingSilver
    BuildingSilver --> SilverReady: quality gate đạt
    SilverReady --> BuildingGold
    BuildingGold --> GoldCommitted: Iceberg commit thành công
    GoldCommitted --> Published: Trino verify đạt

    Extracting --> Failed
    BuildingSilver --> Failed
    BuildingGold --> Failed
    GoldCommitted --> Failed: verify không đạt
    Failed --> Extracting: retry/reprocess
    Failed --> BuildingSilver: dùng Bronze đã có
```

`Published` là trạng thái nghiệp vụ của pipeline, không phải một database riêng. Power BI chỉ refresh sau trạng thái này. Nếu Gold build thất bại trước commit, consumer vẫn đọc snapshot trước; nếu verify sau commit thất bại, DAG phải báo lỗi và không kích hoạt refresh.

## 8. Idempotency

- `id`/`updated` xử lý revision của USGS; JMA dùng source record key và catalog release.
- Source key chỉ deduplicate trong cùng nguồn, không phải canonical ID xuyên nguồn.
- Cửa sổ extract đọc chồng các ngày gần nhất để nhận late update.
- Bronze có thể chứa nhiều version nguồn giữa các lần ingest; Silver giữ lineage, deduplicate từng nguồn và liên kết observation trước khi Gold đếm canonical event.
- Tên output tạm phải gắn `run_id`; output chính thức chỉ được thay đổi bằng thao tác hoàn tất/commit.
- Retry phải ưu tiên dùng lại Bronze hợp lệ thay vì gọi lại API không cần thiết.

## 9. Triển khai local

```mermaid
flowchart TB
    subgraph Host["Máy local"]
        subgraph Compose["Docker Compose network"]
            AFW["Airflow API server / UI"]
            AFS["Airflow scheduler"]
            AFP["Airflow DAG processor"]
            PG["Airflow metadata DB"]
            SM["Spark master"]
            SW["Spark worker"]
            SC["Spark client"]
            MI["MinIO"]
            IC["Iceberg REST Catalog"]
            TR["Trino"]
        end
        VOL["Named volumes"]
    end
    PBI["Power BI trên Windows"]

    AFS --> PG
    AFP --> PG
    AFW --> PG
    AFS --> SC
    SC --> SM
    SM --> SW
    SC --> MI
    SC --> IC
    TR --> IC
    TR --> MI
    PBI --> TR
    AFS --> VOL
    SC --> VOL
    MI --> VOL
    PG --> VOL
    IC --> VOL
```

Chỉ các giao diện cần cho người vận hành và Power BI mới được publish ra host.
MinIO API, Catalog, Spark RPC/worker UI và metadata database giữ trong Docker
network. Spark master UI được bind loopback để theo dõi local.

Ở baseline hiện tại, MinIO Console và Airflow UI/API được bind vào loopback
host; S3 API và PostgreSQL metadata không publish. `minio-init` dùng root
credential để bootstrap, còn pipeline consumer dùng user riêng với policy giới
hạn theo bucket/prefix. Airflow dùng LocalExecutor, PostgreSQL metadata và DAG
processor độc lập theo contract AFL-01.
Spark `3.5.9` dùng Java `17`, master/worker có healthcheck và client smoke chạy
JAR `/opt/spark/jobs/japan-earthquake-etl.jar` theo contract SPK-01.
Iceberg REST fixture `1.10.1` lưu registration state bằng SQLite trong volume
`iceberg_catalog_data`; Trino `483` đọc Catalog qua REST và đọc/ghi file qua
MinIO native S3. Query smoke tạo–ghi–đọc một table Parquet rồi chỉ xóa table
kiểm thử theo contract QRY-01.
FND-01 chạy các component smoke trong cùng một Compose project và đối chiếu
health, init exit code, network membership, volume lifecycle/mount cùng startup
log; xem [Foundation smoke contract](./specs/FOUNDATION_SMOKE.md).

## 10. Bảo mật tối thiểu

- Đặt credentials trong `.env` không commit; cung cấp `.env.example` không có giá trị thật.
- Dùng tài khoản/service credential riêng theo nguyên tắc quyền tối thiểu khi công nghệ hỗ trợ.
- Không đưa access key, password hoặc connection string vào log.
- Không public các port nội bộ ra Internet trong cấu hình local.
- Giới hạn CORS/network exposure của giao diện quản trị.
- Thay credential mặc định trước khi demo qua mạng khác máy.

## 11. Quyết định kiến trúc hiện tại

| Quyết định | Lý do | Hệ quả |
|---|---|---|
| Batch hằng ngày | Phù hợp nguồn và mục tiêu phân tích | Không dùng cho cảnh báo thời gian thực |
| USGS daily + JMA historical | Có dữ liệu vận hành mới và baseline địa phương 40 năm | Phải version archive và giải quyết overlap xuyên nguồn |
| Local-first Compose | Dễ tái tạo, không tốn hạ tầng | Tài nguyên và uptime phụ thuộc máy cá nhân |
| Java cho Spark | Phù hợp yêu cầu học phần/project | Cần quản lý JAR và dependency tương thích |
| MinIO cho toàn bộ data lake | Một nền lưu trữ S3-compatible thống nhất | Cần quản lý bucket policy và volume bền vững |
| Iceberg cho Gold | Snapshot và publish an toàn hơn file rời | Thêm Catalog và cấu hình tương thích |
| Trino làm serving SQL | Power BI không phải hiểu layout object | Thêm một service và ODBC driver |
| Power BI Import | Tương tác dashboard nhẹ trên backend local | Dữ liệu chỉ mới sau lần refresh |

## 12. Điểm còn cần xác nhận khi tiếp tục ETL

- Ngưỡng candidate/match và cách hiệu chỉnh confidence cho source linking.
- Giới hạn kích thước/chia nhỏ từng USGS API request.
- Chiến lược merge Gold cho backfill và late update.
- Driver ODBC được dùng trên máy Power BI.
- Tên bảng fact/dimension Gold cuối cùng và mapping vào semantic model.
