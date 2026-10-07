# Tài liệu dự án Japan Earthquake ETL

Thư mục này là nguồn tài liệu chính thức cho dự án **Nền tảng phân tích dữ liệu động đất tại Nhật Bản**. Bộ tài liệu mô tả hệ thống ở mức đủ để bắt đầu triển khai, kiểm thử và trình diễn; các giá trị phụ thuộc mã nguồn sẽ được cập nhật sau khi project có phiên bản chạy được.

> Trạng thái hiện tại: **Foundation đã có full-stack smoke checklist**; USGS
> request, HTTP client, Bronze writer và Airflow ingest DAG đã có contract cùng
> unit/static acceptance. Silver/Gold DAG vẫn chưa triển khai.

> Phạm vi hiện hành dùng [baseline PLN-01](./specs/MVP_SCOPE_KPI_AND_DOD.md):
> HDBSCAN lifecycle thuộc Core, static report là output bắt buộc và Power BI là
> Stretch. Các tài liệu mô tả Power BI như điều kiện MVP được hiểu theo baseline
> cũ cho đến khi `DOC-01` đồng bộ toàn bộ narrative/runbook.

## 1. Đọc tài liệu theo nhu cầu

| Nhu cầu | Tài liệu |
|---|---|
| Hiểu nhanh đề tài, mục tiêu và công nghệ | [Giới thiệu đề tài](./GIOI_THIEU_DE_TAI_DONG_DAT_NHAT_BAN.md) |
| Chốt yêu cầu và tiêu chí hoàn thành | [Đặc tả dự án](./specs/PROJECT_SPECIFICATION.md) |
| Chốt MVP, KPI và Definition of Done | [Baseline PLN-01](./specs/MVP_SCOPE_KPI_AND_DOD.md) |
| Hiểu phạm vi USGS/JMA, timezone và overlap | [Source coverage contract](./specs/SOURCE_COVERAGE.md) |
| Hiểu cách tạo daily/backfill request USGS | [USGS request contract](./specs/USGS_REQUEST_CONTRACT.md) |
| Hiểu retry, size guard và pagination USGS | [USGS HTTP client contract](./specs/USGS_HTTP_CLIENT_CONTRACT.md) |
| Hiểu validate và ghi raw USGS vào Bronze | [USGS Bronze writer contract](./specs/USGS_BRONZE_WRITER_CONTRACT.md) |
| Hiểu DAG, runner protocol và publish gate USGS | [USGS Airflow ingest contract](./specs/USGS_AIRFLOW_INGEST_CONTRACT.md) |
| Hiểu ma trận QA từ USGS đến Bronze | [USGS Bronze QA contract](./specs/USGS_BRONZE_QA_CONTRACT.md) |
| Dùng hai sample thật USGS/JMA đã khóa cho integration | [Shared real-sample catalog](./specs/SHARED_REAL_SAMPLE_DATA.md) |
| Hiểu Bronze object, manifest, checksum và retry | [Bronze storage contract](./specs/BRONZE_STORAGE_CONTRACT.md) |
| Hiểu schema Silver/Gold, null policy, bands và KPI | [Silver/Gold logical data model](./specs/SILVER_GOLD_DATA_MODEL.md) |
| Hiểu grain, lineage và lifecycle dataset/experiment ML | [ML logical data model](./specs/ML_DATA_MODEL.md) |
| Dùng fixture USGS/JMA và expected test matrix | [Shared source fixtures](../tests/fixtures/README.md) |
| Hiểu cấu trúc module và mount path | [Repository layout](./specs/REPOSITORY_LAYOUT.md) |
| Thiết lập biến môi trường và secret | [Configuration contract](./specs/CONFIGURATION_AND_SECRETS.md) |
| Hiểu Compose network, volumes và baseline | [Compose foundation](./specs/COMPOSE_FOUNDATION.md) |
| Hiểu MinIO, bucket/prefix và cách bootstrap | [MinIO storage contract](./specs/MINIO_STORAGE.md) |
| Hiểu Airflow local, metadata DB và DAG smoke | [Airflow local contract](./specs/AIRFLOW_LOCAL.md) |
| Hiểu Spark standalone, Java build và smoke | [Spark standalone contract](./specs/SPARK_STANDALONE.md) |
| Hiểu inventory, versioning và format archive JMA | [JMA archive inventory](./specs/JMA_ARCHIVE_INVENTORY.md) |
| Preview/ingest JMA theo năm, retry/reuse và summary | [JMA year backfill](./specs/JMA_YEAR_BACKFILL.md) |
| Kiểm thử JMA → Bronze thật, readback DAT-01 và rerun | [JMA Bronze QA](./specs/JMA_BRONZE_QA.md) |
| Hiểu Iceberg REST Catalog, Trino và query smoke | [Iceberg/Trino contract](./specs/ICEBERG_TRINO.md) |
| Chạy full-stack foundation smoke và xử lý lỗi | [Foundation smoke contract](./specs/FOUNDATION_SMOKE.md) |
| Hiểu các thành phần và cách chúng kết nối | [Kiến trúc hệ thống](./SYSTEM_ARCHITECTURE.md) |
| Hiểu một lần chạy ETL hằng ngày | [Luồng ETL hằng ngày](./flows/DAILY_ETL_PIPELINE.md) |
| Chạy bù, chạy lại hoặc xử lý lỗi | [Backfill và phục hồi](./flows/BACKFILL_AND_RECOVERY.md) |
| Xác định quy tắc kiểm tra dữ liệu | [Chất lượng dữ liệu](./DATA_QUALITY_AND_OBSERVABILITY.md) |
| Thiết kế báo cáo Power BI | [Đặc tả dashboard](./ANALYTICS_DASHBOARD.md) |
| Cài đặt và vận hành trên máy local | [Runbook local](./LOCAL_OPERATIONS_RUNBOOK.md) |
| Chuẩn bị buổi trình diễn | [Kịch bản demo](./DEMO_FLOWS.md) |
| Hiểu roadmap Gold → HDBSCAN → ML Iceberg | [Roadmap HDBSCAN](./task/HDBSCAN_WORKSTREAM.md) |
| Pick và theo dõi task trong 8 tuần | [Kế hoạch task](./task/README.md) |
| Chia ba luồng độc lập và kiểm tra cuối tuần 3 | [Kế hoạch tuần 3](./task/WEEK_3_PARALLEL_PLAN.md) |
| Làm việc với Git | [Git workflow](./conventions_and_workflow/GIT_WORKFLOW.md) |
| Viết commit thống nhất | [Commit convention](./conventions_and_workflow/COMMIT_CONVENTION.md) |

## 2. Cấu trúc tài liệu

```text
docs/
├── README.md
├── GIOI_THIEU_DE_TAI_DONG_DAT_NHAT_BAN.md
├── SYSTEM_ARCHITECTURE.md
├── DATA_QUALITY_AND_OBSERVABILITY.md
├── ANALYTICS_DASHBOARD.md
├── LOCAL_OPERATIONS_RUNBOOK.md
├── DEMO_FLOWS.md
├── specs/
│   ├── PROJECT_SPECIFICATION.md
│   ├── MVP_SCOPE_KPI_AND_DOD.md
│   ├── SOURCE_COVERAGE.md
│   ├── USGS_REQUEST_CONTRACT.md
│   ├── USGS_HTTP_CLIENT_CONTRACT.md
│   ├── USGS_BRONZE_WRITER_CONTRACT.md
│   ├── USGS_AIRFLOW_INGEST_CONTRACT.md
│   ├── USGS_LIVE_BRONZE_RUNBOOK.md
│   ├── USGS_BRONZE_QA_CONTRACT.md
│   ├── SHARED_REAL_SAMPLE_DATA.md
│   ├── BRONZE_STORAGE_CONTRACT.md
│   ├── SILVER_GOLD_DATA_MODEL.md
│   ├── ML_DATA_MODEL.md
│   ├── CONFIGURATION_AND_SECRETS.md
│   ├── COMPOSE_FOUNDATION.md
│   ├── AIRFLOW_LOCAL.md
│   ├── MINIO_STORAGE.md
│   ├── SPARK_STANDALONE.md
│   ├── JMA_ARCHIVE_INVENTORY.md
│   ├── ICEBERG_TRINO.md
│   ├── FOUNDATION_SMOKE.md
│   └── REPOSITORY_LAYOUT.md
├── flows/
│   ├── DAILY_ETL_PIPELINE.md
│   └── BACKFILL_AND_RECOVERY.md
├── task/
│   ├── README.md
│   ├── WORK_BLOCKS.md
│   ├── WEEK_3_PARALLEL_PLAN.md
│   ├── HDBSCAN_WORKSTREAM.md
│   └── tasks/
│       ├── README.md
│       └── <TASK-ID>.md
└── conventions_and_workflow/
    ├── GIT_WORKFLOW.md
    └── COMMIT_CONVENTION.md
```

## 3. Phạm vi tài liệu ở giai đoạn này

Đã bao gồm:

- Mục tiêu, phạm vi, yêu cầu chức năng và phi chức năng.
- Kiến trúc local-first với Airflow, Spark, MinIO, Iceberg, Trino và HDBSCAN; Power BI là phần trình bày tùy chọn.
- Happy path, backfill, retry, idempotency và các điểm kiểm soát dữ liệu.
- KPI, bố cục dashboard, quy trình demo và runbook vận hành.
- Cấu trúc repository, ownership module và mount contract.
- Backlog 8 tuần bằng Markdown, với một file riêng cho từng task để pick, review và lưu evidence.
- Quy ước Git và commit cho nhóm.

Chưa bao gồm theo phạm vi hiện tại:

- Thiết kế database chi tiết, DBML, DDL hoặc migration.
- Schema vật lý cuối cùng của các bảng Iceberg.
- Silver/Gold DAG, Gold jobs và lệnh backfill Silver/Gold thực tế; JMA Bronze
  đã có manual workflow/runner ở JMA-04 và [live QA JMA-05](./evidence/JMA-05.md)
  cho 1997/2000/2023, không phải full historical hoặc Silver/Gold Published.

## 4. Quy ước trạng thái

Mỗi yêu cầu hoặc bước kiểm tra có thể dùng một trong các trạng thái sau:

| Trạng thái | Ý nghĩa |
|---|---|
| `Draft` | Đã thiết kế nhưng chưa được kiểm chứng bằng mã nguồn |
| `Implemented` | Đã có mã triển khai |
| `Verified` | Đã chạy và có bằng chứng kiểm thử/demo |
| `Deferred` | Chủ động để ngoài phiên bản hiện tại |

Khi chức năng được triển khai, pull request phải cập nhật tài liệu liên quan hoặc ghi rõ lý do không cần cập nhật.

## 5. Nguyên tắc duy trì

- Không ghi secret, access key, password hoặc endpoint riêng tư vào tài liệu.
- Dùng đường dẫn tương đối khi liên kết giữa các file trong repository.
- Sơ đồ ưu tiên Mermaid để có thể review cùng mã nguồn.
- Nội dung chưa được xác nhận phải ghi rõ là `dự kiến`, `đề xuất` hoặc `TBD`.
- Nếu tài liệu và hệ thống chạy thực tế khác nhau, hành vi đã được kiểm thử là nguồn để sửa lại tài liệu; không âm thầm giữ hai phiên bản mô tả mâu thuẫn.

## 6. Tài liệu tham khảo

- [USGS Earthquake Catalog API](https://earthquake.usgs.gov/fdsnws/event/1/)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Apache Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
- [Trino Iceberg connector](https://trino.io/docs/current/connector/iceberg.html)
- [MinIO Documentation](https://min.io/docs/)
- [Power Query ODBC connector](https://learn.microsoft.com/en-us/power-query/connectors/odbc)
