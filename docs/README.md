# Tài liệu dự án Japan Earthquake ETL

Thư mục này là nguồn tài liệu chính thức cho dự án **Nền tảng phân tích dữ liệu động đất tại Nhật Bản**. Bộ tài liệu mô tả hệ thống ở mức đủ để bắt đầu triển khai, kiểm thử và trình diễn; các giá trị phụ thuộc mã nguồn sẽ được cập nhật sau khi project có phiên bản chạy được.

> Trạng thái hiện tại: **Thiết kế ban đầu** — project chưa có mã triển khai.

## 1. Đọc tài liệu theo nhu cầu

| Nhu cầu | Tài liệu |
|---|---|
| Hiểu nhanh đề tài, mục tiêu và công nghệ | [Giới thiệu đề tài](./GIOI_THIEU_DE_TAI_DONG_DAT_NHAT_BAN.md) |
| Chốt yêu cầu và tiêu chí hoàn thành | [Đặc tả dự án](./specs/PROJECT_SPECIFICATION.md) |
| Hiểu các thành phần và cách chúng kết nối | [Kiến trúc hệ thống](./SYSTEM_ARCHITECTURE.md) |
| Hiểu một lần chạy ETL hằng ngày | [Luồng ETL hằng ngày](./flows/DAILY_ETL_PIPELINE.md) |
| Chạy bù, chạy lại hoặc xử lý lỗi | [Backfill và phục hồi](./flows/BACKFILL_AND_RECOVERY.md) |
| Xác định quy tắc kiểm tra dữ liệu | [Chất lượng dữ liệu](./DATA_QUALITY_AND_OBSERVABILITY.md) |
| Thiết kế báo cáo Power BI | [Đặc tả dashboard](./ANALYTICS_DASHBOARD.md) |
| Cài đặt và vận hành trên máy local | [Runbook local](./LOCAL_OPERATIONS_RUNBOOK.md) |
| Chuẩn bị buổi trình diễn | [Kịch bản demo](./DEMO_FLOWS.md) |
| Pick và theo dõi task trong 6 tuần | [Kế hoạch task](./task/README.md) |
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
│   └── PROJECT_SPECIFICATION.md
├── flows/
│   ├── DAILY_ETL_PIPELINE.md
│   └── BACKFILL_AND_RECOVERY.md
├── task/
│   ├── README.md
│   └── JAPAN_EARTHQUAKE_ETL_TASKS.xlsx
└── conventions_and_workflow/
    ├── GIT_WORKFLOW.md
    └── COMMIT_CONVENTION.md
```

## 3. Phạm vi tài liệu ở giai đoạn này

Đã bao gồm:

- Mục tiêu, phạm vi, yêu cầu chức năng và phi chức năng.
- Kiến trúc local-first với Airflow, Spark, MinIO, Iceberg, Trino và Power BI.
- Happy path, backfill, retry, idempotency và các điểm kiểm soát dữ liệu.
- KPI, bố cục dashboard, quy trình demo và runbook vận hành.
- Backlog 6 tuần và workbook để ba thành viên pick/review task.
- Quy ước Git và commit cho nhóm.

Chưa bao gồm theo phạm vi hiện tại:

- Thiết kế database chi tiết, DBML, DDL hoặc migration.
- Schema vật lý cuối cùng của các bảng Iceberg.
- Giá trị cấu hình, tên service và câu lệnh vận hành đã được xác nhận từ mã nguồn.

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
