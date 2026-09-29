# Danh mục task

Mỗi task là một file độc lập và là nguồn theo dõi chính cho phạm vi, dependency, trạng thái, assignee, reviewer và evidence. Khi nhận task, agent phải đọc file của task trước rồi mới đọc tài liệu khối và đặc tả kỹ thuật liên quan.

## Cách cập nhật

1. Đổi `status` và mục **Theo dõi** sang `In Progress` ngay sau khi tạo branch.
2. Điền assignee; P0/P1 phải có reviewer khác assignee.
3. Chỉ đánh dấu checklist khi có test/evidence kiểm tra được.
4. Khi mở PR, thêm link vào **Evidence / PR** và chuyển sang `Review`.
5. Sau review và mọi acceptance criteria đạt, chuyển sang `Done`.

Trạng thái hợp lệ: `Backlog`, `Ready`, `In Progress`, `Review`, `Blocked`, `Done`.

## Tổng quan

- Tổng số task: **56**.
- Foundation: **9 task đã hoàn tất**.
- Core: **54 task**.
- Stretch: **2 task**.
- Tổng effort kế hoạch: **270 giờ**.

## Foundation

| Task | Tuần | Workstream | Scope | Priority | Effort | Trạng thái |
|---|---:|---|---|---|---:|---|
| [PLN-01](./PLN-01.md) - Chốt scope, KPI và Definition of Done | 1 | Planning | Core | P0 | 4h | Done |
| [REP-01](./REP-01.md) - Scaffold cấu trúc repository | 1 | Repository | Core | P0 | 4h | Done |
| [CFG-01](./CFG-01.md) - Thiết lập cấu hình và secret hygiene | 1 | Configuration | Core | P0 | 3h | Done |
| [CMP-01](./CMP-01.md) - Tạo Docker Compose network và volumes | 1 | Platform | Core | P0 | 5h | Done |
| [MIO-01](./MIO-01.md) - Cấu hình MinIO và bucket init | 1 | Storage | Core | P0 | 5h | Done |
| [AFL-01](./AFL-01.md) - Cấu hình Airflow local | 1 | Orchestration | Core | P0 | 7h | Done |
| [SPK-01](./SPK-01.md) - Cấu hình Spark standalone và Java build | 1 | Compute | Core | P0 | 6h | Done |
| [QRY-01](./QRY-01.md) - Cấu hình Iceberg REST Catalog và Trino | 1 | Serving | Core | P1 | 7h | Done |
| [FND-01](./FND-01.md) - Tạo smoke checklist cho môi trường | 1 | QA | Core | P0 | 4h | Done |

## A - Hợp đồng dữ liệu

| Task | Tuần | Workstream | Scope | Priority | Effort | Trạng thái |
|---|---:|---|---|---|---:|---|
| [CON-01](./CON-01.md) - Chốt phạm vi USGS và JMA | 2 | Source scope | Core | P0 | 5h | Review |
| [CON-02](./CON-02.md) - Thiết kế Bronze object và manifest | 2 | Bronze contract | Core | P0 | 5h | Ready |
| [CON-04](./CON-04.md) - Chuẩn bị fixture và ma trận test dùng chung | 2 | Fixtures | Core | P1 | 3h | Ready |
| [CON-03](./CON-03.md) - Thiết kế mô hình dữ liệu Silver và Gold | 2 | Silver and Gold contract | Core | P0 | 6h | Backlog |

## B - USGS Bronze

| Task | Tuần | Workstream | Scope | Priority | Effort | Trạng thái |
|---|---:|---|---|---|---:|---|
| [USG-01](./USG-01.md) - Đặc tả request USGS và cấu hình runtime | 2 | Request configuration | Core | P0 | 5h | Backlog |
| [USG-02](./USG-02.md) - Xây USGS HTTP client có retry an toàn | 2 | HTTP client | Core | P0 | 6h | Backlog |
| [USG-03](./USG-03.md) - Validate và lưu USGS raw vào Bronze | 2 | Bronze writer | Core | P0 | 7h | Backlog |
| [USG-04](./USG-04.md) - Tích hợp USGS ingest vào Airflow | 2 | Airflow integration | Core | P0 | 4h | Backlog |
| [USG-05](./USG-05.md) - Kiểm thử USGS đến Bronze | 2 | QA | Core | P0 | 4h | Backlog |

## C - JMA Bronze

| Task | Tuần | Workstream | Scope | Priority | Effort | Trạng thái |
|---|---:|---|---|---|---:|---|
| [JMA-01](./JMA-01.md) - Lập danh mục 40 năm archive JMA | 3 | Archive inventory | Core | P0 | 5h | Backlog |
| [JMA-02](./JMA-02.md) - Xây JMA downloader và phát hiện file thay đổi | 3 | Downloader | Core | P0 | 6h | Backlog |
| [JMA-03](./JMA-03.md) - Validate và lưu JMA archive vào Bronze | 3 | Bronze writer | Core | P0 | 7h | Backlog |
| [JMA-04](./JMA-04.md) - Tạo workflow ingest JMA theo năm | 3 | Backfill orchestration | Core | P0 | 5h | Backlog |
| [JMA-05](./JMA-05.md) - Kiểm thử JMA đến Bronze | 3 | QA | Core | P0 | 4h | Backlog |

## D - Silver đa nguồn

| Task | Tuần | Workstream | Scope | Priority | Effort | Trạng thái |
|---|---:|---|---|---|---:|---|
| [SLV-01](./SLV-01.md) - Resolve đúng Bronze input cho Spark | 3 | Input staging | Core | P0 | 3h | Backlog |
| [SLV-02](./SLV-02.md) - Parse và chuẩn hóa USGS observation | 3 | USGS parser | Core | P0 | 5h | Backlog |
| [SLV-03](./SLV-03.md) - Parse và chuẩn hóa JMA fixed-width | 3 | JMA parser | Core | P0 | 6h | Backlog |
| [SLV-04](./SLV-04.md) - Tạo source key và lineage đa nguồn | 3 | Lineage | Core | P0 | 4h | Backlog |
| [SLV-05](./SLV-05.md) - Áp dụng validation và reject metrics | 4 | Quality | Core | P0 | 5h | Backlog |
| [SLV-06](./SLV-06.md) - Deduplicate và xử lý revision trong từng nguồn | 4 | Source dedup | Core | P0 | 5h | Backlog |
| [SLV-07](./SLV-07.md) - Liên kết observation và chọn canonical event | 4 | Entity resolution | Core | P0 | 6h | Backlog |
| [SLV-08](./SLV-08.md) - Ghi Silver Parquet theo source và thời gian | 4 | Silver storage | Core | P0 | 5h | Backlog |
| [SLV-09](./SLV-09.md) - Tích hợp và kiểm thử Silver đa nguồn | 4 | Integration QA | Core | P0 | 4h | Backlog |

## E - Gold & Serving

| Task | Tuần | Workstream | Scope | Priority | Effort | Trạng thái |
|---|---:|---|---|---|---:|---|
| [GLD-01](./GLD-01.md) - Xây canonical event, dimensions và bands | 4 | Analytics model | Core | P0 | 6h | Backlog |
| [GLD-02](./GLD-02.md) - Tạo aggregate phục vụ dashboard | 4 | Aggregates | Core | P1 | 4h | Backlog |
| [GLD-03](./GLD-03.md) - Ghi Gold Iceberg và commit snapshot | 4 | Iceberg publish | Core | P0 | 6h | Backlog |
| [GLD-04](./GLD-04.md) - Tạo Trino views và verification SQL | 4 | Trino verification | Core | P0 | 4h | Backlog |

## F - Điều phối

| Task | Tuần | Workstream | Scope | Priority | Effort | Trạng thái |
|---|---:|---|---|---|---:|---|
| [ORC-01](./ORC-01.md) - Hoàn thiện DAG end-to-end theo contract | 5 | DAG skeleton | Core | P0 | 4h | Backlog |
| [ORC-02](./ORC-02.md) - Cấu hình lịch và readiness cho hai nguồn | 5 | Scheduling | Core | P1 | 4h | Backlog |
| [ORC-03](./ORC-03.md) - Implement backfill và reprocessing | 5 | Backfill | Core | P0 | 5h | Backlog |
| [ORC-04](./ORC-04.md) - Chuẩn hóa logging và run summary | 5 | Observability | Core | P1 | 4h | Backlog |
| [ORC-05](./ORC-05.md) - Chốt recovery, concurrency và tài nguyên | 5 | Recovery and resources | Core | P1 | 4h | Backlog |

## G - Power BI

| Task | Tuần | Workstream | Scope | Priority | Effort | Trạng thái |
|---|---:|---|---|---|---:|---|
| [BI-01](./BI-01.md) - Tạo kết nối Trino và Power Query | 5 | Connectivity | Core | P0 | 4h | Backlog |
| [BI-02](./BI-02.md) - Tạo semantic model và KPI measures | 5 | Semantic model | Core | P0 | 5h | Backlog |
| [BI-03](./BI-03.md) - Xây trang Tổng quan | 5 | Overview dashboard | Core | P1 | 5h | Backlog |
| [BI-04](./BI-04.md) - Xây trang Không gian và Độ sâu - Độ lớn | 5 | Detail dashboards | Core | P1 | 5h | Backlog |
| [BI-05](./BI-05.md) - Thiết lập refresh và đối soát BI | 5 | Refresh and reconciliation | Core | P0 | 5h | Backlog |

## H - QA & Release

| Task | Tuần | Workstream | Scope | Priority | Effort | Trạng thái |
|---|---:|---|---|---|---:|---|
| [DOC-01](./DOC-01.md) - Cập nhật tài liệu theo từng block | 6 | Documentation | Core | P0 | 4h | Ready |
| [DEMO-01](./DEMO-01.md) - Chuẩn bị dữ liệu và kịch bản demo | 6 | Demo | Core | P0 | 4h | Backlog |
| [DEMO-02](./DEMO-02.md) - Rehearsal và tạo release candidate | 6 | Release | Core | P0 | 6h | Backlog |
| [OPS-01](./OPS-01.md) - Smoke test backup và restore metadata | 6 | Backup and restore | Stretch | P2 | 4h | Backlog |
| [QA-01](./QA-01.md) - Chạy E2E daily đa nguồn | 6 | E2E | Core | P0 | 5h | Backlog |
| [QA-02](./QA-02.md) - Test rerun, duplicate và late revision | 6 | Idempotency | Core | P0 | 5h | Backlog |
| [QA-03](./QA-03.md) - Kiểm thử JMA historical backfill | 6 | Historical backfill | Core | P0 | 5h | Backlog |
| [QA-04](./QA-04.md) - Test failure và recovery theo tầng | 6 | Failure recovery | Core | P0 | 4h | Backlog |
| [QA-05](./QA-05.md) - Profile dữ liệu lịch sử và tài nguyên local | 6 | Performance | Stretch | P2 | 4h | Backlog |
| [SEC-01](./SEC-01.md) - Review secret và bề mặt truy cập | 6 | Security | Core | P1 | 4h | Backlog |
