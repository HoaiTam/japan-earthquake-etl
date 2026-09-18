# Docker Compose foundation contract

| Thuộc tính | Giá trị |
|---|---|
| Task | `CMP-01` |
| Trạng thái | Implemented |
| Compose file | `compose.yaml` |
| Project name | `japan-earthquake-etl` |

## 1. Phạm vi

`CMP-01` tạo nền Compose dùng chung: project name, network, named volumes,
resource baseline, health-check defaults và dependency policy. PR này chưa
thêm Airflow, MinIO, Spark, Iceberg Catalog hay Trino runtime service; các task
downstream sở hữu image, command, healthcheck cụ thể và service dependency.

`compose-health-contract` và `compose-contract` chỉ thuộc profile `validation`.
Chúng giúp Compose kiểm chứng healthcheck, long-form dependency và các
network/volume reference; cả hai không chạy trong profile mặc định.

## 2. Network contract

| Network | Driver | Phạm vi | Quy tắc |
|---|---|---|---|
| `pipeline` | `bridge` | Project-scoped | Network duy nhất cho service local ở baseline |

Compose tự tạo tên vật lý theo project, ví dụ
`japan-earthquake-etl_pipeline`; không khai báo `external` hoặc hard-code một
network dùng chung giữa nhiều checkout. Network có outbound access để Airflow
extract gọi USGS. Service nội bộ không được publish port nếu host/Power BI
không cần truy cập.

## 3. Volume lifecycle

| Volume | Lifecycle label | Mount target dự kiến | Dữ liệu |
|---|---|---|---|
| `pipeline_staging` | `transient` | `/opt/pipeline/staging` | File trao đổi tạm giữa Airflow và Spark |
| `airflow_logs` | `operational` | `/opt/airflow/logs` | Log phục vụ vận hành/debug local |
| `airflow_db_data` | `durable` | `/var/lib/postgresql/data` | Metadata Airflow |
| `minio_data` | `durable` | `/data` | Bronze, Silver và Gold warehouse |
| `iceberg_catalog_data` | `durable` | Do `QRY-01` chốt | Trạng thái backend của Catalog |

Staging dùng named volume riêng và được gắn label `transient`; nó không phải
nguồn backup hoặc dữ liệu chính thức. Các volume `durable` không được dùng làm
scratch space. `docker compose down` giữ named volume, còn
`docker compose down -v` có thể xóa cả staging lẫn dữ liệu bền vững nên không
được dùng trong quy trình thường ngày.

## 4. Extension baseline

`compose.yaml` cung cấp YAML anchors để task downstream tái sử dụng:

| Extension | Nội dung |
|---|---|
| `x-runtime-defaults` | `init`, restart policy và network `pipeline` |
| `x-resource-baseline` | Limit 1 CPU/1 GiB; reservation 0.25 CPU/256 MiB |
| `x-healthcheck-defaults` | Interval 10s, timeout 5s, 12 retries, start period 20s |
| `x-healthy-dependency` | Chờ `service_healthy`, restart consumer khi dependency được Compose restart |

Resource baseline là guardrail local ban đầu, không phải sizing production.
Service nặng như Spark/MinIO có thể override trong PR sở hữu service, nhưng phải
ghi lý do và cập nhật resource profile sau khi đo ở `QA-05`.

Mọi dependency runtime phải dùng long syntax và condition phù hợp:

- Database, MinIO, Catalog và query engine: ưu tiên `service_healthy`.
- Init/migration one-shot: dùng `service_completed_successfully`.
- `service_started` chỉ dùng khi dependency không có readiness semantics và PR
  phải giải thích lý do.

Không dùng `sleep` cố định để thay healthcheck/readiness.

## 5. Validation

Chạy từ project root; không cần pull image hoặc khởi động Docker daemon cho
`config` validation:

```bash
./scripts/check-compose.sh
docker compose --env-file .env.example config --quiet
docker compose --env-file .env.example --profile validation config --quiet
```

Xem contract đã resolve:

```bash
docker compose --env-file .env.example --profile validation config --networks
docker compose --env-file .env.example --profile validation config --volumes
```

Kết quả phải có network `pipeline`, volume staging riêng và bốn volume
operational/durable. Không cần chạy hoặc pull image của hai contract service;
profile này chỉ giữ health/dependency/resource reference trong resolved config.

## 6. Handoff cho task downstream

- Service mới merge `x-runtime-defaults` và resource profile phù hợp.
- Mỗi service có healthcheck phản ánh readiness thật trước khi consumer thêm
  `depends_on`.
- Mount source/target phải tuân theo
  [repository layout](./REPOSITORY_LAYOUT.md#4-mount-contract).
- Chỉ publish Airflow UI, MinIO Console và Trino host port đã được cấu hình;
  PostgreSQL, Spark, MinIO API và Catalog giữ nội bộ.
- PR thêm service phải chạy lại `scripts/check-compose.sh` và cập nhật runbook
  khi command khởi động trở thành khả dụng.
