# Japan Earthquake ETL

Nền tảng ETL local-first để thu thập dữ liệu động đất từ USGS, xử lý bằng
Spark Java theo mô hình Bronze–Silver–Gold, công bố bảng Gold qua Iceberg và
Trino, sau đó phục vụ báo cáo Power BI.

> Trạng thái hiện tại: **foundation scaffold (`REP-01`, `CFG-01`)**. Cấu trúc
> module, mount contract và configuration contract đã được chốt; các service
> runtime sẽ được bổ sung bởi những task foundation tiếp theo.

## Chuẩn bị trên máy local

Yêu cầu hiện tại chỉ gồm Git và một shell tương thích POSIX (`sh`). Từ thư mục
gốc repository, chạy:

```bash
./scripts/check-repository-layout.sh
```

Kết quả thành công:

```text
REP-01 repository scaffold check passed.
```

Lệnh này kiểm tra các module, thư mục source/test, tài liệu module và các nguồn
bind mount đã được dành trước. Nó là smoke check cho scaffold, không thay thế
Maven test hay `docker compose config` sau khi các task tương ứng được triển
khai.

Tạo cấu hình local, thay tất cả placeholder `change-me-*`, rồi kiểm tra:

```bash
cp .env.example .env
./scripts/check-config.sh --require-local
```

Không commit hoặc chia sẻ file `.env`. Contract đầy đủ nằm trong
[Configuration and secret contract](./docs/specs/CONFIGURATION_AND_SECRETS.md).

## Cấu trúc repository

```text
.
├── .env.example              # Mẫu cấu hình không chứa secret thật
├── airflow/                  # DAG và test orchestration
│   ├── dags/
│   └── tests/
├── compose/                  # Asset hỗ trợ Compose; compose.yaml đặt ở root
├── docs/                     # Kiến trúc, đặc tả, flow và runbook
├── scripts/                  # Script phát triển/vận hành dùng chung
├── spark/                    # Module Spark Java/Maven
│   └── src/
│       ├── main/java/
│       └── test/
│           ├── java/
│           └── resources/fixtures/
├── tests/                    # Fixture và test tích hợp xuyên module
└── trino/                    # Cấu hình Trino
    └── catalog/
```

Chi tiết ownership, mount path và quy tắc mở rộng nằm trong
[Repository layout và mount contract](./docs/specs/REPOSITORY_LAYOUT.md).

## Trạng thái lệnh local

| Lệnh | Trạng thái | Task cung cấp |
|---|---|---|
| `./scripts/check-repository-layout.sh` | Chạy được | `REP-01` |
| `cp .env.example .env` | Chạy được | `CFG-01` |
| `./scripts/check-config.sh --require-local` | Chạy được | `CFG-01` |
| `docker compose config` | Chưa có | `CMP-01` |
| `./mvnw clean test package` | Chưa có | `SPK-01` |

Không chạy một lệnh được đánh dấu “Chưa có” cho tới khi task sở hữu đã merge.
Hướng dẫn vận hành đầy đủ được duy trì trong
[Local operations runbook](./docs/LOCAL_OPERATIONS_RUNBOOK.md).

## Làm việc với repository

- Đọc [AGENTS.md](./AGENTS.md) trước khi bắt đầu task.
- Tạo branch mới từ `main` và chỉ giải quyết một task trong mỗi branch.
- Tuân theo [Git workflow](./docs/conventions_and_workflow/GIT_WORKFLOW.md) và
  [commit convention](./docs/conventions_and_workflow/COMMIT_CONVENTION.md).
- Không commit secret, dữ liệu runtime, Maven `target/` hoặc volume local.
