# Repository layout và mount contract

| Thuộc tính | Giá trị |
|---|---|
| Task | `REP-01` |
| Trạng thái | Implemented |
| Phạm vi | Cấu trúc module, ownership, build placeholder và mount path |

## 1. Mục đích

Tài liệu này là contract cho các task thêm Airflow, Spark Java, Trino và Docker
Compose. Mỗi task downstream mở rộng đúng module được giao và không tự tạo một
cấu trúc hoặc mount path cạnh tranh.

Scaffold chưa cung cấp runtime service chạy được. Maven Wrapper, `pom.xml`, DAG
và catalog properties thuộc các task riêng trong backlog; `compose.yaml` hiện
chỉ chứa foundation contract và validation profile.

## 2. Cấu trúc chuẩn

```text
project-root/
├── README.md
├── AGENTS.md
├── .env.example
├── compose.yaml
├── airflow/
│   ├── README.md
│   ├── dags/
│   │   └── README.md
│   └── tests/
│       └── README.md
├── compose/
│   └── README.md
├── docs/
│   └── specs/
│       ├── COMPOSE_FOUNDATION.md
│       ├── CONFIGURATION_AND_SECRETS.md
│       └── REPOSITORY_LAYOUT.md
├── scripts/
│   ├── README.md
│   ├── check-compose.sh
│   ├── check-config.sh
│   └── check-repository-layout.sh
├── spark/
│   ├── README.md
│   └── src/
│       ├── main/java/
│       └── test/
│           ├── java/
│           └── resources/fixtures/
├── tests/
│   ├── README.md
│   ├── fixtures/
│   │   └── README.md
│   └── integration/
│       └── README.md
└── trino/
    ├── README.md
    └── catalog/
        └── README.md
```

Không tạo package Java giả trước khi `SPK-01` chốt group/package name. Các thư
mục `java/` được giữ bằng `.gitkeep` để Maven layout sẵn sàng mà không áp đặt
namespace.

## 3. Ownership theo module

| Đường dẫn | Nội dung được phép | Không đặt tại đây |
|---|---|---|
| `airflow/dags/` | DAG và helper chỉ phục vụ DAG | Secret, log, database Airflow |
| `airflow/tests/` | Unit/import test cho DAG | Test Spark hoặc test E2E Compose |
| `spark/` | Maven module, Java source và unit fixture | JAR/`target/` đã build |
| `trino/catalog/` | Catalog properties không chứa secret | Password hoặc access key thật |
| `compose/` | Script init/healthcheck và asset cho service | `compose.yaml`; file này đặt tại root |
| `tests/fixtures/` | Fixture nhỏ dùng chung, có nguồn và mục đích rõ | Data dump hoặc dữ liệu runtime |
| `tests/integration/` | Test xuyên service/module | Unit test riêng của Spark/Airflow |
| `scripts/` | Script lặp lại được cho dev/CI/operations | Credential hoặc thao tác xóa rộng mặc định |

## 4. Mount contract

`CMP-01` dành trước các named volume dưới đây. Task downstream phải dùng đúng
đường dẫn hoặc cập nhật contract này trong cùng PR nếu có lý do kỹ thuật đã
được review.

| Service/consumer | Source | Container target | Mode | Ghi chú |
|---|---|---|---|---|
| Airflow webserver/scheduler | `./airflow/dags` | `/opt/airflow/dags` | Bind, read-only | DAG source duy nhất |
| Trino coordinator | `./trino/catalog` | `/etc/trino/catalog` | Bind, read-only | Chỉ catalog properties |
| Airflow task và Spark runtime | Named volume `pipeline_staging` | `/opt/pipeline/staging` | Read-write | Dữ liệu tạm trao đổi; không commit |
| Airflow components | Named volume `airflow_logs` | `/opt/airflow/logs` | Read-write | Log runtime tách khỏi DAG source |
| Airflow metadata DB | Named volume `airflow_db_data` | `/var/lib/postgresql/data` | Read-write | Chỉ metadata Airflow |
| MinIO | Named volume `minio_data` | `/data` | Read-write | Bronze, Silver và Gold warehouse |
| Iceberg Catalog | Named volume `iceberg_catalog_data` | Do `QRY-01` chốt | Read-write | Catalog state tách khỏi data lake |

Spark source là build context, không bind toàn bộ repository vào container.
`SPK-01` chốt tên JAR; image/runtime đặt artifact tại
`/opt/spark/jobs/japan-earthquake-etl.jar`. Backend và target lưu trạng thái của
Iceberg Catalog do `QRY-01` chốt, nhưng phải dùng named volume riêng và không
được dùng lại các volume ở bảng trên.

Quy tắc chống xung đột:

1. Mọi bind source dùng đường dẫn tương đối từ project root.
2. Source code và cấu hình chỉ đọc phải mount `read-only`.
3. Không bind mount project root, `data/`, `staging/` hoặc `warehouse/` từ host.
4. Dữ liệu bền vững và staging dùng named volume khác nhau.
5. Một service không được khai báo hai mount có cùng container target.
6. Secret chỉ đi qua environment/secret mechanism; không nằm trong catalog,
   DAG hoặc source tree.

## 5. Build placeholder

Chạy smoke check scaffold:

```bash
./scripts/check-repository-layout.sh
```

Script thất bại nếu thiếu module, Maven source/test layout, tài liệu module hoặc
bind source đã dành trước. Maven build thật thuộc `SPK-01`; Compose validation
thuộc `CMP-01`.

## 6. Handoff cho task downstream

| Task | Trách nhiệm tiếp theo |
|---|---|
| `CFG-01` | Đã thêm `.env.example`, config contract và secret hygiene check |
| `CMP-01` | Đã thêm Compose network, named volumes và extension baseline |
| `SPK-01` | Thêm Maven Wrapper, `pom.xml`, package Java, Hello World và test base |
| `QRY-01` | Chốt Trino/Iceberg Catalog config và catalog state volume |
