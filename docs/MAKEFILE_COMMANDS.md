# Lệnh phát triển và vận hành qua Makefile

## Mục đích và cách hoạt động

`Makefile` là entrypoint ngắn cho các lệnh đã triển khai: kiểm tra contract,
unit test, build, khởi động/dừng foundation, smoke test và readback dữ liệu
mẫu. Target kiểm thử gọi lại `scripts/*.sh` hoặc Maven Wrapper/unittest;
logic ETL, validation và publish gate vẫn nằm trong các module/script gốc.
Không có lệnh giả cho Silver/Gold/ML chưa được triển khai.

Chạy từ thư mục gốc `japan-earthquake-etl`, hoặc dùng
`make -C /path/to/japan-earthquake-etl <target>`. Chỉ chạy `make` sẽ hiện help,
không khởi động service. Makefile tương thích GNU Make 3.81 trên macOS.
Các target được chạy tuần tự, kể cả khi thêm `-j`, để tránh hai Maven build
cùng sửa `spark/target` hoặc nhiều smoke cùng thao tác một stack.

```bash
make help
```

## Chuẩn bị và khởi động

Cần GNU Make, JDK 17 trở lên, Python 3, Git, `curl`/`wget`, `jq`, `rg`, `unzip`
và `shasum`/`sha256sum`. Static platform checks cần Docker CLI/Compose plugin;
khởi động, smoke và readback cần thêm Docker daemon đang chạy. Maven/Docker có
thể cần Internet để tải dependency/image lần đầu.

```bash
# Chỉ tạo .env nếu chưa có, không ghi đè cấu hình hiện tại.
make env-init
# Sửa .env: thay toàn bộ change-me-* bằng credential local mạnh.
make check-config-local
make config

# Build image MinIO/Airflow/Spark, start foundation và chờ healthy.
make up
make status
```

`make env-init` dùng quyền tạo file hạn chế bằng `umask 077`. Không commit hoặc
chia sẻ `.env`; không chạy `make up` với `.env.example` vì các secret placeholder
bị preflight chặn. Makefile không `include`/`source` file secret vào shell và
`make config` chỉ gọi Compose `config --quiet`, không dump credential.

| Target | Làm gì / dùng khi nào |
|---|---|
| `build` | Validate local env, build đủ ba image custom; chưa start service |
| `up` / `start` | Build trước, start 9 foundation service, chờ health; dependency init chạy qua Compose |
| `up-minio` | Start MinIO, chờ healthy rồi chạy init bootstrap bucket bằng container tạm |
| `up-airflow` | Build/start API, scheduler, DAG processor cùng MinIO, PostgreSQL và init dependency |
| `up-spark` | Build image chung trước khi start master/worker, tránh worker pull image local chưa có |
| `up-query` | Start Iceberg REST Catalog/Trino cùng MinIO và bucket init |

Khởi động service không đồng nghĩa chạy test hoặc trigger ETL. `up-airflow` bật
scheduler; DAG nào đã unpause sẽ có thể chạy theo lịch/catchup đã cấu hình.
Image đã build được tận dụng cache ở lần chạy sau.

```bash
make up-airflow
make up-spark
make up-query
```

## Kiểm thử không gọi nguồn dữ liệu thật

```bash
make test
```

`test` chạy lần lượt ba nhóm sau; không start Docker, không gọi USGS/JMA thật
hoặc truy cập MinIO. Java test dùng mock HTTP/storage; Maven vẫn có thể cần
Internet khi dependency chưa được cache.

| Target | Phạm vi |
|---|---|
| `test-contracts` | Baseline/scaffold, CON-01..04, inventory JMA, catalog DAT-01 và kế hoạch tuần 3 |
| `test-java` | Maven unit test module Spark: USGS planner/client/Bronze writer/QA/live runner |
| `test-airflow` | Unittest DAG, interval/retry, phase protocol và publish gate |
| `test-jma-backfill` | JMA-04 downloader/Java runner/planner/DAG, fixture/storage/HTTP mock; không gọi nguồn |
| `test-jma-qa` | JMA-05 offline success/error/revision/resume và independent readback verifier; không gọi nguồn/MinIO thật |
| `jma-preview JMA_YEARS=1997,2023` | Preview offline ba exact archive entries; chỉ ghi plan metadata ở staging |
| `package-java` | Maven `clean verify`, gồm unit test và đóng gói JAR |
| `check` | Contract + config hygiene + full static foundation/USG-06, gồm Compose validation và Maven verify |

Các `check-*` riêng được liệt kê đầy đủ trong `make help`; tên target khớp tên
script bỏ `.sh`. Ví dụ:

```bash
make check-jma-inventory
make check-shared-fixtures
make check-real-sample-catalog
make check-foundation
make check-usgs-live
```

Lưu ý: `check-usgs-live` **không** gọi USGS thật; đây là static/unit check cho
code của USG-06. `check-airflow` và `check-spark` cũng chạy unit/build test,
không chỉ kiểm tra văn bản. `build-shared-fixtures` là lệnh riêng có sửa fixture
tracked, chỉ dùng khi maintainer chủ động tái tạo; không nằm trong `test`.

JMA-04 có DAG manual `jma_04_year_backfill`; preview mặc định không download
hoặc ghi Bronze. Hướng dẫn conf years/range, retry/reuse, giới hạn tài nguyên
và cách chuyển sang real mode nằm ở [JMA year backfill](./specs/JMA_YEAR_BACKFILL.md).
Airflow image phải rebuild bằng `make up-airflow` trước khi gọi Java runner mới.

## Smoke runtime và dữ liệu mẫu

Sau khi có local env hợp lệ và Docker daemon:

```bash
make smoke                # Alias smoke-foundation, không gọi USGS thật.
# Hoặc kiểm tra từng component:
make smoke-minio
make smoke-airflow
make smoke-spark
make smoke-query
```

`smoke-foundation` build đủ image custom, kể cả Airflow Java17 trước cold start,
rồi gọi checklist FND-01. Script kiểm tra 9 service healthy, 2 init exit `0`,
network, volume/mount, startup log và bốn smoke hành vi. Các smoke có tạo
object/table/DAG run kiểm thử; chỉ cleanup đúng artifact smoke, giữ service và
named volume để debug. Chúng không xác nhận nghiệp vụ Silver/Gold/ML chưa có.

Các lệnh có tương tác nguồn/storage thật phải được gọi riêng:

```bash
# Gọi USGS thật, chạy DAG với window [2023-01-01, 2023-01-04) UTC,
# ghi Bronze và kiểm tra immutable reuse trong cùng logical run.
make smoke-usgs-live

# JMA-05: preview → ingest → rerun 1997 (hai segment), 2000, 2023;
# readback hai sample DAT-01, raw/manifest, SHA/release/count bất biến.
make smoke-jma-live

# HTTP HEAD 41 archive JMA để đối soát inventory; không tải ZIP.
make check-jma-inventory-live

# Chỉ đọc lại hai sample DAT-01 đã tồn tại trong MinIO.
make verify-samples
```

`verify-samples` / `verify-real-samples` không tự tải hoặc tự start stack; cần
Airflow API container và MinIO đang chạy, sample đã được nạp theo
[DAT-01 sample contract](./specs/SHARED_REAL_SAMPLE_DATA.md). Lệnh kiểm tra USGS
manifest/raw `BronzeReady` cùng checksum/count và JMA staged ZIP
`STAGED_SOURCE` cùng checksum/member/record length/count; không coi staged ZIP
JMA là BronzeReady hoặc Silver đã parse.

`smoke-jma-live` build/start Airflow và dependencies, chạy DAG manual thật rồi
Java verify exact keys/readback và so baseline trước–sau rerun. Giữ nguyên
catalog DAT-01 hai entry; metadata sample 2000/report QA lưu riêng trên staging.
Phạm vi, report paths, timeout/recovery và handoff ở
[JMA Bronze QA](./specs/JMA_BRONZE_QA.md). Không tải full 40 năm hoặc chạy Silver/Gold/ML.

Nếu stack đang chạy nhưng chưa có `.env` trên host, có thể readback bằng
container name (credential được dùng bên trong container, không chép ra host):

```bash
make verify-samples DAT01_AIRFLOW_CONTAINER=japan-earthquake-etl-airflow-api-server-1
```

## Theo dõi và dừng an toàn

```bash
make status
make logs SERVICE=airflow-scheduler TAIL=200
make logs-follow SERVICE=airflow-scheduler
# Ctrl+C chỉ dừng xem log, không dừng service.
make restart SERVICE=airflow-scheduler
make stop
# Hoặc gỡ container/network nhưng giữ named volume:
make down
```

`SERVICE` rỗng áp dụng cho toàn bộ service đối với `status`, `logs`,
`logs-follow`, `restart`, `stop`; có thể chọn nhiều service bằng
`SERVICE="spark-master spark-worker"`. `restart` không rebuild hoặc áp dụng
env/image mới; khi thay code image/config hãy chạy lại `make up` hoặc target
`up-*` tương ứng. `down` luôn áp dụng cả stack và không nhận bộ lọc `SERVICE`.
Không có target xóa volume/bucket hoặc reset data.

## Biến tùy chỉnh

| Biến | Mặc định | Dùng để làm gì |
|---|---|---|
| `ENV_FILE` | `.env` | Config local cho startup/smoke/Compose/readback/config validation |
| `CHECK_ENV_FILE` | `.env.example` | Config cho static platform `check-*`; cho phép check trước khi có secret local |
| `COMPOSE_FILE` | `compose.yaml` | Compose file dùng cho các wrapper/script có hỗ trợ |
| `WAIT_TIMEOUT` | `300` | Số giây chờ health của `up-*`/`up` và full foundation smoke |
| `SERVICE` | Rỗng | Bộ lọc service cho status/log/restart/stop |
| `TAIL` | `100` | Số dòng log gần nhất |
| `DAT01_AIRFLOW_CONTAINER` | Rỗng | Container dùng cho readback DAT-01 thay cho Compose exec |
| `JMA_YEARS` | Rỗng, phải chỉ định | List năm cho `jma-preview`; ví dụ `1997,2023` |

`ENV_FILE` và `CHECK_ENV_FILE` được tách riêng: đặt `ENV_FILE` không tự đổi
static config. Wrapper foundation truyền riêng hai file vào
`check-foundation.sh`, tránh validator local coi placeholder trong
`.env.example` là secret thật. Để check đúng cấu hình local tùy chỉnh, truyền
cả hai:

```bash
make check ENV_FILE=/path/to/local.env CHECK_ENV_FILE=/path/to/local.env
make up ENV_FILE=/path/to/local.env WAIT_TIMEOUT=600
make smoke-foundation ENV_FILE=/path/to/local.env WAIT_TIMEOUT=600
```

Đường dẫn tương đối được tính từ root repository nơi chạy `make` (hoặc thư mục
được chọn bởi `-C`). Dùng dấu nháy nếu đường dẫn có khoảng trắng. Không truyền
giá trị credential trực tiếp trong command line; chỉ truyền đường dẫn env file.

Thứ tự startup, dependency và xử lý lỗi sâu hơn được giữ tại
[Local operations runbook](./LOCAL_OPERATIONS_RUNBOOK.md) và
[scripts README](../scripts/README.md).

## Kiểm tra khi sửa Makefile

```bash
make help
make -n up smoke-foundation logs-follow stop down \
  ENV_FILE='/tmp/team local.env' SERVICE=airflow-scheduler WAIT_TIMEOUT=600
make test
make check
git diff --check
```

`-n` chỉ in command, không start/smoke/stop thật; ví dụ trên dùng để kiểm tra
thứ tự build, quoting đường dẫn và truyền biến. Kiểm chứng ngày `2026-10-05`:
9 contract checks, 34 Java tests, 15 Airflow tests, Maven verify và static
foundation/USG-06 đều passed. Readback qua `make verify-samples` xác nhận USGS
16 event và JMA 257,020 record đúng catalog. `env-init` được kiểm tra với file
tạm: quyền tạo `600`, không ghi đè file có sẵn; preflight chặn env thiếu hoặc
secret placeholder trước khi gọi Docker startup. Các target startup/stop/smoke
mới được kiểm tra bằng dry-run, không restart hay dừng stack đang chạy để thử
wrapper.
