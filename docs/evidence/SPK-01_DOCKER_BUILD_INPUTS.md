# SPK-01 — Hotfix đầu vào Java Docker builder

## Bối cảnh và nguyên nhân

- Ngày kiểm chứng: `2026-10-08`; assignee `HoaiTam`, reviewer `unassigned`.
- Branch: `fix/spk-01-docker-build-inputs`, tạo từ `origin/main` tại `bfe8a35`
  (merge [PR #44](https://github.com/HoaiTam/japan-earthquake-etl/pull/44)).
- Lệnh người dùng gặp lỗi: `make up WAIT_TIMEOUT=600`.
- Spark build `fv4g4fflfodmqcbncyis2acc1` thất bại tại Maven `clean verify`:
  `Tests run: 167, Failures: 0, Errors: 96, Skipped: 0`.
- Lỗi cụ thể: thiếu `../tests/fixtures/jma/archives/success.zip`,
  `../config/jma/hypocenter_archives_v1.csv`, `Could not locate repository
  tests/fixtures` và `Repository fixture root not found`.

Dockerfile Spark trước hotfix chỉ copy POM/source. Unit test đọc fixture và
inventory ngoài module nên thiếu đầu vào trong `/workspace`; `.dockerignore`
đã cho phép các file này. Airflow builder có cả hai COPY và build thành công
với cùng source. Đây không phải lỗi `.env` hay lỗi code parser/contract nguồn.
Thông báo `exit code: 1` cuối build chỉ là kết quả tổng, không phải nguyên nhân.

## Thay đổi và cách hoạt động

- Spark builder copy `tests/fixtures` và `config/jma` trước Maven verify, giữ
  nguyên Java 17, version pin, unit test và runtime JAR-only.
- `scripts/check-java-build-inputs.sh` kiểm tra canonical inputs, allowlist
  của repo và hai COPY trong `AS build` trước Maven verify ở Spark/Airflow.
  Đây là kiểm tra quy ước repo, không phải parser Docker tổng quát.
- `tests/test_java_build_inputs.py` có 12 regression test, bao gồm tái hiện
  lỗi gốc, thiếu từng COPY ở từng builder, COPY sai stage/sau Maven/comment,
  thiếu input/allowlist/Dockerfile, root chứa khoảng trắng và argument sai.
- Makefile thêm `check-java-build-inputs` vào `test-contracts` và trước build
  Java image; `check-spark.sh` gọi shell preflight trước Maven.
- Hướng dẫn team: [Spark contract](../specs/SPARK_STANDALONE.md),
  [Makefile](../MAKEFILE_COMMANDS.md), [local runbook](../LOCAL_OPERATIONS_RUNBOOK.md),
  [scripts](../../scripts/README.md), [tests](../../tests/README.md).

## Kiểm chứng đã chạy

| Lệnh / kiểm tra | Kết quả thực tế |
|---|---|
| `make check-java-build-inputs` | Shell preflight passed; 12 unittest passed |
| `sh -n scripts/check-java-build-inputs.sh scripts/check-spark.sh` | Passed |
| `make -n up up-spark up-airflow smoke-source-readiness ENV_FILE='/tmp/team local.env' WAIT_TIMEOUT=600` | Gate trước Docker build, quoting env path và timeout đúng; chỉ dry-run |
| `make check` với host JDK 21 | Contract/config/platform checks passed; 167 Java + 89 Airflow test passed; không start service/gọi nguồn thật |
| `make build` | Lần đầu Spark/Airflow built, MinIO timeout Docker Hub; retry một lần cả ba image Built, exit `0` |
| Kiểm tra runtime JAR-only bằng container `--rm --network none` | JAR tồn tại; không có `/workspace/tests/fixtures` hoặc `/workspace/config/jma`; exit `0` |
| `make smoke-spark` với host JDK 21 | Static/Maven passed; bước build lặp lại timeout Docker Hub khi resolve Maven image; không ghi target này là passed |
| Compose start `--no-build` + `spark-client` trên image đã build | Master/worker healthy, worker ALIVE, spark-submit exit `0`, count `10`, sum `45` |

Maven layer của Compose build là **CACHED** từ builder đã verify cùng đầu vào;
không ghi cached build thành một lần chạy test mới. Để xác nhận Java 17 với
đúng filesystem trong container, đã chạy thêm:

```bash
docker build --target build -t japan-earthquake-etl/java-build-check:spk01 \
  --progress=plain -f compose/spark/Dockerfile .
docker run --rm --network none --entrypoint mvn \
  japan-earthquake-etl/java-build-check:spk01 \
  --offline --batch-mode --no-transfer-progress clean verify
```

Builder image được tạo từ layer cached; **Maven trong `docker run` chạy mới**,
clean/compile/test/package trong filesystem container cô lập, không mount env,
data hoặc target host. Kết quả `BUILD SUCCESS`, 167 test, 0 failure/error/skipped,
thời gian Maven `36.587 s`. Có log Log4j không resolve hostname trong chế độ
`--network none` và warning dependency/shading; không gây lỗi test/build.
Container test tự gỡ; image builder được giữ ở local để có thể lặp lại offline.

`WAIT_TIMEOUT` chỉ là timeout chờ service healthy sau build, không giới hạn
Maven dependency resolution. Timeout Docker Hub của MinIO lần đầu là lỗi
mạng riêng, retry đã đạt; không sửa MinIO Dockerfile hoặc xóa cache/volume.

## Runtime và bàn giao

Do image đã build thành công và gate static đã đạt, runtime được kiểm chứng
bằng hai bước cuối của smoke flow, không build lại hoặc gọi registry:

```bash
docker compose --env-file .env up -d --no-build --wait --wait-timeout 300 \
  spark-master spark-worker
docker compose --env-file .env --profile smoke run --rm --no-deps \
  --use-aliases spark-client
```

Master/worker healthy; smoke runner xác nhận worker `ALIVE`. Executor chạy trên
`spark-worker`, Spark `3.5.9`, Java `17.0.19`, hai partition. Log kết quả:

```text
event=spark_hello_world_success app_id=app-20261008031557-0000 master=spark://spark-master:7077 record_count=10 id_sum=45
SPK-01 Spark smoke passed: worker ALIVE, spark-submit exit code 0.
```

Trạng thái SPK-01 đồng bộ `Done` sau khi đạt acceptance này. Giữ master/worker
đang chạy để debug; client one-shot tự gỡ. Không tự restart Airflow hoặc chạy
lại toàn bộ foundation để kiểm tra hotfix riêng Spark.

Không chỉnh `.env`, fixture payload, inventory hoặc contract nghiệp vụ; không
trigger DAG ingest, tải archive thật hoặc xóa bucket/volume. Không commit,
push, mở PR hoặc merge thay người dùng. `.metals/` có sẵn không thuộc hotfix.

## Giới hạn / review

- Docker Hub đôi lúc timeout metadata (MinIO lần đầu và build lặp trong smoke).
  Hotfix xử lý thiếu đầu vào Maven, không xử lý kết nối registry; **chưa có
  task** riêng cho vấn đề mạng này. `make build` retry và runtime trên image
  đã build đều đạt; nếu registry timeout trở lại, kiểm tra mạng Docker Desktop
  và retry, không bỏ test, đổi version pin hoặc xóa dữ liệu.
- Reviewer độc lập hotfix SPK-01 - Cấu hình Spark standalone và Java build
  chưa xác nhận; giữ `unassigned`, không tự tạo approval. Review khuyến nghị
  theo quy trình repo và không chặn `Done` khi test/evidence đã đủ.
