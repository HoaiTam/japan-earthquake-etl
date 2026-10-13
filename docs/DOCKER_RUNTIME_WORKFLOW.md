# Build, startup và test runtime tách biệt

## Mục đích

Bản vá FND-01 tách chuẩn bị Docker khỏi mọi `smoke-*`. Trước đây một lệnh
như `make smoke-usgs-live` gọi `up --build --wait`, nên người test phải chờ
build/dependency/init một lần nữa dù stack đã chạy. Smoke hiện chỉ preflight
chỉ đọc rồi thực hiện bài test. Đây không phải bỏ health, checksum hay quality gates.

## Quy trình sử dụng

```bash
make env-init
# Sửa .env nếu vừa tạo mới; không ghi đè/chia sẻ credential đang có.
make check-config-local

# Lần đầu sau bản vá, hoặc build inputs thay đổi:
make docker-build

# Mỗi khi cần khởi động/áp dụng config hoặc image mới:
make docker-up WAIT_TIMEOUT=600
make docker-check

# Có thể chạy lặp lại, không build/start/init lại:
make smoke-usgs-live
make smoke-jma-live
make smoke-silver-integration
make smoke-ml-window
```

Chỉ chạy tuần tự; không build/đổi image/JAR hoặc khởi động lại service khi còn
ETL/QA đang chạy. Lệnh smoke vẫn có thể tạo **container QA tạm**, gọi nguồn,
ghi dữ liệu/bundle QA và kiểm tra rerun theo phạm vi ban đầu. Không có cam kết
test dữ liệu chạy tức thì: thời gian ingest/Spark/Trino vẫn cần thiết.

`build` là alias `docker-build`; `up`/`start` là alias `docker-up`, **không còn
build tự động**. `up-minio`, `up-airflow`, `up-spark`, `up-query` cũng không build/pull.
Thiếu image/artifact sẽ báo lỗi và yêu cầu chuẩn bị, không lén build từ smoke.

## Ba trách nhiệm

| Lệnh | Làm gì | Không làm gì |
|---|---|---|
| `docker-build` | Build 5 image custom (MinIO server/client, Airflow, Spark, Spark Gold QA), chạy Maven verify bằng Java17 trong Docker, export JAR/Iceberg runtime, pull dependency images | Không start foundation, migrate DB, bootstrap bucket hay chạy ETL |
| `docker-up` | Start service chưa ready/current, init khi chưa có kết quả phù hợp, chờ health | Không build/pull, không xóa container dữ liệu/volume/warehouse |
| `docker-check` / smoke preflight | Kiểm tra service/init/config/image/artifact phù hợp | Không build/pull/start/init/restart, không force unlock |

`docker-build` không cần JDK17 trên host. Sau build, một container export mới
được **create nhưng không start**, network `none`; copy đúng artifacts từ Spark
image rồi remove chỉ container export đó (không `-v`, không đụng service).
`make package-java` vẫn là tùy chọn Maven `clean verify` trên host có JDK17,
và ghi receipt artifact; unit/CI Java không bị chuyển thành runtime test.

## Vì sao init không phải chạy lại mỗi lần?

Startup thực hiện theo thứ tự MinIO/PostgreSQL → init → consumer, bằng
`up --no-deps --no-build --pull never`. Chỉ bỏ qua init khi container đúng
Compose project/service, **exited/0**, config hash và image ID đều khớp.
Service đã healthy/current cũng không bị gọi `up` lại. Init đang chạy cùng
config được chờ trong timeout; init đang chạy config cũ bị chặn, không takeover.
Đổi env/credential/init command hoặc init image buộc operator đi qua startup lại.

Không bỏ migration/bootstrap trên môi trường mới. Lần init thật đầu tiên vẫn
có thể chậm; ảnh chờ init không đủ để xác định nguyên nhân. `airflow-init` vẫn
chuẩn bị quyền volume/migration theo contract; bản vá không tự đổi quyền dữ liệu
hoặc bỏ `chown -R` khi chưa chẩn đoán.

## Preflight và freshness

`scripts/docker-runtime.py` dùng Python standard library. Config/inspect/stderr
có thể chứa credential, được giữ trong memory; chỉ reason code/service name
được in. Không dump env hoặc Docker inspect vào evidence.

- Chỉ kiểm tra services liên quan đến component, không bắt USGS chờ Spark/Trino.
- Service phải running/healthy; init phải exited/0, không coi running là ready.
- Compose project, service config hash và running image ID phải khớp.
- Compose tùy chỉnh vẫn phải giữ build contexts/Dockerfiles trong checkout,
  reviewed MinIO build args và host artifact mounts read-only đúng nguồn. Không
  gắn fingerprint của checkout này cho code/binary ở checkout khác.
- Image custom có label `com.japan-earthquake-etl.build-input-sha256` từ build.
  SHA theo nội dung POM, wrapper, Compose/Dockerfiles/runner scripts, Java source,
  fixtures và config JMA; không theo mtime. `.env`, docs, `.metals`, target/staging
  không bị đưa vào source fingerprint. Đổi `.env` được bắt bằng config hash.
- Gold/ML dùng host-mounted runner/Iceberg JAR: SHA thực phải khớp receipt ở
  `staging/docker-runtime/java-artifacts.json` và source fingerprint hiện tại.
  Receipt ghi sau verify/export thành công, không chỉ dựa vào file JAR tồn tại.
  QA checks kiểm tra đúng client service và bind mounts của nó.
- `SILVER_RUNNER_JAR` phải cùng SHA với package đã verify; binary không rõ nguồn
  không được silently dùng để báo nghiệm thu source hiện tại.

Image cũ không có label/receipt bị chặn: cần `docker-build` **một lần** sau bản vá.
`package-java` chỉ cập nhật host artifacts; nếu source/build inputs đổi, vẫn cần
`docker-build` + `docker-up` cho embedded runners trước smoke.

## Timeout và chẩn đoán

Read-only preflight có tổng budget `DOCKER_CHECK_TIMEOUT=20` giây, gồm các lệnh
Docker CLI; không chờ init tự động. Startup dùng `WAIT_TIMEOUT=300` giây tổng,
có thể tăng thành 600. Timeout build/pull không bị gộp vào readiness timeout.

```bash
make docker-check DOCKER_CHECK_TIMEOUT=10
make status
make logs SERVICE=minio-init TAIL=100
make logs SERVICE=airflow-init TAIL=100
```

| Lỗi | Hướng xử lý |
|---|---|
| `IMAGE_MISSING` / `IMAGE_UNPREPARED_OR_STALE` | `make docker-build`, rồi `make docker-up` |
| `SERVICE_MISSING_OR_OUTDATED` | Sau khi xác nhận không có job active, chạy `make docker-up` |
| `SERVICE_NOT_READY` / `INIT_FAILED` | Đọc state/log đúng service; không bỏ gate hoặc tự reset |
| `JAVA_ARTIFACTS_UNPREPARED/STALE/CHANGED` | Chuẩn bị lại artifacts bằng `docker-build` (hoặc package trên JDK17), không sửa receipt bằng tay |
| `DOCKER_COMMAND_UNAVAILABLE_OR_TIMEOUT` | Kiểm tra Docker Desktop/daemon; test không tự restart Docker |
| `INIT_IN_PROGRESS_WITH_OLD_CONFIGURATION` | Chẩn đoán init cũ trước; không force replace/migrate song song |

### MinIO Client không còn phụ thuộc image Quay

`minio-init`/`minio-smoke` cùng build image local từ official `minio/mc`, tag
`RELEASE.2025-08-13T08-35-41Z`, commit
`7394ce0dd2a80935aded936b09fa12cbb3cb8096`. `docker-build` build `minio-init`
nhưng **không chạy init** và không đưa client vào danh sách pull dependency.
Source commit/tag và binary release/commit được verify trong Dockerfile; context
client chỉ có Dockerfile. Hai client phải cùng image/context/build args đã review.
Fingerprint bao gồm Dockerfile và `.dockerignore` của cả server/client; image
client stale cũng bị preflight chặn. Smoke vẫn `--no-deps --pull never`.

Nếu đã gặp `401 UNAUTHORIZED` khi pull `quay.io/minio/mc`, dùng branch chứa bản
vá rồi chạy lại `make docker-build` (không `--no-cache`), sau đó `make docker-up`
khi không có job active. Java/server build layers không đổi được tận dụng cache;
client mới cần tải Go modules/compile một lần. Build source vẫn cần Internet tới
Docker Hub, GitHub, Alpine và Go proxy/checksum service, không phải chế độ offline.
Không sửa mật khẩu MinIO để chữa registry 401 và không reset dữ liệu.

Kiểm chứng bản vá: [MIO-01 pinned client evidence](./evidence/MIO-01_PINNED_CLIENT.md).

Startup timeout có thể để container đã khởi động tiếp tục chạy. Không coi Ctrl+C
là rollback; đọc state trước khi retry. Không dùng `down -v`, xóa owner lease,
bucket hoặc Iceberg metadata. Container export có thể còn lại nếu Docker API
lỗi; chỉ chẩn đoán đúng tên `ie212-artifact-export-<UUID>` của lần build đó.

## Kiểm thử và giới hạn

`make test-docker-runtime` dùng Docker/Maven doubles và Make dry-run; nằm trong
`ci-docs`, không yêu cầu Docker daemon/network. Nó kiểm tra không có build/start
trong smoke, fail-before-test, freshness/config drift, init reuse/cold ordering,
timeout/redaction và artifact receipt. Fixture thành công **không phải** live
USGS/JMA hay evidence startup mới.

- **FND-01 — Smoke checklist cho môi trường:** cần operator chạy lại cold/warm
  startup và smoke thật trên Docker Desktop sau bản vá; kết quả runtime cũ là
  lịch sử, không dùng để khẳng định thời gian/health hiện tại.
- **QA-01 — E2E daily đa nguồn:** production adapters/full-month state chưa
  được thêm bởi bản vá này; các QA scopes/pins hiện có không đổi.
- **SEC-01 — Review secret và bề mặt truy cập:** hash/freshness không thay audit
  credential/permission/runtime logs hoặc chứng nhận bảo mật.
