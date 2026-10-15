# FND-01 — Nghiệm thu lifecycle và foundation thật

## Kết luận

- **PASS / Done**, nghiệm thu ngày **2026-10-15**, timestamp trong evidence là UTC.
- Branch `test/fnd-01-complete-live-lifecycle`, từ main
  `53e520861b74385b278d2de833341207b514caab` (PR #74 đã merge).
- Assignee **HoaiTam**, reviewer **unassigned**; chưa có approval độc lập.
- [Evidence JSON](./FND-01_LIVE_ACCEPTANCE.json) chứa container/image IDs,
  StartedAt/FinishedAt/restart counts, network/volume identities, exact manifest
  URI/SHA và run/task states. Không chứa env, raw logs hoặc payload nguồn.
- Docker Desktop ban đầu tắt: mở ứng dụng, không reset/prune/đổi tài nguyên.
  Docker 28.3.2, 8 CPU, RAM VM 5.295 GiB; project khác không bị stop/restart.

## Lỗi thật đã phát hiện và xử lý

1. **Daemon tắt bị báo nhầm thiếu image.** Preflight trước đây đổi mọi lỗi
   image-inspect thành `IMAGE_MISSING`. Nay chỉ kết luận thiếu khi Docker trả
   `No such image/object`; daemon/connect/permission trả reason riêng đã redacted.
   Không tự mở/restart Docker hoặc build trong preflight/smoke.
2. **Airflow API cold readiness.** Lượt đầu `make docker-up` exit 2 sau 399.92s:
   API chuyển unhealthy với start period 30s/probe timeout 5s. Container bắt đầu
   `02:34:36.095757041Z`, ứng dụng startup complete `02:38:32.901922429Z`
   (236.806s); sau đó có probe timeout dù server đã lên. Không OOM/restart,
   service tự healthy lại. Giữ lần thất bại này, không sửa/xóa log để cho pass.
   API đổi grace thành 240s, timeout 15s; endpoint/curl --fail và yêu cầu healthy
   giữ nguyên. Thành công sớm không phải chờ hết grace; checker khóa cấu hình này.
3. **Config scan bị chạy hai lần.** Các Make target có shell wrapper bỏ
   prerequisite trùng; shell vẫn validate config trước preflight, kể cả gọi trực
   tiếp. Không thêm skip-config, không bỏ secret scan.
4. **Startup log audit fail-closed.** Đọc log từ StartedAt của boot hiện tại,
   tối đa 200 dòng/service; log rỗng hoặc lỗi đọc đều fail. Không dùng stderr của
   lệnh logs thất bại làm bằng chứng log hợp lệ. SEC-01 vẫn audit retained logs
   riêng; thay đổi này không chứng nhận bảo mật hay xóa lịch sử.

## Kết quả thực tế

| Bài kiểm tra | Kết quả | Thời gian tổng |
|---|---|---:|
| `make docker-build` sau sửa Compose | PASS, 5 images; cached Java17 verify layers, export JAR/receipt và pull dependencies; không start/init | Không đo benchmark |
| Cold `make docker-up WAIT_TIMEOUT=600` từ 9 service stopped | PASS, 9 healthy + 2 init exited/0; MinIO init mới chạy, Airflow init current được reuse | 54.80s |
| Warm `make docker-up WAIT_TIMEOUT=600` | PASS, cả 11 container/image/StartedAt/FinishedAt/restart counts không đổi; cả hai init reuse | 17.34s |
| `make smoke-foundation DOCKER_CHECK_TIMEOUT=60`, lượt 1 | PASS toàn checklist | 71.04s |
| Cùng foundation smoke, lượt 2 | PASS toàn checklist | 63.34s |
| `make smoke-usgs-live DOCKER_CHECK_TIMEOUT=60`, lượt 1 | PASS, BronzeReady/readback, 16 records, same-run reuse | 47.86s |
| Cùng USGS live smoke, lượt 2 | PASS, BronzeReady/readback, 16 records, same-run reuse | 48.13s |

Các thời gian bao gồm config gate/QA tương ứng, là số đo riêng từng lần trên
máy local. Tải CPU khác nhau giữa hai lượt cold; **không** suy giảm từ 399.92s
xuống 54.80s hoàn toàn do đổi probe, không coi đây là SLA/benchmark có kiểm soát.
Cold ở đây là **service đã stop, volume cũ giữ nguyên**, không phải bootstrap
database/bucket rỗng. Trường hợp fresh init được regression-test offline;
không xóa dữ liệu thật để kiểm thử lại.

Mỗi foundation smoke xác nhận:

- MinIO ghi/đọc đúng bytes và cleanup đúng object Silver QA.
- `afl_01_smoke` thành công qua scheduler, không có DAG import error.
- Spark worker ALIVE, app thực thi 10 records với `id_sum=45`, exit 0.
- Trino tạo/insert/read Iceberg qua REST Catalog/MinIO, `row_count=1`, cleanup
  đúng bảng `iceberg.earthquakes.qry_01_smoke`.
- 9 services healthy/network/startup logs, 2 init exited/0,
  5 volume lifecycle và 6 mount checks đều PASS.

## Run IDs và kiểm chứng độc lập

| Run | ID | Metadata DB |
|---|---|---|
| Foundation 1 | `afl01-smoke-20261015T081053Z-8d79edbd42ec` | success, 1/1 task success |
| Foundation 2 | `afl01-smoke-20261015T081224Z-e5bb7e1f4064` | success, 1/1 task success |
| USGS 1 | `usg06-live-20261015T081325Z-588ba52e3925` | success, 10/10 tasks success |
| USGS 2 | `usg06-live-20261015T081413Z-1fe7f720948e` | success, 10/10 tasks success |

Spark apps: `app-20261015081113-0000`, `app-20261015081241-0001`.
USGS fixed window `[2023-01-01T00:00:00Z,2023-01-04T00:00:00Z)`, theo vùng/
filter nguồn đã cấu hình, không phải toàn cầu. Hai raw SHA trùng tại lần đo này;
không đặt quy tắc nguồn phải bất biến giữa hai run khác nhau. Mỗi live smoke
assert same-run upload reuse và readback SHA/manifest URI khớp; raw/manifest
Bronze của hai run QA được giữ lại đúng immutable policy.

## Bảo toàn dữ liệu và trạng thái cuối

- Kiểm tra không có queued/running DAG, source lease hoặc active Spark/Trino
  job trước maintenance. Chỉ stop/recreate service của project hiện tại.
- ID network vẫn `0f5d70d03f24`. Tên, CreatedAt và lifecycle cả 5 named volumes
  trước/sau không đổi; không down -v, prune, reset, xóa warehouse hoặc force unlock.
- GET/checksum 5 manifest cũ từ ORC-02 (1 USGS, 4 JMA segments) trước và sau
  trùng expected SHA; exact URIs trong JSON, không wildcard hay re-ingest JMA.
- Airflow giữ run ORC-02 cũ success; tổng run count **26 → 30**, đúng 4 smoke
  runs mới. Đây là kiểm tra sentinel/metadata, không rehash toàn bộ lake payload.
- So snapshot sau cold với warm và sau toàn bộ smoke: cả 11 service IDs,
  image IDs, StartedAt/FinishedAt và restart counts không đổi.
- Cuối cùng không có active DAG/Spark/Trino job, lease không tồn tại;
  5 DAG nghiệp vụ vẫn paused, `afl_01_smoke` vẫn unpaused như trước.
- Artifact receipt hợp lệ với source SHA
  `3faa875d7b8e806d6548a681c4d9212d6407cd4796ec3901aaafd72dd0eb7ff6`;
  runner JAR SHA `fd8138eefa3f0c4bf4362a39a326b49a67e44fd1b5096c2a299c551e3ff4aeb9`.

## Cách nghiệm thu lại

Chạy tuần tự trong root repo với `.env` local đã cấu hình:

```bash
make docker-build
# Xác nhận Airflow/Spark/Trino không có job active và source lease trống.
# Không stop nếu còn job hoặc lease; không tự clear lease.
make stop SERVICE="airflow-scheduler airflow-dag-processor airflow-api-server spark-worker spark-master trino iceberg-rest minio airflow-postgres"
make docker-up WAIT_TIMEOUT=600
make docker-check DOCKER_CHECK_TIMEOUT=60
make status
make docker-up WAIT_TIMEOUT=600
make smoke-foundation DOCKER_CHECK_TIMEOUT=60
make smoke-foundation DOCKER_CHECK_TIMEOUT=60
make smoke-usgs-live DOCKER_CHECK_TIMEOUT=60
make smoke-usgs-live DOCKER_CHECK_TIMEOUT=60
```

So ID/image/StartedAt/FinishedAt/restart counts ngay sau cold với sau warm/smoke
qua Docker inspect, chỉ chọn các field metadata trên, không dump env/config.
Đọc run/task states qua Airflow metadata/UI; đọc exact sentinel URI bằng pipeline
credential trong container và so SHA với JSON. Không đưa credential vào argv/log.
Không cần `docker-build` giữa các smoke khi build inputs không đổi.

## Checks và ranh giới

- `make test-docker-runtime`: **45 tests PASS**, gồm daemon/image distinction,
  một config gate + fail-before-Docker ở cả 7 wrappers, API probe contract,
  current-boot log read/empty/fatal/redaction và lifecycle/freshness checks cũ.
- `ENV_FILE=.env.example ./scripts/check-airflow.sh`: **216 tests PASS** và
  Compose/Airflow static contract PASS.
- `make ci-docs`: PASS (contracts/task parity/plans/build inputs/security31/
  lifecycle45/Compose/MinIO/query/CI workflow4/shell syntax).
- `git diff --check`: PASS; chưa commit/push/mở PR/merge.
- **QA-01 — Chạy E2E daily đa nguồn:** chưa chứng nhận một daily interval đi
  xuyên Bronze → Silver → Gold Published. Foundation + source smoke không thay
  E2E nghiệp vụ hoặc official ML dataset.
- **SEC-01 — Review secret và bề mặt truy cập:** giữ Review; cần retained-log
  re-audit và actual ML handoff acceptance khi DAT-02/EXP-01/MLD-05/MLI-03 có
  output. Startup log check scoped theo boot không đóng các security gates này.
- **QA-05 — Profile dữ liệu lịch sử và tài nguyên local:** không benchmark full-history hoặc
  chứng nhận mọi tải đồng thời trên RAM VM hiện tại. Reviewer độc lập vẫn được
  khuyến nghị, chưa ghi nhận approval.
