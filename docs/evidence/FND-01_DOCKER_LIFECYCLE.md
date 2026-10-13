# FND-01 — Tách Docker lifecycle khỏi smoke test

- Branch: `fix/fnd-01-separate-docker-lifecycle`, từ main `03b6d00`
  (PR#67/68 đã merge). Không commit/push/mở PR trong lượt implementation.
- Assignee: HoaiTam; reviewer unassigned, chưa có approval.
- Status: Review: code/regression checks hoàn tất, còn runtime acceptance.
- Request: smoke USGS chờ build/start/init hơn10 phút; operator ngắt lệnh.
  Ảnh cho thấy init Waiting, không chứng minh một lỗi USGS hay nguyên nhân init.

## Đã triển khai

- `docker-build`: build 4 images, Maven verify Java17 trong Docker, export đúng
  JAR/Iceberg artifacts từ container mới **không start**, network none, cleanup
  chỉ container export của lần build. Host Java26 không bị dùng cho build này.
- `docker-up`: start đúng thứ tự, chỉ init khi chưa có exited/0 khớp image/config,
  không build/pull; service healthy/current không bị gọi up lại. Init đang chạy
  config cũ bị chặn, không force replace hoặc chạy migration song song.
- Tất cả smoke wrappers/Make recipes bỏ build/up/init/Maven verify tự động,
  dùng read-only preflight và `run --no-deps --pull never` hoặc exec đã có.
- Label SHA build inputs, running image ID, config hash và host artifact receipt
  chặn stale image/JAR. Custom Compose không được tráo checkout/build args/mounts
  rồi dùng fingerprint nguồn hiện tại. Không bỏ actual lineage/quality/publish gates.
- Docs entrypoints/runtime runbooks cập nhật theo [workflow mới](../DOCKER_RUNTIME_WORKFLOW.md).

## Test/check

- `make test-docker-runtime`: **33 tests PASS**, Docker/Maven doubles, Make dry-run,
  real shell USGS/JMA wrappers với boundary doubles. Không gọi source/storage thật.
  Bao gồm no-side-effect checks, source/image/config drift, thiếu/unhealthy/paused
  service, init reuse/cold ordering/failed init/current running init/old-config
  refusal, timeout/redaction, export/failed build, checksum receipt và mounts.
- `python3 -B -m unittest discover -s airflow/tests -p 'test_*.py'`:
  **206 tests PASS**; các business/control-plane contracts hiện tại giữ nguyên.
- `make ci-docs`: docs/contracts/task-status/config/example/Compose/Java build-input
  checks, security tests và workflow tests: PASS trước bàn giao.
- `git diff --check`: PASS; không thay `.env`, `.metals`, source data/warehouse
  hoặc Compose volumes/permissions/ports. Không thực hiện build/start/init/live
  ingest trong lượt này. Java source/POM và GitHub CI workflow không thay đổi.

## Read-only check trên Docker thực

Đã chạy `ENV_FILE=.env.example DOCKER_CHECK_TIMEOUT=3 python3 -B scripts/docker-runtime.py check --component usgs`.
Docker command không phản hồi trong budget: exit1, reason
`DOCKER_COMMAND_UNAVAILABLE_OR_TIMEOUT`. Đây là **negative/timeout evidence**, không
health/build/startup thành công; không log config values hoặc tự restart Docker.

## Acceptance còn lại

- **FND-01 — Tạo smoke checklist cho môi trường:** khi Docker Desktop/API hoạt
  động ổn định, chạy `docker-build` một lần để migrate labels/receipts; chạy
  cold `docker-up`, kiểm tra 9 healthy/2 init exited0, rồi gọi lại warm `docker-up`
  để xác nhận reuse. Chạy foundation và USGS live smoke hai lần: không build/start/
  init lại, nhưng vẫn có real readback/rerun. Chưa chứng minh nguyên nhân init chậm
  hay đo được startup/performance trên môi trường operator.
- **QA-01 — Chạy E2E daily đa nguồn:** production adapters/full-month state
  không thuộc bản vá; giữ các QA pin/coverage limits hiện có.
- **SEC-01 — Review secret và bề mặt truy cập:** fingerprint không thay nghiệm
  thu credential strength/role/bucket-prefix/runtime log redaction.

Không đánh dấu FND-01 Done bằng fixtures hoặc runtime cũ. Sau live acceptance
thật, cập nhật evidence/status/index; không xóa data/volume để làm test pass.
