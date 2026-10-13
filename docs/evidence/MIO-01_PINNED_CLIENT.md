# MIO-01 — MinIO Client local source build

## Phạm vi và nguồn

- Kiểm chứng ngày 2026-10-13, branch `fix/mio-01-build-pinned-client`, từ
  `origin/main` `490b0ba` (PR #69 đã merge). Chưa commit/push/mở PR hotfix.
- Lỗi người dùng: `DEPENDENCY_IMAGE_PULL_FAILED`, Quay trả `401 UNAUTHORIZED`
  khi Docker tải `mc`. Không phải mật khẩu object storage.
- Upstream release: [RELEASE.2025-08-13T08-35-41Z](https://github.com/minio/mc/releases/tag/RELEASE.2025-08-13T08-35-41Z),
  commit `7394ce0dd2a80935aded936b09fa12cbb3cb8096`.
- Dockerfile kiểm tra HEAD/tag trước build, dùng generator ldflags chính thức
  với timestamp release (khác commit date), verify version/commit sau build.
- `minio-init` và `minio-smoke` cùng dùng image
  `japan-earthquake-etl/mc:RELEASE.2025-08-13T08-35-41Z`, runtime Alpine,
  shell/CA/LICENSE/CREDITS, user non-root 1000. Không đổi init/policy script.
- Lifecycle build client, không pull nó như dependency; preflight kiểm tra label,
  source/context/pins và cùng image ở hai services. Smoke không tự build/start.

## Checks tự động đã chạy

| Check | Kết quả |
|---|---|
| `make test-docker-runtime` | PASS — 39 tests, gồm 6 regression mới |
| `make check-minio check-compose CHECK_ENV_FILE=.env.example` | PASS |
| `make ci-docs` | PASS — contracts/task parity, Java-input 12 tests, security 11 tests, lifecycle 39 tests, Compose và CI workflow 4 tests |
| `make ci-airflow` | PASS — 206 tests |
| `git diff --check` | PASS |

Regression mới kiểm tra client pins/context không bị override, init/smoke không
dùng hai image khác nhau, missing/stale label fail closed không pull, client
Dockerfile/context allowlist đổi thì fingerprint đổi, image đổi yêu cầu startup
init rõ ràng, và binary/version/shell/TLS/source contract. Test build hiện có
đối chiếu client nằm trong build nhưng không còn trong pull list.

## Build Docker thật

Build riêng `minio-init` bằng Compose với `.env.example`, truyền
`PIPELINE_BUILD_SHA` từ `scripts/docker-runtime.py::source_sha()`. Đây chỉ là
build image, không chạy init/service. Các upstream source checks đạt; Go download/
compile step hoàn tất khoảng 40 giây trên lần chạy này (không phải cam kết SLA).

Output đã kiểm tra:

```text
mc version RELEASE.2025-08-13T08-35-41Z (commit-id=7394ce0dd2a80935aded936b09fa12cbb3cb8096)
minio-init Built
```

## Kiểm chứng component runtime thật, cách cô lập

Harness thủ công chỉ dùng image local đã chuẩn bị, `--pull never`:

1. Tạo đúng một network QA tên `ie212-mc-qa-<UUID>`, `--internal`.
2. Chạy server image hiện có ở container riêng `ie212-mc-server-<UUID>`, alias
   `minio`; không publish host port, không mount named volume. Data chỉ ở tmpfs
   `/data` 128 MiB (UID/GID 1000), `/tmp` 64 MiB; limit 1 CPU/512 MiB,
   read-only root, cap-drop ALL và no-new-privileges.
3. Credential test sinh ngẫu nhiên trong memory, truyền bằng environment;
   không đọc `.env` thật, không ghi/in secret. Bucket riêng `mio-01-client-qa`.
4. Chờ health endpoint, chạy client non-root/read-only/tmpfs để kiểm tra version,
   CA, LICENSE/CREDITS và `/bin/sh`.
5. Bind read-only **script thật** `compose/minio/init.sh`, rồi `smoke.sh`; cold
   bootstrap và write/read/delete đúng temporary smoke object đạt.
6. Ghi một object vào Silver QA, chạy lại init, readback cùng nội dung: không mất
   object sau rerun. Ghi object Bronze, thử xóa bằng pipeline credential phải fail;
   đọc lại Bronze giữ nguyên bytes.
7. Chạy lại smoke thành công. Cleanup trong `finally`: remove chỉ container
   server/network QA mới tạo, không `-v`; tmpfs-only test data được bỏ đi.

Output đã quan sát:

```text
PASS: pinned version/commit, non-root shell, CA and licenses
MinIO bootstrap complete: bucket=mio-01-client-qa prefixes=bronze,silver,warehouse
MIO-01 MinIO write/read smoke test passed.
MinIO bootstrap complete: bucket=mio-01-client-qa prefixes=bronze,silver,warehouse
PASS: init rerun retains data; pipeline cannot delete Bronze
MIO-01 MinIO write/read smoke test passed.
PASS: isolated client integration; no project volumes, credentials, ports or stack restart
Temporary QA resources cleaned; tmpfs-only test data discarded
```

Để kiểm chứng trên stack của operator khi không có job active:

```bash
make ci-docs
make docker-build
make up-minio WAIT_TIMEOUT=600
make smoke-minio
make up-minio WAIT_TIMEOUT=600
make smoke-minio
```

Lần `up-minio` đầu cập nhật init image; lần sau reuse exited/0 đúng config/image.
Sau đó `make docker-up WAIT_TIMEOUT=600` và full-stack smoke theo runbook.
Không dùng lệnh build client thủ công để chứng nhận tất cả images/JAR đã fresh:
source fingerprint tổng đổi nên cần `docker-build` trước startup/preflight.

## Giới hạn / bước tiếp theo

- **MIO-01 — Cấu hình MinIO và bucket init:** acceptance component hotfix đã đủ
  nên trở lại Done; reviewer độc lập `unassigned`, không tự ghi approval.
- **FND-01 — Smoke checklist cho môi trường:** chưa chạy lại cold/warm startup
  và full-stack smoke trên project thật sau hotfix; giữ Review. Component QA
  tmpfs không thay thế nghiệm thu lifecycle toàn bộ Airflow/Spark/Trino.
- **SEC-01 — Review secret và bề mặt truy cập:** pinned legacy upstream/Go/Alpine
  phục vụ học/demo, không phải bảo đảm CVE hay hỗ trợ production. Cần review
  nguồn/license/CVE trước vận hành ngoài baseline; không đổi latest/bỏ TLS.
- Build vẫn cần Docker Hub, GitHub, Alpine và Go proxy/checksum service.
- Không rerun ETL/USGS/JMA, không sửa Bronze/Silver/Gold schema hoặc warehouse;
  không cần data backfill để áp dụng hotfix client.
