# SEC-01 — Review secret và bề mặt truy cập

Owner hiện tại: **HoaiTam**. Status: **In Progress**. Reviewer: **unassigned**.
Trang/Windows và [report static cũ](../evidence/SEC-01-static.json) là mốc lịch
sử, không chứng nhận runtime máy hiện tại. ORC-05 đã Done.

## 1. Làm gì và dùng để làm gì

Có scanner/report an toàn để nhóm kiểm tra trước chia sẻ source, log và demo;
kiểm tra quyền deployed thật thay vì suy ra từ policy source. Bản vá gồm:

- Source/history scan, exact-hash triage fixture URI cố ý reject.
- CFG local strength/default/role checks; deployed env/port/role audit.
- Scan toàn bộ retained Docker stdout/stderr logs, chỉ ghi category/service.
- MinIO allow/deny probes key UUID riêng và exact-key cleanup.
- MinIO alias/user password qua stdin, không còn password trong mc argv.
- Trino catalog bootstrap WARN để INFO không in S3 access-key identifier.

Không tự commit/push, restart/deploy, sửa .env/IAM, rotate hay xóa log/volume.
Code fix khác với runtime đã áp dụng fix; finding chưa xử lý vẫn fail audit.

## 2. Phạm vi và giới hạn scanner

scripts/audit-security.py scan tracked/nonignored worktree files, reachable
Git blobs trên tất cả refs local; kiểm tra .env ignored/untracked và local
logs ở airflow/logs/staging (không Maven cache). .env đọc riêng, không source
hoặc serialize. Tokens/private keys/credential URIs dùng high-confidence patterns;
credential hiện tại dài ít nhất 16 ký tự được đối chiếu exact bytes trong
source/history/logs. Không phải scanner nhận diện mọi secret hoặc mọi giá trị cũ.

SEC-01-synthetic-uris.json chỉ exempt credential_uri của nội dung fixture đã
review, checksum SHA-256 normalized CRLF/LF chính xác; token/key/exact credential
không exempt. Fixture thay đổi phải review lại. .metals/ là cache IDE bị Git
ignore, không bị xóa; không tuyên bố cả ổ đĩa local đã được audit.

Guard file/blob/local log: 2 MiB. Container logs streaming toàn retained
stdout/stderr: 512 MiB/container, line 2 MiB, timeout 120s; không giữ raw log
trong report/disk. Vượt guard/timeout là coverage pending, không skip để Done. Reflogs,
unreachable objects, external Drive/accounts, named-volume files không phát ra
Docker logs, máy khác và arbitrary local files nằm ngoài phạm vi.
.ipynb, notebook/experiment/export/pilot/result paths hiện có trong phạm vi
worktree/log được thêm account-path/token-query patterns; ignored bundle không
tự được chứng nhận. Handoff chỉ scan explicit pinned artifacts theo mục 7.

Compose khai báo dùng .env.example resolve placeholders để kiểm tra kiểu port;
runtime dùng .env thật, giữ resolved config trong memory. Không in resolved
Compose config hoặc Docker inspect environment vào evidence.

## 3. Runtime và permission probes

scripts/security-runtime.py xác nhận container ID/Compose project/working-dir,
không dùng project khác hoặc one-off làm nghiệm thu foundation. Yêu cầu 11
services: runtime healthy, init exited/0. Đối chiếu credential-sensitive env,
root chỉ MinIO/init, 4 port loopback/config. MinIO API, Catalog, Spark RPC và
metadata DB không publish ra host. Trino logging config phải bind đúng source,
read-only; không chỉ tồn tại trên host.

scripts/security-s3-probe.py được gửi qua stdin vào đúng Airflow API container
đã xác minh; dùng boto3/SigV4, pipeline credential của container, không root.
Không gửi credential qua argv; report không có value/raw SDK exception.
Payload cố định 40 byte, ba key mới mỗi lần:

    <bronze|silver|warehouse>/_security/sec-01-<UUID>/probe.txt

- PUT/GET/exact-key LIST cả ba prefix, payload hash/readback đúng.
- Bronze DELETE phải AccessDenied/403; GET lại còn bytes.
- Anonymous GET/LIST và pipeline GET/LIST/PUT ngoài ba prefix phải AccessDenied/403.
- Timeout, SignatureDoesNotMatch hoặc NoSuchKey không là permission-denied PASS.
- Cleanup chỉ hai key mới Silver/warehouse trong finally. Bronze QA giữ lại;
  không tạo manifest BronzeReady hoặc sửa dữ liệu nghiệp vụ. Policy sai cho phép
  outside-prefix PUT thì report giữ exact key đã tạo, không xóa ngoài scope.
- 15 checks phải đầy đủ/true/đúng key/hash; passed=true đơn lẻ không đủ.

Finding retained logs không ngăn scoped probes nếu role/env/ports/health/scope
đạt; vẫn chặn kết quả chung. Deployment finding/health không đạt thì không writes.
Mỗi lần thêm một Bronze QA nhỏ, không chạy audit ghi object theo lịch vô hạn.

## 4. Lệnh chạy và đọc kết quả

Từ repository root:

    make test-security-audit
    make audit-security SECURITY_REPORT=staging/sec-01-static-current.json
    make audit-security-runtime-readonly SECURITY_REPORT=staging/sec-01-readonly.json
    make audit-security-runtime SECURITY_REPORT=staging/sec-01-runtime-current.json

Unit tests không Docker/network. Static cần Compose CLI; runtime cần .env local
hợp lệ và stack đã start, chỉ hỗ trợ .env/compose.yaml mặc định. Readonly target
không chạy S3 permission probes. `SECURITY_HANDOFF_MANIFEST` trỏ tới input ở mục
7; thiếu nó vẫn pending. `SECURITY_DEPLOYMENT_ROOT` chỉ nhận explicit linked
checkout cùng Git common dir, không repo khác; credential/config và Docker
working-dir labels vẫn đối chiếu checkout đang deployed. Không target
nào tự build/pull/start/init. Đọc findings, pending, coverage.runtime.permissions,
coverage.current_baseline_passed, can_mark_done trong report.

Đây là audit chính stack đang deployed, không phải application smoke: không yêu
cầu Java/JAR fingerprint mới để có thể tìm exposure ở runtime cũ. Nó có preflight
security riêng (CFG, project scope, role/env/ports/health trước writes). Functional
smokes hiện có vẫn giữ docker-runtime preflight/freshness, không được bypass.

- Exit **1**: findings/error; Make báo lỗi. Đọc category/service, không in raw logs.
- Exit **2**: coverage/acceptance chưa đủ; Make báo lỗi. Static sạch không là Done.
- Exit **0** dành cho đầy đủ gate, không findings/pending. Handoff gate local đã
  triển khai, chỉ đạt khi đủ roles/files/checksum và scan bytes thật. File
  notebook mới tự xuất hiện hoặc task downstream Done không là authorization.
  `can_mark_done` của scanner vẫn phải đối chiếu acceptance remote Colab/Drive
  nếu đã dùng, không lấy local scope làm chứng nhận account/session khác.

## 5. Checklist hiện tại / còn lại

| Check | Kết quả / evidence |
|---|---|
| Current source + reachable Git history | Không còn finding chưa triage trong phạm vi scanner |
| .env ignore/sample/default/strength/roles | CFG local PASS; không lưu values |
| Deployed env/role/ports | 11 services, 4 loopback bindings; root không ở consumer |
| Retained Docker logs | 22/10 đọc đủ 11 containers, không finding hiện tại; finding/log scope lịch sử giữ riêng |
| MinIO expected allow/deny + cleanup | 15 checks lịch sử đạt; fresh probes chờ sau QA-01 23/10, không reuse làm current PASS |
| MinIO stdin changes | Offline regression PASS, đối chiếu pinned upstream; chưa bootstrap mới trên IAM thật |
| Trino logging fix | Đã mount đúng source/read-only, không cần restart trong audit này |
| Actual notebook/export/import local | PASS: 5 artifacts/23 files của DAT-02/EXP-01/MLD-05/MLI-03; không chứng nhận remote Google account hoặc scientific approval |
| Current full runtime | Spark master/worker đang dừng; owner yêu cầu chờ QA-01 ngày mai, không đổi health gate |

[evidence 22/10](../evidence/SEC-01_20261022.md),
[report redacted mới](../evidence/SEC-01-20261022.json).
[Evidence cũ](../evidence/SEC-01_RUNTIME.md) giữ làm mốc lịch sử, không ghi đè.

**SEC-01 — Review secret và bề mặt truy cập:** sau QA-01 scheduled 23/10/2026
07:15 kết thúc, xác nhận không active run/foreign lease rồi mới nghiệm thu
runtime/permissions; lần hiện tại không start Spark. Không truncate/delete log,
dùng tail/since che finding hoặc exempt
credential. Access-key identifier không phải secret key nhưng vẫn sensitive theo
contract. Nếu logs từng chia sẻ ra ngoài, đánh giá rotate có phối hợp; không
đổi .env/IAM một consumer riêng lẻ để làm xanh audit.

**DAT-02 — Dataset pilot nhỏ để test Jupyter**, **EXP-01 — Tạo Colab harness và
khóa môi trường thí nghiệm**, **MLD-05 — Validate feature snapshot và export
Parquet bundle**, **MLI-03 — Validate/import kết quả và commit bảng ML Iceberg**:
kiểm tra actual notebook/pilot/export/result bundle, account-path và quyền/credential
handoff khi artifacts đổi. Owner xác nhận 22/10 chưa dùng Colab/Drive, chỉ
Jupyter local. Bytes local đã scan không chứng minh remote Colab env/Drive
sharing permissions; không tick acceptance cloud trước triển khai. Khi chuyển lên cloud, chỉ audit đúng project/
session được cho phép; không tự truy cập account khác hoặc coi thiếu file là PASS.

## 6. Hardening và nguồn

umask 077, alias config trong tmpfs. Stdin được hỗ trợ ở pinned
[alias-import.go](https://github.com/minio/mc/blob/7394ce0dd2a80935aded936b09fa12cbb3cb8096/cmd/alias-import.go)
và [admin-user-add.go](https://github.com/minio/mc/blob/7394ce0dd2a80935aded936b09fa12cbb3cb8096/cmd/admin-user-add.go).
Policy attach vẫn dùng --user access-key identifier làm selector, không password;
không bật shell tracing hay chia sẻ process listing. Docker operator vẫn đọc
được env/config: máy/Docker là trusted boundary, không che secret khỏi admin.

Trino hỗ trợ logger hierarchy ở
[log.properties](https://trino.io/docs/current/admin/logging.html).
Chỉ io.trino.bootstrap.catalog=WARN, không tắt warning/error hay query/runtime
loggers; không bảo đảm future query/exception không leak. Full CVE/SBOM/supply-chain
certification của MinIO/Go/Alpine/Trino không nằm trong review này; production
assessment chưa có task riêng.

## 7. Pinned ML handoff protocol

`scripts/security_handoff.py` không thực thi notebook, pickle/model artifacts,
ETL hoặc cloud API. Input JSON (duplicate keys bị từ chối) có đúng hai keys:
`version="sec-01-handoff-v1"`, `artifacts` (5–32 items). Mỗi item có đúng
`task_id`, `kind`, `path`, `sha256`; path relative trong deployment checkout,
không traversal/symlink. Phải đủ năm role pairs:

| task_id | kind | Kiểm tra layout |
|---|---|---|
| DAT-02 | pilot_zip | DEVELOPMENT_PILOT/research false, coverage/mainshocks/features và exact checksum file |
| EXP-01 | notebook | `.ipynb` nbformat 4, cells không rỗng; scan source/output/metadata |
| EXP-01 | result_bundle | Pilot hoặc official completion marker, required artifacts và exact checksums |
| MLD-05 | dataset_bundle | EXPORTED manifest, export-checksums, features và enclosing checksums |
| MLI-03 | result_bundle | Official `_SUCCESS.json` COMPLETED, không nhận pilot marker, exact required artifacts/checksums |

ZIP/notebook pin SHA-256 bytes. Directory pin SHA-256 của canonical JSON
filename→file SHA, sorted keys, separators `(',', ':')` UTF-8; helper
`security_handoff.files_for(path,kind)` trả files/pin mà không ghi dữ liệu.
Không nhận success=true do caller tự điền. Marker/manifest được hash trong pin;
optional `checksums.sha256` của result phải khớp inner marker checksum map.

Guard: 256 files/directories, 32 MiB/file, tổng decoded/file bytes 128 MiB,
Parquet ≤100.000 rows và 256 columns; row-group declared uncompressed size được
kiểm trước decode. ZIP không extract: chặn duplicate names, traversal, symlinks,
encrypted entries và budget. JSON/string/Parquet values/footer được scan sau
decode (không chỉ grep compressed bytes). Thiếu PyArrow hoặc không đọc được file
thì pending; không tự cài thư viện. Dùng môi trường lock của EXP-01/DOC-01.

Report chỉ có artifact index, category và hashes/counts; tên file được hash
trong proof để không echo filename chứa credential. Audit trên local snapshot
trong trusted operator boundary, không là cam kết chống arbitrary hostile local
filesystem race hoặc chứng nhận toàn bộ ổ đĩa/cloud.

Phiên 22/10 ở worktree riêng dùng manifest ignored có sẵn:

```bash
make audit-security-runtime-readonly \
  SECURITY_DEPLOYMENT_ROOT=../japan-earthquake-etl \
  SECURITY_HANDOFF_MANIFEST=staging/sec01-handoff-20261022.json \
  SECURITY_REPORT=staging/sec01-full-readonly-20261022.json
```

Manifest staging không có trên máy mới: phải chọn actual artifacts và tạo pin
mới, không copy một report PASSED thay cho file thật. Sau owner cho nghiệm thu,
stack healthy và QA-01 không còn active, target `audit-security-runtime` thêm
exact-key permission probes; không chạy nó trong thời gian chờ đã được yêu cầu.
