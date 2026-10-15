# SEC-01 — Review secret và bề mặt truy cập

Owner hiện tại: **HoaiTam**. Status: **Review**. Reviewer: **unassigned**.
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

Guard file/blob/local log: 2 MiB; container logs: 64 MiB mỗi container, timeout
30s. Vượt guard/timeout là coverage pending, không skip để Done. Reflogs,
unreachable objects, external Drive/accounts, named-volume files không phát ra
Docker logs, máy khác và arbitrary local files nằm ngoài phạm vi.
.ipynb, notebook/experiment/export/pilot/result paths hiện có trong phạm vi
worktree/log được thêm account-path/token-query patterns; ignored bundle không
tự được chứng nhận.

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
    make audit-security-runtime SECURITY_REPORT=staging/sec-01-runtime-current.json

Unit tests không Docker/network. Static cần Compose CLI; runtime cần .env local
hợp lệ và stack đã start, chỉ hỗ trợ .env/compose.yaml mặc định. Không target
nào tự build/pull/start/init. Đọc findings, pending, coverage.runtime.permissions,
coverage.current_baseline_passed, can_mark_done trong report.

Đây là audit chính stack đang deployed, không phải application smoke: không yêu
cầu Java/JAR fingerprint mới để có thể tìm exposure ở runtime cũ. Nó có preflight
security riêng (CFG, project scope, role/env/ports/health trước writes). Functional
smokes hiện có vẫn giữ docker-runtime preflight/freshness, không được bypass.

- Exit **1**: findings/error; Make báo lỗi. Đọc category/service, không in raw logs.
- Exit **2**: coverage/acceptance chưa đủ; Make báo lỗi. Static sạch không là Done.
- Exit **0** dành cho đầy đủ gate, không findings/pending. Actual ML handoff gate
  hiện luôn pending, chưa có triển khai/nghiệm thu; không hứa code hiện tại sẽ
  tự chuyển Done khi một file notebook mới xuất hiện.

## 5. Checklist hiện tại / còn lại

| Check | Kết quả / evidence |
|---|---|
| Current source + reachable Git history | Không còn finding chưa triage trong phạm vi scanner |
| .env ignore/sample/default/strength/roles | CFG local PASS; không lưu values |
| Deployed env/role/ports | 11 services, 4 loopback bindings; root không ở consumer |
| Retained Docker logs | FAIL: Trino catalog bootstrap in access-key identifier; không thấy secret key/password trong các dòng khớp |
| MinIO expected allow/deny + cleanup | PASS: 15 checks thật, giữ Bronze QA; hai mutable QA keys đã xóa |
| MinIO stdin changes | Offline regression PASS, đối chiếu pinned upstream; chưa bootstrap mới trên IAM thật |
| Trino logging fix | Code/static tests có; container hiện tại chưa deploy |
| Actual notebook/export/import | Pending DAT-02/EXP-01/MLD-05/MLI-03; không chứng nhận artifacts tương lai |

[evidence runtime](../evidence/SEC-01_RUNTIME.md),
[report redacted](../evidence/SEC-01-runtime.json).

**SEC-01 — Review secret và bề mặt truy cập:** cần maintenance window deploy
logging fix, kiểm tra startup/query/logs mới; operator quyết định xử lý retained
log finding. Không truncate/delete log, dùng tail/since che finding hoặc exempt
credential. Access-key identifier không phải secret key nhưng vẫn sensitive theo
contract. Nếu logs từng chia sẻ ra ngoài, đánh giá rotate có phối hợp; không
đổi .env/IAM một consumer riêng lẻ để làm xanh audit.

**DAT-02 — Dataset pilot nhỏ để test Jupyter**, **EXP-01 — Tạo Colab harness và
khóa môi trường thí nghiệm**, **MLD-05 — Validate feature snapshot và export
Parquet bundle**, **MLI-03 — Validate/import kết quả và commit bảng ML Iceberg**:
kiểm tra actual notebook/pilot/export/result bundle, account-path và quyền/credential
handoff khi artifacts tồn tại. SEC-01 giữ acceptance này chưa đạt; fixture/không
có file không chứng minh không leak.

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
