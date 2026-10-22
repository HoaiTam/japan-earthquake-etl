# SEC-01 — Runtime audit và bản vá security

> Mốc lịch sử sau PR #72. Trạng thái runtime mới nhất xem
> [audit 22/10/2026](./SEC-01_20261022.md); không dùng findings/deployment hay
> trạng thái FND-01/ORC-02 bên dưới làm kết luận cho stack hiện tại.

## Phạm vi / kết luận

- Branch `fix/sec-01-complete-security-audit`, từ main
  `7c2b8e8826ef13f4e88d61fc9b59bbc98443c816` (PR #72 đã merge).
- Owner hiện tại HoaiTam, reviewer unassigned; chưa commit/push/mở PR mới.
- Audit trên stack local đang chạy; không restart/build/pull/init, sửa .env,
  thay IAM, rotate hay xóa log/warehouse/volume.
- **SEC-01 giữ Review, không Done:** Trino giữ log chứa access-key identifier
  và logging mount mới chưa deploy; actual notebook/export/import acceptance chưa đạt.
- Timestamp UTC, counts/findings/bindings và permission evidence ở
  [report redacted](./SEC-01-runtime.json). Report đọc cuối và probe là hai
  execution riêng, không ghép thành một clean audit. Raw reports riêng ở staging,
  không đưa raw env/config/log hoặc matched credential vào repo.

## Kiểm tra đã chạy

| Check | Kết quả |
|---|---|
| `./scripts/check-config.sh --require-local` | PASS: config local strength/role/default, .env ignored/untracked |
| `make test-security-audit` | PASS: 31 offline tests, gồm stdin/JSON escaping, deployed role/ports/logging mount, redaction, allow/deny/readback/cleanup/failure và incomplete gates |
| `make ci-docs` | PASS: contracts/task parity, week-5 12 tests, Java-input 12 tests, security 31 tests, lifecycle 39 tests, Compose/MinIO/query static và CI workflow 4 tests |
| `make check-task-status` | PASS: 76 task files/index/tracking đồng bộ |
| `git diff --check`, shell syntax init/smoke | PASS |
| Source + reachable Git blobs | Không còn finding chưa triage trong phạm vi scan; 1.502 blobs tại mốc này |
| Runtime env/role/port/log read-only | Đã đọc 11 services; 4 loopback bindings đúng, root không ở consumer; 2 finding Trino, exit 1 đúng kỳ vọng fail-closed |
| Opt-in MinIO permission probe | PASS: 15 checks thật trên current pipeline credential, không dùng root |

Không chạy Java/ETL end-to-end ở phiên này: không sửa Java/DAG/data contract.
Unit/security audit không thay thế FND-01 startup/smoke hoặc ORC-02 scheduled gate.

## Findings và fix

1. **Fixture URI:** ba nội dung test mới được đọc/triage đúng checksum; chỉ
   deliberate reject/redaction test URI, không có actual token/key exemption.
   Cache IDE .metals chứa copy fixture, được ignore; không xóa cache hay tuyên bố
   toàn bộ private filesystem sạch.
2. **MinIO argv exposure:** init/smoke alias JSON chuyển sang stdin, user/password
   add chuyển sang hai dòng stdin; umask 077, private tmpfs config, sensitive CLI
   errors cố định. Source pinned upstream và mock regression xác nhận hỗ trợ.
   Chưa chạy bootstrap mới vào IAM thật; --user policy selector vẫn chứa public
   access-key identifier trong argv, không password/secret key.
3. **Trino logs:** scanner exact-current-value tìm access-key identifier ở hai
   catalog-bootstrap INFO records (hai lần startup trong retained logs). Không
   thấy secret key/password trong các dòng khớp. Fix source: logger hierarchy
   io.trino.bootstrap.catalog=WARN, bind read-only log.properties. Warning/error
   và query/runtime logging giữ nguyên. Container hiện tại chưa có mount này;
   audit không giả vờ file trên host đã deploy.

## Permission test và dữ liệu QA

Payload cố định 40 byte, SHA-256 và exact keys trong report. Tạo ba object QA mới
độc lập dữ liệu nghiệp vụ ở bronze/silver/warehouse; UUID cùng một attempt.

- PUT thành công + GET đúng bytes + exact-key LIST mỗi prefix (6 checks).
- Bronze DELETE trả AccessDenied/403, GET lại còn bytes (2).
- Anonymous GET/LIST trả AccessDenied/403 (2).
- Pipeline GET/LIST/PUT ngoài ba prefix trả AccessDenied/403 (3).
- Cleanup đúng hai key vừa tạo Silver/warehouse thành công (2).

Bronze QA giữ tại:

    s3://japan-earthquake/bronze/_security/sec-01-dddb426257854d21b37a5737c6dffb00/probe.txt

Hai mutable object cùng UUID đã xóa, là dữ liệu QA tổng hợp có thể tái tạo bằng
payload cố định; không có object nghiệp vụ/bucket/volume nào bị xóa. Không cố xóa
Bronze bằng root vì policy cố ý cấm. Probe không ghi _SUCCESS/BronzeReady, không
pin dataset hoặc thay đổi Gold Published. Không chạy lặp vô hạn vì mỗi lần thêm
40 byte Bronze QA (chưa tính object/storage metadata overhead).

## Việc cần để đóng task

- **SEC-01 — Review secret và bề mặt truy cập:** operator duyệt maintenance
  window deploy/recreate riêng Trino, kiểm tra startup/query và logs mới; xử lý
  retained-log finding có phối hợp. Không dùng truncate/delete/tail/since để che
  finding; đánh giá rotate nếu log đã chia sẻ ra ngoài. Chưa được duyệt nên phiên
  này không restart service hay thay credential.
- **DAT-02 — Dataset pilot nhỏ để test Jupyter**, **EXP-01 — Tạo Colab harness
  và khóa môi trường thí nghiệm**, **MLD-05 — Validate feature snapshot và export
  Parquet bundle**, **MLI-03 — Validate/import kết quả và commit bảng ML Iceberg**:
  actual artifacts/Drive handoff chưa có nghiệm thu; kiểm tra credential và local
  account-path boundary khi triển khai. Gate này chặn Done theo acceptance gốc,
  không chỉ là khuyến nghị re-audit.
- **FND-01 — Tạo smoke checklist cho môi trường**, **ORC-02 — Cấu hình lịch và
  readiness cho hai nguồn:** vẫn Review riêng, security probe không chứng nhận
  cold/warm startup hoặc scheduled/first/rerun/contention.
- Full CVE/SBOM/production threat assessment chưa có task riêng; pinned versions
  phục vụ baseline học/demo không phải cam kết production-safe.

Lệnh và cách đọc exit 1/2: [checklist SEC-01](../security/SEC-01-CHECKLIST.md).
