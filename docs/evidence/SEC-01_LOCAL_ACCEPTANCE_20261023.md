# SEC-01 — Nghiệm thu Docker/Jupyter local ngày 23/10/2026

## Kết luận và phạm vi

**Done trong phạm vi local được owner duyệt.** Assignee HoaiTam, reviewer
`unassigned`; chưa có independent approval. User đồng ý nghiệm thu local,
start đúng hai Spark containers cũ và fresh MinIO permission probes. Colab/Drive
chưa triển khai, **không áp dụng/chưa chứng nhận**, không tự tick remote PASS.
Trước migration phải mở lại **SEC-01 — Review secret và bề mặt truy cập** ở
`Needs Update`, kiểm actual session/env, bundle và sharing permissions; chưa có
task cloud-security riêng. Source/history/log/role/port/permission gates giữ đủ.

Auditor ở branch `fix/sec-01-ml-handoff-audit`, commit `3d2250d` cộng docs
acceptance; deployment là checkout PR #96 tại `b93456e`. Hai checkout có cùng
Git common directory, không checkout lại branch đang bind Airflow. Phiên này
không commit/push/merge hoặc mở PR.

## An toàn vận hành

- Scheduled QA-01 `scheduled__2026-10-23T00:15:00+00:00` đã **failed** tại
  `usgs_bronze`; đây không là E2E PASS và SEC-01 không sửa/xác nhận thay QA-01.
- Trước audit: active queued/running DagRuns = **0**. Pause đúng
  `orc_02_daily_sources`, readback `is_paused=true`. Không đổi cron 07:15,
  trigger/clear/retry/mark-success hoặc sửa run state.
- Source lease vẫn thuộc exact failed run trên; SHA-256 trước và sau audit:
  `a0416eda1133685335f12d980cdf0ec1f6f05b5e4d514eb9662911b0baff41e7`.
  Không acquire/takeover/release lease. Probe keys độc lập với ingest/ETL.
- Start đúng `spark-master` và `spark-worker` containers cũ sau xác minh
  project/service/working-dir labels. Không build/pull/init/recreate, reset,
  thay credential/IAM, xóa log/bucket/volume hoặc dữ liệu nghiệp vụ.
- Sau audit: active runs vẫn **0**, daily vẫn paused, exact lease hash không đổi.
  Spark master/worker đang healthy, không chạy thêm Spark job.

## Kiểm tra thực tế

Lệnh chạy từ SEC-01 worktree (manifest ignored đã có sẵn, không chứa secret):

```bash
make test-security-audit
make audit-security-runtime \
  SECURITY_DEPLOYMENT_ROOT=../japan-earthquake-etl \
  SECURITY_HANDOFF_MANIFEST=staging/sec01-handoff-20261022.json \
  SECURITY_REPORT=staging/sec01-runtime-20261023.json
```

- Offline regression: **46 tests PASS**. Cảnh báo sysctl của PyArrow trong
  sandbox không làm test fail; không cài thêm package.
- `make ci-docs-base`: PASS, gồm docs/contracts, 46 security regressions,
  54 Docker lifecycle guards và 38 CI workflow tests; không build/start runtime.
- `make check-task-status check-week-5-plan test-week-5-plan check-docs`: PASS
  (76 task statuses khớp, kế hoạch vẫn 15 task/86h, 17 planning tests).
- `git diff --check`: PASS. Phiên này chỉ thêm/sửa docs/evidence, không đổi
  Java/DAG/experiment code nên không rebuild Maven hoặc chạy lại ETL.
- Final source/history/handoff re-scan sau cập nhật docs: 0 findings. Report
  ignored `staging/sec01-final-static-20261023.json` dùng chế độ static nên
  exit 2 với hai pending dự kiến (local strength/runtime không chạy ở chế độ
  này); không lấy report static thay full runtime exit 0 ở trên và không chạy
  thêm permission writes để đổi exit code.
- Runtime audit: **exit 0, findings=0, pending=0, can_mark_done=true**.
- Audit từ **14:38:02 đến 14:38:33 ngày 23/10/2026 (Asia/Ho_Chi_Minh)**;
  UTC timestamps chính xác trong [report redacted](./SEC-01-20261023.json).
- Source: 744 files tại thời điểm scan, 2.243 reachable Git blobs, 19 local
  log files. URI fixtures chỉ triage exact reviewed hashes, không exempt secret.
- 11 services đúng trạng thái: 9 runtime services healthy, 2 init exited/0.
  Config strength/roles đạt; root credential không ở consumer; 4 host bindings
  đều loopback. MinIO API/Catalog/Spark RPC/database không public ra host.
- Toàn bộ retained stdout/stderr logs của 11 containers đọc đủ, không tail/since
  hoặc truncate; per-service byte count/SHA/complete được lưu trong report.
- Handoff **5 artifacts/23 files** giữ exact manifest SHA
  `1f982a76a180ae28a3027fa61085d5e8a60db32c5eac5b2ed59209e935b48e1f`:
  DAT-02 pilot ZIP, EXP-01 notebook/result, MLD-05 exported dataset và MLI-03
  official-layout QA result. Checksum/marker/ZIP/decoded Parquet scan đạt.
  MLI-03 result và MLD-05 acceptance có dữ liệu tổng hợp; không nhận đây là
  output model đã huấn luyện hoặc scientific approval.

### Fresh MinIO permission evidence

Đủ **15 checks PASS**, bằng pipeline credential của đúng Airflow API container:
PUT/readback và exact-key LIST Bronze/Silver/warehouse; Bronze DeleteObject
AccessDenied và vẫn đọc được; anonymous GET/LIST bị từ chối; pipeline GET/LIST/PUT
ngoài allowed prefixes bị từ chối; hai mutable probe cleanup đạt. Chỉ
AccessDenied/403 được tính deny PASS, không dùng timeout/not-found thay bằng chứng.

Ba key được tạo dưới `<bronze|silver|warehouse>/_security/` với cùng suffix:
`sec-01-133fde88e0684edfb55fa43fd0699d12/probe.txt`.

- Hai key Silver/warehouse đã được xóa đúng key QA vừa tạo, không xóa dữ liệu
  nghiệp vụ; cleanup API success nằm trong 15 checks.
- Key **Bronze QA giữ lại** do DeleteObject bị cấm. Payload cố định 40 bytes,
  SHA-256 `9b7d28093029a73cb6c21fcadd84a9df3b9ca30e3ab77c23823a07c4ce6100ca`;
  không tạo BronzeReady manifest, không dùng làm input khoa học.
- Không chạy lặp audit có writes theo lịch. Các key chính xác và payload hash
  nằm trong report; không chứa credential hoặc raw SDK exceptions.

## Evidence và giới hạn

- [Report redacted hiện tại](./SEC-01-20261023.json) giữ runtime log proofs,
  permission checks, handoff digests và phạm vi local được owner duyệt.
- Raw safe report ở ignored `staging/sec01-runtime-20261023.json`, SHA-256
  `5d844fe63c55ad7d4ac2b28659e6e2d8421125c343846003aa82119768c56e14`.
  Bản redacted commit bỏ danh sách fixture triage dài và per-file hashed names,
  không giả là byte-for-byte cùng raw report.
- [Audit 22/10](./SEC-01_20261022.md) giữ nguyên lịch sử thiếu runtime/probes;
  evidence mới này thay kết luận hiện hành, không viết lại lịch sử.
- Scanner là high-confidence patterns/current-config exact matching trong scope,
  không chứng nhận mọi secret có thể tồn tại, toàn ổ đĩa, Git unreachable blobs,
  cloud account hoặc full CVE/SBOM/supply-chain assessment.
- **QA-01 — Daily E2E** còn failed/current-month acceptance chưa đạt; cần chẩn
  đoán `usgs_bronze` và recovery có kiểm soát, không tự giải phóng lease ở SEC-01.
- **SEC-01 — Review secret và bề mặt truy cập** phải được mở lại trước cloud
  migration hoặc khi handoff/config/permission/log exposure đổi. Local security
  Done không nâng research/ML lifecycle và không cho phép bỏ quality gates.
