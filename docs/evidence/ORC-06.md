# ORC-06 — Evidence full-month state resolver

Owner **HoaiTam**, reviewer **unassigned**. Branch `feat/orc-06-full-month-state`
từ main `f2d64ef` (PR #77 đã merge), ngày 2026-10-15. Không tự commit/push/PR
hoặc merge. Chuyển ownership từ ThanhTris theo yêu cầu, giữ estimate 8h;
đồng bộ roster/checker thay vì chỉ sửa assignee một file.
Hoàn tất và chạy lại checks ngày 2026-10-16; giữ nguyên capture/request của
2026-10-15 để kiểm tra reuse, không sửa timestamp nhằm tạo identity mới.

## Phạm vi nghiệm thu

Exact immutable Bronze → source coverage → baseline/delta revision và link
closure → bốn Silver datasets → readback/grain/count → FullMonthReady receipt.
Không ghi Gold, không Published, không daily DAG hoặc ML lifecycle transition.

[Contract](../specs/FULL_MONTH_STATE_CONTRACT.md),
[runbook](../runbooks/ORC-06_FULL_MONTH_STATE.md),
[request template](../../config/orchestration/orc06-request.example.json).

## Nguồn thật và coverage

- Target UTC month `[2023-09-01T00:00:00Z,2023-10-01T00:00:00Z)`.
- USGS whole month + 16-second link halo:
  `[2023-08-31T23:59:44Z,2023-10-01T00:00:16Z)`, earthquake, ROI
  latitude 20..50/longitude 120..155, first page limit 20.000; không magnitude,
  depth, updatedafter filter; count 148 dưới limit. Inclusive API endtime là
  `2023-10-01T00:00:15.999Z`.
- JMA `h2023.zip` nguyên bản, exact inventory/member `h2023`, native interval
  `[2023-01-01T00:00:00+09:00,2024-01-01T00:00:00+09:00)`. SHA/CRC và toàn
  bộ 257.020 physical records được kiểm tra; parse temporal scope/halo,
  không first-N, truncate hoặc random sample.
- Backend nghiệm thu thật là **file-backed public-capture QA**, production
  Bronze writers/Silver publisher và Spark. Không gọi MinIO hoặc đưa các URI
  file-backed vào Gold production. Lượt MinIO/Airflow whole pipeline thuộc
  ORC-07/QA-01, không bị ngầm nhận Done từ evidence này.

Nguồn:
[USGS FDSN](https://earthquake.usgs.gov/fdsnws/event/1/),
[JMA hypocenter archives](https://www.data.jma.go.jp/eqev/data/bulletin/hypo.html).
Raw payload/archive/citation giữ trong scoped staging, không commit Git.

## Kết quả dữ liệu thật

| Nguồn | Full Bronze records | Temporal selected | Ngoài temporal scope | Parser rejects |
|---|---:|---:|---:|---:|
| JMA_BULLETIN | 257020 | 22351 | 234669 | 0 |
| USGS | 148 | 148 | 0 | 0 |

- Selected = unique revisions = output revisions = current = memberships:
  **22.499**; repeated IDs/history/out-of-affected-scope = 0 trong bootstrap này.
- Canonical events: **22.390**, source-link rows **177**; reconciliation balanced.
  Source-link rows gồm ACCEPTED/REJECTED/AMBIGUOUS theo SLV-07, không phải
  mọi row đều là một cặp được gộp; không lấy 22.499 trừ thẳng 177.
- Hai nguồn COMPLETE cho required month + halo, không tự suy từ số dòng.
- Bốn dataset/schema/manifests/Parquet được readback bằng publisher;
  Spark GoldInputReader transform cho canonical/current/bridge counts khớp.
- `status=FullMonthReady`, `scope_completeness=COMPLETE_MONTHS`,
  `gold_published=false`, `gold_handoff_verified=true`, `rerun_identical=true`.
- Run ID: `orc06-ac4d20344e17-e5ced2bf7275`.
- USGS capture SHA256:
  `ac4d20344e17b9e07ef277d3694741acb922691545ebfc899c37784e8b313698`.
- JMA archive SHA256:
  `e5ced2bf7275825ba75405b071bb54e9d4c2a5eb55aa6bc9b8d670de1f58b98f`.
- Registry SHA256:
  `00154bd67da276156314fead579ca272147fa0864c5aa0684514d6a9fc081397`.
- Silver bundle SHA256:
  `713977f0baf2c318a0557affa8e760e6b7290a03f29a7265b63ec4dd50da1de0`.

Evidence local: `staging/orc06-acceptance-final-20261015/qa/acceptance.json`,
exact request, registry/receipt/bundle manifests nằm trong cùng root. SHA phụ
thuộc source captures, fixed context và store URI; máy/root khác không bắt
buộc cùng bundle SHA. Không đổi metadata để ép hash giống evidence.

## Kiểm thử tự động

- ORC-06 Java suite gồm bootstrap/GoldInputReader compatibility, immutable
  rerun, baseline+delta giữ event ngoài overlap, moved event cả hai tháng,
  explicit empty month, source gaps/pilot rejection, stale snapshot/identity/
  checksum conflict, typed inputs và future interval rejection.
- JMA replacement kiểm tra same-release different bytes, full-member record
  diff và thiếu coverage tháng mới; không đoán key tương đương giữa releases.
- Old/new link graph closure fixed-point, cross-month link không được cắt;
  explicit replanning giữ ambiguity/canonical policy cũ.
- Partial receipt write fail không tạo completion marker; retry đúng identity
  reuse Silver bundle; immutable parent/ngoài affected scope không thay đổi.
- Actual **local Iceberg** six-table commit dùng làm nonzero baseline cho
  lần resolver tiếp theo; commit/registry lineage verified, snapshots trước/
  sau resolver giống nhau. Đây là local fixture storage, không MinIO serving.
- S3 key regression dùng adapter chỉ nhận full configured `bronze/` prefix:
  không probe key giả ngoài prefix, reject bucket khác/traversal/query string.
- Python wrapper tests không build/start Docker, giữ TLS verification, bounded
  raw download, existing/partial/symlink capture handling và artifact freshness.

Môi trường local: JDK **21.0.11**, Spark **3.5.9**, Python **3.13.3**.
Không gọi đây là GitHub-hosted Ubuntu/Temurin17 result hoặc reviewer approval.

Kết quả chạy lại ngày 2026-10-16:

| Kiểm tra | Kết quả |
|---|---|
| `make test-full-month` | 17 Java/Spark + 6 Python tests, pass |
| `make ci-java` | 380 tests; 0 failure/error/skip; package/fingerprint pass |
| `make ci-airflow` | 216 tests, pass |
| `make ci-docs` | Contracts/config/Compose tĩnh + Python guards + 16 pilot tests, pass |
| `make test-ci-workflow` | 37 tests, pass; inventory nhận nhóm Java ORC-06 |
| `make test-week-5-plan` | 13 tests, pass |
| `make check-task-status check-week-5-plan` | 76 task metadata/index đồng bộ; roster 32/17/23h |
| `actionlint` 1.7.12; `git diff --check` | Pass |
| Source security audit | 0 findings, 3 pending coverage, exit 2; **không** nhận SEC-01 Done |
| `make qa-full-month-live` trên capture đã pin | Pass trên JAR cuối; rerun/handoff true, Gold unpublished |
| `make full-month-local` trên exact request đã pin | Pass trong JVM riêng; giữ nguyên registry và Silver bundle SHA ở trên |

Lượt test Spark đầu tiên bị sandbox chặn socket loopback (`Operation not
permitted`); chạy lại với quyền mở cổng local đạt, không sửa test để bỏ qua lỗi.
Source audit ghi `staging/orc06-security-20261016/report.json`, không runtime
permission probes. Raw captures, runtime artifacts và reports đều không stage Git.

## Giới hạn / handoff

- [ORC-07 — Kết nối real ETL adapters đến Gold Published](../task/tasks/ORC-07.md):
  còn daily/manual wiring, shared lease qua mọi phase, MinIO integration và
  six-table Gold → Trino → publication. Nhận request/proof/Gold request ORC-06,
  không dùng file-backed receipt để thay production S3 proof.
  ORC-06 v1 chỉ chứng nhận tháng đã đóng, đủ halo. Current-month-as-of-cutoff
  cần version hóa contract/tests nếu adapter muốn hỗ trợ; không hạ gate.
- [QA-01 — Chạy E2E daily đa nguồn](../task/tasks/QA-01.md): còn nghiệm thu
  một run thật toàn chuỗi với traces/counts/snapshots, không chỉ component QA.
- [QA-03 — Kiểm thử JMA historical backfill](../task/tasks/QA-03.md): còn
  historical coverage/resume/resource rollout; một tháng không là 40 năm.
- [SEC-01 — Review secret và bề mặt truy cập](../task/tasks/SEC-01.md): runtime
  và ML handoff gates riêng; source pattern tests không tự hoàn tất SEC-01.

Sau chuyển ORC-06, task hours tuần 5 thành 32/17/23, tổng vẫn 72h. Giữ buffer
cũ thì budget 40/24/32h; cần replan capacity, không tự báo cân bằng 32h/người.
Không đổi assignee MLD-05 trong phạm vi implementation ORC-06.
