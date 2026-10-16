# ORC-07 — Real ETL adapters: implementation và acceptance

Trạng thái Done. Owner HoaiTam, reviewer unassigned. Branch `feat/orc-07-real-etl-adapters`,
tạo từ main `accdb24` (ORC-06 đã merge PR #78), ngày 2026-10-16.
Chưa commit/push/mở PR; không ghi approval hoặc GitHub-hosted CI giả.

## Implementation

- `RealEtlAdapter` / `RealEtlPhaseJob`: actual MinIO Bronze readback, ORC-06
  full-month proof/Silver bundle, GLD-03 writer, Trino verify và publication.
- `GoldPublisher.verify`: read-only gate riêng; test xác nhận không CREATE/
  INSERT/publication journal trước publish.
- `real_etl_runtime.py`: exact versioned context/receipts, fixed executable,
  bounded private output, persistent execution/summary và fail-closed lease.
- `orc_02_daily_sources`: thêm opt-in real_etl trước release (8 tasks), giữ
  cron owner duy nhất. `orc_07_real_etl`: manual scoped DAG (5 tasks), no cron.
- Airflow Dockerfile đóng gói Spark/Java17 và Iceberg runtime; QA image/tag
  riêng, không restart foundation. Tests không implicit build/start/init.
- [Contract](../specs/REAL_ETL_ADAPTER_CONTRACT.md),
  [runbook](../runbooks/ORC-07_REAL_ETL.md), Make commands và CI suite đã bổ sung.

## Offline và build checks đã chạy

| Kiểm tra | Kết quả thực tế |
|---|---|
| Targeted Java: RealEtlAdapterTest + GoldPublicationTest | 12 tests, pass |
| Full Java/Spark JDK21.0.11 trên macOS | 388 tests, 0 failure/error/skip; package/fingerprint pass |
| Docker build Java17.0.20.1 / Linux ARM | 388 tests, 0 failure/error/skip; image build pass |
| Airflow/control-plane | 227 tests, pass; gồm 11 ORC-07 guards, daily full-month pins/cutoff và symlink |
| Host lifecycle CLI | 4 tests, pass; QA không build/start/pull, bind source được normalize |
| CI regression | 37 tests, pass; ORC-07 có Java suite riêng |
| Docs/contracts/config/Compose static và pilot tests | Pass |
| actionlint 1.7.12, git diff --check | Pass |
| Source security audit | 0 findings, 3 pending coverage; không nhận SEC-01 Done |

Security triage giữ hash cũ và thêm exact hash của fixture source-readiness QA
sau khi bổ sung task `real_etl`. Thay đổi chỉ task-state fixture; dummy invalid
credential URI trong redaction test không đổi, không blanket-exempt test folder.
Scoped source report: `staging/orc07-security-final-20261016/report.json`.
Exit2 của scoped audit là ba pending coverage items (config/runtime/ML handoff),
không phải findings. `make check-config-local` đã chạy riêng và pass; scoped
source report không tự coi bước riêng đó là deployed runtime/ML acceptance.

Build explicit dùng `scripts/real-etl.py prepare`; private build log trong
`staging/orc07-host-23f8b0e763734bc8b59de19db696d878/private.log`.
Final hardened Java17 build (bao gồm Gold journal/predecessor check trước SQL)
cũng đạt 388 tests; log
`staging/orc07-host-8912372bac6048548c78486d38acf5e1/private.log`.
Prepare cuối sau resource configuration 3 GiB dùng cache, thành công tại
`staging/orc07-host-bbb3d5161ca44039bbbfb0b52dc31460/private.log`.
Maven verify trong Docker mất khoảng 13–17 phút ở môi trường này. Chi phí build
chỉ thuộc prepare; `qa-real-etl` không lặp build hoặc init service.

## Runtime acceptance

Evidence key `75b2a4a0d4ad4a49817ac8cde6b1f868`, backend actual
MinIO/Iceberg/Trino và production DAGs qua `DAG.test`, target month 09/2023.
Raw captures/staging/private logs không commit Git.

### Daily đã Published

- DagRun `manual__2026-10-16T02:26:22.739029+00:00` của
  `orc_02_daily_sources`, 8 task instances đều success, không mark-success.
- Operation `orc07-c6395ae7d795ac7f00b18041a07f807233cc43f3`.
- Context SHA `542ff2353c99d785bb03ea2a43f2ab50c4d3114b648b27dd4c19b937bbfedac4`.
- Publication `pub_c76a33780d848e918a910f025cefc93c0e179cf67c8a230ab4522c339a95ac8f`.
- Namespace `iceberg.gold_qa_75b2a4a0d4ad4a49817ac8cde6b1f868`.
- Original JMA full-year Bronze: 257020 records, selected month 22351,
  ngoài temporal selection 234669, parser rejects 0. USGS full month + halo
  148 và actual daily overlap 13; 13 repeated observation IDs được nhận diện.
- Reconciliation balanced=true: selected 22512 → unique revisions/current 22499,
  history 0, memberships 22499, canonical 22390, source-link rows 177,
  revisions outside affected months 0. Không lấy tổng link rows trừ observations:
  links còn gồm non-accepted candidates, canonical policy do Silver/Gold tính.

| Gold table | Exact daily snapshot ID |
|---|---|
| dim_date | 8189260964658316231 |
| dim_depth_band | 4076372337159785588 |
| dim_magnitude_band | 415978713778516876 |
| dim_region | 5708334243099944536 |
| event_current | 2778208712098966349 |
| event_source_bridge | 4288865953953650116 |

Detailed phase reports/receipts tại
`s3://japan-earthquake/silver/real-etl/<operation>/`;
`verify.json` chứa actual Trino checks và `publish.json` có Published metadata.
Completed-publication replay không tạo snapshots mới hoặc đưa serving alias lùi.

### OOM và partial-commit recovery

Manual DagRun đầu `manual__2026-10-16T02:34:42.150319+00:00`, operation
`orc07-272417cc857ad2300a1269983d97bfa334a1fd82`, bị Docker OOM ở limit 2 GiB
trong Gold, chưa đến verify/Trino. Namespace riêng
`iceberg.gold_qa_94b77ef519714876bda9314da998d868` đã commit event_current
2654524337756421883 và event_source_bridge 1541275531725051435; dim_date mới
created empty. Container đã thoát/removed, local JVM đã dừng, readback stable
và snapshot summaries thuộc exact operation trước khi xác nhận recovery.

Không xóa lease/warehouse hoặc clear task. Explicit recovery dùng limit 3 GiB,
cùng owner/context/operation, giữ nguyên hai snapshot trên và đã Published.
Bốn final dimension snapshots: dim_date 568920740381677995,
dim_depth_band 7645539745954382353, dim_magnitude_band 7307677843076500435,
dim_region 480395246816490495. `partial-recovery.json` giữ original_dag_state
failed và backend_status Published riêng biệt, không sửa lịch sử Airflow.

Final-binary daily full six-phase replay đã đạt identical summary/receipts/
snapshots/publication và exact-owner release. Runner JAR thực tế trong QA có
SHA256 `21b5de5217cbe4e6604189df1c011c73c4db9c15b867c63a162a8024670ecbb7`.
Manual DagRun mới `manual__2026-10-16T06:31:23.238449+00:00`, operation
`orc07-2ca63d76b832ea44491d0b057d77978f7f1ebc32` đã success lúc
06:36:33 UTC (13:36:33 Asia/Ho_Chi_Minh), 5/5 task instances success.
Private recovery log: `staging/orc07-host-cefc514168004cf586a01548a565f1bb/private.log`.

### Final manual và acceptance PASSED

- Namespace `iceberg.gold_qa_a8d51a73b91d4254903561922336338e`.
- Context SHA `90e1a609e3d7ae0b6b82a3890289cee03be2b7336507eef400b217135e4f8dc2`.
- Publication `pub_90268db98d7748ce64be62a672cd092635162133fd722ce16f06e275f1135111`.

| Gold table | Exact final manual snapshot ID |
|---|---|
| dim_date | 7896765405898089395 |
| dim_depth_band | 8336457838001959496 |
| dim_magnitude_band | 2066221583031664110 |
| dim_region | 7690623912842848680 |
| event_current | 3174095425047527582 |
| event_source_bridge | 9000739851992928667 |

Shared staging `real-etl/qa/75b2a4a0d4ad4a49817ac8cde6b1f868/acceptance.json`
và `real-etl/last-qa.json` đều PASSED. Acceptance ghi daily/manual Published,
actual task states success, rerun_identical=true, partial_recovery với hai
snapshot preserved, scheduled_run=false, production_namespace_modified=false.
Host terminal `event=orc07_recover_passed`, one-off exit0. Manual mới chạy
khoảng 310 giây; chỉ là wall-clock bounded QA này, không benchmark 40 năm.
Không rebuild/start/init/restart foundation trong acceptance; .env không đổi.
Final readback source owner absent; ORM xác nhận daily success, manual OOM cũ
vẫn failed và manual mới success. Không sửa state thủ công hoặc release owner khác.

## Giới hạn / handoff

- QA là historical eligible daily interval qua `DAG.test`, không scheduled cron
  run. QA-01 — Chạy E2E daily đa nguồn còn nghiệm thu scheduled deployment độc lập.
- ORC-06 v1 chỉ closed months + halo; JMA inventory tới 2023. Daily publication
  mặc định disabled, không bật current-2026 unattended với historical request.
  Current-month-as-of-cutoff ngoài ORC-06/ORC-07 v1 cần version hóa
  policy/coverage/tests, hiện chưa có task riêng cho phiên bản mới.
- Manual v2 dùng DAG ORC-07 và same adapters; không nhận scoped ORC-03 v1 runner
  cũ tự nhiên đã được cấu hình hoặc sửa shape legacy 1.0.
- QA-02/03/04 còn revision, historical coverage và failure/recovery rollout rộng;
  bounded QA không chứng minh toàn 40 năm/research-ready.
- SEC-01 còn deployed-log/security và actual ML handoff gates. Source audit không
  chứng minh task security toàn dự án đã Done.
- Reviewer vẫn unassigned; user review/hosted CI sau push là bước riêng.
