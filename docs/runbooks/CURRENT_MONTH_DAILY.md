# QA-01 — Daily operational current-month

## Phạm vi và trạng thái

Bước 3 của PR #96 nối Source/Silver và Gold vào **cùng** DAG
`orc_02_daily_sources`, không tạo thêm daily owner. Implementation và tests đã
có; deployment local ngày 22/10/2026 chưa nghiệm thu thành công. Owner chọn
giữ lịch **07:15 Asia/Ho_Chi_Minh**, chờ lần scheduled 23/10, không clear run
22/10 đã failed. [QA-01](../task/tasks/QA-01.md) vẫn In Progress.

Đây là operational USGS-only. JMA chỉ chứng nhận baseline historical readiness,
không cung cấp observation current-month. Output `Published` vẫn
`OBSERVED_AS_OF`, `PROVISIONAL`, `research_ready=false`,
`ml_upstream_eligible=false`; không là upstream official ML.

## Thành phần và flow

| Thành phần | Có gì / dùng để làm gì |
|---|---|
| `current_month_runtime.py` | Control plane riêng `current-month-daily-v1`, không import planner DESIGN_ONLY; metadata, subprocess Java, lease và receipt handoff |
| `CurrentMonthDailyJob` | Java entrypoint capture/silver/audit; Java sở hữu response nguồn, ETL và SQL, Python không xử lý từng event |
| `qa_01_current_month.py` | Collector v2 kiểm actual scheduled run, context/control SHA và reconciliation |
| `CurrentMonthAudit` | Đọc lại Bronze/Silver/journal/marker, SQL Trino độc lập trên six snapshots và versioned views; không viết lake |

Flow giữ nguyên tám task:

`resolve_daily → acquire_source_lease → jma_readiness → usgs_bronze → sources_ready → real_etl → release_source_lease → completion_gate`.

- Resolve đọc `dag_run` SDK thật, normalize enum `.value`, kiểm scheduled/24h
  interval/empty conf. Pin code version, runner SHA và previous publication;
  retry không được đổi identity.
- Shared lease giữ xuyên readiness/capture/Silver/Gold; Java cũng kiểm exact
  owner trước mutation. Không tự chiếm/release lease khác.
- JMA dùng production readiness runner và exact archive scope trong profile.
  USGS capture current-month có count proofs, không dùng delta overlap ba ngày
  cho full-month replacement.
- `real_etl` gọi Java Silver → Gold commit → SQL verify → provisional publish.
  Chỉ receipts/marker/summary COMPLETE mới advance published head và release.
  Lỗi/timeout giữ lease để chẩn đoán. Completion là strict sole leaf; cleanup
  không thể che upstream failure thành run xanh.

## Cấu hình và chuẩn bị explicit

Cấu hình mới trong `.env.example` / Compose mặc định **disabled**:

- `CURRENT_MONTH_DAILY_MODE=operational` yêu cầu `ORC07_DAILY_MODE=disabled`,
  `SOURCE_SCHEDULE_PROFILE=multi-source`.
- `CURRENT_MONTH_CODE_VERSION`: commit SHA 40 hex; dùng kèm binary pin,
  không coi commit SHA một mình là bằng chứng cho uncommitted working tree.
- `CURRENT_MONTH_RUNNER_SHA256`: SHA-256 runner đang mount, 64 hex; mismatch
  dừng trước Java. Không rebuild/replace JAR khi có active run.
- `CURRENT_MONTH_PHASE_TIMEOUT_SECONDS=1800`: mỗi phase, tối đa 7200 giây.
- `CURRENT_MONTH_DAILY_ROOT`: Compose cố định
  `/opt/pipeline/staging/current-month-daily`, không dùng root symlink.

Chuẩn bị JAR bằng `make prepare-current-month` với JDK phù hợp. Deployment phải
có Spark Java, hai Iceberg runtime jars và runner tương ứng. Không bật mode rồi
giả định cached image đã có code mới. Tests/smoke không build/pull/start/init.

Deployment local hiện dùng ignored override
`staging/qa01-current-month-deploy-20261022/compose.override.yaml`: readonly
prebuilt JAR/classpath, bật mode cho ba service Airflow, không sửa `.env`,
không đổi cron. Pins tại [evidence](../evidence/QA-01_CURRENT_MONTH_DAILY.md).
Nếu cần recreate service, dùng cả base và override, chỉ khi không có active run:

```bash
docker compose --env-file .env -f compose.yaml \
  -f staging/qa01-current-month-deploy-20261022/compose.override.yaml \
  up -d --no-build --pull never --no-deps \
  airflow-api-server airflow-scheduler airflow-dag-processor
```

Override riêng máy, không commit. Chạy lại chỉ base Compose có thể trả mode về
disabled; kiểm mode/pin thực tế trước nghiệm thu. Giữ máy/Docker/stack hoạt động
qua giờ scheduled; không trigger manual thay thế khi lỡ lịch.

## Test và nhận biết thành công

```bash
TMPDIR=/private/tmp make test-qa-01
make smoke-qa-01 QA_01_RUN_ID='scheduled__2026-10-23T00:15:00+00:00'
```

Smoke chỉ dùng sau khi run tồn tại; không sinh DagRun. Nếu preflight cached
image chưa tương thích, diagnostic read-only có thể chạy:

```bash
python3 scripts/qa-01.py --run-id 'scheduled__2026-10-23T00:15:00+00:00'
```

Diagnostic không thay preflight deployment. Collector tự chọn `qa-01-v2` khi
exact operation có current-month context; v1 vẫn giữ COMPLETE_MONTHS.

Chỉ thành công khi:

1. Airflow `run_type=scheduled`, đúng interval, cả tám task `success`.
2. Control folder `current-month-daily/cm1-<hash>/` có context, source request,
   capture, sources, Silver/Gold receipts, Gold request, summary và COMPLETE
   execution SHA; cùng operation, không ghép artifacts của smoke khác.
3. MinIO có immutable capture/count/selection, Silver registry/Parquet và Gold
   commit/verification/publication/marker trong current-month scope.
4. Trino `<catalog>.operational_current_month` có sáu pinned snapshots,
   publication status và versioned views đúng policy.
5. Report `PASSED`, `report_version=qa-01-v2`, `runtime_complete=true`,
   reconciliation balanced, ít nhất **42 SQL readbacks** đúng expected;
   research/ML flags false. Collector không refetch/ETL/publish.

Report ở `staging/qa-01/<evidence>/report.json` và `report.md`. Logs private,
không commit. Java audit chỉ ghi stdout vào staging private, không ghi lake;
JMA manifest readback theo exact URI/SHA.

## Giới hạn và xử lý lỗi

- **QA-01 — Daily E2E:** chưa scheduled v2 PASSED; 22/10 failed ở resolve trước
  lease/capture. Chờ 23/10 07:15. Khi run nghiệm thu kết thúc success hoặc failed,
  kiểm tra không active run rồi pause DAG về trạng thái ban đầu; failure giữ
  lease/artifacts. Tests/SDK probe không thay runtime acceptance.
- **QA-02 / QA-04:** component fixtures không thay live operational replay/failure
  evidence. Historical acceptance giữ riêng.
- **MLD-01 / MLD-05 / MLI-02:** operational không đủ official ML gates/export;
  persisted snapshot guard và reserved namespace không được bypass.
- **Chưa có task riêng:** JMA sau baseline, auto deletion/ID reprocess, replan
  quá hai tháng/state budget; các guard hiện fail closed.

Nếu fail: giữ log/run/lease, đọc exact phase private log. Không xóa warehouse,
bucket/volume, sửa DB/task state, clear/mark-success hoặc release foreign owner.
Nếu lỡ lịch: báo thiếu evidence, không tạo manual rồi đổi nhãn scheduled.
