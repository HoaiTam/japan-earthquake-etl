# ORC-02 — Evidence lịch và readiness hai nguồn

## 1. Phạm vi nghiệm thu

- Ngày `2026-10-08`; branch `feat/orc-02-cau-hinh-lich-va-readiness`, tạo từ
  `origin/main` `f5a62d3` (merge PR #43 ORC-01, fetched trước triển khai).
- Dependency ORC-01 đã Done. Assignee HoaiTam; reviewer unassigned, chưa có
  approval độc lập. Done theo acceptance task, không phải Gold Published.
- Deliverables: [daily DAG](../../airflow/dags/orc_02_daily_sources.py),
  [schedule runtime](../../airflow/dags/source_schedule_runtime.py),
  [cross-DAG lease](../../airflow/dags/source_run_guard.py),
  [live QA](../../compose/airflow/source-readiness-qa.py),
  [runbook](../specs/SOURCE_SCHEDULE_AND_READINESS.md).
- Metadata tóm tắt, không payload: [ORC-02-runtime.json](./ORC-02-runtime.json).

## 2. Test/check thực tế

| Kiểm tra | Kết quả | Phạm vi |
|---|---|---|
| `make test-source-schedule` với host JDK 21 | 23 Python + 19 Java tests, đạt | Profile/window/JMA change/no-change/probe/lease/graph/QA harness offline |
| `./mvnw -pl spark -am clean verify` với host JDK 21 | 167 Java tests, 0 failure/error/skip | Clean package; compile release 17 |
| `env JAVA_HOME=/Users/hoaitam/Library/Java/JavaVirtualMachines/azul-21.0.11/Contents/Home make check` | Exit 0; 167 Java + 89 Airflow Python tests | Regression, contracts, task status, config/secret hygiene và static Compose |
| DagBag import bằng Airflow 3.3.2 trong container | No import errors | ORC-02 7 tasks; USG-04 10; JMA-04 8; ORC-01 8; strict completion leaves |
| `docker compose --profile live run --rm --no-deps source-readiness-qa` sau runtime fallback tại §5 | Exit 0; workflow VERIFIED | Scheduler/LocalExecutor thật, USGS/JMA/MinIO thật, 3 daily runs và controlled contention |
| `git diff --check` | Exit 0 | Whitespace |

Không cộng các test chạy lặp thành số test độc lập. Python offline dùng SDK
double/process mock, không thay live evidence. Changed JMA segment, checksum
audit, corrupt/missing publication, retry same run và foreign lease cleanup
đã có test xác định được; không chủ động sửa archive trên server JMA để tạo
revision live.

## 3. Acceptance mapping

| Tiêu chí | Bằng chứng |
|---|---|
| Daily run ổn định | Một scheduled run tự nhiên và hai manual runs chạy cùng fixed interval; mỗi run đủ **7 task instances success**, real SourcesReady/verified, published=false |
| JMA chỉ kích hoạt năm thay đổi | Cả 3 runs probe 4 exact segments của watchlist, `ingest_invoked=false`, `jma_changed_years=[]`; cùng URI/SHA/count được verify qua MinIO. Unit tests kiểm tra chỉ changed segment được ingest, weekly audit và same-byte reuse |
| Timezone/data interval đúng | Explicit CronDataIntervalTimetable `15 7 * * *` Asia/Ho_Chi_Minh; historical interval end 2023-01-04T00:15Z tạo target 2023-01-03 UTC, query [2023-01-01,2023-01-04). Unit tests UTC/JST cùng instant, không lấy host now/logical-date đầu interval |
| Không chạy chồng daily/backfill | Actual JMA backfill thất bại tại acquire, ingest upstream_failed; all_done cleanup giữ nguyên foreign holder; strict completion leaf không che failure |

Scheduled run chứng minh scheduler thực thi một interval, không phải benchmark
uptime nhiều tháng. Watchlist `[1997,2000,2023]` là pilot, không toàn bộ 40 năm.
Weekly forced checksum audit có thể GET năm không đổi nhưng không tạo publication
mới khi bytes giống nhau; không tuyên bố mọi ngày đều không tải ZIP.

## 4. Live workflow và exact pins

Evidence ID: **`20261008T022836Z-5936f782e3d9`**.
Reports gốc trên shared staging volume:
`/opt/pipeline/staging/readiness/qa/20261008T022836Z-5936f782e3d9/`
gồm `scheduled.json`, `first.json`, `rerun.json`, `workflow-evidence.json`.

| Phase / run ID | Interval end UTC | Processing date / USGS query | USGS count | Daily tasks |
|---|---|---|---|---|
| scheduled / `scheduled__2026-10-08T00:15:00+00:00` | 2026-10-08T00:15Z | 2026-10-07 / [2026-10-05,2026-10-08) | 13 | 7 success |
| first / `orc02-20261008T022836Z-5936f782e3d9-first` | 2023-01-04T00:15Z | 2023-01-03 / [2023-01-01,2023-01-04) | 16 | 7 success |
| rerun / `orc02-20261008T022836Z-5936f782e3d9-rerun` | 2023-01-04T00:15Z | 2023-01-03 / [2023-01-01,2023-01-04) | 16 | 7 success |

Scheduled run được scheduler tạo trong lần QA trước, final harness reuse
đúng latest completed interval/profile; không giả lập scheduled run bằng
manual trigger. Hai fixed-window runs dùng native `trigger_dag` với
**run_after và logical_date** cùng instant, unique microseconds. Airflow
3.3.2 CLI chỉ pin logical-date vẫn lấy run_after=now nên không đủ kiểm tra
historical interval.

JMA cả 3 runs reuse exact manifest/raw pins từ JMA-05:

| Year / segment | Record count estimate | Raw SHA-256 | Manifest SHA-256 |
|---|---|---|---|
| 1997 / jan-sep | 39951 | c2ce3fd9f8fc023a6463086324b1ed29b53ae2c55d021998a0c3210e162172dd | e14b46526a37c470eef3016320169a5791700cd61010a8efb487aceacb3a5d51 |
| 1997 / oct-dec | 16284 | 25349273dfc4ce50ed24ee555c881bd64af5662d400be2d7fbd0d6044d4caead | eddc63154542870adf9596014c778c589dc64ed3d2802f9d28025f689a2dab0e |
| 2000 / full-year | 109967 | 268441420d9c90b1cb96f42934aefdb107a4d12c5947adafbfeb24af597d15c2 | 0c54b3c664833819099926b1a773647c71e6819abecdb8a321d82211b95fb615 |
| 2023 / full-year | 257020 | e5ced2bf7275825ba75405b071bb54e9d4c2a5eb55aa6bc9b8d670de1f58b98f | 977defa4f2a1d4a2ba41a211ac0419419ca812d010522da4ef4d95c95f3582db |

Tổng estimate JMA = **423222**. Exact URIs và USGS raw/manifest SHA của từng
run nằm trong JSON evidence. Manifest SHA là hash **manifest bytes**, không
raw ZIP/GeoJSON SHA. USGS hai run ID khác nhau có raw SHA khác nhau; đây là
hai response mới, không kiểm chứng bytes bất biến xuyên run hoặc suy đoán
nguyên nhân revision từ checksum. Java tests riêng xác nhận cùng run ID
retry không refetch/overwrite Bronze đã publish.

Controlled contention giữ **synthetic QA lease**, trigger actual backfill
`orc02-20261008T022836Z-5936f782e3d9-contention` cho JMA 2023:

| JMA task | State |
|---|---|
| resolve_plan | success |
| acquire_source_lease | failed |
| select_archives / ingest_archive | upstream_failed |
| run_summary / release_source_lease | success |
| bronze_ready_gate | failed |
| completion_gate | upstream_failed |

DAG failed đúng kỳ vọng; holder không bị foreign cleanup xóa. QA release
chỉ lease do chính QA sở hữu, không force-unlock run khác. Hai DAG
`orc_02_daily_sources` và `jma_04_year_backfill` đều khôi phục paused=true;
đã đọc lại ORM/task states và xác nhận `owner.json` không còn. Không xóa
raw, MinIO bucket, warehouse, volume hoặc failed-run logs.

Hai lần QA diagnostic trước **không đạt**, giữ lại trong staging:
`20261008T021107Z-3987ed93c82f` (start date cũ khiến historical success có
zero tasks) và `20261008T021951Z-dd1a3e40ddb8` (CLI logical-date không pin
run_after). Đã sửa start date 2023, task-instance guard và native trigger API;
chỉ final evidence ID phía trên được nghiệm thu. Source retry có thực sự
xảy ra rồi thành công; không suy đoán nguyên nhân chỉ từ safe generic error.

## 5. Docker build và runtime fallback

**Chưa xác nhận fresh Docker image build.** Ba lần build bị timeout lấy
DockerHub metadata hoặc dependency Maven Central (`plexus-xml`). Diagnostic
HTTP trong container tới Maven Central/JMA/USGS trả 200, không đủ kết luận
BuildKit hết lỗi. `make smoke-source-readiness` full build wrapper chưa đạt;
không ghi thành passed.

Để kiểm thử logic thật trong phạm vi task, đã:

1. Clean verify/package JAR trên host JDK 21, compile release 17, 167 tests đạt.
2. Xác nhận không có source run active; start/recreate Airflow từ existing
   images bằng `docker compose up -d --no-build --wait --wait-timeout 300
   airflow-api-server airflow-scheduler airflow-dag-processor`.
3. `docker compose cp spark/target/japan-earthquake-etl-runner.jar` vào
   `/opt/pipeline/lib/usgs-ingest-runner.jar` của **cả 3** Airflow services
   (api-server, scheduler, dag-processor). DAG/helper bind mounts dùng code mới.
4. Chạy profile live QA tại §2; scheduler thực thi source I/O thật trên
   runtime **Temurin 17.0.19+10**, không mock Java hoặc storage.

JAR deployed khi QA có SHA-256
`353c1ed59bb56f336386a129411a3ebfbac8d3b65d3371ca5298d619d94f144d`.
Đây là hash artifact runtime đã kiểm tra, không đảm bảo hash JAR sau lần
host rebuild tiếp theo (build timestamps có thể khác).

**Copy JAR chỉ sửa container layer, chưa tạo image bền vững.** Recreate từ
image cũ sẽ mất cập nhật JAR. Khi mạng build ổn định, chạy lại
`make smoke-source-readiness` để build/deploy bình thường trước dùng lâu dài.
Không docker commit container, không sửa `.env` hoặc mất volume để né lỗi.
Lỗi mạng Docker Build **chưa có task riêng** phụ trách.

## 6. Giới hạn và handoff

- [ORC-03 - Implement backfill và reprocessing](../task/tasks/ORC-03.md):
  historical dispatcher và affected Silver/Gold scopes, reprocess pinned inputs.
- [ORC-04 - Chuẩn hóa logging và run summary](../task/tasks/ORC-04.md):
  counts/duration/reasons toàn ETL; hiện report chỉ source readiness.
- [ORC-05 - Chốt recovery, concurrency và tài nguyên](../task/tasks/ORC-05.md):
  stale-lease recovery và admission/resource guard Spark/Iceberg/ML; lease hiện
  chỉ shared LocalExecutor source DAGs, chưa distributed lock hoặc benchmark.
- [SLV-09 - Tích hợp và kiểm thử Silver đa nguồn](../task/tasks/SLV-09.md),
  [GLD-03 - Ghi Gold Iceberg và commit snapshot](../task/tasks/GLD-03.md),
  [GLD-04 - Tạo Trino views và verification SQL](../task/tasks/GLD-04.md):
  real downstream adapters, committed snapshots và Trino verification.
- [QA-01 - Chạy E2E daily đa nguồn](../task/tasks/QA-01.md): full daily đến
  Gold Published, không lấy SourcesReady thay nghiệm thu toàn chuỗi.
- [QA-05 - Profile dữ liệu lịch sử và tài nguyên local](../task/tasks/QA-05.md):
  full-history sizing; pilot 3 năm không chứng minh đủ training coverage.

Task ORC-02 đủ acceptance source scheduling/readiness nên Done. Không chuyển
Done downstream. Chưa commit/push/mở PR/merge; reviewer độc lập được khuyến nghị.
