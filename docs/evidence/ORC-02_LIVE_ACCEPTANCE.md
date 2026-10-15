# ORC-02 — Nghiệm thu live sau bản vá readiness

## 1. Kết luận và phạm vi

- Ngày 2026-10-15 (Asia/Ho_Chi_Minh); branch
  `fix/orc-02-complete-live-readiness` từ latest `origin/main` `7c2b8e8`, PR #72.
  Dependency ORC-01 Done. Assignee HoaiTam; reviewer unassigned, không có approval giả.
- **ORC-02 Done:** đủ acceptance source scheduling/readiness bằng scheduler,
  Java và MinIO thật, không chỉ fixture. Chưa commit/push/mở PR nghiệm thu này.
- Evidence ID `20261015T013428Z-025972f7114c`; reports gốc ở
  `/opt/pipeline/staging/readiness/qa/20261015T013428Z-025972f7114c/`:
  scheduled.json, first.json, rerun.json, workflow-evidence.json.
- [Metadata whitelist và kiểm tra ORM độc lập](./ORC-02_LIVE_ACCEPTANCE.json):
  timestamps UTC, exact manifest/raw hashes, counts, task states và cleanup.
  Không đưa raw ZIP/GeoJSON, `.env`, XCom hoặc credential/log riêng tư vào repo.
- Chỉ source → Bronze **SourcesReady**, `verified=true`, **published=false**;
  không chứng nhận Silver/Gold Published hoặc MVP/ML research coverage.

## 2. Làm gì / có gì / dùng để làm gì

1. Đọc current schedule/profile, active runs và source lease trước I/O.
   Ba source DAG paused=true, không queued/running; lease trống. Không có failed
   scheduled run current interval cần clear/recovery trước test.
2. Preflight kiểm tra image/container freshness, init exit0 và Airflow/MinIO
   health. Runner trong API/scheduler và image gốc cùng SHA, có HEAD retry.
   Không copy JAR hoặc build/pull/start/restart/migrate/sửa `.env` để chạy QA.
3. `make smoke-source-readiness DOCKER_CHECK_TIMEOUT=60` unpause có kiểm soát
   để scheduler tạo current completed cron interval; chạy 2 fixed-window runs
   bằng native trigger API với unique logical_date và pinned run_after.
4. Java probe HEAD + cache SHA/length + MinIO raw/manifest readback cho JMA;
   USGS fetch/validate/upload/verify thật theo ROI/cửa sổ UTC đã chốt.
5. Giữ một QA lease riêng, trigger JMA backfill 2023 để kiểm tra bị chặn trước
   Java ingest. Cleanup chỉ release QA owner và khôi phục pause state ban đầu.
6. Sau harness exit0, đọc ORM/report độc lập: đủ 7 successful task instances
   mỗi daily run, no-change pins đúng, contender failed đúng, pause/lease đúng.

Kết quả dùng để các task consumer tin source pins/readiness đúng, không phải
chứng nhận một watchlist nhỏ đủ training catalog hoặc thay full-month resolver.

## 3. Bằng chứng theo phase

| Phase | Run ID | USGS window UTC / count | Kết quả |
|---|---|---|---|
| scheduled | scheduled__2026-10-15T00:15:00+00:00 | [2026-10-12,2026-10-15), 6 | 7 tasks success; processing_date=2026-10-14 |
| first | orc02-20261015T013428Z-025972f7114c-first | [2023-01-01,2023-01-04), 16 | 7 tasks success; processing_date=2023-01-03 |
| rerun | orc02-20261015T013428Z-025972f7114c-rerun | [2023-01-01,2023-01-04), 16 | 7 tasks success; cùng interval end với first |
| contention | orc02-20261015T013428Z-025972f7114c-contention | JMA năm 2023; không ingest | Expected DAG failed; acquire failed, ingest upstream_failed, holder_preserved=true |

Mỗi successful run có SourcesReady/verified=true/published=false và 5 exact
input manifests: 1 USGS + 4 JMA segments. Không nhận run success nhưng zero tasks.
USGS counts theo earthquake/ROI Nhật Bản của CON-01/USG-01, không toàn cầu.

JMA watchlist `[1997,2000,2023]`, gồm 1997 jan-sep/oct-dec, 2000 full-year,
2023 full-year. Tổng raw estimate **423222**, không phải canonical/feature count.
Cả 3 runs `jma_changed_years=[]`, 4 `ingest_invoked=false`; HEAD attempts đều1.
URI/manifest SHA/raw SHA/count pins bằng nhau giữa scheduled/first/rerun.
Changed-archive và bounded retry/failure paths được unit test, không giả vờ
đã sửa archive JMA hoặc gây timeout nguồn để tạo revision live.

USGS first/rerun là **hai run IDs khác nhau**, response/raw SHA có thể khác
hợp lệ. Không suy chúng thành byte-identical hoặc full ETL logical dedup.
Unit runner tests kiểm tra retry cùng operation/run ID theo contract riêng.

## 4. Deployment, cleanup và khả năng chạy lại

Airflow **3.3.2**. JAR trong API server, scheduler và original image đều SHA-256:

    fd8138eefa3f0c4bf4362a39a326b49a67e44fd1b5096c2a299c551e3ff4aeb9

Original-image probe dùng container tạm `--pull never`, network none,
read-only, không port/volume, CPU0.5/memory256m. Probe chỉ đọc JAR và tự remove
container tạm; không sửa containers/volumes thật. Image reference ngắn ban đầu
không vượt validation; resolve full image ID rồi mới chạy probe thành công.
Không dùng image label hoặc JAR copy tạm làm bằng chứng duy nhất.

Sau QA đọc lại:

- `orc_02_daily_sources`, `jma_04_year_backfill`, `usg_04_usgs_ingest` paused=true
  như trước; hai DAG harness chạm đã restore, USGS DAG độc lập không bị đổi.
- Không có source DagRun queued/running; source owner.json không còn sau release
  đúng QA owner. Không force-unlock/xóa lease của người khác.
- Expected failed contention run/log vẫn còn. Không clear run/task, xóa raw,
  cache, bucket, warehouse, volume, journal hoặc log lịch sử.
- Harness giữ paused=true có chủ đích. Muốn daily vận hành liên tục, operator
  unpause đúng daily DAG sau khi kiểm tra source/backfill quiescent; không tự
  để cron bật sau test. Đây là nghiệm thu bounded, không benchmark uptime nhiều tháng.

## 5. Test/check và các lần thử không đạt

| Check | Kết quả |
|---|---|
| `./scripts/check-config.sh --require-local` | PASS; không sửa/in `.env` |
| `make test-source-schedule`, host JDK21 Azul21.0.11, compile release17 | PASS: 32 Python + 32 Java, không failure/error/skip |
| `make ci-airflow` | PASS: 216 Python tests, gồm source tests ở trên; không cộng trùng |
| `make ci-docs` | PASS: contracts/status parity, week5 12 tests, Java-input 12, security11, lifecycle39, Compose/MinIO/query static và CI workflow4 |
| `make smoke-source-readiness DOCKER_CHECK_TIMEOUT=60` | PASS: exit0, final workflow VERIFIED, đủ scheduled/first/rerun/contention và cleanup |
| `git diff --check`, task/index parity | PASS |

Các lần đầu không đạt, không gọi là passed:

- Target Java trong sandbox: 1 SocketException Operation not permitted khi
  HTTP test bind loopback. Chạy lại với quyền test cần thiết và cùng JDK21:
  32 Java tests đạt; không sửa/bỏ test hoặc gọi nguồn thật để né lỗi.
- Smoke preflight default20s timeout lúc checks chạy đồng thời; chưa trigger
  DAG/source I/O. Sau đó preflight60s đạt, chạy lại lệnh chuẩn có override60s
  và hoàn tất workflow. Không đổi default timeout, HTTP policy, TLS hoặc skip
  image/health checks; không restart Docker để làm xanh test.

Runbook: [SOURCE_SCHEDULE_AND_READINESS](../specs/SOURCE_SCHEDULE_AND_READINESS.md).

## 6. Giới hạn / bước tiếp theo

- **ORC-06 — Resolve affected months và dựng full-month Silver state:** cần
  current state đủ tháng; overlap/sample source không thay full-month proof.
- **ORC-07 — Kết nối real ETL adapters đến Gold Published**, **QA-01 — Chạy
  E2E daily đa nguồn:** cần source → Silver → committed/Trino-verified Gold
  Published thật; SourcesReady không thay các gate này.
- **FND-01 — Tạo smoke checklist cho môi trường:** full lifecycle startup/health/
  full-stack smoke vẫn nghiệm thu riêng. **SEC-01 — Review secret và bề mặt
  truy cập:** security/logging/ML boundary theo branch/PR riêng; kết quả source
  không nâng trạng thái hai task đó.
- **QA-05 — Profile dữ liệu lịch sử và tài nguyên local:** sizing/full-history
  coverage chưa có trong watchlist 3 năm này; không nhận training40-year-ready.
- Mốc nghiệm thu ban đầu dùng main `7c2b8e8`, trước khi bản vá SEC-01 được
  merge qua PR #73. PR #74 sau đó đồng bộ main `11ec546` để resolve conflict
  docs: giữ ORC-02 Done và SEC-01 Review/owner HoaiTam, đầy đủ evidence cả hai.
  Không chạy lại live, deploy logging/IAM hoặc chứng nhận SEC-01 từ việc đồng bộ
  Git; metadata JSON ở trên vẫn phản ánh đúng code/deployment tại lúc đo.
