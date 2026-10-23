# QA-01 — Scheduled current-month acceptance, 23/10/2026

## Kết luận

**Done cho operational current-month v2**: actual scheduled run đủ **8/8 task
success**, collector `qa-01-v2` **PASSED**, SHA chain/đối soát và **42 SQL
readbacks** trên **sáu snapshots** đạt. Đây là run được scheduler tạo, phục hồi
có kiểm soát sau owner đồng ý; **không** phải lần chạy đầu không gián đoạn hoặc
manual/DAG.test. Không đổi cron/type/interval/cutoff, không mark-success.

Gold `Published` vẫn **OBSERVED_AS_OF / PROVISIONAL**, `research_ready=false`,
`ml_upstream_eligible=false`, `official_ml_dataset_exported=false`. JMA chỉ là
historical readiness, không có observation JMA 2026. Không nhận đây là catalog
hai nguồn current đầy đủ hoặc research/official ML acceptance.

Assignee HoaiTam; reviewer `unassigned`, chưa có approval độc lập. Branch
`test/qa-01-scheduled-runtime-acceptance`, tiếp tục PR #96; không commit/push/merge
trong phiên này. Không gộp thay đổi SEC-01 ở worktree khác vào branch này.

## Run và recovery thực tế

- DAG `orc_02_daily_sources`, run `scheduled__2026-10-23T00:15:00+00:00`,
  `run_type=scheduled`, `conf={}`.
- Logical date `2026-10-22T00:15:00Z`; interval
  `[2026-10-22T00:15:00Z, 2026-10-23T00:15:00Z)`.
- Operation `cm1-847b2845f3e2f5ded8e815776e69d9756a65fcfa` giữ nguyên.
- Lần đầu failed capture khi host sleep/dark-wake; ba upstream task success,
  lease giữ đúng owner, không có Gold/publication từ lần lỗi. Nhật ký macOS
  ghi Sleep 07:39:13, nhiều maintenance wakes, rồi wake 08:40:03 giờ local;
  source log không tiến triển trong khoảng tương ứng.
- Owner chọn phục hồi chính run 23/10. Đã kiểm active DagRuns=0, scheduler
  không còn Java writer, Spark master activeapps=0; preview đúng năm task rồi
  gọi API clear chính thức bằng exact run_id cho capture và downstream.
  Không clear resolve/acquire/JMA đã success hoặc run lỗi ngày 22/10; không
  release/takeover lease để phục hồi. Chi tiết ở
  [deployment/recovery log](./QA-01_CURRENT_MONTH_DAILY.md).
- Recovery được scheduler thực thi **17:04:08–17:08:09 ngày 23/10/2026**
  (UTC 10:04:08.900344–10:08:09.953381); không tính là SLA chạy đúng 07:15.

| Task | State cuối | Attempt | Phạm vi |
|---|---|---:|---|
| resolve_daily | success | 1 | Giữ context/pins gốc |
| acquire_source_lease | success | 1 | Giữ exact owner |
| jma_readiness | success | 1 | Giữ historical readiness gốc |
| usgs_bronze | success | 2 | Reuse parts và hoàn tất capture |
| sources_ready | success | 1 | SHA handoff cùng operation |
| real_etl | success | 1 | Silver → Gold → verify/publish |
| release_source_lease | success | 2 | Release thật sau COMPLETE |
| completion_gate | success | 1 | Strict sole leaf, không giả state |

Log capture lần đầu còn nguyên SHA-256
`6350f99f799b693dccae18ebb8abc08c10deff1677492454706f1f731ae7e7cd`.
Bản vá Python giữ private logs từng lần trong `phase-attempts/<phase>-<random>/`
(directory 0700, streams 0600), outcome reason code không echo argv/payload.
Timeout vẫn giữ lease, không tự nhận writer quiescent hoặc tự retry.

## Data flow và counts

USGS ROI 20–50°N, 120–155°E, earthquake, không magnitude filter. Bootstrap
**22 daily leaf parts** cho `[2026-10-01T00:00:00Z, 2026-10-23T00:00:00Z)`;
count trước/sau từng GeoJSON, raw/manifest/count SHA được đọc lại độc lập.
Observation cutoff thực tế `2026-10-23T10:04:47.429744461Z`, khác event cutoff.
Năm receipts đã ghi trước lỗi được reuse, không overwrite Bronze để reset.

| Metric | Count |
|---|---:|
| Capture records / unique revisions | 58 / 58 |
| Repeated IDs / reused baseline revisions | 0 / 0 |
| Parser rejects / quality rejects | 0 / 0 |
| Silver current / history / output revisions | 58 / 0 / 58 |
| Memberships / links | 58 / 0 |
| Gold event_current / bridge | 58 / 58 |
| Natural/ROI serving count | 58 |

`balanced=true` và phương trình current+history/output, membership/current,
capture unique+repeat/raw và baseline+capture−reuse được collector kiểm giá trị,
không chỉ tin boolean. JMA readiness pin exact 1997 jan-sep/oct-dec, 2000 và
2023 full-year manifests; không cộng số dòng JMA historical vào current-month.

| Bảng trong iceberg.operational_current_month | Snapshot ID | Rows |
|---|---|---:|
| event_current | 1543775439517699343 | 58 |
| event_source_bridge | 6717303813908221083 | 58 |
| dim_date | 8242784952305021235 | 22 |
| dim_region | 4948724480068210812 | 1 |
| dim_magnitude_band | 3514905636761141744 | 7 |
| dim_depth_band | 9052112578348555548 | 5 |

Publication ID:
`pub_744bb52905e31c0a7d59250333c50178048156fc3992ab87a47865e9cdab132f`.
Journal: `s3://japan-earthquake/silver/current-month/gold-verification/cm1-847b2845f3e2f5ded8e815776e69d9756a65fcfa/publication.json`.
Historical namespace/aliases không bị current-month writer thay thế.

## Collector và deployment proof

Read-only collector thật, exit 0:

```bash
python3 scripts/qa-01.py --run-id 'scheduled__2026-10-23T00:15:00+00:00'
```

Full report local: `staging/qa-01/20261023T102331Z-91c94f791b4e/report.json`.
SHA-256 `4dc36a80876d0adc6403b3c283b7581f89e2d4bb2baeea504430bcff81927649`.
[Proof rút gọn có pins/counts/42 SQL](./QA-01_CURRENT_MONTH_20261023.json)
không copy audit/schema lặp; snapshot IDs lưu **string thập phân** để không mất
độ chính xác 64-bit khi đọc bằng JavaScript. Full report vẫn giữ JSON gốc.
Collector re-read source/Silver/Gold marker/journal và JMA manifests, không refetch,
ETL, commit, publish hoặc giải phóng lease.

**Không ghi default Make preflight là PASS:** `make smoke-qa-01` dừng trước
collector với `IMAGE_UNPREPARED_OR_STALE_RUN_MAKE_DOCKER_BUILD`. Deployment này
dùng cached image + explicit readonly prebuilt override của ngày 22/10; chưa
nghiệm thu rebuild/default-image freshness. Không sửa image label/receipt hoặc
bypass `docker-runtime.py` để làm Make xanh.

Ngoài collector, [deployment proof độc lập](./QA-01_DEPLOYMENT_20261023.json)
đã kiểm cả ba Airflow services: healthy, exact project/checkout, mode operational,
ORC07 disabled, readonly DAG/runner/Iceberg mounts và actual file SHA khớp host.
Source mount `/host_mnt/Users/...` của Docker Desktop được đối chiếu host mapping
và actual bytes, không chỉ bỏ prefix để tin đường dẫn. Runner SHA giữ nguyên
`cd30d98d09808d1ca617b866c57bcff00bd0aef1f70cefe3fd806146e00aa51d`, context
code version `7e95eb49d4523010403470bb7a15a1e441fd5689`; Python patch SHA được
ghi riêng. Đây là nghiệm thu **explicit prebuilt deployment**, không chứng nhận
default lifecycle hoặc một fresh image trên máy khác.

## Kiểm thử và trạng thái cuối

- `make test-qa-01`: **75 tests PASS** (thêm bốn tests về log preservation,
  timeout reason/redaction, invalid/oversized/duplicate JSON và symlink guard).
- `TMPDIR=/private/tmp make ci-airflow`: **450 tests PASS**. Lần đầu dùng TMPDIR
  mặc định macOS fail path guards ở QA-03/QA-04; giữ guard, chạy lại canonical
  `/private/tmp` đạt. Không coi lần fail là PASS.
- `make ci-docs-base`: PASS, gồm contracts/static configuration/CI/Docker guards;
  không Maven rebuild, Java/JAR không đổi.
- Task status/planning/docs links và `git diff --check`: PASS. Secret-pattern và
  exact-config credential scan của **15 file QA-01 thay đổi**: 0 findings.
- Full static security scan trên branch QA-01 trả exit 1: 9 flags ngoài thay đổi
  QA-01 (guard literal ở `notebooks/experiment_harness.py`, synthetic URI fixture
  và reachable historical versions), 3 coverage pending vì không chọn runtime/
  handoff trong lệnh static. Không ghi full security PASS. Bản vá/triage
  **SEC-01 — Review secret và bề mặt truy cập** ở branch riêng
  `fix/sec-01-ml-handoff-audit`, chưa được gộp vào branch QA-01; không tự sửa
  allowlist hoặc gộp task khác để làm scan xanh. Report local:
  `staging/qa01-final-security-20261023.json`.
- Blocker runtime đã quan sát: capture không hoàn tất → downstream không
  publish, run failed/lease retained. Các fault cases SQL/pins/eligibility còn
  dùng offline regression; không nhận là live fault injection đầy đủ.
- Sau collector: **active DagRuns=0**, daily **paused=true**, source lease
  **không còn owner** do release task thật. Cron giữ 07:15. Artifacts/run/log giữ
  nguyên, không build/pull/init/reset/IAM/credential change hoặc xóa business data.

## Giới hạn / bước tiếp theo có tên

- **QA-01 — Daily E2E:** Done cho operational v2 được owner chọn. Không có
  evidence một run đầu tiên liên tục đúng giờ qua đêm; máy local phải thức và
  Docker/network hoạt động. Daily đang pause, không tự chạy ngày kế tiếp.
- **FND-01 — Smoke checklist môi trường:** evidence lifecycle cũ không chứng
  nhận fresh image/default preflight cho v2 override này. Chuẩn bị image và
  nghiệm thu lifecycle/preflight riêng trước khi bỏ override; không auto build
  trong smoke. Helper chuẩn hóa preflight cho readonly override chưa có task riêng.
- **QA-02 / QA-04:** historical runtime acceptance không tự chứng minh mọi
  revision/deletion/failure case current-month ngoài run recovery này.
- **MLD-01 / MLD-05 / MLI-02:** output operational không đủ research/official ML
  gates. **JMA sau baseline, deletion/ID reprocess, state compaction/replan vượt
  budget:** chưa có task triển khai riêng; không bỏ guard để tiếp tục.
