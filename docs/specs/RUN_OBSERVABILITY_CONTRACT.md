# ORC-04 — Structured logging, metrics và run summary

| Thuộc tính | Giá trị |
|---|---|
| Task / version | ORC-04 / `orc-04-v1` |
| Baseline GitHub | PR #46 `97f9ac58d52c2fee787b5d57d1f974f83264752b`; PR #47 được rebase và bổ sung ORC-03 integration |
| Phạm vi | Metadata điều phối ORC-01/02/03; không parse hoặc transform record bằng Python |

## 1. Làm gì, có gì, dùng để làm gì?

`run_observability.py` ghi log JSON, journal và summary nhỏ theo DAG/run/phase/
attempt. Team dùng chúng để đối soát count, tìm phase lỗi, truy exact input/
snapshot và biết một run mới thực sự đạt gate nào. Không cần thêm database
nghiệp vụ hoặc Prometheus/Grafana để dùng tính năng này.

| Thành phần | Có gì | Dùng để làm gì |
|---|---|---|
| `airflow/dags/run_observability.py` | Projection allowlist, count validator, atomic persistence, flock, safe reasons, DAG failure callback | Metadata được kiểm tra trước khi log; lỗi không bị che bởi summary |
| `etl_pipeline_runtime.py` + ORC-01 DAG | Observed resolve/6 phase/final gate | Giữ graph 8 task và strict `all_success` sole leaf |
| ORC-02 daily DAG | Observed resolve/lease/JMA/USGS/readiness/completion | Trace hai nguồn thật; SourcesReady không phải Published |
| `backfill_observability.py` + ORC-03 DAG | Parent Airflow run + operation/scope binding, source readback, scoped phases, cleanup/completion | Cùng operation dùng lại business child IDs nhưng mỗi Airflow run có journal riêng |
| `run_summary_cli.py` | Smoke, inspect theo DAG/run, replay hai exact source metadata files | Test/đọc report mà không tải dữ liệu hoặc trigger scheduler |
| `test_run_observability.py` | Empty/failure/rerun, equations, secret, stale state/attempt, CLI | Regression độc lập bằng fixture |

Theo yêu cầu cập nhật, PR #47 được đặt trên nền PR #46 (ORC-03), không còn
đứng riêng trên main. Phải merge PR #46 trước PR #47. Nền triển khai ban đầu
`ff4f460` được giữ trong evidence lịch sử, không coi là nền hiện tại.

## 2. Vị trí và vòng đời

Trong Compose, `RUN_SUMMARY_ROOT=/opt/pipeline/staging/run-summary` trên
`pipeline_staging`, không nằm trong Bronze/Gold. Host smoke mặc định ghi
`staging/run-summary` đã gitignore.

Mỗi run dùng key SHA-256 đầy đủ của UTF-8 `dag_id + "|" + run_id`:

```text
RUN_SUMMARY_ROOT/<key>/
  .lock
  state.json
  run_summary.json
  events/00000001.json ...
```

- File JSON tạo bằng temporary file quyền `0600`, replace atomically; lock
  `flock` chỉ giữ khi cập nhật metadata, không bao quanh source HTTP/Spark job.
- Journal giữ các attempt cũ. `state`/summary chỉ giữ phase hiện hành, không
  cộng count của lần retry vào count business.
- Start phase mới loại trạng thái/count/snapshot của chính phase và các phase
  phía sau khỏi **state hiện hành**, không xóa journal hoặc dữ liệu lakehouse.
  Mỗi attempt có token sequence; completion cũ không ghi đè attempt mới.
- Resolve chỉ lưu hash của context đã resolve. Phase đầu tiên phải khớp hash
  đó. Đổi config/scope cần resolve lại, không nhận receipt của context cũ.
- CLI inspect đối chiếu sequence và summary với state dưới lock; interrupted
  write/stale file fail closed. Không chỉ đọc một file `published=true` cũ.
- Normal failure ghi `FAILED`, phase/reason và những bước chưa thực hiện,
  sau đó raise fixed reason để task/DAG vẫn thất bại. Callback DAG là fallback
  cho terminal failure/cleanup; không thêm success leaf `all_done` vào ORC-01.
- Persistence lỗi không bypass gate; chỉ báo `RUN_SUMMARY_WRITE_FAILED`.
  Disk full có thể không lưu được failure report, nên vẫn cần Airflow task state.

Khóa này bảo vệ telemetry trên shared local volume, không thay source lease,
Iceberg transaction hoặc recovery/admission policy.

## 3. Summary và metric

Summary gồm run/DAG ID, mode, context hash, config version, processing date,
USGS UTC interval, trạng thái, reason/failed phase, counts theo nguồn, JMA
release/year/segment, exact Bronze manifest/raw URI/SHA, Silver manifests,
Gold table/snapshot bundle và verification/publication URI.

Bronze receipt của ETL có `ingest_run_id` từ contract; receipt nguồn daily
dùng `receipt_run_id` riêng: run gọi probe **không** mặc nhiên là run đã tạo
Bronze manifest reuse. Raw SHA và manifest-byte SHA cũng là hai field riêng.
ETL Bronze legacy v1.0 chưa trả manifest-byte SHA; không tự suy ra nó.

Metric là **gauge theo run/scope**, không cumulative counter:

- `sources[].counts`: input/fetched/parsed/parse_error/ignored/valid/rejected/
  duplicate/superseded/current/linked/unlinked.
- `gold_counts`: canonical current, source bridge, published count;
  arithmetic reconciliation riêng với `gold_verification_status`.
- Phase status, attempt, start/end UTC, `duration_seconds` đo monotonic.
- Run `duration_seconds` là tổng execution time **attempt hiện hành**, không
  bao gồm scheduler wait hoặc các attempt cũ. `wall_duration_seconds` là span
  UTC từ lần resolve đầu tới lần update cuối, có cả chờ/retry; không benchmark
  throughput bằng hai loại thời gian khác nhau.
- Freshness event/snapshot time/age hiện `null/not_reported`: adapter chưa
  cung cấp. Không lấy processing date hoặc log time làm thời điểm event/snapshot.

Log phase dùng `event=pipeline_phase`; final/failure dùng
`event=pipeline_run_summary`. Các source/count/reason trong summary log có
thể filter trực tiếp bằng run ID. Journal cũng giữ metadata projection thành
công của phase trước nếu phase sau thất bại.

`null/not_reported` nghĩa là chưa biết/chưa cung cấp; **không phải zero**.
`0/passed` nghĩa là có report rỗng hợp lệ đã đối soát. Bronze-only run giữ
parsed/dedup/link/Gold null. `input` là count estimate readback theo nguồn:
USGS features, JMA structural fixed-width records, không phải natural/canonical
event count. Không cộng hai nguồn rồi gọi tổng là số động đất.

## 4. Extension receipt cho owner adapter

Envelope ORC-01 `contract_version=1.0` giữ nguyên; thêm **optional** field
`observability` có version độc lập `orc-04-v1`. Strict allowlist vẫn loại mọi
field lạ. Hash upstream bao gồm extension khi có; không tái sử dụng hash
receipt cũ. Legacy adapter không gửi extension vẫn chạy gate cũ, nhưng summary
sẽ ghi count downstream `not_reported`.

Silver gửi đầy đủ hai source rows, Gold gửi `sources=[]` và Gold counts;
verify/publish nhắc lại **cùng** Gold counts. Xem JSON executable tại
`airflow/dags/fixtures/orc_01_mock_v1.json`, mục `observability` (synthetic).

Schema Silver:

```json
{
  "version": "orc-04-v1",
  "sources": [{
    "source_system": "USGS",
    "counts": {
      "input": 10, "fetched": null, "parsed": 8, "parse_error": 1,
      "ignored": 1, "valid": 6, "rejected": 2, "duplicate": 1,
      "superseded": 2, "current": 3, "linked": 2, "unlinked": 1
    },
    "reject_reasons": {"INVALID_COORDINATES": 2},
    "catalog_releases": []
  }],
  "gold": null
}
```

Ví dụ trên chỉ minh họa một row; receipt thật bắt buộc thêm row JMA_BULLETIN.
Không có business payload hoặc danh sách rejected records trong report này.

Gold/verify/publish:

```json
{"version":"orc-04-v1","sources":[],"gold":{"current":4,"bridge":6}}
```

Count là nonnegative signed-64-bit integer, không bool/float/NaN. Source
counts/reasons/releases có field set chính xác, tối đa 64 reason/release entries.
`fetched=null` nếu không có bằng chứng về network fetch trong lần chạy này;
không biến Bronze reuse/cache thành newly fetched records. Nếu có số liệu thì
`0 <= fetched <= input` trong cùng input scope.

### Grain và equations bắt buộc

```text
input = parsed + parse_error + ignored
parsed = valid + rejected
valid = duplicate + superseded + current
current = linked + unlinked
sum(primary_quality_reject_reasons) = rejected
Gold bridge = sum(source current)
0 <= Gold canonical current <= Gold bridge
Gold counts at verify = Gold counts at commit
Gold counts at publish = Gold counts at verify
```

- `input` trong Silver report phải bằng tổng count của exact Bronze inputs
  cùng source. `parsed` ở đây là observation parse thành công trước quality;
  parser errors riêng, ignored structural types riêng, phải được giải thích.
- `duplicate` là identical repeated observation; `superseded` là non-current
  revision. Hai bucket loại trừ nhau; retained history row **không** phải current.
- `linked` là current observation **memberships đã được gán qua source link**,
  không candidate pairs/link rows. Gold bridge cũng là current memberships;
  canonical event count có grain khác, ví dụ 6 memberships thành 4 events.
- Reject reasons dùng **một primary quality reason mỗi record**. `slv-05-v1`
  `reason_counts` hiện là multi-reason occurrence counts: không copy/sum trực
  tiếp vào field này. Adapter phải tổng hợp primary reasons bằng Java/Spark;
  full multi-reason reject dataset vẫn giữ nguyên theo CON-03/SLV-05.
- Scope là toàn replacement population mà các phase đối soát, không trộn
  count new input với global current-state hoặc retained history. Contract v1
  này chưa có baseline/new/updated/unchanged reconciliation. Nếu job merge với
  existing state không thể đáp ứng equations, để counts `not_reported` hoặc
  đề xuất version mới trước khi emit; không đổi nghĩa field ngầm.

Thiếu count upstream, sai arithmetic, đổi count lúc verify/publish hoặc input
không khớp Bronze làm phase fail với reason cố định; downstream không chạy.
Report count không tự chứng minh physical readback: trusted Java/Spark/Trino
adapter phải cung cấp count từ output/query thật và exact snapshots.

## 5. Published, mock và replay

- ORC-01 final gate kiểm tra đủ sáu persisted phase success, hash **toàn bộ**
  publish receipt (kể cả counts), cùng snapshot/report/publication references
  và strict publish receipt; không nhận một receipt sửa count sau phase gate.
- Chỉ `mode=real` + committed Gold bundle + passed Trino receipt + publication
  gate mới ra `Published`. Exit code `0` hoặc summary file không đủ.
- Mock → `MockComplete`, `evidence_kind=synthetic`, `published=false`, Gold
  published count null dù synthetic current count có giá trị.
- ORC-02 → `SourcesReady`, `published=false`; không chứa Gold snapshots.
- ORC-03 ingest/reuse → `BronzeVerified`, `published=false`; reprocess chỉ
  `Published` sau scoped six-phase gates, execution receipt và released lease.
- Replay saved source metadata → `MetadataReplayComplete`, `mode=replay`,
  `evidence_kind=saved_metadata`, `published=false`. Không impersonate run cũ,
  không tuyên bố fresh readback hoặc scheduler run mới.

## 6. Kiểm thử và vận hành

Từ repo root:

```bash
make test-observability
make observability-smoke
# Copy run_id mà smoke trả về:
make observability-read RUN_SUMMARY_RUN_ID=<run_id-vua-tra-ve>
# Foundation đang chạy, không restart/unpause/download:
make smoke-observability
# Read-only Bronze sample đã tồn tại: build image mới, one-off, không restart stack:
make smoke-backfill-observability
# Một run thực của DAG sau khi deploy có summary:
make observability-read-runtime RUN_SUMMARY_DAG=orc_02_daily_sources RUN_SUMMARY_RUN_ID=<run_id>
```

Host smoke lưu trong `staging/run-summary`; runtime smoke cố ý lưu dưới
`/opt/pipeline/staging/run-summary/qa`. Đọc report QA container bằng exact root:

```bash
docker compose exec -T -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor python -m run_summary_cli --inspect --root /opt/pipeline/staging/run-summary/qa --dag-id orc_01_etl_pipeline --run-id <run_id-qa>
```

Command là template với ID operator copy từ output, không có lệnh thực thi
business Silver/Gold còn thiếu. `etl-preview`/`etl-mock` cũ vẫn offline không
persist; muốn kiểm tra persistence dùng `observability-smoke`.

Deploy bình thường theo runbook `make up-airflow` sau review; thay compose env
cần recreate service. Module DAG bind-mounted, root default đã giống Compose
nên smoke không cần recreate. Không tự unpause nguồn để test logging.

Giới hạn file/event/state 256 KiB, receipt adapter vẫn 64 KiB; không log raw
stdout/stderr/env/conf/exception. URI có credentials/query/fragment/wildcard/
traversal bị reject. Reason lỗi chỉ registry cố định, không dump trace payload.
Operator IDs/release IDs phải là metadata không nhạy cảm, không dùng credential
làm identifier. Không có cơ chế bảo vệ secret cố tình nhét trong một ID hợp lệ.

## 7. Evidence và giới hạn còn lại

[Evidence ORC-04](../evidence/ORC-04.md) ghi đúng unit/mock, runtime API/graph và
saved real Bronze metadata, không coi chúng là E2E Published.

- **SLV-06 - Deduplicate và xử lý revision trong từng nguồn**, **SLV-07 - Liên
  kết observation và chọn canonical event**, **SLV-09 - Tích hợp và kiểm thử
  Silver đa nguồn**: cung cấp counts/primary reasons/current memberships thật
  vào extension; hiện chưa có integrated Silver adapter.
- **GLD-01 - Xây canonical event, dimensions và bands**, **GLD-03 - Ghi Gold
  Iceberg và commit snapshot**, **GLD-04 - Tạo Trino views và verification SQL**:
  output/query counts, commit time/latest event time và verified snapshots thật;
  freshness vẫn not_reported, không có Gold Published thật trong evidence này.
- **ORC-03 - Implement backfill và reprocessing**: observer đã nối trên nền
  PR #46; scoped adapter thật vẫn cần SLV-09/GLD-03/GLD-04, không có mock mode
  runtime trong DAG backfill.
- **ORC-05 - Chốt recovery, concurrency và tài nguyên**: abandoned RUNNING,
  stale lease/retry admission/resource policy theo [local operator profile](./RECOVERY_AND_RESOURCES.md);
  callback không chạy cho mọi manual state change/kill. Không tự force-unlock,
  TTL-delete journal hoặc suy failure recovery; controlled QA chỉ inject trước store call.
- **QA-01 - Chạy E2E daily đa nguồn**: scheduler daily toàn ETL, không được
  thay bằng metadata smoke/replay. **SEC-01 - Review secret và bề mặt truy cập**:
  security review tổng thể; tests ở đây chỉ là regression trong scope telemetry.
- Retention/rotation journal và backend metrics ngoài JSON **chưa có task**
  được chốt; chưa benchmark volume lưu log nhiều tháng hoặc cài Prometheus.

Reviewer độc lập chưa có (`unassigned`); additive contract cần team review
trước khi owner adapters bắt đầu emit real counts.

## 8. ORC-03: backfill/reprocessing được quan sát bằng cách nào?

- Preview vẫn pure: không telemetry directory/lock/journal, không lease,
  không runner/storage. Resolve invalid cũng không persist vì chưa xác định
  preview hay real; Airflow task nhận reason đã sanitize. Callback không tạo
  telemetry cho ORC-03 nếu run chưa có state. Xem Airflow task state/log trong
  trường hợp này, không suy rằng mọi failed resolve đều có JSON summary.
- Real resolve tạo journal; identity gồm parent `dag_id/run_id`,
  `operation_id/action/scope_sha256` và processing/config/UTC context. Hash
  context bind cả parent run ID lẫn scope. Ingest/reuse UTC envelope bao quanh
  các source đã chọn, **không chứng minh coverage liên tục**; reprocess dùng
  business window đã khai báo. Child IDs và scope vẫn ổn định qua rerun.
- Phase order riêng: resolve → lease → source → (readiness/bronze/silver/gold/
  verify/publish chỉ khi reprocess) → execution → cleanup → complete. Graph
  vẫn 5 Airflow tasks, strict sole leaf, cleanup all_done. Source phase đo cả
  scoped pin/preflight/ingest hoặc reuse/readback; không bịa duration từng
  child HTTP request. Source ownership được assert trước side effect.
- Java `BronzeReuseVerifier` nhận optional `observability_version=orc-04-v1`.
  Sau khi kiểm raw/manifest SHA + structure/count, trả optional `observability`
  đúng `{version,bronze_inputs}`. Mỗi row là exact pin + `raw_object_uri` +
  `record_count_estimate` đã readback. Không truyền raw record/ZIP qua Python;
  không scan/list/download source hoặc ghi object. Legacy request vẫn response
  5 fields cũ. Runner cũ thiếu extension vẫn nhận `BronzeVerified` nhưng count
  telemetry là null/not_reported; `bronze_pins` giữ lineage/release đã biết.
- Runtime kiểm version, số row, thứ tự/exact pin fields, URI và integer count.
  Report lỗi/null/extra field fail closed; không bỏ qua để tiếp tục. Reuse
  `fetched=null`, không giả định mỗi manifest là một record hoặc một fetch.
- Scoped adapter giữ `orc-03-v1` wrapper; hash request upstream bind wrapper,
  ORC-01 hash bind **inner etl_receipt**, gồm optional `orc-04-v1` counts.
  Cả scoped baseline/idempotency/outside-scope gates và inner receipt gates
  phải pass trước success event. Không biến scoped adapter thành unscoped.
- Source readback và ETL bronze nói về cùng URI chỉ tính input một lần;
  khác raw SHA/count fail `BRONZE_INPUT_CHANGED`. Silver/Gold counts dùng
  equations ở trên. Không trộn input mới với existing/global replacement
  population; adapter không thể đáp ứng thì không emit counts hoặc chốt version
  mới trước, không đổi nghĩa ngầm (owner: SLV-09/GLD-03).
- `execution` ghi hash toàn bộ summary đã được executor xác nhận. Complete
  đối chiếu hash, exact persisted result/pins và snapshot/report/publication
  refs, cần source/lease/execution/cleanup (+ six ETL phases khi reprocess)
  thành công. Không nhận XCom/file sửa sau execution hoặc completion cũ.
  `BACKFILL_STAGING_ROOT/.../run_summary.json` vẫn là executor receipt cũ;
  **summary cuối DAG** là `RUN_SUMMARY_ROOT/.../run_summary.json` sau cleanup.
  Failed cleanup giữ failed telemetry; không rollback snapshot/object đã commit.
- `make smoke-backfill-observability` dùng hai exact manifests fixture đã có,
  fresh readback hai lần cùng parent run/operation dưới lease, assert counts
  không cộng dồn và released lease. Chỉ ghi QA metadata, không tải nguồn,
  không ghi lake, không scheduler trigger/unpause/restart. Object thiếu thì
  fail, không fallback download. QA root là run-summary/qa; inspect dùng
  `--dag-id orc_03_backfill --run-id <ID>` với root QA này.
