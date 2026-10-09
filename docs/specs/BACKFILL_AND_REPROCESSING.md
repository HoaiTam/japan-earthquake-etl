# ORC-03 — Backfill và reprocessing có phạm vi

## 1. Đang làm gì, có gì, dùng để làm gì?

`orc_03_backfill` là DAG **manual, paused khi tạo, preview mặc định**, không
catchup, tối đa một run/một task. Python chỉ lập kế hoạch/điều phối metadata;
GeoJSON/ZIP và checksum/readback được xử lý bởi Java. Không có downloader mới.

| Action | Input | Thực hiện | Kết quả tối đa |
|---|---|---|---|
| `ingest` | UTC range USGS và/hoặc năm/segment JMA | USG-06 phases; workflow API JMA-04; Java exact readback | `BronzeVerified`, không Published |
| `reuse` | Exact Bronze manifest + raw SHA + manifest byte SHA + source scope | Chỉ đọc manifest/raw trên MinIO; không HTTP nguồn/ghi lake | `BronzeVerified`, không Published |
| `reprocess` | Bronze pins, existing Silver refs, Gold baseline snapshots và affected partitions | Reuse/readback → versioned scoped adapter → ORC-01 six-phase gates | `Published` chỉ khi adapter thật commit/Trino verify/publish đạt |

`preview=true` không gọi runner/nguồn/MinIO, không lấy lease và không ghi
persistent staging. Planner JMA-04 được dùng trong temporary directory rồi
đóng; preview không chứng minh object tồn tại, checksum đúng hay coverage đủ.
`make backfill-preview` đọc **public settings từ `.env.example`**, không đọc
`.env`; muốn kiểm đúng môi trường chạy cần preview thêm trên Airflow UI.

```mermaid
flowchart LR
    P[Resolve exact plan] --> L[Whole-run source lease]
    L --> I[Ingest bounded sources OR reuse exact Bronze]
    I --> B[Java raw + manifest readback]
    B --> R[Optional scoped Silver / Gold / Trino adapter]
    R --> S[Summary with exact input pins]
    S --> C[all_done release lease]
    S --> G[all_success completion gate]
    C --> G
```

Preview đi qua graph với `PREVIEW/verified=false/published=false`, bỏ các
side effect. Cleanup là `all_done`, final leaf `completion_gate` là
`all_success`; source/adapter lỗi không bị đổi thành successful DAG.

## 2. Backfill từ nguồn

Configuration mẫu có sẵn tại
`airflow/dags/fixtures/orc_03_ingest_preview.json`:

```json
{
  "operation_id": "pilot-2023-v1",
  "processing_date": "2026-10-08",
  "action": "ingest",
  "preview": true,
  "usgs": {
    "window_start_utc": "2023-01-01T00:00:00Z",
    "window_end_utc": "2023-01-08T00:00:00Z",
    "chunk_days": 3
  },
  "jma": {
    "years": [1997],
    "segments": [{"year": 1997, "segment": "oct-dec"}],
    "force_download": false
  }
}
```

- `operation_id` bắt buộc, safe label tối đa 100 ký tự. Đây là identity của
  **công việc**, không phải Airflow run ID; cùng ID/scope dùng lại child IDs.
- `processing_date` bắt buộc và cố định giữa retry/rerun, không lấy ngày host.
- USGS UTC hậu tố `Z`, half-open `[start,end)`, end không tương lai. Chunk
  liền nhau, không overlap tự động, `1 <= chunk_days <= USGS_MAX_WINDOW_DAYS`
  (runtime guard 31 ngày); tối đa **32 chunks/operation**. Với default 3 ngày,
  nạp lịch sử dài hơn 96 ngày cần chia các operation không trùng biên hoặc
  giảm phạm vi, không bỏ guard. Runner vẫn chặn request overflow theo USG-06.
  Tổng USGS chunks + JMA archives tối đa **64 inputs**, chặn ngay ở planner
  trước execution, không đợi đã ghi Bronze rồi mới phát hiện danh sách quá lớn.
- JMA dùng CSV inventory pin SHA-256, năm 1984–2023. Bỏ `segments` thì lấy đủ
  segment của từng năm; truyền `segments` phải chọn đúng year/segment thuộc
  `years`, không URL tùy ý. Một segment 1997 ready không có nghĩa cả năm ready.
- Gọi **workflow API JMA-04** (`resolve_plan`, `execute_archive`, summary/gate)
  trong cùng owner lease, không trigger child DAG để tránh deadlock lease.
  Java downloader/writer/cache/publication reuse giữ nguyên. ORC-03 tuần tự
  archive/chunk; không dùng mapped concurrency 2 của standalone JMA-04.
- JMA `force_download=true` chủ động kiểm revision. Release hiện tại chỉ biết
  sau download; **không thể yêu cầu URL hiện tại trả bytes của release cũ**.
  Release cũ phải có exact Bronze manifest để `reuse`/`reprocess`.
- JMA native JST envelope và USGS query interval chỉ là source coverage,
  **không tự suy ra Silver/Gold write partitions hoặc research train/test split**.

Planner hash public USGS request settings, bucket/prefix, inventory/config và
exact input/output. `scope_sha256` không phụ thuộc preview flag hoặc Airflow
run ID. Child IDs hash toàn operation/scope, không dùng ID đã sanitize dễ đụng.
Execution chặn nếu environment settings thay đổi sau resolve.

## 3. Bronze reuse / release selection

Mẫu đã pin từ evidence ORC-02/JMA-05:
`airflow/dags/fixtures/orc_03_reuse_sample.json`. Mẫu là metadata reference,
không phải payload; object có thể chưa tồn tại trên máy thành viên mới.

`bronze_inputs` là list 1–64 entries, không trùng URI:

- Common: `source_system`, `manifest_uri`, `sha256` (**raw bytes**),
  `manifest_sha256` (**manifest bytes**).
- USGS: thêm `window_start_utc`, `window_end_utc` của **manifest ingest**.
- JMA: thêm `year`, `segment`, `catalog_release`, phải khớp URI và manifest.

Không nhầm `input_manifests.sha256` trong ORC-02 source summary (manifest hash)
với `input_scope.sha256` trong ORC-01 ETL (raw hash). Mẫu ORC-03 có cả hai tên.
`reuse` không được kèm `usgs`/`jma` download configuration.

`BronzeReuseVerifier` đọc đúng manifest/raw key trong bucket được cấu hình,
kiểm manifest SHA, BronzeReady/version/source, 5 validation flags, raw URI/key/
length/SHA, source interval hoặc year/segment/release, structure và count.
USGS dùng `UsgsGeoJsonValidator`, JMA dùng `JmaArchiveValidator`; không giải nén
ZIP rồi upload các member vào Bronze. Empty payload hợp lệ count=0 vẫn qua;
duplicate records giữ nguyên cho Silver. Không scan prefix/latest, không
put/delete object, không sửa DAT-01 sample catalog hoặc manifest cũ.

`BronzeVerified` xác nhận **các input đã chọn**, không phải SourcesReady cho
mọi năm/range giữa hai biên, SilverReady hay Published.

## 4. Scoped reprocessing protocol `orc-03-v1`

Action `reprocess` chỉ nhận `bronze_inputs` đã pin (đủ hai nguồn) và object
`reprocess` với chính xác các fields:

Mẫu đầy đủ: `airflow/dags/fixtures/orc_03_reprocess_preview.json`, **bootstrap
preview**, không phải recovery conf cho bảng đã có. Đổi sang recovery cần pin
existing Silver/baseline snapshots và old/new scope thật; không bỏ baseline
về null để làm cho gate qua.

| Field | Ý nghĩa / yêu cầu |
|---|---|
| `window_start_utc`, `window_end_utc` | Phạm vi nghiệp vụ ETL tường minh; khác source archive envelope được |
| `output_scope` | ORC-01 shape: exact Silver `{event_year_utc,event_month_utc,source_system}` và Gold tables |
| `gold_partitions` | Exact `{table,event_year_utc,event_month_utc}`, phải bao phủ mọi Gold table trong scope |
| `existing_silver_manifests` | Exact `{manifest_uri,sha256}` của existing state; SHA ở đây là manifest bytes; không scan latest |
| `baseline_snapshots` | Mỗi Gold table một `{table,snapshot_id}`; signed positive 64-bit string; `null` chỉ khi bootstrap table chưa tồn tại |

Affected scope phải là **hợp old + new partitions**, kể cả revision đổi tháng,
dedup/link đổi membership. Operator/downstream resolver cần pin existing state
để xác định scope này; ORC-03 **không tự biết** partitions chỉ từ năm JMA hay
USGS range. Nếu baseline nào không null thì existing Silver refs không được
rỗng. Empty existing refs/null baselines là assertion bootstrap phải được
adapter kiểm tra, không phải quyền overwrite bảng đã có. Bootstrap/revision
khác input/scope dùng operation ID mới; rerun sau partial commit phải dùng
idempotency ledger của adapter để nhận diện snapshot đã ghi bởi operation cũ.

Hiện `gold_partitions` là **logical month scope**, không chốt physical Iceberg
partition transform (GLD-03 resolve transform khi triển khai writer). Dimension/global table
không có partition này chưa được phép âm thầm full overwrite. Extension cho
global-table replacement: **chưa có task**; cần versioned contract/review trước
khi bổ sung vào scope này.

Real command riêng `BACKFILL_ETL_RUNNER_COMMAND` default **rỗng**, không dùng
ORC-01 unscoped adapter thay thế. Không có mock execution mode trong DAG này.
Thiếu command fail trước staging/source execution. Command do môi trường quản
lý, không nhận executable từ conf; gọi `shell=False`, thêm `--context-file`.

Mỗi request JSON chứa version, `phase`, exact `plan`, verified `bronze`,
`upstream` wrapper và hash. Các phase giữ thứ tự ORC-01
`readiness/bronze/silver/gold/verify/publish`. Response đúng một JSON object:

```json
{
  "etl_receipt": "<object theo ORC-01 1.0, không phải chuỗi trong implementation>",
  "scope_receipt": {
    "contract_version": "orc-03-v1",
    "scope_sha256": "<same plan hash>",
    "operation_id": "<same operation>",
    "phase": "<same phase>",
    "baseline_verified": true,
    "idempotency_verified": true,
    "outside_scope_unchanged": true
  }
}
```

Adapter phải validate exact manifests/baselines trước side effect, dùng scoped
replace/upsert theo operation identity, giữ outside scope, readback counts/
keys, commit snapshots, Trino verify cùng snapshot rồi mới publish. Runtime
kiểm cả scoped receipt và ORC-01 context/upstream/Silver/Gold/Trino gates;
field lạ, hash lệch, false hoặc số `1` thay boolean chặn phase sau.

Receipt/hash là **trusted adapter protocol, không phải chứng minh hoặc rollback
physical side effect**. Tests bằng adapter double chỉ kiểm handoff/gate, không
được coi là Spark/Iceberg/Trino live acceptance. Chưa có adapter toàn chuỗi
nên `reprocess` thật hiện **fail closed**, không tạo Published giả.

## 5. Retry, recovery và concurrency

Shared `SOURCE_GUARD_ROOT` cùng USG-04, JMA-04, ORC-02 giữ ownership toàn run.
Contention fail trước ghi Bronze; cleanup không release foreign owner. Không
TTL/autosteal. ORC-03 retries=0 bảo thủ; operator clear task lỗi + completion
hoặc tạo Airflow run mới **giữ nguyên operation_id/conf/processing_date**.

`BACKFILL_STAGING_ROOT/<sha256(operation_id)>/plan.json` pin plan immutable.
Đổi scope/settings/checksum dưới cùng ID bị chặn. Trước attempt, summary về
RUNNING; lỗi source/adapter trả FAILED, không tái dùng summary thành công cũ.
USGS cùng child ID reuse fetch-state/raw; JMA dùng verified publication cache.
Mất staging/cache không bảo đảm exactly-once physical storage; không xóa
bucket/volume để retry. Java readback luôn chạy lại, không tin receipt cũ.

Metadata ở cùng root: `bronze-verify-input.json`, `bronze-result.json`, từng
`<phase>-input.json` và `run_summary.json`; raw không đi qua XCom. Summary gồm
operation/scope hash, exact Bronze pins, published boolean và optional verified
publication. Counts/durations/per-phase persistent failure summary đầy đủ
thuộc ORC-04, chưa được bịa từ wrapper này.

## 6. Lệnh test và chạy

```bash
make backfill-preview
make backfill-preview BACKFILL_CONF=airflow/dags/fixtures/orc_03_reuse_sample.json
make backfill-preview BACKFILL_CONF=airflow/dags/fixtures/orc_03_reprocess_preview.json
make test-backfill
make smoke-backfill-readback
make test-airflow
make check
git diff --check
```

Maven dùng JDK 17/21 được repo chấp nhận, không Java 26. Build Java/image mới
trước chạy reuse/verifier thật: `make up-airflow WAIT_TIMEOUT=600` (có rebuild/
recreate service, chọn thời điểm không có DAG đang chạy). `.env` cũ không bắt
buộc thêm keys; Compose có default verifier và scoped command rỗng.

`smoke-backfill-readback` là live read-only QA: build image mới, chạy one-off
container `--no-deps`, gọi runtime API reuse hai manifests trong sample rồi
rerun cùng operation dưới shared lease. Không restart foundation, trigger
scheduler, unpause DAG, tải nguồn, ghi lake hoặc thay runner container đang
chạy. Chỉ ghi QA/control metadata lên staging. Object mẫu chưa có trên máy
mới thì phải ingest bằng workflow nguồn trước; không tự fallback download.
Evidence API helper không được gọi là scheduler E2E.

`make smoke-backfill-pilot` là opt-in **có gọi nguồn/ghi Bronze**: USGS hai
chunk 1 ngày `[2023-01-01,2023-01-03)`, JMA 1997/oct-dec qua workflow/cache
JMA-04, rerun cùng operation, readback hai input mẫu ngoài namespace ghi trước/
sau. Không Silver/Gold, không full 40 năm, không restart hoặc unpause stack.
Mỗi lần gọi tạo operation QA mới; chỉ rerun **bên trong một lần gọi** dùng
cùng operation. Không chạy target này để kiểm read-only; dùng
`smoke-backfill-readback` nếu không muốn tạo Bronze mới. Old-object readback
không phải bằng chứng mọi Silver/Gold partition ngoài scope được kiểm live.

Airflow UI → `orc_03_backfill` → chạy manual với file JSON mẫu preview=true.
Kiểm scope, bucket, chunk/year/segment, expected output và `published=false`.
Khi muốn chạy thật, run mới với preview=false; bắt đầu USGS một ngày/JMA một
segment hoặc `reuse` sample đã có. Không nạp full 40 năm để nghiệm thu.
Giữ nguyên operation ID cho rerun. Muốn lấy revision mới: operation ID mới,
phạm vi nhỏ, JMA force flag nếu cần; không đổi existing successful plan.

## 7. Handoff / giới hạn có owner

- [SLV-06 - Deduplicate và xử lý revision trong từng nguồn](../task/tasks/SLV-06.md)
  và [SLV-07 - Liên kết observation và chọn canonical event](../task/tasks/SLV-07.md):
  còn revision/dedup/link thật và old/new affected scope.
- [SLV-09 - Tích hợp và kiểm thử Silver đa nguồn](../task/tasks/SLV-09.md):
  còn adapter/readback đủ Silver bundle và existing-state resolution.
- [GLD-03 - Ghi Gold Iceberg và commit snapshot](../task/tasks/GLD-03.md):
  còn scoped writer, baseline/idempotency ledger và real snapshot commit.
- [GLD-04 - Tạo Trino views và verification SQL](../task/tasks/GLD-04.md):
  còn SQL evidence outside-scope, counts/lineage và publication thật.
- [ORC-04 - Chuẩn hóa logging và run summary](../task/tasks/ORC-04.md):
  dùng operation/scope/input refs để thêm counts/durations/reconciliation.
- [ORC-05 - Chốt recovery, concurrency và tài nguyên](../task/tasks/ORC-05.md):
  còn resource benchmark, stale-lease recovery, cache-loss/partial Iceberg
  commit và distributed/all-writer locks (ORC-01 real writer chưa dùng lease này).
- [QA-01 - Chạy E2E daily đa nguồn](../task/tasks/QA-01.md) và
  [QA-03 - Kiểm thử JMA historical backfill](../task/tasks/QA-03.md): còn
  E2E Published/historical rộng, không thay bằng protocol fixture tests.

Evidence task: [ORC-03](../evidence/ORC-03.md). Không đổi trạng thái các task
downstream chỉ vì ORC-03 preview/reuse thành công.
