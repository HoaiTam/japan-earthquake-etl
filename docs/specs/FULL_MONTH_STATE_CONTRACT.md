# ORC-06 — Full-month Silver state, versions orc-06-v1/v2

## Làm gì và dùng để làm gì?

Daily USGS overlap là delta, không phải toàn bộ state của tháng. Resolver dựng
lại current state của affected UTC months từ **exact verified baseline inputs
+ delta**, trước khi GLD-03 overwrite tháng. Không sửa CON-03 key/grain/schema,
không ghi Gold, không publish, không tự chọn latest hoặc suy coverage từ số dòng.

Implementation: `FullMonthContract` (typed request), `FullMonthBronzeReader`
(SHA/CRC/count/source coverage/parse), `FullMonthStateResolver` (state/closure/
proof/publication), entrypoint `ie212.earthquake.spark.FullMonthStateJob`.
Business ETL dùng Java, Spark và parser/quality/dedup/link SLV-02..09 hiện có.
Python wrapper chỉ kiểm tra runtime, tải nguyên bản public capture khi opt-in
và gọi Java; không đọc/normalize/sampling records.

## Request và input identity

Shape đầy đủ ở [orc06-request.example.json](../../config/orchestration/orc06-request.example.json).
Đây là **template có SHA/URI placeholder**, không phải input đã verify hoặc
lệnh nghiệm thu. Thay bằng exact pins thật trước khi dùng backend MinIO.

| Field | Quy tắc / mục đích |
|---|---|
| resolver_version | `orc-06-v1` mặc định cũ; `orc-06-v2` opt-in scoped replay DAT-03; reject version/field lạ |
| run_id, dag_id | Safe IDs; production cần exact `SOURCE_GUARD_ROOT/owner.json` của cùng dag/run |
| namespace | Exact `catalog.namespace` của Gold; không tạo table hoặc ghi Gold trong task này |
| code_version, config_version | Pin phiên bản, tham gia identity; đổi code/config phải dùng đúng identity mới |
| processed_at_utc | UTC Z cố định cho rerun; không lấy clock mới giữa các phase |
| source_cutoff_utc | Upper bound retrieval của input, <= processed; tháng phải đã đóng |
| target_months | 1..24 tháng UTC explicit, không wildcard; affected có thể mở rộng |
| baseline_snapshots | Đủ sáu tên bảng GLD-03, integer >=0; 0 nghĩa chưa có snapshot, không tự hiểu là empty production |
| baseline | `null` bootstrap; hoặc `{run_id, registry_sha256}` nếu Gold vẫn zero; Gold nonzero cần thêm `commit_key`, `commit_sha256` |
| bronze_inputs | Exact new/revised manifest URI, SHA bytes manifest, SHA raw và source-specific pins; <=64 entries |

USGS pins thêm `window_start_utc/window_end_utc`; JMA thêm integer `year`,
`segment`, `catalog_release`. Typed booleans/numbers được kiểm tra, không dùng
coercion để nhận `true` thành snapshot 1. Baseline commit key chỉ được là
`gold-commits/<safe-run>/commit.json`; commit phải pin cùng Silver bundle,
namespace và đủ snapshots đang dùng. Thiếu registry khi Gold đã có data bị chặn.

Production đọc current snapshot IDs chỉ để **so sánh stale baseline**, không
chọn input/snapshot mới thay expected. Kiểm tra lại trước writes và trước
marker. Whole-run lease thuộc ORC-07, phải giữ tới handoff/Gold commit; không
CAS hoặc TTL takeover giả. `verify` chỉ readback immutable artifacts; muốn
fresh-check Bronze/current Gold phải gọi lại `resolve` với request đã pin.

## Coverage được tính, không nhận boolean COMPLETE từ caller

- Đọc đúng store/key; verify manifest SHA, BronzeReady, năm validation flags
  typed true, raw SHA/size, count, interval và source pin. Không scan prefix
  để tìm input. Store list của publisher chỉ kiểm tra reservation của output run.
- USGS chỉ nhận full first-page query đúng ROI 20..50/120..155, earthquake,
  không magnitude/depth/updatedafter filter. Response phải dưới limit, không
  dùng trang offset>1 hoặc response chạm limit làm proof đầy đủ. Query phải
  khớp interval `[start,end)`; endtime USGS inclusive được trừ 1 ms.
- JMA verify full ZIP/CRC/count và matching inventory entry, native JST bounds,
  member/year/segment/release. Quét hết member theo physical lines; không first-N.
  Chỉ parse chi tiết các record trong các tháng cần dựng + temporal halo, giữ
  đúng physical line locator. Không xác định được origin time thì fail, không bỏ qua.
- Cả nguồn phải có hợp các verified windows phủ tháng **và halo 16 giây**
  theo maxTimeDelta của SLV-07. Gap hoặc thiếu một nguồn → fail trước output.
  JMA native-year boundaries chuyển sang UTC; không tự gọi một file năm là
  đủ mọi UTC tháng ở hai biên.
- USGS trước `2023-01-01T00:00:00Z` là not-applicable theo CON-01; vẫn cần
  coverage ở halo phía sau nếu cửa sổ chạm seed. Không làm giả response rỗng
  cho giai đoạn nguồn không hoạt động.
- Empty month cần proof coverage đầy đủ + canonical count 0 explicit. Không
  có manifest/source proof không tương đương empty month.

`COMPLETE_MONTHS` nghĩa state đầy đủ **đối với source policy và exact inputs/
known revisions đã pin**, không chứng minh USGS không bao giờ sửa event ngoài
overlap ba ngày hoặc dữ liệu nghiên cứu toàn kỳ đã complete. Cutoff là upper
bound retrieval, không phải bằng chứng mọi event đều được query lại tại cùng
một thời điểm. SHA chứng minh byte identity, không phải chữ ký xác thực nguồn;
Bronze producer/trusted capture vẫn là boundary của provenance.

## Revision, replacement và closure

1. Re-verify registry/bundle baseline và exact Bronze inputs; giữ pinned parent.
2. USGS merge mọi exact baseline+delta revisions, dùng tie-break SLV-06 hiện có.
   Timestamp cũ/mới của source key thay đổi đưa cả hai tháng vào affected.
3. JMA annual/segment snapshot mới **thay active pin của slot cũ**, không union
   key cũ với key mới rồi resurrect record đã bị rút khỏi archive. So sánh raw
   record hashes của **toàn member cũ/mới**, đưa tháng của mọi record bị thêm/
   bỏ/sửa vào scope. Không đoán physical event identity khi jma_k1 thay vì
   time/position. Cùng release khác bytes hoặc release cũ hơn bị reject.
4. Dùng matching/canonical hiện có trên old và new state; đóng cả hai link
   graphs tới fixed point. Bao gồm candidate/rejected/ambiguous links theo
   time threshold, không cắt cạnh/đổi ambiguity chỉ vì khác tháng.
5. **V1:** nếu closure vượt vùng JMA đã parse, reject `MONTH_EXPANSION_REQUIRES_EXPLICIT_REPLAN`:
   thêm các tháng cần thiết và coverage vào request mới, không chứng nhận
   snapshot nửa vời. Nếu đủ vùng đã parse nhưng coverage thiếu vẫn fail.
6. Chỉ output observations/history, links và memberships của affected months.
   Các tháng khác không overwrite. Registry giữ đủ input pins và tính lại
   coverage cho toàn bộ complete months đã tích lũy, không làm mất tháng cũ.

Resource guards chung: tối đa 24 **affected write** months/operation, registry
tích lũy <=480 tháng, 64 active pins, 32 MiB/raw, 100.000 unique selected
revisions (bao gồm USGS history và JMA temporal halo); JMA native record diff
tối đa 1.000.000 dòng/archive. Vượt guard reject, không truncate. V1 giữ nguyên
128 MiB active raw và 128 MiB parent raw, parse lại cả lịch sử đã đăng ký.

### Scoped replay v2 (DAT-03, opt-in)

- Không đổi request shape, key/grain/link/source policy hoặc mặc định v1.
  Version nằm trong request identity, Silver context, registry và receipt.
  V2 đọc được parent v1; v1 không được dùng parent v2 (`RESOLVER_DOWNGRADE_FORBIDDEN`).
- Fresh SHA/structure/CRC/count của **mọi** pin vẫn được kiểm tuần tự. Registry
  chỉ giữ descriptor, không giữ tổng raw bytes của các năm trong heap. Khi dùng
  descriptor để parse/diff, đọc lại exact manifest/raw và kiểm pin, không cache
  unverified bytes hoặc dùng ETag thay SHA. V2 bỏ *aggregate retained-byte* cap
  của v1, không tăng per-archive/row/input/affected-month caps. Old/new selected
  observations và diff hash maps vẫn có trong RAM; đây chưa phải distributed resolver.
- So sánh toàn bộ old/new USGS stable keys trước để nhận cả timestamp chuyển
  tháng xa. JMA replacement vẫn diff toàn native archive; thay catalog release
  còn rebuild các registered months liên quan vì release tham gia revision ID,
  kể cả record bytes không đổi. Native slot mới bảo vệ registered overlap/halo.
- Parse chi tiết JMA chỉ ở affected months +16s, không mọi registered months.
  Đóng cả old/new graph bằng **mọi evaluated edge**. Khi graph chạm tháng khác,
  reparse **toàn tháng mới**, lặp tới fixed point; vượt 24 tháng/100k rows hoặc
  thiếu coverage thì fail trước identity/Silver output. Không cắt link hay chỉ
  thêm tên tháng vào receipt mà chưa parse dữ liệu tháng đó.
- Registry coverage vẫn tính cho **toàn bộ** complete months/pins tích lũy.
  Reconciliation `sources` giải thích toàn archive/selected/outside scope;
  bootstrap một tháng không chứng nhận các tháng còn lại của annual ZIP.
- Whole-year revision có thể vẫn vượt 100k do cần rebuild nhiều tháng; v2
  **không** giải quyết bằng tăng cap, bỏ revision hoặc overwrite một phần.
  DAT-03 còn phải đo/replan trường hợp này và nghiệm thu runtime trước rollout.

## Output và handoff

Trong Silver prefix (hoặc local Silver root khi QA):

```text
bundles/<run_id>/                  # SLV-09 immutable four-dataset bundle
full-month/<run_id>/identity.sha256 # canonical request reservation
full-month/<run_id>/registry.json  # complete months + exact active pins + parent + coverage
full-month/<run_id>/receipt.json   # FullMonthReady + affected + counts + Gold request
full-month/<run_id>/_SUCCESS       # SHA256 receipt; ghi cuối
```

Bundle gồm source_observation, reject_record (empty khi pass), source_link,
canonical_membership, schema/count/SHA và bundle marker của SLV-09. Spark
kiểm observation grain; publisher readback mọi Parquet/schema/count. GoldInputReader
đọc thẳng bundle này, không cần format/matching mới.

Receipt: `scope_completeness=COMPLETE_MONTHS`, `status=FullMonthReady`,
`gold_published=false`, request/registry/Silver SHA, affected months, baseline
snapshots, coverage, reconciliation và `gold_request` theo GLD-03.
Counts giải thích full Bronze records → temporal selected → repeated IDs →
unique revisions → out-of-scope/current/history → canonical/membership/link,
kèm `canonical_by_month` và `explicit_empty_months`.

Retry cùng request phải giữ exact bytes/SHA. Identity khác không được dùng
lại run ID. Partial Silver/registry/receipt writes có reservation; chỉ retry
cùng request, không xóa metadata/volume. Rerun sau khi Gold đã tiến sang
snapshot khác bị stale: ORC-07 phải resume operation journal đúng tầng,
không chạy lại resolver với baseline cũ để reset state.

## Giới hạn và task tiếp theo

- [ORC-07 — Real ETL adapters](../task/tasks/ORC-07.md), [QA-01 — Daily E2E](../task/tasks/QA-01.md)
  và [QA-03 — Historical backfill QA](../task/tasks/QA-03.md) đã Done theo scope
  component/acceptance riêng, không phải bằng chứng full research catalog v2.
- [DAT-03 — Catalog nghiên cứu thật](../task/tasks/DAT-03.md) còn rollout 225+63
  tháng, source biên UTC/JST, resource profile, Gold/Trino/ML export và ba máy handoff.
- [SEC-01 — Security](../task/tasks/SEC-01.md) Done local; mở lại cloud acceptance
  trước Colab/Drive. Không suy security approval từ reader/resolver profile.

Nguồn chính thức:
[USGS query/limit/time semantics](https://earthquake.usgs.gov/fdsnws/event/1/),
[JMA hypocenter archives](https://www.data.jma.go.jp/eqev/data/bulletin/hypo.html).
