# ORC-06 — Full-month Silver state, version orc-06-v1

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
| resolver_version | `orc-06-v1`, reject version/field lạ |
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
5. Nếu closure vượt vùng JMA đã parse, reject `MONTH_EXPANSION_REQUIRES_EXPLICIT_REPLAN`:
   thêm các tháng cần thiết và coverage vào request mới, không chứng nhận
   snapshot nửa vời. Nếu đủ vùng đã parse nhưng coverage thiếu vẫn fail.
6. Chỉ output observations/history, links và memberships của affected months.
   Các tháng khác không overwrite. Registry giữ đủ input pins và tính lại
   coverage cho toàn bộ complete months đã tích lũy, không làm mất tháng cũ.

Resource guards: tối đa 24 registered/parsed months, 64 active pins, 32 MiB
mỗi raw, 128 MiB active raw và 128 MiB parent raw, 100.000 unique selected
revisions; JMA native record diff tối đa 1.000.000 dòng/archive. Vượt guard
thì reject, không truncate. Đây là bounded resolver, không benchmark 40 năm.

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

- [ORC-07 — Kết nối real ETL adapters đến Gold Published](../task/tasks/ORC-07.md):
  còn wiring real daily/manual adapters, giữ lease xuyên phase và kiểm tra
  fresh proof trước sáu Gold commits/Trino publication. Task này không tạo
  DAG mới hoặc tự kích hoạt real ETL.
- [QA-01 — Chạy E2E daily đa nguồn](../task/tasks/QA-01.md): còn nghiệm thu
  toàn chuỗi trên MinIO/Airflow/Gold/Trino. File-backed public capture không thay gate đó.
- [QA-03 — Kiểm thử JMA historical backfill](../task/tasks/QA-03.md): còn
  coverage/history rollout rộng; một tháng không chứng minh 40 năm.
- [SEC-01 — Review secret và bề mặt truy cập](../task/tasks/SEC-01.md): runtime
  và ML handoff gates riêng, không đổi Done từ nghiệm thu metadata ORC-06.

Nguồn chính thức:
[USGS query/limit/time semantics](https://earthquake.usgs.gov/fdsnws/event/1/),
[JMA hypocenter archives](https://www.data.jma.go.jp/eqev/data/bulletin/hypo.html).
