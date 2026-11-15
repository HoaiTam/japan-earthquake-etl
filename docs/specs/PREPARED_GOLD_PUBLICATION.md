# DAT-03 — PREPARED Gold publication v1

## Phạm vi

Đây là phần2 của [DAT-03](../task/tasks/DAT-03.md): dùng
[prepared catalog v1](DAT03_PREPARED_CATALOG.md) đã nghiệm thu để ghi **sáu bảng
Gold mới**, verify/publish bằng Trino và kiểm rerun. Không restore Iceberg graph
máy worker, replay ETL hoặc cấp official ML export chỉ bằng metadata.

Contract `dat03-prepared-gold-v1` chỉ cho phép namespace đã được owner duyệt:
`iceberg.dat03_unified_20261114`. Namespace/source cũ không bị thay đổi. Initial
namespace phải rỗng; baseline sáu bảng bằng0. Một operation mới không được ghi
đè namespace đã có dữ liệu từ operation khác.

## Input và proof có version

Request immutable pin runner SHA, operation/run/dag IDs, processing UTC timestamp,
prepared root + externally trusted catalog SHA và expected counts đủ sáu bảng.
`PreparedGoldProof` giữ:

- Prepared catalog/request SHA.
- Coverage/provenance receipt SHA và source cutoff gốc.
- Input contract `dat03-prepared-catalog-v1`, version và namespace được duyệt.

Source request/coverage/payload/footer/checkpoint chain và physical Bronze mapping
được verify lại trước commit và publication. Không dùng caller booleans để cấp
readiness. Source cutoff không đổi theo ngày thực hiện Gold commit.

Gold scope có `prepared_input`, không có `silver_bundle_sha256`. Context bao phủ
đúng288 tháng `[2000-01-01,2024-01-01)` UTC. Không tạo SilverReady/FullMonthReady,
registry hoặc legacy288 checkpoints giả. Field `gold_run_id` và
`record_updated_at_utc` trong input Gold được chuẩn hóa sang operation mới, theo
timestamp request đã pin; observation/canonical IDs, source values và lineage
không bị tái tạo.

Legacy constructor/scope serialization và writer/publisher mặc định giữ nguyên.
Publisher legacy từ chối prepared proof; prepared publisher cần explicit opt-in,
đúng namespace, zero baselines, full288 months và measured row bound.

## Commit và atomicity

GoldIcebergWriter dùng lại quality/count/uniqueness, deterministic full-row
fingerprints, per-table operation/identity/baseline/count summaries và snapshot
readback. Prepared branch dùng physical bridge month đã có, không tạo lại phép
join lớn chỉ để thêm cùng field đó. Trino vẫn kiểm bridge month với event time.

- `event_current`: Iceberg partition `months(event_time_utc)`.
- `event_source_bridge`: partition `event_month_utc`.
- Date/region/magnitude/depth dimensions: giữ key và label semantics.
- Prepared input chỉ phủ liên tục full288 months nên event overwrite predicate
  là đúng range UTC tương đương, không phải wildcard hoặc unscoped overwrite.
- Cache prepared frames `DISK_ONLY`, local[1], shuffle16; legacy cache không đổi.

Mỗi bảng atomic riêng; **sáu bảng không atomic cùng nhau**. Commit vẫn chỉ là
`COMMITTED`, `published=false`. Request anchor dưới
`silver/dat03-prepared-gold/<run_id>/request.json` và legacy-compatible Gold
commit/identity journals dưới `silver/gold-commits/<run_id>/` giữ exact bytes.
Retry writer chỉ reuse own operation/identity, không overwrite foreign baseline.

### Continuation hẹp sau lỗi dimension

Một phiên thật đã ghi hai fact rồi lỗi `EXCEPT` ở `dim_date`: Spark USING join
đưa key INT lên đầu, trong khi input PREPARED bắt đầu bằng `calendar_date` DATE.
Writer căn lại projection theo **tên cột của incoming** trước phép EXCEPT theo vị
trí; không đổi thứ tự/schema của incoming, fingerprint, identity hoặc source rows.
Regression fixture chứa DATE trước INT, injected partial commit và kiểm fact pin
không đổi; cùng phép kiểm áp dụng an toàn cho legacy writer.

`dat03-dimension-column-alignment-v1` là explicit continuation, không auto retry:

- Original request/runner/controller/failed execution giữ nguyên; không ghi đè
  code pin để giả rằng bản vá là runner ban đầu.
- Manifest riêng pin original/new runner SHA, failed session/execution SHA,
  original identity raw SHA, sáu partial snapshot IDs dạng chuỗi int64, scripts.
- So bytecode toàn bộ class ETL: chỉ writer và entry adapter được đổi; chỉ thêm
  validator continuation. Đổi class transformation/source khác bị từ chối.
- Trước ghi: giữ đúng own lease, services không đổi, Java/DagRun/Trino idle,
  anchor/identity/source pins và partial baselines phải khớp. Không acquire foreign
  lease, rewrite fact đã commit, reset namespace hoặc skip verification.
- Sau đủ Published/readbacks/capture mới release own lease. Resolution immutable
  liên kết failed execution với accepted recovery; **phiên lỗi vẫn là lỗi**.
  Rerun không được bỏ qua session lỗi chưa có resolution đã xác minh.

Same-request rerun có thể dùng original pinned runner: đã có complete commit
receipt nên chỉ đi nhánh readback/reuse, không đi nhánh ghi dimension đã sửa.
Actual recovery runner SHA được ghi riêng trong result của hai phase.

## Trino verification/publication

Spark commit kết thúc trước phase Trino để giảm tranh chấp bộ nhớ. Source whole-run
lease vẫn giữ xuyên hai phase. `GoldPublisher.preparedCatalog` dùng các blockers
hiện có, không bỏ check:

1. Sáu schema/count/operation-identity snapshots.
2. Unique keys, required fields/quality/coordinates/era, bridge lineage,
   source counts và primary provenance, UTC/JST/date keys/dimensions/aggregate.
3. Outside scope và changed-run scope. Baseline0 có outside state rỗng;
   các `SELECT 0` là readbacks có ý nghĩa đó, không fake full-row comparison.
4. Fact blockers dùng đủ16 hash buckets, mọi row thuộc đúng một bucket. Bridge
   uniqueness bucket theo observation ID; fact joins theo canonical ID ở cả hai
   phía. Không sampling hoặc nhận hash equality thay full relational checks.

Bound prepared facts tối đa4.527.126 rows lấy từ capacity Trino đã đo trước,
không dùng con số6.012.210 của fingerprint Spark để nới cap Trino. Không dùng
single-pass primary profile vốn chỉ nghiệm thu ở1,3triệu rows. SELECT polling
300s/query là profile research đã có; DDL/DML giữ120s và không resubmit khi lỗi.

Verify có139 SQL calls theo planner v1. Publish lần đầu tái dùng proof verify
**cùng process**, tối đa300s, vẫn đọc freshness/PASSED/view counts thật; không
dùng cache giữa process. Sau publication có thêm12 readbacks current refs/
snapshot-history counts của sáu bảng và một readback new Gold run provenance.
Planner/progress thông báo `query=A/total`; actual counters ghi trong result.

Published chỉ sau PASSED status row, pinned serving views, natural/ROI view,
stable alias và S3 publication journal readback. Views/status chỉ thuộc namespace
mới. Không có consumer chính thức nào được phép coi sáu current tables đang ghi
dở là Published bundle.

## Rerun

Same request/runner/input context; rerun cần externally accepted commit/publication
pins và snapshot bundle lấy từ session đầu. Writer đọc lại snapshots/fingerprints,
không tạo snapshot mới. Rerun journal adapter chặn put/delete/move; publisher SQL
adapter chặn mọi câu không phải SELECT. Nếu receipt biến mất hoặc pin khác, dừng
trước khi tự tạo lại metadata publication.

Acceptance yêu cầu commit/publication raw SHA và sáu snapshot IDs không đổi,
history mỗi bảng vẫn đúng1snapshot của operation mới; readbacks thật vẫn chạy.
Chưa có receipt không được nhận `reused=true` chỉ do caller yêu cầu rerun.

## Control plane và giới hạn

Controller pin files/JARs/source guard code, kiểm healthy/idle, host physical disk,
preflight namespace rỗng rồi acquire **own** shared source lease bằng API ORC-05.
Mỗi session600..6000s, tối đa2giờ cả overhead; không chạy song song. Guard4GiB
physical free dùng host, không lấy logical VM free. Preflight disk estimate không
thay actual full-write/runtime acceptance; stop/unknown luôn giữ lease/evidence.

Không auto retry/recovery/skip/clear hoặc release foreign lease. Optional progress
timeout không được dừng authoritative worker. Host intent lưu service IDs/start/
restart counts trước launch; capture chỉ nhận exact terminal success và immutable
result/execution SHA, services unchanged/idle rồi mới release own source lease.

**DAT-03 phần3 chưa hoàn tất:** official collection adapter qua MLD-05/MLI-02,
REPRODUCTION/EXTENSION EXPORTED, loader/rerun và immutable team release. Gold
Published không đồng nghĩa dataset EXPORTED hoặc scientific approval; receipt
ba máy vẫn cần người nhận chạy verifier thật.
