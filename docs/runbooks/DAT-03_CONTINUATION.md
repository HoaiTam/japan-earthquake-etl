# DAT-03 — Khối 2: tiếp tục bounded E2E trên retained run

Sửa ngay [PR #100](https://github.com/HoaiTam/japan-earthquake-etl/pull/100),
branch fix/dat-03-trino-recovery theo yêu cầu owner; không mở branch/PR khác.
[Khối 1](./DAT-03_RECOVERY.md) chỉ đọc. Khối 2 cho phép verify/publish operation
2016-04 đã commit, chạy case2023-12 và release **own lease** sau toàn bộ gates.
Không rebuild/pull/start/init/reset service, sửa DAG/cron/DB state hoặc dữ liệu cũ.
Ngoại lệ owner duyệt ngày25/10: lưu toàn bộ retained Trino logs/config rồi
recreate **riêng Trino**, cached image đúng digest, không dependency, để áp dụng
[ngân sách bộ nhớ](../../trino/README.md). Không biến ngoại lệ này thành hành vi
tự động trong controller hoặc test.

## Cơ chế và provenance

- Controller Python chỉ pin/copy bytes/metadata, kiểm idle/service/lease, gọi
  Java và verify receipts. Business ETL vẫn dùng Spark Java/adapter cũ.
- JAR recovery có pin riêng, original JAR/request/execution/progress được giữ
  nguyên. So sánh **mọi entry gốc** trong JAR (trừ manifest đóng gói): không
  thiếu entry, không đổi bytecode/resource ETL; chỉ cho phép TrinoSqlClient.class
  đúng bản vá của khối 1. Java kiểm lại proof độc lập trước mọi mutation.
  Vì code ETL không đổi, giữ nguyên code_version trong context cũ; report ghi
  riêng recovery runner/compatibility SHA, không giả binary recovery là JAR gốc.
- Trước ghi: assert own lease, không Java/DagRun active, fresh13-query preflight,
  exact committed pins; verify lại sáu Bronze manifests/raw đã có từ run cũ.
  Keys được resolve từ original run/processing date/release, không list/latest,
  không gọi downloader hoặc Bronze writer. Thiếu/mismatch thì dừng trước publish.
- 2016-04: tái dùng readiness/bronze/silver/gold receipts, **chỉ execute verify
  và publish**. Rerun hai phase này với cùng context; record/receipt/publication/
  sáu snapshot phải giống nhau. Không reparse hay recommit tháng đã hoàn tất.
- 2023-12: operation phải chưa có context/reservation/Gold identity. Dùng exact
  h2023+h2024+USGS pins/cutoff gốc; parent là publication2016-04. Sáu phase adapter
  và rerun cùng operation; affected scope chỉ2023-12, không đưa2024 vào ML split.
- Đối chiếu fingerprint event_current của các tháng trước với snapshot2011-04
  và các snapshot mới; publisher còn kiểm outside scope của cả event/bridge.
  Reconciliation balanced, COMPLETE_MONTHS, six snapshots và Trino blockers đều
  phải đạt. Không coi bool tự khai hoặc reuse report cũ là SQL fresh-check mới.

## Acceptance và lease

Mỗi attempt có thư mục UUID mới dưới staging/dat03-continuation, không đổi run
hoặc namespace. Chuyển đúng bảy file code/config vào volume bằng user Airflow;
copy39 exact retained files vào attempt mới bằng CREATE_NEW và SHA. Không copy
raw private logs, không overwrite original stdout/stderr hoặc failed execution.

1. Java xong đủ4cases, ghi report.json mới và dừng Spark.
2. Worker verify capture/input/code/compatibility/receipt/publication/SQL pins,
   original failed evidence và idle; ghi validation.json = VALIDATED_LEASE_HELD.
3. Host kiểm bốn service vẫn healthy, StartedAt/RestartCount/OOMKilled không đổi
   từ trước chạy; nếu khác thì **giữ lease**, không finalize.
4. Finalizer kiểm lại whole report/source/measurement/own lease/idle và exact
   report SHA rồi mới release. acceptance-recovery.json pin original failure,
   recovery code/config/execution/report và release proof. Không sửa acceptance
   gốc hoặc thay execution.json exit1 thành exit0.

Timeout/nonzero/unknown SQL outcome/conflict: giữ artifacts/lease, không auto
retry hay finalize. Sau lỗi cần đọc exact checkpoint và xin hướng xử lý nếu
phải mở rộng recovery; không tạo attempt khác rồi chạy mù. Bản continuation v1
nhận hai checkpoint tường minh, không tự dò latest hoặc nhận publication một phần.

### Checkpoint verify/publish2023-12 sau OOM

Attempt9ff8e2bee9454ad69f63896dd8ebcc4c đã hoàn tất publication/rerun2016-04,
commit2023-12 rồi fail ở verify. Docker ghi OOM-kill exit137 lúc02:00:35Z ngày
25/10/2026; không suy diễn nguyên nhân này cho failed run gốc24/10.

`RESEARCH_CHECKPOINT_ROOT` chọn **exact failed attempt/runtime**, không chọn
thư mục latest. Prepare pin18 metadata files: request/compatibility/failed
execution/live preflight/source pins, progress2016 và context/artifacts2016/2023.
Worker kiểm SHA/code/failure/idle/own lease rồi copy vào checkpoint/ của UUID mới;
file trùng với39 original files phải byte-identical, không overwrite evidence.
Java kiểm SHA độc lập, ba published receipt chains, parent registry/commit,
source/cutoff/scope và13 fresh metadataSELECT. Verify/publish2023 hoặc publication
row/journal đã tồn tại một phần thì fail closed, không tự sửa trạng thái.

Chỉ execute verify/publish2023-12 lần đầu và rerun; readiness/bronze/silver/gold
được đọc lại theo SHA, không execute. Sáu current pins phải bằng exact commit
2023-12; ba tháng cũ fingerprint phải giữ nguyên. Report mới chứa đủ4cases và
recovery provenance; original failure và failed continuation vẫn giữ nguyên.

## Các lệnh

Chạy từ checkout PR #100, có JAR package đúng code. Không tự build trong test:

```bash
make test-research-continuation test-research-recovery test-research-e2e
make prepare-research-continuation RESEARCH_DEPLOYMENT_ROOT=/Users/hoaitam/Desktop/Learn/23521394/HK1-Nam-4/IE212/japan-earthquake-etl RESEARCH_RETAINED_ROOT=/Users/hoaitam/Desktop/Learn/23521394/HK1-Nam-4/IE212/japan-earthquake-etl/staging/dat03-e2e/8ca2be1939db4111ae783f6566650fce/runtime RESEARCH_PREFLIGHT_ROOT=/Users/hoaitam/Desktop/Learn/23521394/HK1-Nam-4/IE212/japan-earthquake-etl/staging/dat03-recovery/1270d7a7dc734b4ca7a10618ae83a8e2
```

Để phục hồi checkpoint đã duyệt, thêm vào lệnh prepare trên (không rerun attempt
cũ): `RESEARCH_CHECKPOINT_ROOT=/Users/hoaitam/Desktop/Learn/23521394/HK1-Nam-4/IE212/japan-earthquake-etl/staging/dat03-continuation/9ff8e2bee9454ad69f63896dd8ebcc4c/runtime`.

Prepare in đường dẫn attempt mới; gán đúng giá trị đó cho biến shell
`dat03_continuation_folder`. Chỉ chạy sau khi quyết định phục hồi được phê duyệt:

```bash
make run-research-continuation RESEARCH_DEPLOYMENT_ROOT=/Users/hoaitam/Desktop/Learn/23521394/HK1-Nam-4/IE212/japan-earthquake-etl RESEARCH_CONTINUATION_FOLDER="$dat03_continuation_folder"
make verify-research-continuation RESEARCH_CONTINUATION_FOLDER="$dat03_continuation_folder/runtime"
```

Runtime local[1]/driver768MiB/direct256MiB và existing Iceberg1.10.1 JARs như
run gốc; timeout3600s là bound, không ETA. Không dùng `verify-research-e2e`
cho recovery vì execution.json của run gốc cố ý vẫn exit1. Verifier mới là
offline metadata/byte verification; không tự truy vấn Trino hoặc ghi lại lake.

Thành công: event dat03_continuation_month_passed cho2016-04/2023-12,
dat03_continuation_passed, dat03_continuation_verified; bốn cases đầy đủ,
rerun_identical/retention_verified, final six pins và RELEASED own lease.
Các flags research_ready/full_research_catalog vẫn false.

Lần nghiệm thu25/10 đã đạt: xem [evidence khối2](../evidence/DAT-03_CONTINUATION.md).
Run này đã release own lease; không chạy lại lệnh recovery trên completed state.

## Giới hạn / phần còn lại

**DAT-03 — Catalog thật và team handoff, khối 3/4/5:** bốn tháng bounded không
thay source proof225+63 tháng, capacity/rollout full catalog, official
collection-aware MLI-02 lifecycle/export hoặc receipts ba máy. Không mở
EXP-02..05 scientific gate. **SEC-01 — Security review:** cloud/Colab/Drive
vẫn chưa nghiệm thu; không upload hay đổi ACL trong task này.
