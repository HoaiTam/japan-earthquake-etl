# QA-01 — Thiết kế daily current-month / as-of-cutoff

## 1. Trạng thái và mục tiêu

Ngày 22/10/2026, HoaiTam chọn thiết kế thêm current-month để phục vụ **daily
2026**, thay vì dùng scheduled run lịch sử làm nghiệm thu thay thế. Tài liệu
này mở rộng phần thiết kế của [QA-01 — Daily E2E](../task/tasks/QA-01.md).

**Cập nhật bước 3/3 ngày 22/10:** đã triển khai [Source capture và
Silver](../runbooks/CURRENT_MONTH_SOURCE_SILVER.md) với protocol
`current-month-source-silver-v1` và [Gold/publication operational](../runbooks/CURRENT_MONTH_GOLD_PUBLICATION.md)
với protocol `current-month-gold-v1`. Gold có Iceberg local/SQL fixture tests.
Ngày 23/10 [daily runtime/collector v2](../runbooks/CURRENT_MONTH_DAILY.md) đã
[nghiệm thu scheduled recovery PASSED](../evidence/QA-01_CURRENT_MONTH_ACCEPTANCE.md),
8/8 tasks/six snapshots/42 SQL trên explicit readonly prebuilt deployment.
Run 22/10 giữ failed; run 23/10 được owner cho phục hồi sau host sleep. Cron vẫn
07:15, không manual/fake state; default Make image preflight chưa đạt. Policy planner vẫn
`DESIGN_ONLY`; không phải nút bật runtime.
ORC-06/07 v1 và collector `qa-01-v1` vẫn giữ nguyên gate `COMPLETE_MONTHS`.
Không đổi một flag trong `.env` để kích hoạt thiết kế này.

Hai sản phẩm phải được phân biệt:

| Sản phẩm | Nguồn và coverage | Được dùng để làm gì? |
|---|---|---|
| Historical/research v1 | Exact JMA/USGS releases theo [SOURCE_COVERAGE](./SOURCE_COVERAGE.md), complete closed-month và halo đã kiểm | Gold lịch sử; ML còn phải qua MLD-01..05 và lifecycle export |
| Operational v2 | USGS đến event cutoff; JMA chỉ readiness baseline lịch sử; `OBSERVED_AS_OF`, `PROVISIONAL` | Theo dõi dữ liệu cập nhật hằng ngày; **không** tự đủ điều kiện research/official ML |

`Published` trong thiết kế operational chỉ có nghĩa snapshot đã commit và SQL
verify đúng **phạm vi provisional đã khai báo**, không có nghĩa catalog đầy đủ
hoặc được ML sử dụng. Tách namespace, publication journal và serving aliases
operational khỏi historical trước khi cho phép publish; không chỉ thêm một
boolean mà vẫn để consumer cũ đọc cùng alias.

## 2. Hai loại cutoff khác nhau

- `event_cutoff_utc`: đầu ngày UTC của `data_interval_end` từ scheduler. Chỉ
  chọn sự kiện có origin time trước mốc này; không lấy clock hiện tại để đổi
  cửa sổ trong retry.
- `observation_cutoff_utc`: thời điểm hoàn thành thu thập của capture đã chọn,
  kèm `capture_started_at_utc`, `capture_completed_at_utc` trên từng manifest.
  Trước thu thập phải là `null`, không gán bằng event cutoff.
- Với nhiều captures, run lưu cả từng khoảng thu thập và mốc hoàn thành lớn
  nhất. Đây là **retrieval-as-of**, không phải bằng chứng toàn catalog tại một
  thời điểm nguyên tử. USGS API không cung cấp truy vấn lịch sử “catalog được
  biết chính xác tại 00:00 hôm đó”; pagination cũng không tạo snapshot nguyên tử.
- `processed_at_utc` là mốc xử lý đã pin, không thay hai cutoff trên.

Ví dụ **metadata giả lập**, không phải một Airflow run đã chạy:

| Trường | Giá trị |
|---|---|
| Airflow interval | `[2026-10-21T00:15:00Z, 2026-10-22T00:15:00Z)` |
| Ngày đích | `[2026-10-21T00:00:00Z, 2026-10-22T00:00:00Z)` |
| USGS revision window danh nghĩa | `[2026-10-19T00:00:00Z, 2026-10-22T00:00:00Z)` |
| Capture bắt buộc cho tháng bị ảnh hưởng | `[2026-10-01T00:00:00Z, 2026-10-22T00:00:00Z)` |
| Observation cutoff | Chưa biết trước khi capture thực sự hoàn tất |

Scheduler vẫn là `orc_02_daily_sources`, cron/timezone hiện hành, interval đóng
24h và không sớm hơn 00:15 UTC. Không tạo DAG daily thứ hai. Planner kiểm tra
metadata đầu vào, **không** chứng minh metadata đó đến từ Airflow; collector
runtime phải đọc DagRun/TaskInstance thật, không tin JSON người gọi truyền vào.

## 3. Source policy: không giả định JMA có daily 2026

Policy `USGS_OPERATIONAL_WITH_JMA_HISTORICAL_BASELINE`:

- `required_event_sources=["USGS"]` cho operational 2026. Giữ nguyên ROI và
  quy tắc hợp lệ của [SOURCE_COVERAGE](./SOURCE_COVERAGE.md); không mở rộng ROI
  hoặc bỏ filter để tăng count.
- JMA readiness chỉ kiểm release baseline đã pin. `JMA historical readiness`
  thành công **không** chứng minh có observation JMA năm 2026. Report phải tách
  readiness, event coverage và source freshness; không gọi đây là catalog
  hai nguồn đầy đủ năm 2026 hay gán JMA missing thành zero events đã chứng nhận.
- Inventory project cố định `1984–2023`, không phải giới hạn mới nhất của nguồn.
  Trang JMA kiểm tra 22/10/2026 đã có link năm 2024; không tự thêm vào inventory,
  không suy từ đó có daily 2026. Mở rộng JMA cần release pin, JST/UTC scope và
  acceptance riêng; **chưa có task triển khai riêng** cho JMA sau baseline.
- Giữ nguyên historical/research gates: reproduction 225 tháng và extension
  63 tháng; operational provisional không bù coverage còn thiếu của các tập này.

## 4. Bootstrap và revision không làm mất dữ liệu tháng

Phiên bản đầu chọn cách dễ kiểm chứng: **refetch các tháng giao với overlap ba
ngày**, từ đầu tháng đến `min(month_end, event_cutoff)`, clamp seed USGS
`2023-01-01T00:00:00Z`. Thông thường một tháng, tối đa hai tháng ở đầu tháng mới.
Đây là thiết kế đánh đổi thêm request/lưu raw để có proof rõ ràng; chưa đo
throughput/chi phí, không tuyên bố nhanh tương đương ingest ba ngày hiện hành.

- Ngày 01/11: chỉ refetch tháng 10; không tạo tháng 11 rỗng giả.
- Ngày 02/11: refetch tháng 10 đầy đủ và tháng 11 đến cutoff ngày 02/11.
- Không dùng delta ba ngày làm input replacement toàn partition tháng.
- Chạy lần đầu giữa tháng vẫn cần bootstrap từ đầu tháng. Không có baseline
  lịch sử không được giả định các tháng cũ đã nạp hoặc complete.
- Tháng vừa đóng vẫn `OBSERVED_AS_OF/PROVISIONAL`; đóng theo lịch không tự
  chứng nhận source completeness, link closure hoặc research readiness.

Capture phải có exact request/filter, HTTP/result metadata, raw/manifest SHA,
source totals, bounds thực tế và trạng thái hoàn tất. USGS giới hạn tối đa
20.000 kết quả mỗi request: chia cửa sổ có giới hạn, kiểm tra truncation và
không âm thầm lấy trang đầu. Giữ nguyên response Bronze; Java lọc về `[start,end)`
vì USGS `endtime` bao gồm mốc cuối. Count raw, ngoài cửa sổ, reject, revision và
output phải đối soát được; duplicate biên chunk không được làm tăng canonical.

Nếu đổi catalog giữa các request làm không thể chứng minh capture hoàn tất,
dừng và lưu evidence; không gọi pagination là point-in-time completeness.
Giới hạn ban đầu là hai tháng affected và 100.000 rows trong resolver; vượt
budget phải báo lỗi có scope, không truncate hay âm thầm bỏ những tháng liên quan.
Planner chỉ tính scope danh nghĩa; chưa đọc row count hoặc chứng minh giới hạn
data-plane. Scope closure thực tế phải do resolver kiểm lại.

## 5. Revision xa, event di chuyển, deletion và halo

Overlap ba ngày dựa trên **origin time**, không bắt mọi revision của sự kiện
cũ hơn. Refetch tháng hiện tại bắt thêm revision trong tháng đó, nhưng không
bảo đảm phát hiện revision trên toàn bộ lịch sử. Phải report rõ giới hạn này.
`updatedafter` có thể hỗ trợ dò revision, không tự thay coverage proof hoặc
biến API thành point-in-time snapshot.

- Không coi sự vắng mặt trong response mới là tombstone. Dữ liệu có thể bị
  đổi thời gian, tọa độ, ROI, merge ID hoặc tạm thiếu ở nguồn.
- Trước replacement phải đối chiếu baseline IDs với capture; missing ID chưa
  giải thích được phải chặn commit. Muốn xử lý tự động cần exact ID readback/
  explicit source deletion evidence, old/new lineage và policy version hóa;
  HTTP lỗi/404 riêng lẻ không phải bằng chứng đủ để xóa.
- Event chuyển tháng hoặc source link thay đổi phải đưa **cả tháng cũ và mới**
  vào closure. Vượt hai tháng/row budget thì dừng, yêu cầu bounded reprocess
  rõ ràng; không giữ một nửa revision hoặc sửa partition ngoài scope.
- Source-link temporal halo giữ baseline SLV-07 (mặc định 16 giây), không bỏ
  guard để publish tháng đang mở. V2 phải pin input halo đủ tới boundary cần
  link. Nếu tương lai vượt event cutoff hoặc JMA current không có, scope thiếu
  đó phải được khai báo; không cấp proof historical cross-source completeness.
- Operational v2 chỉ có USGS current observations không thực hiện hay tuyên bố
  JMA↔USGS current linking. Historical baseline/aliases không bị ghi lại bởi
  USGS-only provisional writer. Khi có JMA release mới cần một rebuild có
  policy và proof riêng, không tự promote output operational đang tồn tại.

Component Source/Silver đã có guard missing ID, stale revision và move; chỉ
cho phép move khi cả tháng cũ/mới nằm trong capture. Chưa có automatic ID
readback/deletion handling hoặc replan ngoài scope. Không dùng planner metadata
làm bằng chứng các phần còn thiếu đã được xử lý.

## 6. Protocol, identity và publication gate đề xuất

Version riêng cho context/resolver/receipts/coverage, không thêm field ngầm vào
`orc-06-v1`, `orc-07-v1` hoặc `qa-01-v1` đang dùng allowlist. V2 phải truyền:

1. Exact Airflow owner/interval; policy SHA và event cutoff cố định.
2. Capture selection immutable: từng manifest/raw URI+SHA, event bounds,
   retrieval interval, source counts và complete capture marker.
3. Source policy, `OBSERVED_AS_OF`, `PROVISIONAL`, explicit required sources
   và JMA historical-only; không nhận cờ completeness từ caller.
4. Exact baseline registry và six Gold snapshots của namespace operational;
   actual affected/closure scope, run/context/receipt SHA chain.
5. Actual reconciliation, snapshot IDs và SQL proof trên chính snapshots đó.

Retry trước khi chọn capture có thể tiếp tục/thu lại phần chưa hoàn tất theo
capture identity riêng; partial capture không đủ điều kiện chọn. Sau khi đã
pin selection, cùng run chỉ reuse exact bytes/baseline/cutoffs. Fresh capture
là run/operation mới, không đổi input của operation đã pin. Giữ shared source
lease và journal/resume; không tự takeover/release lease khác owner.

Các phase vẫn readiness → bronze → silver → gold → verify → publish, nhưng
Silver v2 chỉ có thể trả `ObservedAsOfReady`, không `FullMonthReady`. Publish
v2 chỉ sau blocker SQL readback, ghi cả coverage/quality vào journal, status
table và versioned serving views. Alias lịch sử/research v1 giữ nguyên.
Official ML consumers phải fail-closed trước provisional policy, kể cả khi
`publication_status=PASSED`. MLD-01/MLD-05/MLI-02 không được coi một boolean
`ml_upstream_eligible=true` của caller hoặc một alias chưa pin là authorization.

## 7. Thiết kế và component đã triển khai

- `config/orchestration/current_month_asof_v1.json`: policy sealed, flags quyền
  publish/research/ML đều false; `release_status=DESIGN_ONLY`.
- `airflow/dags/current_month_contract.py`: module metadata, không có DAG,
  ETL/network/storage/publication writes. Tính capture scopes và canonical
  SHA-256; không đọc credentials, không được import/wire vào daily runtime.
- `airflow/tests/test_qa_01_current_month_contract.py`: tests deterministic,
  rollover/năm nhuận/seed, scope, invalid metadata và chống nâng policy.
- `make test-qa-01` tự discover planner và host CLI guards.
- `CurrentMonthCapture` / `CurrentMonthSilverResolver` / `CurrentMonthSourceSilverJob`:
  component Java/Spark capture nguyên bản, observed coverage, revision/history
  và Silver Parquet/registry/receipt tách biệt; entrypoint Source/Silver không Gold hoặc cron mutation.
- `CurrentMonthGold` / `CurrentMonthGoldJob`: verified Silver → affected-month
  transformation → sáu Iceberg snapshots → Trino → publication provisional;
  reserved namespace, snapshot policy và fail-closed ML consumers.
- `current_month_runtime.py` / `CurrentMonthDailyJob`: runtime độc lập với
  planner, cùng DAG daily; pin actual SDK context và runner SHA, source lease,
  JMA historical readiness, capture → Silver → Gold, chỉ release sau publication.
- `qa_01_current_month.py` / `CurrentMonthAudit`: collector `qa-01-v2` read-only,
  kiểm source/Silver SHA và ít nhất 42 SQL readbacks cùng publication; không
  trigger/clear/mark-success. Xem [daily runbook](../runbooks/CURRENT_MONTH_DAILY.md).
- `make test-current-month`, `make prepare-current-month`, `make current-month-local`:
  tests, explicit host preparation và opt-in source thật → file stores local.
  Xem [runbook component](../runbooks/CURRENT_MONTH_SOURCE_SILVER.md).
- `make test-current-month-gold`, `make current-month-gold`: regression và
  explicit MinIO/Iceberg/Trino entrypoint có held lease; xem [runbook Gold](../runbooks/CURRENT_MONTH_GOLD_PUBLICATION.md).
  `make smoke-qa-01 QA_01_RUN_ID=...` tự chọn v2 khi có current-month context của
  exact run; không chạy pipeline. Scheduled recovery 23/10 đã PASSED theo evidence
  riêng; Make preflight cached-image không được ghi PASS thay explicit deploy proof.

`DESIGN_PLAN_ONLY`, `deployable=false`, `NOT_CAPTURED` và
`observed_coverage_proven=false` là kết quả mong đợi, không phải lỗi deployment.
Planner không start Docker, trigger Airflow hay upload MinIO.

## 8. Nghiệm thu operational và ranh giới còn lại

**QA-01 — Daily E2E** Done cho operational v2 sau scheduled recovery 23/10 do
owner duyệt. [Evidence](../evidence/QA-01_CURRENT_MONTH_ACCEPTANCE.md) pin actual
run, counts, source/Silver SHA, six snapshots và 42 SQL. Implementation mở rộng
không tự đổi effort 5h/budget tuần; component tests không thay proof runtime.
Các giới hạn ngoài acceptance đã chạy giữ rõ dưới đây:

| Boundary/task hiện hữu | Phần cần làm cho v2; không nằm trong Done của v1 |
|---|---|
| USG-01..06 / ORC-02 | Chunk/manifest/observed proof và scheduled capture/JMA historical readiness đạt trên run recovery 23/10; không tuyên bố JMA 2026 coverage |
| ORC-06 — Full-month resolver | Observed resolver riêng đã có bounded revision/history/closure guards; chưa automatic deletion/replan; không hạ v1 COMPLETE_MONTHS |
| ORC-07 — Real adapters / GLD-03..04 | Gold/publication và Airflow wiring đã có reserved namespace, commit/SQL quality và aliases; MinIO/Trino same-run acceptance 23/10 đạt |
| MLD-01 / MLD-05 / MLI-02 | Đã có reserved namespace/persisted snapshot guard ở manifest/pinned consumers; còn live negative acceptance trong v2 chain; không đổi trạng thái Done của v1 |
| QA-01 — Daily E2E | Scheduled recovery 23/10 và collector v2 PASSED; explicit prebuilt deployment, không first-attempt/on-time SLA/default image freshness. Output vẫn ML-ineligible |
| QA-02 / QA-04 | Regression bổ sung retry/rollover/moved ID/deletion và fail-before-publish cho policy mới; evidence v1 không chứng minh v2 |

Live acceptance sau implementation phải có scheduler tạo run tại deployment
thật; đủ tám task, same-run chain, exact captures, reconciliation, sáu snapshots,
publication quality và KPI readback. Không dùng manual/DAG.test, sửa DB/state,
run lịch sử hoặc namespace khác thay thế. Cần test thiếu bootstrap, missing ID,
request quá giới hạn, đổi cutoff/pins khi retry, thiếu halo và SQL blocker đều
dừng đúng tầng, không publish. Current-month guard cases dùng offline regression,
capture failed→downstream blocked/lease retained có runtime evidence; không
nhận mọi fault case đã inject live hoặc mọi ngày tương lai đều được chứng nhận.

## Nguồn tham khảo

- [USGS FDSN Event API](https://earthquake.usgs.gov/fdsnws/event/1/): UTC,
  `starttime/endtime`, `updatedafter`, query limits. Policy project suy ra từ
  các semantics này; API không cam kết completeness của dataset nghiên cứu.
- [JMA Hypocenter Data](https://www.data.jma.go.jp/eqev/data/bulletin/hypo.html):
  annual releases và update notes; kiểm tra 22/10/2026, không tự đổi baseline.
- [REAL_ETL_ADAPTER_CONTRACT](./REAL_ETL_ADAPTER_CONTRACT.md),
  [SOURCE_COVERAGE](./SOURCE_COVERAGE.md), [QA-01 runbook](../runbooks/QA-01_DAILY_E2E.md).
