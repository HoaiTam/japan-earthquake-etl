# DAT-03 — Chuẩn bị rollout và đo capacity

Mốc chuẩn bị trước: **chưa đủ điều kiện chạy full catalog**. Đây là control plane,
source capture và phép đo có giới hạn; chưa phải controller backfill 288 tháng
hoặc official collection exporter. [Kết quả thực](../evidence/DAT-03_ROLLOUT_CAPACITY.md).
Owner chọn máy hiện tại, tối đa **2 giờ/phiên**, có checkpoint. Không coi một
probe thành công là chấp thuận toàn bộ Spark/Gold/export capacity.

**Cập nhật25/10:** hai full-scale event/bridge probes43copies đã đủ16/16 mỗi
bảng; catalog controller đã nghiệm thu72/288tháng (25%) đến2005-12, own lease release
sau từng phiên. [Evidence mới](../evidence/DAT-03_CATALOG_ROLLOUT.md),
[contract](../specs/RESEARCH_CATALOG_ROLLOUT.md). Không đồng nghĩa288tháng hoặc
official ML export đã hoàn tất; các phép đo thất bại bên dưới giữ nguyên lịch sử.

## Thành phần làm gì?

| Thành phần | Công việc | Không làm |
|---|---|---|
| `scripts/research-rollout.py plan` | Fresh verify 25 original JMA capture/profile pins; lập 225/63 tháng, archive JST và USGS halo dependency | Không tạo BronzeReady, Gold coverage hoặc chạy ETL |
| `scripts/capture-research-usgs.py` | Capture nguyên response API 12 tháng 2023; metadata/bytes/SHA và gọi Java validator | Không parse/transform event bằng Python, không ghi lake |
| `ResearchUsgsCaptureCheck` | Kiểm envelope/query/ROI/seed/time/SHA/count và guard saturation bằng Java | Không chứng nhận source completeness hoặc canonical count |
| `research-rollout.py probe` | Read-only full-row symmetric difference trên exact bounded snapshots; giữ request/log/receipt và resource budget | Không mutate Iceberg, acquire/release lease hoặc tự retry |
| `scripts/research-trino-profile.py` | Private backup, explicit Trino-only profile trial, read-only late-health reconciliation | Không build/pull/init, đổi credential hoặc chạm service khác |
| `GoldPublisher.researchRollout` / `researchCatalogCheckpointed` | Opt-in outside-scope comparison16bucket, fact blockers và same-process proof cache trong catalog runtime | Không thay default/operational publisher hoặc bỏ quality gate; chưa full288-month acceptance |

Business ETL vẫn Java/Spark. Python chỉ gọi API lấy bytes, kiểm pin/control
plane và evidence. Bucket SHA chỉ định tuyến hàng vào một trong 16 nhóm;
**không** dùng hash equality thay so sánh nội dung. Cả 16 queries phải đạt cho
mỗi bảng; một bucket đạt không có nghĩa toàn bảng đạt.

## 1. Offline và kế hoạch

Chạy từ root checkout, không Docker/mạng:

```bash
make test-research-rollout
make test-research-trino-profile
python3 -m unittest discover -s tests -p 'test_research_usgs_captures.py'
make plan-research-usgs
```

Sau khi có exact original archives/profile metadata ở staging:

```bash
make plan-research-rollout RESEARCH_PROFILE_ROOT=staging/dat03-research-profile-20261024
```

Output phải có `batches=288`, REPRODUCTION=225, EXTENSION=63, PLAN_ONLY và
full_capacity_accepted=false. Có thể thêm RESEARCH_ROLLOUT_PLAN để lưu JSON
**mới** dưới staging; không overwrite evidence. Guard 100k selected rows,
64 pins, 24 affected months và concurrency=1 không bị nới.

Mọi tháng 12 cần archive JMA năm kế tiếp do UTC/JST. Riêng **2022-12** cũng
cần USGS January2023 vì halo phải16s chạm seed CON-01; không chỉ đợi tháng2023.
Đủ hàng trong profile là điều kiện planning, không thay receipt-bound coverage.

## 2. USGS original captures

`capture-research-usgs` là lệnh **explicit network**; không nằm trong unit test
hoặc startup. Cần prebuilt JAR đúng class; không tự build, download dependency
hay thay deployed runner. Mỗi response tối đa16MiB, timeout có giới hạn;
redirect/error/saturated response fail, không silent truncate hoặc retry mù.

```bash
make capture-research-usgs RESEARCH_USGS_ROOT=staging/dat03-usgs-2023
make verify-research-usgs RESEARCH_USGS_ROOT=staging/dat03-usgs-2023
```

Chọn root thật mới hoặc đúng bộ đã pin; RESEARCH_JAVA trỏ Java tương thích nếu
PATH chưa có. Nếu chủ động reuse December đã có, truyền exact root bằng
RESEARCH_USGS_REUSE_DECEMBER; giữ bytes/retrieved_at và origin receipt SHA.
Không copy pin của evidence vào source mới rồi giả cùng release.

Thành công: 12 CAPTURE_STRUCTURE_VERIFIED, summary pin và Java runner SHA.
Counts là features trong từng response có halo, **có thể trùng giữa các tháng**.
FDSN catalog response thật thiếu metadata.count; Java đếm features array làm
authoritative count. Nếu count field có mặt thì phải đúng kiểu và đúng count;
không bỏ count mismatch. Offset1/limit20000, không magnitude/depth filter,
ROI20–50N/120–155E; endtime API inclusive được trừ1ms từ exclusive boundary.
January clamp seed2023-01-01, các biên còn lại có halo16s.

Verify không download; rehash exact bytes/metadata, gọi Java lại từng response
và đối chiếu summary cũ. Capture không tự là BronzeReady/research-ready.
Summary gắn exact runner SHA: giữ JAR content-addressed ngoài target và truyền
RESEARCH_USGS_JAR khi cần verify lại sau một build khác. Không đổi runner pin
trong summary lịch sử. Capture trên exact root cũ chỉ reuse/verify bytes đã có;
nếu chủ động kiểm bằng JAR mới thì tạo summary mới mang SHA mới, không tải lại
hoặc ghi đè metadata nguồn.

## 3. Capacity probe trên services có sẵn

Chỉ chạy khi services healthy, không active DagRun/Java/source lease. Namespace,
snapshot và report/acceptance phải đúng bounded continuation đã nghiệm thu.
Không lấy production/latest hoặc caller-supplied SQL thay input đã pin.

```bash
make probe-research-capacity RESEARCH_DEPLOYMENT_ROOT="$dat03_deployment_root" RESEARCH_CAPACITY_RUNTIME="$dat03_bounded_runtime"
```

Hai biến phải là exact local paths đã kiểm. Mặc định 32 copies, full strategy,
query90s; RESEARCH_CAPACITY_QUERY_SECONDS chỉ nhận30..300, server có cả run/
execution limits, client thêm40s, **query loop** tối đa600s. Preflight và
postflight có các timeout riêng (inspect20s, metadata30s, query-idle25s mỗi
pha);600s không phải wall deadline gồm mọi Docker call. Field v1
total_probe_budget_seconds trong request chỉ giới hạn query loop. Bản buckets-v1
phải hoàn tất cả16 trong cùng budget. Synthetic expansion không là research data.

- STRESS_PROBE_PASSED cần integral changed_rows=0 và postflight unchanged.
  Đây chỉ là probe, full_capacity_accepted vẫn false.
- BLOCKED_TIME/MEMORY, invalid readback, unknown client outcome hoặc service
  drift đều chặn rollout. Không coi CLI exit0 hoặc partial buckets là PASS.
- Request/receipt và private partial stdout/stderr nằm ở staging/dat03-capacity/UUID.
  Unknown outcome: chỉ đọc query state phía server, không submit lại/cancel
  hoặc restart để làm mất bằng chứng. Preflight fail trước khi tạo attempt thì
  không có query/request/receipt mới; không phát minh query_id.

## 4. Profile Trino — ngoại lệ cần owner approval

Default `trino/config.properties`/`jvm.config` và Compose không đổi. Candidate
ở `trino/research/`: task.concurrency=1, JVM tự chọn G1 region và bật GC markers;
giữ heap1536MiB/direct512MiB/query budgets/container3GiB/2CPU/image/security.
Đây là **thử nghiệm chưa đạt capacity**, không mặc định profile tốt hơn.

Prepare chỉ backup/compile in-memory/preflight; không recreate:

```bash
make prepare-research-trino-profile RESEARCH_DEPLOYMENT_ROOT="$dat03_deployment_root"
```

Lưu exact folder in ra. Full env/Compose config không xuất ra log hoặc Git;
backup config/log riêng tư700/600, pin SHA, image/env/resource/security trước
apply. Owner phải duyệt riêng thao tác sau; không chạy trong test:

```bash
make apply-research-trino-profile RESEARCH_TRINO_PROFILE_FOLDER="$dat03_trino_profile_folder" TRINO_PROFILE_OPERATOR_APPROVED=yes
```

Chỉ `up -d --no-deps --no-build --pull never trino` với exact project/Compose
chain đã pin. Plan mới còn pin SHA nội dung từng Compose source; apply từ
plan thiếu pins này bị từ chối. Actual trial trước khi bổ sung guard giữ nguyên
evidence cũ, không retroactively gán content pins chưa tồn tại. Refuse active
query/lease/DagRun/Java, profile/backup drift hoặc
attempt đã có execution. Không rerun apply để xử lý timeout.

Sau health deadline180s mà container sẵn sàng muộn, có thể đối soát read-only
**một lần** cho attempt đó:

```bash
make verify-research-trino-profile RESEARCH_TRINO_PROFILE_FOLDER="$dat03_trino_profile_folder"
```

LATE_HEALTH_VERIFIED_NOT_CAPACITY_ACCEPTED không đổi lần180s ban đầu thành PASS.
Reconcile kiểm exact backup/candidate/Compose mounts/image/env/security, các
service khác unchanged, idle; không gọi compose up lần hai. Giữ receipt cũ,
không overwrite. Muốn rollback hoặc đổi profile tiếp phải kiểm quiescence và
duyệt thao tác riêng, dùng backed-up config/Compose chain; không `down -v`,
rebuild, reset, xóa warehouse hoặc dùng Compose thiếu override.

## 5. Phiên giảm tải bốn container — approval riêng

Owner duyệt25/10: tạm dừng **đúng** Spark master/worker và Airflow API server/
DAG processor để đo, rồi bật lại các container cũ. Không phải quyền đổi cron,
credential, Docker resource settings hoặc dừng scheduler/PostgreSQL/MinIO/REST/
Trino. UI Airflow và Spark không truy cập được trong khoảng stop; không trigger
công việc khác trong phiên này. Không dùng cho session có active run/lease/query
hoặc ứng dụng/driver Spark.

`scripts/research-light-session.py` pin identity/image/config/mounts của9
containers, exact retained runtime/report/acceptance và source quiescence.
Mounts được chuẩn hóa theo Destination: Docker có thể trả cùng set theo thứ tự
khác. Đổi source/RW/content/ID vẫn chặn, không hạ guard chỉ vì order khác.
Không ghi raw Env/command/credential vào evidence.

```bash
make test-research-light-session
make measure-research-light-session RESEARCH_LIGHT_OPERATOR_APPROVED=yes RESEARCH_DEPLOYMENT_ROOT="$dat03_deployment_root" RESEARCH_CAPACITY_RUNTIME="$dat03_bounded_runtime"
```

Lệnh đầu mock-only. Lệnh sau cần **approval thực** của owner, roots đã kiểm;
không nằm trong `make check`, startup hoặc CI runtime. Nó chỉ `docker stop`
đúng4 old IDs, chờ giảm tải ngắn, đo lại full/buckets-v1 trên cùng snapshots/
32copies/query90s, rồi `docker start` lại đúng4 IDs trong finally. Giữ tối đa
2h/session có phần budget dành cho khôi phục; không chạy ETL/catalog/export.
Không dùng compose up, recreate, build/pull/init hoặc thay deployment.

- Before/light/after host samples có10s interval và swap/compression deltas;
  không dùng cumulative counters làm rate. Số đo không chứng minh root cause
  hay đảm bảo máy đủ capacity toàn kỳ.
- Evidence immutable ở deployment/staging/dat03-light-session-UUID: before,
  stop-intent/stopped, host samples, từng probe receipt, restored và receipt
  cuối. Actual query/request/private logs vẫn ở staging/dat03-capacity/UUID.
- Cả16bucket phải đạt; một query timeout không được resubmit. Unknown outcome
  hoặc failed postflight dừng phép đo, khôi phục4container rồi bàn giao.
- Khôi phục dùng đúng ID/image/config/mounts cũ; chờ health tối đa600s và
  Spark worker đăng ký lại. Năm service giữ lại phải same ID/start/config.
  Không start container thay thế hoặc tự sửa service drift.
- Pre-stop failure chưa gửi stop thì không gọi start/restore. Stop timeout
  thì inspect và chỉ start approved IDs đang exited; không lặp stop mù.
  Nếu restore fail, giữ evidence, báo operator và kiểm trạng thái thật; không
  tuyên bố đã khôi phục từ việc chỉ gọi docker start.
- Finally/SIGTERM handler không chống được SIGKILL, mất điện hoặc Docker daemon
  hỏng; khi bị gián đoạn, đối chiếu before/stop-intent và current IDs trước mọi
  phục hồi. Không chạy toàn session lần nữa trên folder cũ để che lỗi.

## 6. Controller catalog có checkpoint — explicit ETL

Không chạy các lệnh này như unit test/startup hoặc song song với ETL khác.
Chỉ dùng prebuilt JAR đúng code và hai **full-scale** capacity probe folders
đã đạt; giữ existing deployment/credential/source bytes.

```bash
make test-research-catalog
make prepare-research-catalog RESEARCH_DEPLOYMENT_ROOT="$dat03_deployment_root" RESEARCH_PROFILE_ROOT=staging/dat03-research-profile-20261024 RESEARCH_USGS_ROOT=staging/dat03-usgs-2023-20261025 RESEARCH_EVENT_CAPACITY_PROBE="$dat03_event_probe" RESEARCH_BRIDGE_CAPACITY_PROBE="$dat03_bridge_probe"
```

Folder mới được in ra là request/pins bất biến. Gán đúng giá trị thực đó cho
`dat03_catalog_folder`; không lấy namespace/snapshot trong evidence làm input
cho catalog mới, không chạy lại fresh-run trên catalog đã có runtime.

```bash
make verify-research-catalog-inputs RESEARCH_CATALOG_FOLDER="$dat03_catalog_folder"
make run-research-catalog RESEARCH_DEPLOYMENT_ROOT="$dat03_deployment_root" RESEARCH_CATALOG_FOLDER="$dat03_catalog_folder" RESEARCH_CATALOG_MAX_MONTHS=1
```

Sau checkpoint đầu và fresh idle/lease/health checks đạt, tiếp tục **cùng**
catalog chỉ khi session trước có acceptance; không resume failed/partial:

```bash
make resume-research-catalog RESEARCH_DEPLOYMENT_ROOT="$dat03_deployment_root" RESEARCH_CATALOG_FOLDER="$dat03_catalog_folder"
```

Default Java budget6000s, outer host7200s có reserve/staging allowance; stop
trước tháng/phase mới khi gần budget. Source cutoff/code không đổi giữa các
phiên. Success cần result/checkpoint/record chain thật, six pins và lease-release
receipt; log `month_published` một mình không chứng nhận cả session/full kỳ.
Host observations là folder mới; runtime đầu không bị overwrite. Xem contract
cho timeout, partial-month, own lease và independent stored parent readback.

### Continuation tối ưu sau checkpoint sạch

Không dùng khi session cũ còn active hoặc failed. Prepare chỉ pin artifact ở
host, không đổi runner/deployment/source. Cần prebuilt JAR có batch/cache/
opt-in fact verifier và bytecode compatibility thật với original runner:

```bash
make prepare-research-catalog-continuation RESEARCH_DEPLOYMENT_ROOT="$dat03_deployment_root" RESEARCH_CATALOG_FOLDER="$dat03_catalog_folder"
```

Gán exact folder mới được in cho `dat03_catalog_continuation`; không sửa request
cũ hoặc overwrite candidate cũ. Sau old session accepted/idle và lease trống,
nghiệm thu batch đầu bằng limit nhỏ trước khi mở tiếp:

```bash
make resume-research-catalog RESEARCH_DEPLOYMENT_ROOT="$dat03_deployment_root" RESEARCH_CATALOG_FOLDER="$dat03_catalog_folder" RESEARCH_CATALOG_CONTINUATION_FOLDER="$dat03_catalog_continuation" RESEARCH_CATALOG_MAX_MONTHS=7
```

Limit7 là số **tháng tối đa**, không bảo đảm một batch; actual next-fit còn phụ
thuộc profile/prefix/split. Sau acceptance/resource review mới chạy không limit
nhỏ, vẫn giữ budget/session. Control folder có thể reuse khi exact5files/SHA
trong volume khớp; không overwrite hoặc retransmit source captures. Nếu code/
proof drift, chuẩn bị candidate UUID mới, không sửa staged artifact cũ.

Xem contract cho75k-profile/100k-actual guards, JVM-local structural cache và
exact fact blockers. `EXPLAIN TYPE VALIDATE` chỉ xác nhận cú pháp/kiểu, không
thay runtime SQL readbacks hoặc full-scale capacity.

## Giới hạn / bước tiếp theo

- **DAT-03 — Catalog nghiên cứu thật:** controller/session guards và Trino
  full-scale probes đã có; đang nghiệm thu runtime, chưa288monthly publications
  hoặc full Spark/lake/export capacity. Chưa có ETA toàn kỳ từ tháng đầu.
- **DAT-03 — Official collection export:** còn versioned MLI-02 lifecycle/
  registry/loader/import wiring và real full-period export. PREPARED không là
  EXPORTED; không lấy bốn tháng mẫu hoặc 12 response làm full research bundle.
- **DAT-03 — Team handoff:** receipts hai máy thành viên thuộc khối5; chưa có.
  [EXP-02/03](../task/tasks/EXP-02.md) tiếp tục pilot, không mở scientific gate.
- [SEC-01](../task/tasks/SEC-01.md) chỉ local; chưa cloud/Drive/Colab hoặc ACL.
