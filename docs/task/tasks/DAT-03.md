---
task_id: "DAT-03"
status: "In Progress"
week: 6
block: "A - Hợp đồng dữ liệu"
workstream: "Verified research data and team handoff"
scope: "Core"
priority: "P0"
effort_hours: 16
assignee: "HoaiTam"
reviewer: "unassigned"
dependencies: ["ORC-07", "MLD-05", "MLI-02", "EXP-01"]
---

# DAT-03 - Chuẩn bị catalog nghiên cứu thật và bàn giao dataset dùng chung

## Giao việc hiện hành — 11/11/2026

**14/11 — portable integration từ main #127:** tiếp tục trên
`feat/dat-03-portable-integration`, assignee HoaiTam, reviewer unassigned.
[Evidence mới](../../evidence/DAT-03_PORTABLE_READBACK_20261114.md) ghi nhận đã
nhận/kiểm original follow-up của cả Trang/Trí; không yêu cầu gửi lại24+23/2+1files.
Java đọc125tháng worker qua13batch/42groups, kiểm pages/schema/counts/memberships/
source links, không giải nén dataset hoặc ghi S3. Inventory tái dùng từ PR #131/
c56f153; ngày15/11 owner yêu cầu commit/push phần đã làm trực tiếp vào PR #131.
Không merge PR vào main. Chưa phải Gold hợp nhất288tháng.
Giữ **In Progress**, còn capacity/import adapter, boundary/Trino, official exports
và receipts ba máy. Owner duyệt namespace mới `iceberg.dat03_unified_20261114`
**sau khi các gate đạt**, không build/pull/recreate, bật thêm container hoặc sửa
namespace cũ.
Capacity v2 timeout trong startup Spark/BlockManager, chưa có capacity report;
không tự retry hoặc ghi Gold. Còn versioned mapping original worker Bronze URIs
sang physical source đã xác minh, không giả markers hoặc source keys.

**Lịch sử 14/11 — trước khi nhận follow-up, PR #131:** [Audit/evidence mới](../../evidence/DAT-03_BYTE_AVAILABILITY_20261114.md)
xác nhận119/119payload references Trí,144/144Trang và37/37raw-source bytes
mỗi worker có thể tái dùng theo SHA/size trên máy coordinator. Content595bytes
đã tìm được bản nguyên bytes; không regenerate, không restore original S3keys.
Trang đã bàn giao114frontier metadata khớp pin. Không còn yêu cầu gửi lại
ZIP data lớn hoặc coi114objects này còn thiếu như mốc13/11 bên dưới.

Còn original identities của Trang và current head cuối mỗi worker:
[request Trang24pinned objects/23publication URIs](../../../config/research/dat03_lineage_rosy179_followup_20261114.json),
[request Trí2pinned objects/1publication URI](../../../config/research/dat03_lineage_ThanhTris_followup_20261114.json).
Full Iceberg graph, portable integration/Trino và official exports/receipts
chưa đạt; giữ **In Progress**, reviewer `unassigned`. Các mốc12–13/11 bên
dưới là lịch sử, không ghi đè evidence cũ hoặc chuyển Done vì đủ content bytes.

**13/11 — tiếp tục trên `feat/dat-03-payload-reuse-inventory` từ main `df2fb01`:**
HoaiTam kiểm lại metadata đã merge #128 và inventory payload theo bytes/SHA
trong các gói đã chia sẻ; chỉ thu phần thiếu thực sự, không replay ETL hoặc
nhận metadata references là portable Gold. Giữ **In Progress**, reviewer
`unassigned`; còn chờ original bytes frontier của rosy179 và các gate tích hợp/
official export/receipts ba máy.

**Actual13/11:** [Inventory/evidence](../../evidence/DAT-03_PAYLOAD_REUSE_20261113.md)
xác nhận100/119payload references Trí có bytes khớp trong data ZIP đã pin;
19refs còn lại cùng1content595bytes. [Delta request](../../../config/research/dat03_lineage_ThanhTris_payload_delta.json)
không yêu cầu upload lại498MiB. Publication giữ NOT_VERIFIED; Trang còn đúng
114metadata objects, không thay bằng138journals. Chưa full closure/import/export.

**12/11 — đã tiếp tục:** In Progress trên feat/dat-03-lineage-readiness.
[Actual metadata audit](../../evidence/DAT-03_LINEAGE_FRONTIER_20261112.md)
xác nhận75/83first-hop và Trí76dataset manifests/18parent commits.
[Requests/runbook](../../runbooks/DAT-03_LINEAGE_FRONTIER.md); còn closure/
payload/identity/publication/snapshots, unified Gold và official exports.
Không lấy metadata audit làm Done toàn task.

**12/11 — ThanhTris bàn giao metadata frontier:** Đã trích xuất và đưa lên Drive
gói `ThanhTris-dat03-frontier-metadata-20261112.zip` (18 identities khớp 100%
`identity_sha256`, 18 publications `PASSED`/`published=true`, cryptographic
linkage tới 108 snapshots Iceberg và kiểm kê 119 payload 522.659.149 bytes tại MinIO).
[Evidence ThanhTris](../../evidence/DAT-03_LINEAGE_ThanhTris_METADATA_20261112.md).
DAT-03 vẫn giữ In Progress.

- **Owner:** HoaiTam; **status:** In Progress.
- **Làm phần gì / dùng để làm gì:** Lineage → catalog tích hợp → official export.
- **Bắt đầu độc lập:** Đối chiếu bytes/SHA tầng tiếp theo của Trí/Trang; không chạy lại ETL đã accepted.
- **Đầu ra và điều kiện Done:** Catalog thống nhất có boundary/reconciliation, Gold/Trino pins; REPRODUCTION/EXTENSION EXPORTED, loader/rerun và receipts ba máy.
- **Lịch:** chuẩn bị 11–15/11, nghiệm thu tuần9 16–22/11; xem
  [kế hoạch và setup](../WEEK_9_REPLAN_20261111.md). Giữ hard dependencies,
  scope/effort lịch sử; các mục tuần cũ dưới đây là lịch sử.

Owner yêu cầu11/11: In Progress → Needs Update cho phần đã triển khai nhưng
chưa đủ acceptance. Không khẳng định từng Done toàn task hoặc mất code.
[Đối soát](../../evidence/TASK_STATUS_AUDIT_20261111.md).

## Mục đích

Chạy pipeline trên catalog nghiên cứu **thật**, có source coverage, quality và
export proof; HoaiTam, ThanhTris và rosy179 nhận cùng phiên bản, đọc được trên
máy riêng không cần credential pipeline. Không lấy synthetic QA hoặc DAT-02
PARTIAL thay full research data.

## Phạm vi và từng khối công việc

**Bản vá29/10 cho máy rosy179:** [range recovery](../../runbooks/DAT-03_RANGE_RECOVERY.md)
và [evidence](../../evidence/DAT-03_RANGE_RECOVERY_20261029.md) bổ sung
publication-only recovery + pinned resume bridge cho terminal Trino failure ở
Verify sau COMMITTED. Giữ failed evidence/request/JAR/own lease, không skip/quarantine.
33/63tháng là số chủ máy báo, chưa phải runtime acceptance mới; agent máy Trang
phải chạy preflight/recovery/postflight thật trước resume. DAT-03 vẫn In Progress.

**Tuần7 (02–08/11), HoaiTam:** [kế hoạch](../WEEK_7_COLAB_PLAN.md) và
[setup đầy đủ](../../runbooks/WEEK_7_COLAB_SETUP.md) giao phần pilot handoff:
chốt đúng release/ZIP/feature pins, mỗi người tự verify/download và gửi receipt.
Colab receipt ghi rõ VM/runtime, không thay receipt ba máy thật của DAT-03.
Không làm lại catalog288tháng hoặc chuyển task Done trong budget pilot tuần này;
full research rollout/export còn giữ acceptance phía dưới.

**Agent máy thành viên đọc trước:** [DAT-03 TEAM START](../../runbooks/DAT-03_TEAM_START.md).
Ngày28/10 owner chốt phân công và input/config pins trong
[config](../../../config/research/team_assignments_v1.json): ThanhTris62tháng
08/2013–09/2018, rosy17963tháng10/2018–12/2023; HoaiTam còn22tháng10/2011–07/2013.
CLI `scripts/dat03-team-setup.py` có plan/verify-inputs, kiểm approved commit,
inventory/source SHA và ZIP; không execute ETL. Setup không cần hỏi lại phạm vi.
**Bản bổ sung:** [Range worker](../../runbooks/DAT-03_RANGE_WORKER.md) có
prepare/run/resume/report, range62/63tháng và namespace/lease/checkpoint riêng;
frozen37sources gồm12USGS2023 đã upload/readback Drive. Mỗi máy phải chạy một
tháng accepted trước phiên12tháng. Chưa chứng nhận runtime máy thành viên,
portable ETL import/merge hoặc official ML export. Không sửa flag setup để pass.

1. **Dùng ngay:** theo lựa chọn owner 24/10, verify và đóng gói pilot JMA thật
   DAT-02 cho nhóm phát triển; giữ DEVELOPMENT_ONLY/PARTIAL/research_ready=false.
   Bản này không hoàn tất task hoặc mở official fit/import gate.
2. **Profile và kế hoạch tải:** kiểm archive thật đã có, counts/size/UTC months,
   runtime và resource limits. Pin source/release/SHA; tái dùng bytes đã verify.
   Từ số đo ước lượng lại effort/disk; 16h là estimate ban đầu, không SLA.
3. **Catalog thật:** backfill REPRODUCTION [2000-01-01,2018-10-01) và EXTENSION
   [2018-10-01,2024-01-01) UTC qua Bronze → Silver → Gold Published. Ghi exact
   manifest/snapshot/SHA, missing months và count reconciliation. JMA là primary
   research source; USGS overlap theo CON-01. Không dùng current-month PROVISIONAL.
4. **Giới hạn có version:** profile trước khi mở rộng bounded resolver/exporter;
   không tăng limit mù, truncate hoặc cắt window để pass. Chốt memory, batch/shard,
   UTC/JST archive boundary và window vượt split. Thay contract/code phải có
   regression/evidence và cập nhật consumer; không dùng scalar resource limit
   của QA nhỏ để cam kết chạy được toàn catalog.
   Owner chốt 24/10: **giữ split nghiêm ngặt**, loại mainshock nếu nguyên PRE/POST
   vượt period, ghi rõ từng ID/required interval/reason; không mượn data split khác.
5. **ML và bàn giao:** chạy MLI-02 từ exact Gold pin, audit/Mc/window/features/
   VALIDATED/EXPORTED, verify local loader và rerun. Release bất biến có bundle,
   code/config versions, README và expected SHA từ trusted receipt. Mỗi người
   dùng input readonly, output/experiment_run_id riêng.

## Deliverable

- [Hướng dẫn dataset dùng chung](../../runbooks/DAT-03_SHARED_DATASET.md).
- [Agent setup/check trên máy thành viên](../../runbooks/DAT-03_TEAM_WORKER_SETUP.md):
  release/input pins, offline/Drive/runtime readiness, handoff report và
  chia187tháng theo owner; [range worker](../../runbooks/DAT-03_RANGE_WORKER.md)
  đã test real first-month/resume trên máy owner, chưa portable export/import
  hoặc acceptance trên hai máy thành viên.
- Archive profile và rollout/coverage/quality/resource reports; chỉ metadata vào Git.
- Real Gold pins, MLI-02 acceptance, immutable research bundle và external SHA.
- Công cụ đóng gói/verify offline, metadata handoff và receipts của ba người.
- [Bản Parquet partial101tháng trên Drive](../../runbooks/DAT-03_PARTIAL_CATALOG_HANDOFF.md):
  unchanged Spark readback/ID/month/remote restore proof, EXPLORATORY_ONLY;
  không thay official ML export hoặc backup warehouse. Cleanup chỉ exact
  temporary copies đã verify; frozen inputs/CAS/checkpoint/warehouse giữ nguyên.
- [Evidence theo giai đoạn](../../evidence/DAT-03_PROGRESS.md); không viết lại
  evidence DAT-02/MLI-02 thành kết quả full catalog mới.

## Tiêu chí hoàn thành

- [x] Giai đoạn dùng trước: kit pilot thật đã verify/giải nén độc lập trên máy owner;
  giữ PARTIAL/development-only, chờ hai người nhận tự gửi receipt.
- [x] Profile đủ 25 archive h2000–h2024 bằng shared Java parser v1.2, pin SHA/
  headers/release và metadata: 4.793.337 accepted, 0 parser rejects, 18 warnings.
  [Bước 1/2](../../evidence/DAT-03_RESEARCH_PROFILE_ACCEPTANCE.md) xử lý đủ 18
  depth tokens năm 2016 có fixture/version; profile không là quality/source gate.
- [x] Đo compressed/member bytes, parser runtime/peak RSS và lập
  [kế hoạch batch/UTC boundary](../../runbooks/DAT-03_RESEARCH_PROFILING.md).
  Overlay 2024 đã capture/profile, không đổi default baseline hoặc ML split.
- [x] Bước 3 kỹ thuật: collection v1 whole-mainshock sharding, full-assignment,
  global pinned Mc và strict split; local Spark/Parquet readback/rerun/tamper
  tests đạt. Giữ RESEARCH_COLLECTION_PREPARED/BUILDING, không official EXPORTED.
  [Contract](../../specs/RESEARCH_COLLECTION.md),
  [evidence bước 3/4](../../evidence/DAT-03_COLLECTION_BOUNDED_E2E.md).
- [x] Bước 4 bounded: original-source Bronze/Silver/six Gold/Trino/rerun và
  số đo partial/recovery cho2000-01,2011-04,2016-04,2023-12+h2024/USGS đạt.
  [Khối2](../../evidence/DAT-03_CONTINUATION.md): đủ4cases, receipt/SQL/SHA/
  retention, report/acceptance và own lease RELEASED;124.969 canonical cộng dồn.
  Đây không là full-period capacity/rollout acceptance. Giữ failed evidence cũ.
- [x] Khối 1 phục hồi (25/10): chẩn đoán và read-only preflight cho retained
  run đạt13/13 metadata queries, input/journal/six pins/own lease/quiescence
  khớp. [Evidence](../../evidence/DAT-03_RECOVERY_PREFLIGHT.md),
  [runbook](../../runbooks/DAT-03_RECOVERY.md). Chưa xác định nguyên nhân gốc
  Trino restart/capacity; không chạy verify/publish, replay ETL hoặc release
  lease trong khối1. Khối2 được nghiệm thu riêng bên dưới; full-period chưa đạt.
- [x] Khối 2 (25/10, owner yêu cầu sửa ngay PR #100): continuation có code pin
  riêng và bytecode compatibility gate; fresh own-lease/current-pin preflight,
  verify/publish/rerun 2016-04, case2023-12+h2024/USGS và final acceptance/release.
  Không replay commit2016, không thay original context/JAR/evidence failed.
  Attempt đầu publish2016 rồi OOM khi verify2023; owner duyệt memory fix và
  recreate riêng Trino sau lưu log. Checkpoint verify/publish2023/rerun đạt,
  six pins không đổi, Trino restart0/oom_kill0, own lease release và idle đạt.
- [ ] Đo capacity/ETA thực của Spark/joins/lake/export và duyệt rollout full kỳ;
  chưa suy 51 giây parser scan hoặc ~9,8 GiB disk trống thành budget full ETL.
  [Chuẩn bị rollout25/10](../../evidence/DAT-03_ROLLOUT_CAPACITY.md) có25 JMA
  fresh pins/288-month plan và12 USGS capture/Java check; không BronzeReady/
  source completeness. Trino stress timeout/ABANDONED_TASK, trial profile chỉ
  late healthy; SELECT1 mất56,44s. Probe sau profile bị scheduler unhealthy
  chặn trước SQL; sau khi service tự healthy lại, full32copy probe mới vẫn
  BLOCKED_TIME90s, postflight unchanged. Chưa có full-period capacity hoặc
  official collection export.
  [Phiên giảm tải bổ sung](../../evidence/DAT-03_LIGHT_SESSION.md) đã stop/
  restore đúng4ID,9healthy/5kept unchanged; full query chặn512MB,3bucket đạt/
  bucket3timeout/12chưa chạy. Không nới cap hoặc gọi partial là full acceptance.
  Mốc mới sau Docker hoạt động lại: event/bridge43copies=4.527.126rows mỗi
  phía, **16/16 mỗi bảng đạt**,90s/query giữ nguyên; không full Spark capacity.
  [Catalog rollout](../../evidence/DAT-03_CATALOG_ROLLOUT.md) có checkpoint
  2000-01 thật, đang chạy phiên tiếp tục, chưa đủ288tháng hoặc official export.
- [x] Controller kỹ thuật `dat03-catalog-rollout-v1`:37exact original sources,
  frozen cutoff/code/inventories, phase/month checkpoints và contiguous chain,
  budgets dưới2h, fail/timeout giữ lease. Tháng đầu2000-01 có68reported checks/
  six pins, session exit0/release own lease. Không gọi controller hoặc checkpoint
  đầu là acceptance toàn catalog. [Contract](../../specs/RESEARCH_CATALOG_ROLLOUT.md).
- [ ] Khối 3 tiếp theo: capacity gate và rollout catalog 225/63 tháng có
  source pins, checkpoint, six-snapshot publication và bounded recovery; chỉ
  tick khi runtime toàn kỳ đạt, không lấy planner hoặc bốn tháng mẫu thay thế.
- [ ] Khối 4 tiếp theo: official collection-aware ML export từ full Gold pin,
  lifecycle/registry/loader/import và rerun; không đổi PREPARED thành EXPORTED
  chỉ bằng metadata. Khối 5 là receipts từ ba máy thành viên.
- [ ] Resource/boundary policies được kiểm trên dữ liệu thật; không silent sampling.
  Đã triển khai opt-in resolver `orc-06-v2` và `dat03-strict-split-v1` trong nhánh
  PR #99; regression/reader profile không thay full-period runtime acceptance.
- [ ] Source proof đủ JMA 225 tháng reproduction và 63 tháng extension; xử lý biên
  UTC/JST và source-link halo, không nhận boolean tự khai.
- [ ] Gold publication/Trino/reconciliation đạt; audit/Mc/exclusion reasons có proof.
- [ ] MLI-02 build/export thật, counts/pins/checksum/loader và rerun đạt.
- [ ] HoaiTam, ThanhTris, rosy179 verify cùng release trên máy riêng; receipt có
  người nhận, dataset/pilot ID, SHA và loader result. Không giả receipt của hai
  người khác bằng ba lần chạy trên máy owner.
- [ ] Runbook, giới hạn và metadata task/index đồng bộ, không còn blocker chưa xử lý.

## Hard dependency / làm song song

ORC-07, MLD-05, MLI-02 và EXP-01 đã Done theo scope component riêng. DAT-03 sở
hữu actual research rollout còn thiếu, không đổi các task ấy về chưa Done.
EXP-02/03 tiếp tục local development bằng pilot; official scientific run cần
release đủ gate. Không tạo dependency ngược từ DAT-03 sang EXP-02.

## An toàn và ranh giới

- Không build/pull/start/init trong test/handoff; không đổi deployment, cron,
  pause/run, lease hoặc registry production để đóng gói local.
  Ngoại lệ owner duyệt25/10 cho bounded recovery: giữ run/namespace/cutoff,
  recreate riêng Trino với cached image sau backup, chỉ release own lease sau
  acceptance. Controller/test không tự triển khai hoặc retry service.
  Phần rollout tiếp theo owner chọn máy hiện tại, <=2h/phiên; duyệt riêng
  backup/lower-concurrency/recreate chỉ Trino bằng cached image, không service
  khác hoặc dữ liệu. Trial không thay approval full rollout/capacity.
  Owner duyệt bổ sung25/10 một phiên giảm tải: stop đúng Spark master/worker,
  Airflow API/DAG processor rồi start lại cùng4ID; giữ5services khác, không
  DAG/credential/data mutations. Chỉ đo read-only, phải xác nhận restore thật.
- Không xóa dữ liệu; release khác bytes dùng identity/output mới.
- Không upload cloud/thay ACL/tạo link public. SEC-01 Done local; mở lại phần
  Colab/Drive trước migration. Người dùng tự chọn nơi chia sẻ.
  Ngoại lệ26/10 owner chọn folder nhóm và duyệt pilot/source, tiếp theo ZIP
  Parquet101tháng và xóa copies local sau remote readback. SEC-01 đã mở lại
  Needs Update; scoped payload/ACL đạt, còn recipient/identity acceptance.
  Không đổi ACL hoặc cấp official research/ML readiness từ partial ZIP.
- Hash kiểm byte identity theo expected pin ngoài bundle, không là chữ ký nguồn.
- Không fit/tune/cấp scientific approval: EXP-02/03/04/05 sở hữu experiment,
  MLI-04/MLQ-01 sở hữu report/ML E2E.

## Theo dõi

- **Actual14/11 — portable Silver readback:** [evidence/pins](../../evidence/DAT-03_PORTABLE_READBACK_20261114.md),
  [runbook](../../runbooks/DAT-03_PORTABLE_READBACK.md). Trang63/63tháng,
  Trí62/62tháng;4.973.482rows giải mã là tổng bốn Silver datasets, không phải
  4.973.482canonical earthquakes. Chưa tạo Gold mới hoặc official export. Các
  ghi chú chờ metadata follow-up bên dưới là lịch sử, không yêu cầu gửi lại.

- **14/11 — nghiệm thu bytes gốc 114 objects frontier của rosy179 trên Google Drive:**
  Hoài Tâm đã tải và đối soát độc lập gói `rosy179-dat03-lineage-frontier.zip` (101.446 bytes, SHA-256
  `0dd1c504f27489709c46748b313e9f764ecfc25a58ef50a2699cbcf0626e8453`, Drive ID `1QsZHjXXsGmWvBDFHiicMdejmAN2Lehb-`):
  đủ 114/114 objects gốc khớp 100% danh sách URI/SHA đã pin (92 Silver dataset manifests + 22 parent Gold commits),
  không thiếu/thừa metadata, không duplicate ZIP members, không unsafe paths và checksum sidecar khớp hoàn toàn.
  Phân định rõ nghiệm thu 114 objects frontier này không đóng toàn bộ lineage: Cổng 1 tiếp tục MỞ (`CONG_1_TRANSITIVE_CLOSURE_OPEN`),
  chờ đối soát follow-up metadata theo `config/research/dat03_lineage_rosy179_followup_20261114.json` (PR #131: 22 parent identities
  + commit/identity của current head `orc07-22b5e7647a3df9ed83370139e11c48048845ee8f` = 24 pinned objects + 23 original publication URIs)
  và phần việc của ThanhTris. Hoàn thiện `scripts/pack-worker-lineage.py` và `tests/test_pack_worker_lineage.py`:
  loại bỏ flags secret trên CLI, tách biệt MinIO và AWS credentials, hỗ trợ `AWS_SESSION_TOKEN`, cô lập mocking an toàn không phụ thuộc
  boto3 SDK runner (51 tests PASS, toàn bộ 73/73 worker tests PASS).
  [Evidence](../../evidence/DAT-03_PROGRESS.md), [Báo cáo 288 tháng](../../evidence/DAT-03_INTEGRATION_EVIDENCE_288_MONTHS.md).
  Giữ Needs Update / In Progress chờ nghiệm thu 4 cổng.

- **12/11 — đóng gói lineage bổ sung (138 phase journals) & lập đặc tả 114 objects frontier của rosy179:**
  đóng gói gói bằng chứng giai đoạn thực thi bổ sung `rosy179-dat03-lineage-metadata.zip` (401.335 bytes, SHA-256
  `45c1f02ff80454aca2a6bad5587e6b0bfabc4f8cb7fa08a8043a8c6bb7b6de01`) gồm đủ 138/138 phase journals (23 runs x 6
  phases: readiness, bronze, silver, gold commit, verify, publish), khớp 100% cryptographic receipts trong 63 tháng.
  Đồng thời phân định rõ 3 tầng bằng chứng và lập đặc tả kỹ thuật chi tiết cho 114 objects transitive closure frontier
  (92 Silver dataset manifests + 22 parent Gold commits) tại `staging/dat03-range-handoff-rosy179/rosy179-frontier-114-spec.json`.
  Cổng 1 transitive closure vẫn giữ trạng thái MỞ / PENDING, chờ bytes gốc của 114 objects từ object storage.
  Đã hoàn thiện công cụ `scripts/pack-worker-lineage.py` (chốt chặn symlink file/parent/ZIP entry mode 0o120000, chống thoát root,
  ràng buộc nghiêm ngặt layout `silver/real-etl/<operation_id>/<phase>.json`, kiểm tra nhất quán manifest, chống trùng lặp logic,
  và bổ sung lệnh `frontier-spec`) cùng bộ kiểm thử tự động `tests/test_pack_worker_lineage.py` (14/14 tests PASS).
  [Evidence](../../evidence/DAT-03_INTEGRATION_EVIDENCE_288_MONTHS.md).
  Giữ Needs Update theo phân công 11/11 của HoaiTam chờ nghiệm thu 4 cổng.

- **Lịch sử14/11 — byte audit trước follow-up / PR #131:** [Byte availability evidence](../../evidence/DAT-03_BYTE_AVAILABILITY_20261114.md)
  và [summary/pins](../../evidence/DAT-03_BYTE_AVAILABILITY_20261114.json): đủ119/119
  payload refs Trí,144/144Trang,37/37raw-source contents mỗi worker trên owner.
  Trang114metadata matched; còn identities/current-head/publication/graph theo
  follow-up requests hiện hành. Giữ In Progress; original S3keys, portable
  Gold/live Trino/official exports và receipts3máy chưa nghiệm thu mới.

- **13/11 — actual metadata/payload inventory:** branch `feat/dat-03-payload-reuse-inventory`
  từ main #128,75roots/76manifests/18commits/18identities matched;100/119payload
  refs byte-identical, delta1content595bytes. [Evidence](../../evidence/DAT-03_PAYLOAD_REUSE_20261113.md).
  Không ETL/runtime/Drive mutations; giữ In Progress và reviewer unassigned.

- **05/11 — đối soát toàn kỳ 288 tháng và 75/75 provenance của ThanhTris:**
  đối soát offline 02/11 và cập nhật từ Hoài Tâm xác nhận đủ 288/288 tháng qua
  4 gói phân đoạn (101 nền, 62 HoaiTam, 62 ThanhTris, 63 rosy179), 0 gap và
  0 overlap. Gói ZIP provenance của ThanhTris trên Drive đã được tải về và kiểm
  tra đạt 75/75 first-hop objects (37 Bronze manifests, 19 Silver manifests,
  19 registry JSON) khớp 100% SHA-256. [Evidence](../../evidence/DAT-03_INTEGRATION_EVIDENCE_288_MONTHS.md).
  Còn 4 cổng kỹ thuật để Done: hoàn tất đối soát transitive lineage (Cổng 1),
  tích hợp catalog và khử trùng ranh giới (Cổng 2), official ML export cho 2 split
  REPRODUCTION/EXTENSION (Cổng 3) và đóng gói release kèm đủ 3 receipts (Cổng 4).
  Phần việc first-hop của rosy179 (63 tháng 2018-10..2023-12) đã hoàn thành: đã đóng gói
  và tải lên Google Drive gói `rosy179-dat03-provenance-metadata.zip`, Hoài Tâm đã xác
  nhận khớp 100% SHA-256 cho 83/83 first-hop objects (37 Bronze manifests, 23 Silver bundle
  manifests, 23 snapshot registry JSONs). Đã hoàn thiện công cụ `scripts/pack-worker-provenance.py`
  (enforce fail-closed, unique paths, phân biệt self-integrity và trusted provenance) và
  `scripts/extract-minio-provenance.py` kèm bộ kiểm thử tự động. Giữ In Progress.

- **29/10 — recovery range sau sleep:** branch `fix/dat-03-range-sleep-recovery`
  từ main `ec7431244df1e5858fc0d148ec71755d36915033`; thêm helper publication-only,
  own lease/postflight gate và pinned bridge cho session lỗi của rosy179.
  [Evidence](../../evidence/DAT-03_RANGE_RECOVERY_20261029.md) ghi offline tests;
  không thay failed execution, chưa live acceptance máy Trang. Giữ In Progress.

- **28/10 — mở đường ETL thành viên:** branch `feat/dat-03-range-worker`
  từ main `327a275`; thêm Spark Java range worker, controller và offline
  verifier gói frozen37sources. [Evidence](../../evidence/DAT-03_RANGE_WORKER_20261028.md)
  có fresh run2018-10/resume2018-11 đạt2/63 trên máy owner, không coi là
  receipt máy ThanhTris/rosy179. Test2013-08 đã recovery publication riêng
  sau lỗi namespace gate, giữ failure. Catalog cũ141/288 đến2011-09, không
  cộng range tests vào catalog ấy. Giữ In Progress.

- **28/10 — team setup:** branch `feat/dat-03-team-readiness` từ main
  `c4410dbcfb325671f485ce192aa15f4d1db07a62`; chốt assignment/pins và CLI
  offline. Không đưa các thay đổi recovery chưa commit từ checkout owner vào
  release này. Full commit sau push được gửi qua nhóm làm external code pin.
- **28/10 — dữ liệu:** accepted134/288 (46,528%) đến2011-02;
  Drive có101 + 24 + 9tháng partial, fresh download/SHA/semantic/ACL đạt.
  [Evidence](../../evidence/DAT-03_DRIVE_RANGES_20261028.md). Giữ In Progress;
  các mốc101/113 bên dưới là lịch sử, chưa officialMLexport/receipts ba máy.

- **Trạng thái:** In Progress
- **Assignee:** HoaiTam
- **Reviewer:** unassigned
- **Cập nhật tối27/10:** đã hủy automation, mở lại Docker và benchmark chỉ đọc
  batch06–11/2009. Recovery V2 bị gián đoạn ở ordinal134, thiếu terminal receipt,
  own lease giữ; accepted vẫn113. Primary whole-query16,47s so16bucket88,70s,
  cùng0vi phạm; thêm opt-in có capacity guard, chưa wiring/recover/publish.
  [Evidence benchmark](../../evidence/DAT-03_VERIFICATION_BENCHMARK_20261027.md),
  [quy trình mới](../../runbooks/DAT-03_VERIFICATION_BENCHMARK.md).
- **Chỉ đạo mới nhất27/10:** chỉ hoàn tất batch06–11/2009/recovery V2 đang
  chạy rồi dừng; không mở normal continuation/batch12/2009. Quyền tiếp tục
  các phiên sau17:00 đã giới hạn lại. Không cộng119 trước actual acceptance.
- **Cập nhật chiều27/10:** accepted113/288 đến2009-05 (39,236%),
  1.200.455canonical/bridge,12/62tháng phần HoaiTam. Batch06–11/2009 đã commit
  Gold nhưng verify failed/ABANDONED_QUERY; owner yêu cầu xử lý rồi tiếp tục.
  Recovery V1 cũng failed; đã có bản vá V2 SELECT-only polling300s, write/default
  vẫn120s/không retry,13metadata+slow-query probe58,637s/0vi phạm đạt.
  Recovery V2 không Spark/không replay ETL đang verify/publish;
  chỉ cộng119 khi actual acceptance/own release/host CAS reporter đạt.
  [Evidence](../../evidence/DAT-03_CATALOG_VERIFY_RECOVERY_200906.md).
- **Cập nhật27/10:** owner yêu cầu phục hồi batch06–11/2008 đã commit nhưng
  chưa Published. Branch `fix/dat-03-catalog-publication-recovery` từ main
  `fe80830`; giữ failure và toàn bộ pins. Read-only preflight13/13 và actual
  verify/publish/186SQL/own release đạt, không Spark/ETL replay. Host capture
  allowlist lỗi đã phục hồi riêng chỉ đọc. [Checkpoint107](../../evidence/DAT-03_CATALOG_CHECKPOINT_107.json)
  accepted107/288 đến2008-11;6/62tháng phần HoaiTam. Đã mở phiên tiếp từ2008-12,
  tối đa18tháng/budget6000s; không cộng tháng active. [Evidence](../../evidence/DAT-03_CATALOG_RECOVERY_20261027.md),
  [runbook](../../runbooks/DAT-03_CATALOG_PUBLICATION_RECOVERY.md).
- **Evidence / PR:** [DAT-03 progress](../../evidence/DAT-03_PROGRESS.md). Branch
  `feat/dat-03-research-dataset-handoff` từ main `bc0ce9e` (PR #98).
  [PR #99](https://github.com/HoaiTam/japan-earthquake-etl/pull/99), bước3/4
  đã được owner commit ở `19073bd`. Ngày25/10 khối 1 tiếp tục trên branch vá
  `fix/dat-03-trino-recovery` từ đúng commit này (không từ main thiếu DAT-03).
  Khối 1 đã được owner commit/push ở `1910de8`, [PR #100](https://github.com/HoaiTam/japan-earthquake-etl/pull/100).
  Theo yêu cầu mới khối 2 sửa tiếp ngay branch/PR này, không tạo branch/PR mới;
  owner cho phép commit/push trực tiếp sau acceptance. Khối2 đã đạt, không merge
  hoặc sửa original failed evidence. [Evidence](../../evidence/DAT-03_CONTINUATION.md).
  PR #100 đã được merge vào main `8ef6c41` ngày25/10; phần tiếp theo mở branch
  `feat/dat-03-research-rollout` từ đúng main này. Đang làm capacity/source plan
  trên branch này;12 USGS captures/Java validation đạt, Trino-only trial chưa
  đủ capacity. Official export wiring/full rollout chưa triển khai, không
  chứng nhận khối3/4. [Evidence](../../evidence/DAT-03_ROLLOUT_CAPACITY.md),
  [runbook](../../runbooks/DAT-03_RESEARCH_ROLLOUT.md).
  Theo yêu cầu mới hoàn thiện toàn bộ DAT-03, tiếp tục từ commit0487a83 đã
  push. Hai SQL probes đủ full-profile upper bound đã đạt; catalog controller
  mới có first acceptance2000-01 và đang chạy phiên resume cùng pins/cutoff.
  [Evidence mới](../../evidence/DAT-03_CATALOG_ROLLOUT.md). Chưa full catalog/
  official collection export hoặc receipts ba máy; không tự commit/push/merge.
  Chưa có reviewer approval, full research hoặc ba máy acceptance. ThanhTris và
  rosy179 chưa chạy verifier theo xác nhận owner; tiếp tục phần kỹ thuật độc lập.
  Mốc tiếp theo27/288tháng đến2002-03 đã accepted, own lease release. Owner yêu
  cầu25/10 chạy các đợt khoảng3h/commit, giữ <=2h/phiên và báo mỗi25% catalog
  (72/144/216/288), không gọi catalog100% là DAT-03 Done. Xem
  [kế hoạch batch](../../runbooks/DAT-03_BATCH_EXECUTION.md). Được commit theo
  đợt; chưa có yêu cầu push/merge. Khối3/4/5 vẫn chưa đạt toàn bộ acceptance.
  Controller full-catalog ML export đã có code/12Python mocks/2Java pure gate
  tests; [runbook](../../runbooks/DAT-03_RESEARCH_ML_EXPORT.md) giữ full288 gate
  và lease qua checkpoint, chưa chạy trên research catalog toàn kỳ. Các ghi
  chú "wiring chưa triển khai" phía trên là lịch sử trước component này.
  Phiên kế tiếp5de5ac2f746642d49d98677ad207cf5a accepted45/288 đến2003-09:
  thêm18tháng/3batch,2460,963s, exit0/no timeout/no pending và own release.
  [Metadata](../../evidence/DAT-03_CATALOG_CHECKPOINT_045.json) có six pins/
  466.990 canonical/bridge. Catalog15,625%, chưa mốc25% hoặc full DAT-03.
  [Paired research kit component](../../evidence/DAT-03_RESEARCH_TEAM_COMPONENT.md)
  có11kit/9loader tests và full138pilot regression PASS bằng existing doc01
  venv; chưa actual paired release hoặc receipts ba máy. Không ship fixture
  ZIP, tự tạo receipt thành viên khác hoặc chuyển task Done từ code/tests.
  Phiên0c09edf220f842b0bbc4cb9fd17847f2 accepted63/288 đến2005-03:
  thêm18tháng/3batch,3575,76s, exit0/no timeout/no pending, own release.
  [Metadata063](../../evidence/DAT-03_CATALOG_CHECKPOINT_063.json) giữ six pins,
  673.105 canonical/bridge;21,875% catalog. Chưa đủ mốc25% hoặc full DAT-03.
  Mốc25%: phiên33489cff7443484eb7e161b12a3cef4b accepted72/288 đến2005-12,
  thêm9tháng/2batch,3097,855s, exit0/no timeout/no pending, own release.
  [Metadata072](../../evidence/DAT-03_CATALOG_CHECKPOINT_072.json) giữ six pins,
  774.260 canonical/bridge. Hậu kiểm9healthy/no active Java/DagRun/query và
  lease trống. Đã báo owner25% catalog; chưa full288/official export/ba máy,
  task vẫn In Progress. Các commit theo đợt đang local, chưa tự push/merge.
  Phiên26/10 từ72 đã accepted79/288 đến2006-07,843.404canonical/bridge,
  exit0/1562,109s/own release, postflight idle/lease trống. Năm host copies cũ
  đã archive/verify rồi restore COW đầy đủ tại original paths trước bỏ ZIP
  trung gian; newest giữ nguyên. COW duplicate host
  copies giữ nguyên bytes/paths/SHA/metadata. [Evidence26/10](../../evidence/DAT-03_OVERNIGHT_20261026.md)
  và [metadata079](../../evidence/DAT-03_CATALOG_CHECKPOINT_079.json). Chưa50%,
  full288 hoặc actual ML export/receipts; giữ In Progress. Fresh preflight/
  physical reserve đạt trước mở batch2006-08–2007-03, không tự nới guard.
  Batch này sau đó accepted87/288 đến2007-03: thêm8tháng,2173,924s, exit0/no
  timeout/no pending/own release,918.168canonical/bridge. [Metadata087](../../evidence/DAT-03_CATALOG_CHECKPOINT_087.json).
  Vòng nối tiếp kiểm health/idle/COW unchanged-progress/reserve rồi mở phiên
  kế từ87, max18tháng/6000s; không auto recovery hoặc tự chuyển Done.

  Cập nhật26/10: [checkpoint101](../../evidence/DAT-03_CATALOG_CHECKPOINT_101.json)
  đã accepted101/288 (35,069%) đến2008-05,1.064.869canonical/bridge,
  exit0/no timeout/own release. Coordinator đã dừng sạch vì physical host
  free3.913.068.544bytes dưới reserve4GiB sau COW; fresh postflight healthy/
  idle/lease-empty. Cần giải phóng dung lượng có chủ đích trước resume2008-06;
  không restart/reset/xóa source hoặc gọi catalog là official ML export.
  [Bàn giao](../../evidence/DAT-03_OVERNIGHT_20261026.md#dừng-sạch-tại-checkpoint101).
  Giữ In Progress, chưa full288/export/receipts ba máy; follow-up đã dừng.

## Tài liệu liên quan

- Storage26/10 ở worktree local `fix/dat-03-incremental-observations`:
  có code/tests, migrate10accepted copies với full SHA/acceptance, real
  read-only capture và808file full restore101. Code/contract/evidence storage
  còn chưa commit trong lần bàn giao Drive này; không đặt link tới các file
  chỉ có local hoặc coi chúng đã được phát hành cho máy thành viên. Không
  ETL mới/official export; giữ In Progress cho187tháng/full ML export/ba máy.
  Host đã migrate vẫn cần tooling tương ứng; không downgrade hoặc chạy helper
  cũ trên descriptor layout. Xem [hướng dẫn setup máy thành viên](../../runbooks/DAT-03_TEAM_WORKER_SETUP.md)
  để kiểm release trước khi nhận việc.

- [Source coverage](../../specs/SOURCE_COVERAGE.md), [baseline](../../specs/MVP_SCOPE_KPI_AND_DOD.md).
- [Full-month state](../../specs/FULL_MONTH_STATE_CONTRACT.md), [feature/export](../../specs/FEATURE_EXPORT.md).
- [MLI-02](../../runbooks/MLI-02_DATASET_BUILD.md), [DAT-02](./DAT-02.md).
- [Profile/rollout](../../runbooks/DAT-03_RESEARCH_PROFILING.md),
  [kết quả bước 1/2](../../evidence/DAT-03_RESEARCH_PROFILE_ACCEPTANCE.md).
- [Collection và bounded E2E](../../runbooks/DAT-03_RESEARCH_COLLECTION_E2E.md).
- [SEC-01](./SEC-01.md), [EXP-02](./EXP-02.md), [EXP-03](./EXP-03.md).
