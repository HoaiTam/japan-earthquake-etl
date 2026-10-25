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

## Mục đích

Chạy pipeline trên catalog nghiên cứu **thật**, có source coverage, quality và
export proof; HoaiTam, ThanhTris và rosy179 nhận cùng phiên bản, đọc được trên
máy riêng không cần credential pipeline. Không lấy synthetic QA hoặc DAT-02
PARTIAL thay full research data.

## Phạm vi và từng khối công việc

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
- Archive profile và rollout/coverage/quality/resource reports; chỉ metadata vào Git.
- Real Gold pins, MLI-02 acceptance, immutable research bundle và external SHA.
- Công cụ đóng gói/verify offline, metadata handoff và receipts của ba người.
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
- Hash kiểm byte identity theo expected pin ngoài bundle, không là chữ ký nguồn.
- Không fit/tune/cấp scientific approval: EXP-02/03/04/05 sở hữu experiment,
  MLI-04/MLQ-01 sở hữu report/ML E2E.

## Theo dõi

- **Trạng thái:** In Progress
- **Assignee:** HoaiTam
- **Reviewer:** unassigned
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
  đã archive/verify rồi xóa bản thư mục, newest giữ nguyên; COW duplicate host
  copies giữ nguyên bytes/paths/SHA/metadata. [Evidence26/10](../../evidence/DAT-03_OVERNIGHT_20261026.md)
  và [metadata079](../../evidence/DAT-03_CATALOG_CHECKPOINT_079.json). Chưa50%,
  full288 hoặc actual ML export/receipts; giữ In Progress. Fresh preflight/
  physical reserve đạt trước mở batch2006-08–2007-03, không tự nới guard.

## Tài liệu liên quan

- [Source coverage](../../specs/SOURCE_COVERAGE.md), [baseline](../../specs/MVP_SCOPE_KPI_AND_DOD.md).
- [Full-month state](../../specs/FULL_MONTH_STATE_CONTRACT.md), [feature/export](../../specs/FEATURE_EXPORT.md).
- [MLI-02](../../runbooks/MLI-02_DATASET_BUILD.md), [DAT-02](./DAT-02.md).
- [Profile/rollout](../../runbooks/DAT-03_RESEARCH_PROFILING.md),
  [kết quả bước 1/2](../../evidence/DAT-03_RESEARCH_PROFILE_ACCEPTANCE.md).
- [Collection và bounded E2E](../../runbooks/DAT-03_RESEARCH_COLLECTION_E2E.md).
- [SEC-01](./SEC-01.md), [EXP-02](./EXP-02.md), [EXP-03](./EXP-03.md).
