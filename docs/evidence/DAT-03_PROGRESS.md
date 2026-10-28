# DAT-03 — Tiến độ thực tế (cập nhật 27/10/2026)

## Bổ sung28/10 — range worker và USGS frozen cho thành viên

[Worker runbook](../runbooks/DAT-03_RANGE_WORKER.md) thay blocker thiếu
implementation của setup PR #106: controller/Spark range và37frozen sources
đã có, Drive readback/prepare nguồn thật được kiểm. Fresh first-month2018-10
và resume2018-11 đạt2/63,45.826canonical/bridge trên máy owner; không cộng test
vào global catalog. Catalog cũ terminal accepted141/288 đến2011-09; Drive
partial vẫn134. Xem
[evidence](DAT-03_RANGE_WORKER_20261028.md) cho test và giới hạn runtime.
Chưa có receipt ETL thực trên hai máy; portable merge/official export vẫn thiếu.
Các số134 và ghi chú thiếu range/USGS bên dưới là mốc setup trước bổ sung này.

## Mốc mới28/10 — bàn giao Drive và setup team

Accepted134/288 đến2011-02 (46,528%),1.445.918canonical/bridge;
HoaiTam33/62, còn29tháng. [Drive evidence](DAT-03_DRIVE_RANGES_20261028.md)
ghi actual accepted parent và hai gói24/9tháng fresh download/semantic đạt.
Drive có partial134tháng, chưa official ML export. Các số113/119 và trạng thái
lease/recovery bên dưới là lịch sử trước nghiệm thu mới.

[TEAM START](../runbooks/DAT-03_TEAM_START.md) chốt assignment/config/source pins
và CLI kiểm input cho từng người. Component setup đạt11unit tests,
9source-handoff tests,12partial-catalog tests,42CI tests,10docs tests;
fresh source download25archives/128.784.513bytes verifier đạt trên máy owner.
Không nghiệm thu runtime/Drive identity của hai máy khác. Distributed worker,
portable import/merge và USGS frozen transport còn thiếu; DAT-03 In Progress.

**Cập nhật tối27/10:** automation đã hủy; owner duyệt mở Docker và test quy
trình tối ưu trên batch hiện tại. V2 bị gián đoạn tại verify134, thiếu terminal
receipt; không còn worker, giữ lease và accepted113/288. Benchmark primary
whole16,47s so16bucket88,70s, cùng0vi phạm; mới là read-only A/B, chưa publication.
[Evidence và giới hạn](DAT-03_VERIFICATION_BENCHMARK_20261027.md),
[quy trình benchmark](../runbooks/DAT-03_VERIFICATION_BENCHMARK.md).
Ghi chú recovery "đang chạy" bên dưới là lịch sử, không phải trạng thái mới nhất.

**Chỉ đạo mới nhất27/10:** chỉ hoàn tất recovery batch06–11/2009 rồi dừng,
không mở batch12/2009 hoặc normal continuation mới. Accepted hiện113/288;
chỉ cộng119 sau actual acceptance/release/host report. Không auto retry nếu lỗi.

**Cập nhật chiều27/10 — batch06–11/2009:** accepted113/288 (39,236%) đến
2009-05,1.200.455canonical/bridge,12/62tháng phần HoaiTam. Phiên kế đã commit
Gold nhưng verify failed/ABANDONED_QUERY; giữ nguyên failure/own lease, không
cộng batch active. Recovery V1 cũng đã dừng; owner yêu cầu xử lý rồi tiếp tục
cả sau17:00, tối đa2h/phiên. Bản vá V2 SELECT-only polling300s (write/default
vẫn120s, không retry) qua13metadata+actual slow-query probe58,637s/0vi phạm;
recovery riêng không Spark/ETL replay đang verify/publish, chưa acceptance mới.
[Evidence và giới hạn](./DAT-03_CATALOG_VERIFY_RECOVERY_200906.md).
Các mốc107 và trước đó bên dưới là lịch sử.

**Mới nhất27/10 — publication-only recovery:** branch
`fix/dat-03-catalog-publication-recovery` từ main `fe80830`. Batch06–11/2008
đã commit nhưng session exit1/unknown Trino outcome. Owner yêu cầu bắt đầu lại;
read-only13/13 preflight, actual186SQL/verify/publish/own release đã đạt trong
1679,196s. [Checkpoint107](./DAT-03_CATALOG_CHECKPOINT_107.json) accepted107/288
(37,153%) đến2008-11,1.137.791canonical/bridge;6/62tháng phần HoaiTam.
Host capture từng lỗi allowlist recovery record; đã lưu own dead lock và
recapture chỉ đọc, latest107/full SHA report đạt. Không replay Bronze/Silver/
Gold hoặc publication. Đã mở phiên tiếp tối đa18tháng từ2008-12, chỉ cộng
tháng mới sau acceptance. [Evidence mới](./DAT-03_CATALOG_RECOVERY_20261027.md)
và [runbook](../runbooks/DAT-03_CATALOG_PUBLICATION_RECOVERY.md). Các mốc dưới
là lịch sử, không là current runtime/lease. Task vẫn In Progress.

Branch hiện tại `fix/dat-03-incremental-observations`, tiếp tục từ rollout
`3625f9a`; nhánh rollout trước từ0487a83 sau PR #100.
Lịch sử trước đó: `feat/dat-03-research-dataset-handoff` từ main `bc0ce9e` (#98).
Assignee HoaiTam, reviewer unassigned. **In Progress**. Phần pilot/profile đã
được owner commit, HEAD khi tiếp tục bước3/4 `258710d`, trên [PR #99](https://github.com/HoaiTam/japan-earthquake-etl/pull/99).
Bước3/4 đã được owner commit ở `19073bd`. Ngày25/10 khối 1 recovery được
tiếp tục trên `fix/dat-03-trino-recovery`; owner đã commit/push khối1 ở1910de8,
PR #100 và cho phép commit/push trực tiếp khối2 sau acceptance.
Owner chọn gửi pilot thật dùng trước trong lúc chuẩn bị full research catalog.
Owner xác nhận hai thành viên chưa chạy verifier và yêu cầu tiếp tục kỹ thuật;
chốt strict split, loại window thiếu PRE/POST coverage và báo chi tiết từng mainshock.

## Cập nhật kỹ thuật tiếp theo

**Mới nhất — storage26/10:** [incremental observations](./DAT-03_INCREMENTAL_OBSERVATIONS_20261026.md)
đã migrate10host copies sang manifest/CAS, giữ đủ bytes/SHA/acceptance.
Latest vẫn101/288, không ETL mới. Real read-only capture chuyển0old payload
bytes; full restore101 đủ808files khớp reporter. Catalog `du` khoảng805MiB,
không suy physical freed3,8GiB vì COW; delta host free quan sát chỉ~5,86MiB.
Giữ frozen root + blob store + manifests; không dùng helper ZIP/COW/overnight
cũ hoặc xóa store. Các mô tả physical directories phía dưới là lịch sử;
original paths của10observations hiện là descriptors. ZIP13 vẫn giữ nguyên.
Không tự resume hoặc hồi sinh follow-up; DAT-03 còn full catalog/export/receipts.

**Mới nhất 26/10 — dừng sạch vì host disk reserve:** đã xác minh
101/288 tháng (35,069%) đến2008-05,1.064.869 canonical/bridge;
[metadata101](./DAT-03_CATALOG_CHECKPOINT_101.json). Phiên
`9515ebd43baa451a847e849fba3c103a` exit0/2545,568s/own release, host2582,908s.
Mốc93 giữ [metadata riêng](./DAT-03_CATALOG_CHECKPOINT_093.json). Coordinator
đã kết thúc bình thường sau COW và không mở phiên mới: physical free
3.913.068.544bytes, dưới reserve4GiB. Fresh postflight required services healthy,
không active DagRun/Java/Trino query và lease trống. Không reset/retry hoặc
xóa source/warehouse/log/evidence; cần giải phóng thêm dung lượng có chủ đích
trước fresh preflight/resume từ2008-06. Chưa50%, full288 hoặc official ML export;
DAT-03 vẫn In Progress. [Chi tiết bàn giao](./DAT-03_OVERNIGHT_20261026.md#dừng-sạch-tại-checkpoint101).
Các đoạn dưới giữ mốc lịch sử, không là current process/lease state.

Owner yêu cầu thử giữ ZIP/xóa bản giải nén: đã thực hiện riêng host
observation13 với289files, full unzip/hash/progress verification trước xóa.
ZIP/receipt giữ nguyên, source/runtime/warehouse không chạm. Host free giảm
169,52MiB, không tiết kiệm; dừng sau một target vì COW sharing không cho phép
suy logical bytes thành reclaimed space. [Kết quả/restore](./DAT-03_OVERNIGHT_20261026.md#thử-giữ-zip-và-xóa-bản-giải-nén-theo-yêu-cầu-mới),
[metadata](./DAT-03_ZIP_SPACE_TRIAL_20261026.json). Những mô tả5directories
hiện diện bên dưới là lịch sử trước trial; observation13 hiện chỉ giữ ZIP.

[Phiên tiếp tục đêm26/10](./DAT-03_OVERNIGHT_20261026.md): owner cho phép bật
lại9containers cũ;9healthy và idle/lease preflight đạt. Phiên tiếp theo từ72
đã accepted87/288 đến2007-03,918.168canonical/bridge. Phiên gần nhất thêm8tháng,
exit0/2173,924s/own release; [metadata087](./DAT-03_CATALOG_CHECKPOINT_087.json).
Mốc79 giữ [metadata riêng](./DAT-03_CATALOG_CHECKPOINT_079.json). Năm host observations
cũ13/20/27/45/63 đã qua ZIP verification rồi restore độc lập bằng COW; toàn bộ
original paths/file bytes/modes/progress đều khớp. Chỉ xóa ZIP trung gian sau
restore proof, giữ receipt; [compaction](./DAT-03_HOST_COMPACTION_20261026.json).
Giữ observation72,
source inputs và container runtime. Host physical space có lúc xuống dưới4GiB;
đã COW437byte-identical host copies, hashes/metadata giữ nguyên và reporter079
đạt lại. Batch2006-08–2007-03 đã accepted, vòng nối tiếp kiểm lại health/idle,
COW new host copy và reserve đạt rồi mở phiên kế từ87, max18tháng/budget6000s.
Không mở phiên mới dưới reserve hoặc đổi guard để chạy tiếp. Chưa mốc50%.

[Mới nhất — capacity đủ quy mô và catalog controller](./DAT-03_CATALOG_ROLLOUT.md):
hai SQL probes event/bridge43copies đều16/16PASS với4.527.126rows mỗi phía,
90s/query/512MB không đổi. Controller đã stage37original captures, chạy thật
72/288tháng (25%) Published đến2005-12. Phiên mới nhất đã terminal/acceptance/own
release; thêm9tháng qua2batch trong3097,855s, không timeout/pending. Có
774.260 canonical/774.260 bridge, [six pins và SHA receipt](./DAT-03_CATALOG_CHECKPOINT_072.json).
Source namespace/cutoff giữ nguyên, continuation có code/compatibility pins
riêng; không đổi deployment. Chưa288tháng/official export, không lấy synthetic
expansion hoặc checkpoint đầu thay full research acceptance. Code official
collection opt-in/registry/loader/result-scope đang nghiệm thu component theo
[contract](../specs/RESEARCH_COLLECTION_EXPORT.md); controller ML checkpoint
có code/tests nhưng chưa actual research export. Báo25%/50%/75%/100% catalog
tại72/144/216/288 accepted months, không dùng phần trăm này làm DAT-03 Done.
Mốc25% đã báo owner. Hậu kiểm sau phiên:9service healthy, không Java/DagRun/
Trino query active và lease trống. Host physical free sau copy5,5GiB; không
thay bằng logical volume free, không tự xóa dữ liệu để mở phiên tiếp.

[Mới nhất — phiên giảm tải theo approval riêng](./DAT-03_LIGHT_SESSION.md):
đã stop/start đúng4containers cũ,9service healthy,5service giữ nguyên ID/start/
config; không active run/lease/query. Full stress chạm query-memory512MB;
bucket0–2 đạt, bucket3timeout,12chưa chạy. Session450,547s, không data writes/
recreate/DAG mutation. Còn capacity/full catalog/export, giữ In Progress.
Checkpoint12:43 cùng ngày: read-only Docker check có quyền vẫn không kết nối
được daemon; chưa xác định nguyên nhân.9healthy là postflight cuối phiên đo,
không phải current health. Không tự restart Docker hoặc lặp stop/start;
offline `make ci-docs-base` và secret hygiene đã đạt, runtime chờ daemon trở lại.

[Chuẩn bị khối3/4 ngày25/10](./DAT-03_ROLLOUT_CAPACITY.md): branch mới từ main
`8ef6c41` sau PR #100 merge. Fresh25 JMA pins/288-month plan,12 original USGS
captures2023 (1.809 features gồm halo duplicates) có Java validation/SHA.
Read-only Trino capacity chưa đạt; owner duyệt backup/recreate riêng Trino,
profile mới late healthy nhưng SELECT1 mất56,44s, scheduler/DAG processor
đã từng unhealthy, sau đó tự healthy lại. Preflight đầu sau trial dừng trước
SQL, không tự đổi service khác. Phép đo full32copy mới sau readiness vẫn
BLOCKED_TIME90s, postflight unchanged/no active queries. Chưa có full rollout
controller, monthly catalog writes hoặc official collection export.
Ở đợt chỉ chỉnh Trino, probe16bucket dừng tại bucket0/time limit; host swap-in/
out thực đo~29/28MiB/s. Đợt ấy không stop service khác; phiên giảm tải theo
approval bổ sung được nghiệm thu riêng ở link mới nhất bên trên.
Giữ In Progress, không chuyển Done từ source capture/planner hoặc component tests.

[Bước 3/4](./DAT-03_COLLECTION_BOUNDED_E2E.md) bổ sung collection v1 có
whole-mainshock assignment/global Mc và local Parquet tests, không official
EXPORTED. Bounded E2E thật được nghiệm thu riêng; task vẫn In Progress cho
full catalog/export/ba máy. Các đoạn dưới giữ lịch sử trước đợt này.
Mốc trước recovery: bước4 mới đạt2000-01/2011-04;2016-04 đã COMMITTED nhưng Trino restart làm
verify EOF,2023-12 chưa chạy. Owner chọn giữ lỗi/bàn giao trước; own lease
còn giữ, không tự phục hồi/release. Source runs khác sẽ bị lease chặn.

**Mốc nghiệm thu25/10 — [Khối2 PASSED](./DAT-03_CONTINUATION.md):** đủ4cases
với124.969 canonical/125.069 bridge/124.856 serving cộng dồn; first/rerun,
retention, six pins và SHA/SQL chain đạt. Attempt đầu publish2016 rồi OOM-kill
Trino tại verify2023. Owner duyệt backup/recreate riêng Trino, memory budget
và exact checkpoint recovery; không replay Silver/Gold. Final acceptance đạt,
own lease đã RELEASED, không Java/DagRun active; original failures vẫn giữ.
Task vẫn In Progress cho full225+63 tháng/capacity/official export/ba máy.
Những đoạn mô tả failed/lease bên dưới là lịch sử tại mốc cũ, không trạng thái
lease hiện tại. Không đổi evidence cũ thành PASS.

Ngày25/10 owner yêu cầu làm lại **khối 1**: chẩn đoán và chuẩn bị read-only
preflight theo [runbook recovery](../runbooks/DAT-03_RECOVERY.md). Không chạy
khối 2 verify/publish/rerun, không release lease hoặc đổi failure receipt cũ.
[Evidence khối 1](./DAT-03_RECOVERY_PREFLIGHT.md): live preflight13/13 metadata
queries đã đạt lúc08:32 ngày25/10; exact source/context/commit/six pins/own lease
và quiescence khớp. Chưa root-cause/capacity acceptance, chưa phục hồi phase.

[Kết quả mới nhất bước 1/2](./DAT-03_RESEARCH_PROFILE_ACCEPTANCE.md): parser
v1.2 sửa đúng 18 token có fixture/hash và warning; đủ 25 archive2000–2024,
4.793.337 accepted/0 parser rejects, có RSS/timing/source pins và nguồn biên
2024. Kế hoạch batch từng tháng đã ghi; chưa full ETL capacity hoặc research
acceptance. Những profile/runner số đo dưới đây giữ là lịch sử trước đợt này.

[Evidence scoped replay / strict-split / resource](./DAT-03_TECHNICAL_PROGRESS.md)
ghi chi tiết implementation, 60 test nhóm mới/consumer, reader probe 2011 thật
và blocker mới từ 18 depth tokens trong archive 2016. Phần dưới giữ evidence
giai đoạn pilot/profile đầu, không phải thông báo mọi gate mới đã đạt.

- Resolver v2 opt-in không replay toàn JMA history trong mỗi batch; vẫn fresh
  verify toàn pins, giữ guard 100k và đóng cả old/new source-link graphs.
- Strict split opt-in loại window thiếu coverage, giữ ID/reason/interval và
  report hash trong export identity; default legacy không thay.
- [2011 reader probe](./DAT-03_JMA_2011_REPLAY_PROFILE.json) cho thấy tháng 3/4
  riêng đạt 56.015/57.044 rows (có halo), gộp bị guard 100k chặn như thiết kế.
- [2016 profile cũ](./DAT-03_JMA_2016_PROFILE.json): 313.487 physical, 18 rejects
  `INVALID_NUMBER` ở depth; giữ làm baseline trước sửa. Bản
  [v1.2](./DAT-03_JMA_2016_PROFILE_V12.json) giữ đủ 313.487 accepted, 0 rejects,
  18 warning rows, archive SHA không đổi. Không sửa evidence cũ thành PASS.
- Ở mốc kỹ thuật trước bước3/4 chưa có collection/sharding; mốc hiện tại đã
  có preparation/component tests, chưa full225+63 months, official collection
  MLI-02 export hoặc receipt hai máy thành viên. Pilot kit/pins vẫn nguyên.

## Handoff pilot đạt trên máy chuẩn bị

Source đúng pilot DAT-02 đã nghiệm thu, không synthetic: pilot ID
`pilot_196b6d4081d347e1c289e331022658fbbe572dd33181ad987900895c1f1b189c`.
Original five-file archive SHA
`b53cd7bfdc3768c3fd0ee0bcc5e037ef1c9c9b007d0df900ec223c8dc139bc3e` giữ nguyên.
Kit mới có 10 file: 5 data/metadata + README/requirements/verifier/loader/handoff.

- ZIP 55.408 bytes, SHA `f58291b72873e1b26791fef97818fdcf5e2b028a5dfa528885a666d8cdc1b870`.
- Handoff SHA `9121d6839f9ef8e5646a06cd0bf870fcde50b65da00c03e7b268760237cddc9a`.
- Giải nén thư mục mới, standalone verifier và semantic loader đạt:
  TEAM_PILOT_VERIFIED, 279 rows/2 mainshock, PARTIAL, research_ready=false.
- Receipt HoaiTam tạo lúc `2026-10-24T07:48:41.370979+00:00`; tự khai tên theo
  owner đang điều khiển máy, không xác thực danh tính hoặc reviewer approval.
  [Receipt metadata](./DAT-03_HOAITAM_RECEIPT.json) lưu đúng kết quả thực tế.
- Chưa có receipt ThanhTris/rosy179; không coi ba lần chạy local là ba máy.
- Không upload cloud, không đổi ACL; local paths không phải download URL công khai.

## Archive profile thật, read-only

Tái dùng captures h2000.zip/h2023.zip đã có ở deployment checkout, không fetch
hoặc thay hai archive này. Bổ sung capture h2011.zip từ URL đã pin trong inventory:
HTTP200, 7.955.061 bytes, Last-Modified 09/01/2025 06:10:38 GMT khớp inventory;
chỉ lưu staging để profile, chưa tạo BronzeReady hoặc đổi source production.
Java 21.0.11 target 17, exact SHA + ZIP CRC/width/count và
shared JMA parser. Chỉ ghi metadata, không source/lake writes hoặc fit model.

| Chỉ số | h2000 | h2023 |
|---|---:|---:|
| Compressed bytes | 2.892.423 | 6.977.812 |
| Member bytes | 10.666.799 | 24.930.940 |
| Physical records / parser accepted | 109.967 / 109.967 | 257.020 / 257.020 |
| Parser rejected | 0 | 0 |
| Missing magnitude | 4.432 | 9.973 |
| Pre-Mc research-filter rows | 104.696 | 246.914 |
| Pre-window mainshock rows | 14 | 10 |
| Local scan wall time (time -p, hai process có thể overlap) | 2,25s | 2,98s |

Profile bổ sung 2011: 309.527 records / accepted, 0 rejected, 5.696 missing
magnitude, 303.338 pre-Mc filter rows, 27 pre-window mainshock rows; compressed
7.955.061 bytes, member 30.024.119 bytes, local scan 4,23s. SHA
`30980319b7fb72101c508e16b44a9e1ad124f4a94a9c31b0260df1df6745729b`.
Tháng 3/4 UTC có 56.015/57.043 raw accepted rows. Đây không phải thống kê
canonical cuối hoặc benchmark Spark/Trino. Inventory **estimate** 24 ZIP năm
2000–2023 là 120.587.487 compressed bytes (~115 MiB), chưa phải actual measured
bytes của mọi năm hoặc đủ archive biên 2024/USGS/cache/warehouse.

Raw-archive profile reports có thể đối chiếu:
[2000](./DAT-03_JMA_2000_PROFILE.json), [2011](./DAT-03_JMA_2011_PROFILE.json),
[2023](./DAT-03_JMA_2023_PROFILE.json). Runner SHA
`5bc918240e587027f31af819dcc48cb5697b6526c71537ebd05cc640b73caf38`.

Archive SHA:

- h2000: `268441420d9c90b1cb96f42934aefdb107a4d12c5947adafbfeb24af597d15c2`.
- h2023: `e5ced2bf7275825ba75405b071bb54e9d4c2a5eb55aa6bc9b8d670de1f58b98f`.

Counts reconcile physical = accepted + rejected. Profile không dedup/canonical/
quality/Mc/window nên **không** là final eligible count, complete-month proof,
published catalog hoặc scientific evidence. SHA/CRC không xác thực nguồn mới;
tool giữ source_authenticity_verified=false/coverage_verified=false.
Kết quả có rows tháng 12 UTC năm trước vì native JST; không gọi là full UTC year.

## Chưa hoàn tất / next work DAT-03

- Đủ archive profiles/biên và parser depth fix; còn đo Spark/joins/lake/export
  memory/disk/ETA và ước lượng lại effort từ actual E2E.
- Scoped replay/strict-split/collection preparation có code/test; bounded
  real-source runtime/source proofs ghi riêng ở evidence bước3/4, không thay
  full-period research proof hoặc official collection export của DAT-03.
- Backfill thật 225 + 63 tháng; Gold coverage/quality/Trino và real MLI-02 export.
- Release nghiên cứu và đủ receipt ba máy. Estimate 16h ban đầu, không SLA.
- EXP-02/03 official fit/evidence, EXP-04/05 scientific evaluation, MLI-04/MLQ-01
  report/E2E chưa được nghiệm thu bằng kit hoặc profile này. SEC-01 cloud chưa làm.

## Kiểm thử

- Team-kit synthetic tests: 14 PASS, gồm source đổi bytes sau validation không
  được rehash thành kit hợp lệ; Java profile tests: 6 PASS, 0 skipped.
- Java scoped regression cuối: JmaArchiveValidatorTest 5 + JmaFixedWidthParserTest
  31 + JmaResearchProfileJobTest 6 = **42 PASS**, 0 failures/errors/skips.
- Local package build (`-DskipTests package`) đạt, không tính thành test suite.
- Task/index, week3 historical accounting, week5 lanes và MVP accounting checks đạt.
- PyArrow in cảnh báo sandbox sysctl CPU-cache trên macOS nhưng loader/tests
  vẫn PASS; không tắt hash/schema guard để bỏ warning.
- Không build/pull/start/restart/init Docker, không đổi DAG/lease hoặc ghi lakehouse.
- Full pilot regression cuối: 16 loader/staging + 14 team + 46 harness + 19
  baseline + 23 adaptive = **118 PASS**, make exit0; fresh kernel dùng quyền loopback.
- CI policy **40 PASS**; docs regression **10 PASS**; week5 policy **19 PASS**,
  Java build-input policy **13 PASS**; contracts/planning checks PASS.
- Docs cuối: **231 files / 2091 links / 0 problems**; git diff --check và source/
  example secret hygiene PASS. ZIP/raw/receipt staging được gitignore; chỉ
  metadata evidence vào Git. [Release metadata](./DAT-03_TEAM_RELEASE.json).
- Lần chạy sandbox đầu lỗi kernel bind và treo cleanup: không tính PASS; đã
  dừng đúng hai process test local và chạy lại bằng loopback được phép. Không
  đụng process người dùng hoặc container. Có ResourceWarning ZMQ không làm fail
  lần chạy được phép; không suy đây là benchmark/stability acceptance.
