# DAT-03 — Catalog recovery 27/10/2026

Branch `fix/dat-03-catalog-publication-recovery` từ main `fe80830` (#104).
Giữ nguyên các thay đổi CAS/storage local trước phiên này, không tự commit/
push/merge. Assignee HoaiTam, reviewer unassigned; DAT-03 **In Progress**.

## Failure được giữ nguyên

- Catalog `b6db356fb30e46f9a26a419ebacb5393`, continuation
  `bc29aca1e816437884a34eb17de19d29`, cutoff2026-10-25T06:13:04.747Z.
- Last accepted101/288 đến2008-05,1.064.869canonical/bridge. Session lỗi
  `e6ae76995e064b858e2a93e444c5e1b9`: exit1, timed_out=false,2206,938s.
- Batch06–11/2008: Silver balanced72.922 observations/canonical/memberships,
  Gold COMMITTED1.137.791event/bridge; chưa verify/publish/checkpoint accepted.
- SQL client `TRINO_QUERY_OUTCOME_UNKNOWN method=GET page=86`, query
  `20261026_162850_00113_4k5jd`; lần read-only trước Docker shutdown đã thấy
  FAILED/ABANDONED_QUERY. Không kết luận sleep, OOM hoặc root cause từ mã này.
- Failed host observation `runtime-observation-85bb26dbb5854654850caff144c667f6`,
  manifest SHA2ef81203e8b4659c040cddb0ade1b127847406260f7b99b303841e90c473bf29.
  Latest accepted không nâng từ observation lỗi này.

## Preflight và thực thi

Owner yêu cầu bắt đầu lại27/10. Mở Docker Desktop, start đúng9containers cũ,
không build/pull/init/recreate/reset. Cả9 healthy; own lease đúngrun, không
Java/DagRun/Trino query active. Tạo attempt
`231e61a839df4e5ca81477381af1bf6e` mới, giữ nguyên source/context/failed files.

Read-only preflight Java **PASS13/13 metadataSELECT**,19,991s, peakRSS509.984KiB,
exit0/no timeout. Bronze raw/manifest, Silver bundle/registry, first-four phase
chain và parent/six pins khớp. Không ETL hoặc publication trong preflight.

Sau preflight, actual `verify → publish` của batch06–11/2008 đã **PASS**:

- Session mới `42ced5be7ad54958b9f5c88bbabb9a5d`, exit0/no timeout,
  1679,196s (~28phút), peakRSS516.476KiB. Đây là Java recovery RSS, không
  phải toàn bộ RAM của stack hoặc capacity full-period.
- 186SQL statements thật; verify140checks/publication148checks, gồm các
  derived exhaustive bucket sums được ghi đúng kind, không gọi tất cả checks
  là SQL độc lập. Six current refs/identity và serving readbacks đạt.
- Source/Silver/Gold commit không replay; Gold journal SHA
  `6361638cf3dab562a89219ac97401b76b111babdd97a7a81bec7e972990d7781`.
  Publication `pub_6361638cf3dab562a89219ac97401b76b111babdd97a7a81bec7e972990d7781`.
- Original failed session/files giữ nguyên, acceptance mới tham chiếu failure.
  Required4service IDs/start/restarts unchanged; own lease RELEASED, idle.
- [Checkpoint107](./DAT-03_CATALOG_CHECKPOINT_107.json): **107/288 (37,153%)**
  đến2008-11,1.137.791canonical/bridge. Phần HoaiTam06/2008–07/2013 đạt6/62tháng;
  không tính những tháng của phiên đang chạy vào accepted prefix.

Recovery JAR SHA `fe0f9fc7fb70d84825c4e3bfd1e8b73320b5c1d2bc6d1090a87baf612cc1dd9c`;
mọi entry base runner30a3d191 giữ nguyên, chỉ thêm một class. Recovery request
SHA `1712de37551fc3c5663e7bacca6ed40880816ef2b28ccc1afe8d71cf83c2a363`.

## Host capture lỗi rồi phục hồi chỉ đọc

Host CLI lần đầu exit1 **sau runtime acceptance/release**, vì CAS từ chối file
mới `session-resolutions/e6ae76995e064b858e2a93e444c5e1b9.json` với
OBSERVATION_UNEXPECTED_RUNTIME_FILE. Không đổi runtime execution/acceptance thành
lỗi, không rerun verify/publish hoặc ETL để chữa host copy.

Thêm allowlist chỉ nhận `session-resolutions/<32hex>.json` có regression;
`.env`, nested path, invalid ID và latest.json vẫn bị từ chối. Xác minh PID13018
đã chết và exact own host-lock token,9healthy/idle/lease-empty, failed-file/
recovery acceptance SHA chain trước bước xử lý lock. Lưu nguyên lock ở attempt
`failed-host-capture-lock.json` (SHA
`05dee65ef9edc4f1494bbede02c255b8079074f662bc0df6569b0b4bb66f42c6`),
không xóa hay takeover foreign/source lease. Đây là phục hồi operator có chủ
đích; generic capturer vẫn giữ lock khi lỗi và không auto takeover.

Fresh read-only capture/reporter thành công:

- Observation `runtime-observation-17688094741c4dd0942bbf8f04ad78f6`;
  manifest SHA `162572216ebc049c1ef0589782c3728f9e1268d97913c0e7daadc5e37e32918b`.
- Chỉ chuyển475.226bytes/17new payloads; logical view710.982.491bytes. Không
  copy lại toàn bộ history, không suy logical size thành reclaimed physical space.
- Latest accepted107 đã publish, host writer lock vắng, quiescence/services
  trước/sau unchanged. `accepted-progress.json` và capture-recovery receipt
  giữ trong exact attempt; không đưa private logs/JAR/data vào Git.

## Phiên tiếp theo đã mở

Sau checkpoint107 và fresh preflight, mở phiên tối đa18tháng còn lại của
đợt24:12/2008–05/2010, Java6000s/host7200s, dừng ở checkpoint sạch nếu thiếu
budget; không cam kết cả18tháng xong trong một phiên. Continuation mới
`739e2ebafecd4da4a0a7b3cfd127b525` pin controller có failure-resolution guard.
Đối chiếu mọi JAR entry với base30a3d191: **0 entry khác bytes**; package SHA
mới `407607bebc51011d9e868d5d45cb9e8e93c12259263ddd06e2c60ff9b5c35568`
do đóng gói, không đồng nhất hai SHA khác nhau. Source/cutoff/namespace không đổi.
Own lease release ở trên thuộc recovery; phiên mới acquire lease riêng theo
cùng catalog owner. Chỉ cộng tháng mới sau terminal acceptance/host report.
Actual session mới `5b45bd078c7e46bf8fce7575e0a0b660`, continuation request SHA
`5ee706d5cc96cd27688acbd4652ada3bc1fe77d71ced78f01f47e9af86a1bbe9`;
đã qua readiness/bronze/silver và đang gold, một Java process/đúng own lease,
chưa execution hoặc acceptance. Đây là process status tại lúc bàn giao,
không chứng nhận batch mới Published. Drive vẫn giữ immutable partial101;
chưa đóng gói/upload bản107 hoặc các tháng active.

## Kiểm thử đã chạy

- Local Maven offline package: PASS sau sửa lỗi compile tên VERSION ambiguous.
- Python `test_research_catalog*.py`:31tests PASS (catalog/progress/recovery),
  storage28 và ML-export16tests PASS; tổng75tests liên quan.
- Java `ResearchCatalogRecoveryJobTest,TrinoSqlClientTest`:9tests PASS; không
  SparkSession/fixture ETL. Mock tests không thay actual runtime acceptance.
- CI planner/workflow/suite regression:42tests PASS; recovery Java thuộc nhóm
  ORC-07, Python recovery chạy trong docs CI. 26files thay đổi tại mốc kiểm
  ban đầu không có secret findings; không đọc/in credential vào output.

## Còn lại

**DAT-03:** full288 months, official collection export và ba máy receipts.
**SEC-01:** cloud recipient identity/receipts. Không đổi scientific flags hoặc
coi batch/catalog recovery là hoàn tất DAT-03.
