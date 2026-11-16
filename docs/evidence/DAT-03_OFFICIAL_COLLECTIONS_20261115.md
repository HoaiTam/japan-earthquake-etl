# DAT-03 — Profile90k và hai official collections, 15/11/2026

## Trạng thái

**In Progress**, HoaiTam, reviewer unassigned. Owner đã duyệt thiết kế/đo profile
90.000/window và tiếp tục hoàn thiện hai collections, giữ nguyên science/các
guard khác. Đây là continuation PR #131, không phải task hoặc PR mới.

**Hoàn tất kỹ thuật:** cả hai collection EXPORTED/rerun/all-shard loader PASS;
311 SQL lifecycle/preflight/rerun và 13/13 readbacks độc lập đạt. Paired immutable
kit/fresh-process restore/Drive fresh download verifier cũng đạt. Giữ In Progress
vì hiện chỉ có receipt thật máy HoaiTam, còn ThanhTris và rosy179.

[Evidence JSON](DAT-03_OFFICIAL_COLLECTIONS_20261115.json) là nguồn exact SHA,
counts và accepted sessions. Không gọi profile/preflight hoặc một phase đang
chạy là official EXPORTED. Không tự scientific approval hoặc tạo hộ receipt.

## Resource profile và tính bất biến

- `dat03-whole-window-90k-v1`, runner SHA
  `648d7779835ce8673a72d7d47852f68fa9b402da07a9a724f7d62be9a117e2b0`.
- Java17.0.19/Spark3.5.9, actual benchmark 90.000rows nguyên một window:
  **205,959giây**, peak child RSS **1.236.388KiB (~1,18GiB)**, Parquet2.304.543bytes.
  Candidate fingerprint so guard cũ, materialize/readback, immutable rerun và
  2nullfaultinjections PASS; không S3 writes, không model fit.
- Receipt SHA `52474ede77429ec7364ad4d952440c3d857d703345fd50cd79c629fb50897ec3`.
- Giữ nguyên Mc/MAXC, selection M>5,5/depth50–200km, window math/strict split,
  4-D vector/scales/tolerances, 100.000rows/shard,128mainshocks/shard,64shards
  và4.096mainshock metadata. Chỉ opt-in version/scaling/resource cap thay đổi.
- So 19 class bytecode audit/window/feature/planner/lifecycle giữa runner40k
  đã nghiệm thu và runner90k: **0 class thay đổi** trong danh sách kiểm.
  Resolver chọn resource profile và wrapper được regression riêng. Không suy
  resource profile tạo feature thành capacity/approval cho HDBSCAN fit.
- Giữ mọi request/assets/evidence10k/40k. Không sửa report BLOCKED cũ thành PASS.

## Actual preflight — cả hai đạt

Operation `d02942d588154ca0abb74c63919358ee`, request SHA
`883bba6e4d7d5b1554e1338c4528f21ac7754dcf84d677a67b8bb71628105dd5`.
Source/Gold pins dùng [phần2 đã Published](DAT-03_PORTABLE_GOLD_20261115.md).

| Split | Coverage | Mc | Mainshocks | Candidate memberships | Shards |
|---|---:|---:|---:|---:|---:|
| REPRODUCTION | 225tháng | 0,5 | 194 | 156.710 | 2:85.972/70.738rows |
| EXTENSION | 63tháng | 0,4 | 45 | 233.664 | 4:33.413/83.504/95.242/21.505rows |

Hai preflight `PREFLIGHT_PASSED`, mỗi split9/9SQL, exit0/own lease release;
Gold six pins unchanged. MAXC Mc/configSHA, counts và toàn bộ strict exclusion
report khớp v1. Resource-blocked windows không bị drop; strict temporal exclusions
vẫn6/3, không clip hoặc lấy cross-split rows. Benchmark không thay các gate này.

## Official build / handoff

Chạy tuần tự trong namespace ML riêng `iceberg.dat03_ml_d02942d588154ca0abb74c63919358ee`.
Controller gọi10fixed steps MLI-02 và actual lifecycle/exporter; không tự viết
registry markers. Session max6000giây, phase mới cần reserve3600giây; checkpoint
giữ exact own lease, chỉ resume cùng split/build. Failure/unknown giữ evidence/
lease, không retry mù. Không build/pull/start/restart/recreate hoặc đổi credential.

Capture chỉ đọc exact indexed Parquet/manifests từ accepted EXPORTED session,
external acceptance/report/execution/candidate SHA và byte bounds; không lấy
warehouse/source ZIP/private logs. `PORTABLE_CAPTURED_BYTES_VERIFIED` chưa là
semantic loader acceptance. Sau đó chạy all-shard loader, immutable rerun,
paired packer và verifier trên máy owner; không tạo receipts hai người khác.

REPRODUCTION first build đã accepted `DATASET_EXPORTED`, session
`96f220088af84e759d73cc6627de4903`,10/10steps,117SQLcalls,2365,85giây,
exit0/ownlease release.194mainshocks/156.710features/2shards,qualityPASSED/
invalid0/trustedhandoffverified,Goldsixpins giữ nguyên. Acceptance SHA
`672315b0527465b6580989955341b6261d70f71e30c63d3b54f80a9919deb3ef`.
Đây là acceptance của split REPRODUCTION, không dùng nó thay acceptance EXTENSION.

Independent rerun REPRODUCTION đã accepted, session
`2d70c5b03ec04aa7a4b18b3aa7699959`,24SQL chỉ SELECT, cùng pins/manifest/checksums,
reused=true, ownlease release. Capture8indexedfiles/16.075.558bytes và actual
offline loader đủ2shards/156.710rows trên máy HoaiTam đã PASS.
Loader lần đầu chặn `PILOT_SCHEMA_TYPE_MISMATCH:candidate_time_utc`: frozen
writer dùng INT96, Arrow trả naive ns. Đã thêm explicit reader protocol
`prepared-spark-utc-int96-v1` có proof code_version648d…/UTC anchor/physical type/
microsecond alignment/epoch roundtrip;32loader/team/interop tests PASS. Không
rewrite bundle, rehash pins hoặc đổi source science để pass. Receipt thật lưu
ngoài bundle; không tạo receipt hai thành viên khác.

EXTENSION session `6793fe303a0e495d97b591e358303f58` đã accepted CHECKPOINTED
tại 7/10 bước, 57 SQL, 2.621,362 giây, exit0/no timeout. Đủ 45 mainshocks,
233.664 features/4 shards; Gold six pins giữ nguyên. Controller giữ đúng own
lease, tiếp tục cùng split/build/pins sau kiểm tra idle và SHA chain. Checkpoint
này không phải EXPORTED; original receipt/evidence được giữ nguyên.

Continuation `8a236e7744cb4450af623aad5599feba` đã accepted `DATASET_EXPORTED`,
10/10 bước, 71 SQL, 1.115,196 giây, exit0/own lease release. EXTENSION đủ
45 mainshocks/233.664 features/4 shards, quality PASSED/invalid0/trusted handoff
verified; Gold six pins unchanged. Tổng first build gồm checkpoint + continuation
là 128 SQL/3.736,558 giây; không replay 7 bước đã accepted.

EXTENSION rerun session `ad39bd5439674b99874772a40f10f7ed` accepted10/10,
24SQL SELECT-only/230,678giây, reused=true; snapshot IDs/portable và trusted
manifest/checksum SHA giữ nguyên, own lease release. Acceptance SHA
`fa741270d20c7354e34775c65ec55998575b8563d27ed9cd703b1d980d64e12e`.
Capture12indexedfiles/21.340.756bytes và loader đủ4shards/233.664rows PASS.

## Independent readbacks và release đã bàn giao

13/13 SELECT readbacks độc lập kiểm six Gold refs, hai current/pinned EXPORTED
registry rows, count/unique-grain/mainshock candidate sums/global Mc và exact
ML snapshots. Min/max candidate/mainshock timestamps của từng split khớp view
UTC Arrow với Trino. Đây là kiểm tra bổ sung cho decoder INT96; không rewrite
Parquet, nâng cap khác hoặc tự sửa registry/lifecycle flags.

Paired kit `jma-research-team-20261115-v2.zip`: **37.523.195bytes/27files**,
390.374features/239mainshocks/6shards. ZIP SHA
`0d287f035c80e975d83efdbb36e65ad818d620791361d954ac4d6ef8e7afe099`;
handoff SHA `725a00305e44b904579f4f43f6d25321342fd42f2138d2b0d99ece1cf046fa13`.
Pack lần hai byte-identical/reused=true; fresh restore + shipped verifier trong
isolated Python process PASS. Candidate ZIP trước giữ nguyên, không promote;
V2 chỉ sửa README về lệnh chạy từ thư mục cha/receipt không chứa `..`, không
đổi collection bytes/pins. Test fresh-process cũng dùng command pattern này.

[Drive ZIP](https://drive.google.com/file/d/1cT4uNq7-Zgh8fPeMXglzaGJH350mcuDK/view)
và [folder4files](https://drive.google.com/drive/folders/1wPMvwuUcmTvxXvTSnwd4ov9ait-D_Vh1)
đã upload immutable, so exact remote file set/size/SHA256, fresh download và
fresh isolated verifier đủ6shards PASS. Scan27file gồm decoded Parquet values,
configured secret values và metadata/support code đạt0findings. Không chứa
source ZIP/warehouse/private logs/.env/token; metadata ba file cũng scan sạch.
4files có5user permissions/canDownload=true, không anyone/domain, không đổi ACL;
đây không là identity/roster acceptance hoặc SEC-01 Done.

[Hướng dẫn hai máy/expected hashes/lệnh setup](../runbooks/DAT-03_OFFICIAL_RESEARCH_HANDOFF.md).
Local và fresh Drive receipt đều của **cùng một máy HoaiTam**, không tính thành
hai người nhận. Final runtime0Java/DagRun/Trino query, source lease empty,
5existing services healthy;4tracked IDs/start/restarts khớp first preflight.
Không build/pull/start/restart/recreate hoặc đổi cron/credential/ACL.

Đã dọn đúng4copies tạm do phiên này tạo sau full restore/semantic verifier:
150.078.512logical bytes; cả hai ZIP gốc, portable captures, mọi receipt/evidence
và source/warehouse/pins giữ nguyên. Copies có thể khôi phục từ pinned ZIP còn
lưu/Drive. Physical free sau dọn14.920.376.320bytes, không lấy logical free của
Docker VM thay host reserve.

## Kiểm thử và giới hạn

- 52 Java metadata/regression tests + package PASS.
- 26 Python controller/Gold/capture tests PASS; 32 loader/team/interop tests PASS.
- 42 CI policy/workflow/suite tests PASS; docs links/task status/whitespace PASS.
- Actual benchmark và preflights ở trên; không dùng fixture/pilot thay full input.
- [Contract](../specs/PREPARED_ML_EXPORT.md), [runbook](../runbooks/DAT-03_PREPARED_ML.md),
  [paired kit](../specs/RESEARCH_TEAM_KIT.md).

Còn thiếu thuộc **[DAT-03 — Chuẩn bị catalog nghiên cứu thật và bàn giao dataset
dùng chung](../task/tasks/DAT-03.md)**: receipts thật của ThanhTris và rosy179
chạy cùng release trên máy riêng; không tạo hộ trên máy owner. Reviewer vẫn
unassigned. **[SEC-01 — Review secret và bề mặt truy cập](../task/tasks/SEC-01.md)**
còn identity/recipient/cloud acceptance, không tự đóng từ scoped upload scan.
**[EXP-02 — Chạy Window, DBSCAN và HDBSCAN global](../task/tasks/EXP-02.md)** /
**[EXP-03 — Xây HDBSCAN adaptive và giải quyết multi-sequence](../task/tasks/EXP-03.md)**
còn official scientific runs/evaluation/capacity fit; không suy từ feature
benchmark90k thành model fit capacity hoặc scientific approval. Toàn task DAT-03
không Done sớm, nhưng actual build/export/loader/rerun/release ở trên đã đạt.
