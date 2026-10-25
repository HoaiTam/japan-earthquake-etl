# DAT-03 — Tiến độ thực tế ngày 24/10/2026

Branch `feat/dat-03-research-dataset-handoff` từ origin/main `bc0ce9e` (#98).
Assignee HoaiTam, reviewer unassigned. **In Progress**. Phần pilot/profile đã
được owner commit, HEAD khi tiếp tục bước3/4 `258710d`, trên [PR #99](https://github.com/HoaiTam/japan-earthquake-etl/pull/99).
Phần kỹ thuật tiếp theo ở working tree, chưa tự commit/push.
Owner chọn gửi pilot thật dùng trước trong lúc chuẩn bị full research catalog.
Owner xác nhận hai thành viên chưa chạy verifier và yêu cầu tiếp tục kỹ thuật;
chốt strict split, loại window thiếu PRE/POST coverage và báo chi tiết từng mainshock.

## Cập nhật kỹ thuật tiếp theo

[Bước 3/4](./DAT-03_COLLECTION_BOUNDED_E2E.md) bổ sung collection v1 có
whole-mainshock assignment/global Mc và local Parquet tests, không official
EXPORTED. Bounded E2E thật được nghiệm thu riêng; task vẫn In Progress cho
full catalog/export/ba máy. Các đoạn dưới giữ lịch sử trước đợt này.
Bước4 mới đạt2000-01/2011-04;2016-04 đã COMMITTED nhưng Trino restart làm
verify EOF,2023-12 chưa chạy. Owner chọn giữ lỗi/bàn giao trước; own lease
còn giữ, không tự phục hồi/release. Source runs khác sẽ bị lease chặn.

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
