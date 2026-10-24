# DAT-03 — Kết quả bước 1/2: depth parser và archive/resource profile

Ngày 24/10/2026; branch `feat/dat-03-research-dataset-handoff`, tiếp tục PR #99
từ HEAD `6356a44`. Các thay đổi đợt này chưa tự commit/push. Assignee HoaiTam,
reviewer unassigned; **DAT-03 In Progress**, không research release/approval.

## 1. Depth 2016: đã sửa có version, không bỏ 18 record

- ZIP SHA giữ nguyên `2c438d8189fd9e76ac0b05c0587c7a8ee2611e6ecbf21d5a5bca7887dab360da`.
- [Trước sửa](./DAT-03_JMA_2016_PROFILE.json), parser slv-03-v1.1:
  313.487 physical = 313.469 accepted + 18 INVALID_NUMBER.
- [Sau sửa](./DAT-03_JMA_2016_PROFILE_V12.json), parser **slv-03-v1.2**:
  313.487 physical = 313.487 accepted + **0 rejected**, 18 warning rows.
  Missing magnitude vẫn 5.834; pre-Mc filter 307.500 → 307.518; pre-window
  mainshock vẫn 9. Đây không là final canonical/Mc/eligible count.
- [Regression metadata](../../tests/fixtures/jma/depth-blank-2016-v1.json)
  giữ đủ 18 physical line/record SHA/token/expected value; test đối chiếu exact
  identities với report mới. Không chép raw row/archive vào Git.

[JMA format](https://www.data.jma.go.jp/eqev/data/bulletin/data/format/hypfmt_e.html)
quy định depth-free dùng F5.2, precision code 1. JMA không giải thích riêng
blank-control mode. [Fortran numeric input](https://www.intel.com/content/www/us/en/docs/fortran-compiler/developer-guide-reference/2025-1/general-rules-for-numeric-editing.html)
phân biệt BN (bỏ blank) và BZ (blank thành zero);
[F editing](https://www.intel.com/content/www/us/en/docs/fortran-compiler/developer-guide-reference/2025-1/f-editing.html)
đặt 2 chữ số sau dấu thập phân nếu không có decimal point. Project suy ra:

| Token depth | BN, chia 100 | BZ, chia 100 | Quyết định project |
|---|---:|---:|---|
| `  0 4` | 04 / 100 = 0,04 | 00004 / 100 = 0,04 | Chấp nhận với precision 1, thêm warning |
| `  1 4` | 14 / 100 = 0,14 | 00104 / 100 = 1,04 | Vẫn INVALID_NUMBER, không đoán |

Rule chỉ nhận regex `  0 [0-9]` ở depth-free; không thay decimal parser chung,
trailing blank, negative depth, null, depth-slice/fixed hoặc optional fields khác.
Giữ warning `DEPTH_BLANK_ENCODING_BN_BZ_AGREE`, raw hash/key/revision/lineage
nguyên bản. Đây là **diễn giải của project**, không xác nhận JMA sửa nguồn.

[Reader probe 2016](./DAT-03_JMA_2016_REPLAY_V12.json) trên cùng ZIP/parser:
April 42.821 unique; May 39.308 (có halo); combined 82.128 PASSED. Không bằng
tổng hai số vì union halo có giao nhau. Thời gian 9.851/2.918ms lần đo này,
không benchmark ổn định. Shared reader không còn chặn 18 token đó; probe chưa
phải resolver/source completeness, quality/canonical/Gold acceptance.

## 2. Toàn bộ archive 2000–2023 và nguồn biên 2024

[Metadata/pins/counts/measurement đầy đủ](./DAT-03_RESEARCH_ARCHIVE_PROFILES.json).
Tải mới tuần tự từ exact HTTPS URLs chính thức, lưu HTTP headers, final URL,
Last-Modified, nullable ETag, size, SHA/release vào staging riêng. 25/25 metadata
khớp inventory quan sát; bốn ZIP đã profile trước vẫn cùng SHA. Không overwrite
captures/report cũ, không ghi Bronze/Silver/Gold.

| Số đo | Thực tế |
|---|---:|
| ZIP năm 2000–2024 | 25; 2024 chỉ nguồn biên/halo |
| Compressed bytes | 128.784.513 (~122,82 MiB) |
| Member bytes, stream, không extract lưu toàn bộ | 464.953.689 (~443,41 MiB) |
| Physical / accepted / rejected | 4.793.337 / 4.793.337 / 0 |
| Warning rows / missing magnitude | 18 / 152.825 |
| Accepted UTC rows trong REPRODUCTION, 225 months | 3.127.011 |
| Accepted UTC rows trong EXTENSION, 63 months | 1.361.003 |
| Tháng nhiều accepted rows nhất trong research period | 2011-04: 57.043 |
| Tổng wall time 25 Java scans tuần tự | 50,938 giây |
| Heap cap mỗi process / peak RSS lớn nhất (macOS time -l) | 512 MiB / 271.089.664 bytes (~258,53 MiB) |

Counts trước quality/dedup/canonical/Mc/window, **không** phải ML features hay
số event độc lập sau source linking. Các tháng có rows không tự chứng minh
coverage. Tổng 4.793.337 gồm phần ngoài split (đặc biệt phần lớn năm 2024),
không đưa tổng đó vào báo cáo như kích thước dataset nghiên cứu đã phát hành.

Runner SHA khi đo/probe:
`6b0fe06442982c5b7fbd90526a786120e30e641223db027e5e29cf4dddcf5de5`.
Java 21.0.11 target17; build local, không thay deployed runner. Measurements
không gồm download, Spark joins/dedup/canonical, S3, Iceberg, Trino, ML/export.
Không dùng 51 giây để ước lượng thời gian full ETL hoặc fit model.

### Biên thời gian

Baseline inventory v1 SHA
`61dc2c73f1bb9cee48fba851cbb941cc62c893201c36db583cdd55ae19f8199a`
không đổi. Overlay `dat03-boundary-1.0` SHA
`0274a4bf5b452f5fb3f33c6e65a6180af73e9725c71fa0392e6d7c6c5c147dc1`
thêm h2024 đã pin, không append ngầm vào default v1.

JST 2023 kết thúc `2023-12-31T15:00Z`, thiếu 9 giờ để đủ EXTENSION UTC.
h2024 có **186 accepted rows thuộc 2023-12 UTC**, first
`2023-12-31T15:00:04.580Z`. Vẫn lưu nguyên ZIP, chỉ chọn UTC month/halo lúc
replay. Research kết thúc `2024-01-01T00:00Z`; không mở ML window sang năm 2024.
Source-link halo 16s và graph closure có thể lan scope; không tuyên bố buffer
9 giờ tự đủ cho mọi dependency. Runtime chưa nối overlay và chưa có source proof.

[Shared reader probe h2024](./DAT-03_JMA_2024_BOUNDARY_REPLAY.json) chọn đúng
2023-12 UTC: **186 selected/unique revisions**, PASSED, không bỏ qua native JST
year validation. Đây là reader-only trên bytes local, không ghép h2023/USGS,
không source completeness hoặc Gold acceptance. Lần đo này chịu tải host,
không dùng elapsed_ms làm benchmark/ETA.

## 3. Kế hoạch rollout an toàn — chưa chạy backfill

1. Chọn **orc-06-v2 tường minh**, combined inventory/source pins có version.
   Default v1 và deployed files không đổi. Chỉ 1 run ETL tại một thời điểm,
   khởi đầu 1 target UTC month/run. Không gom cả năm vào RAM/operation.
2. Guard 100k selected unique revisions, 64 pins, 24 affected months,
   32 MiB/raw và registry 480 months giữ nguyên. Probe 2011 March/April gộp
   vẫn BLOCKED; month lớn nhất 57.043 là raw accepted count, không bảo đảm
   closure + USGS + revision giữ dưới 100k. Nếu closure vượt cap: **dừng**,
   báo exact expanded months/rows; không truncate/ép scope hoặc clear để pass.
3. DAT-03 cần actual bounded E2E trước full rollout: 2011-04 (peak),
   2016-04 (parser fix), 2000-01 (left UTC edge), 2023-12 + h2024 + USGS
   (right boundary/cross-source); verify reconciliation/six snapshots/Trino
   và rerun trước khi mở tiếp. Chưa chạy những E2E này trong đợt profile.
4. Disk preflight trước capture ~10 GiB, sau 25 captures ~9,8 GiB free;
   staging mới ~131 MiB gồm metadata. Parser peak RSS không là Spark driver/
   warehouse budget. Chưa đo growth Bronze/Silver/Iceberg/checkpoints/export,
   nên **chưa phê duyệt capacity hoặc ETA full 288 months**. Operator đo disk
   trước/sau các E2E nhỏ, giữ checkpoint, replan effort từ số đo; không xóa
   dữ liệu/volume để lấy chỗ hoặc tăng limits mù. Không load 25 ZIP cùng lúc.
5. MLD-05 exporter vẫn cap 128 mainshock/100k feature rows. DAT-03 tiếp theo
   phải thiết kế collection/sharding có full-assignment proof, global pinned
   Mc và strict split; không gọi batch-month hoặc profile là research release.

## 4. Lặp lại và kiểm thử

Các lệnh cụ thể ở [runbook profile/rollout](../runbooks/DAT-03_RESEARCH_PROFILING.md).
Capture là bước network riêng; tests/profile không tự Docker/build/download.
Kết quả thực tế: **90 Java tests** qua hai lượt (regression88 + inventory2),
**12 Python capture/evidence**, **41 CI policy**, **10 docs regression**,
19 week-plan và 13 build-input tests PASS. Contracts/status checks đạt; build
local package thành công, không tính thành test. Các lượt lặp không cộng lại
vào số unique tests. Không full Java suite, remote CI hoặc reviewer approval.
Lưu profile v1.1 bên cạnh v1.2, không sửa lịch sử thành PASS.
Docs cuối 234 files/2143 links/0 problems, diff và source/example secret hygiene
đạt; không có .env ở checkout này nên không nghiệm thu runtime credentials.

DAT-03 còn collection/sharding, actual source/Bronze → Gold/Trino full
225+63 months, MLI-02 research export và ba máy handoff. EXP-02/03 official
experiments, EXP-04/05 scientific evaluation, MLI-04/MLQ-01 report/E2E giữ
gates riêng; SEC-01 cloud phải mở lại trước Colab/Drive migration.
