# DAT-03 — Profile và kế hoạch rollout catalog thật

## Trạng thái / dùng để làm gì

Bước 1/2 ngày 24/10/2026 đã có parser fix và profile **25 archive thật**;
[kết quả/pins](../evidence/DAT-03_RESEARCH_PROFILE_ACCEPTANCE.md). Đây là input
planning/debug, không dataset gửi nhóm hoặc BronzeReady/source completeness.
Task [DAT-03 — Catalog thật/team handoff](../task/tasks/DAT-03.md) vẫn In Progress.

Business parsing dùng Java; Python chỉ đọc inventory, capture bytes, pin/check
metadata và gọi Java tuần tự. Không Python ETL, không tự build/download trong
profile/test, không bật Docker hoặc đổi deployed runner/DAG/lease.

## Các lệnh đã có

Từ root checkout DAT-03, JDK local phải phù hợp project; test dùng Java21 target17.

```bash
make test-research-profile
make plan-research-profiles
```

Plan báo 25 archive, 128.784.513 observed bytes, không network hoặc tạo files.
Mặc định giữ root đã dùng `staging/dat03-research-profile-20261024`.

**Capture tường minh** (gọi mạng, không Bronze/ETL):

```bash
make capture-research-profiles
```

Chạy lại reuse chỉ khi existing capture receipt, inventory, size và full SHA
khớp. Không HEAD/re-download khi reuse, không tuyên bố nguồn còn mới nhất.
Muốn kiểm release mới, chọn **root mới** rồi capture; giữ nguyên bytes/pins cũ.
Ví dụ subset được kiểm soát, không wildcard tự đoán source:

```bash
make plan-research-profiles RESEARCH_PROFILE_ROOT=staging/dat03-next-profile RESEARCH_PROFILE_YEARS=2011,2016,2024
make capture-research-profiles RESEARCH_PROFILE_ROOT=staging/dat03-next-profile RESEARCH_PROFILE_YEARS=2011,2016,2024
```

Mỗi ZIP <=32 MiB, HTTPS exact inventory URL, 90s request timeout, kiểm final
URL/status/type/length, Last-Modified/ETag/retrieval/SHA. Header drift giữ explicit
`inventory_metadata_drift=true` để review, không sửa baseline hoặc lặng lẽ repin.
Capture lỗi giữ folder/partial bytes; không overwrite/cleanup/retry mặc định.
Chọn root khác cho lần mới và giữ artifacts lỗi để chẩn đoán.

**Profile local**: cần `spark/target/japan-earthquake-etl-runner.jar` đã build
đúng code. Lệnh không tự build JAR hoặc tải source:

```bash
make profile-research-archives
```

Một Java process/ZIP, `-Xmx512m`, validate SHA/ZIP CRC/member/96-byte và dùng
shared parser. Ghi `profile-<runnerSHA>.json`, measurement/RSS theo process và
summary; runner khác được file mới, không overwrite evidence. macOS đo RSS bằng
`/usr/bin/time -l` (bytes, không KiB), có thể cần quyền đọc sysctl trong sandbox;
OS khác giữ `peak_rss_measured=false`, không giả số đo. Lệnh trực tiếp chọn Java:

```bash
python3 scripts/profile-research-archives.py profile --root staging/dat03-research-profile-20261024 --java /Users/hoaitam/Library/Java/JavaVirtualMachines/azul-21.0.11/Contents/Home/bin/java
```

## Nhận biết thành công, và những gì chưa được chứng nhận

- Capture: `event=dat03_capture_pinned`; đủ receipt của từng year, SHA/size khớp.
  Báo drift=false cho cả 25 lần ngày 24/10, không phải chữ ký xác thực JMA.
- Profile: `event=dat03_profiles_complete archives=25`; từng report
  structural_validation=PASSED, physical=accepted+rejected, parser_rejected=0.
  HTTP200 hoặc exit0 một mình không đủ: đọc cả report/reasons/flags.
- Parser v1.2: 18 depth warnings của năm 2016, hash/line/token khớp fixture;
  không xóa warning hoặc thay raw record. Các ambiguous token vẫn reject.
- Summary ghi **research_ready=false / coverage_verified=false**; không dùng
  month counts hoặc min/max làm COMPLETE_MONTHS, không gọi là Gold Published.

## Rollout kế tiếp — chỉ sau gate tương ứng của DAT-03

| Batch / đầu vào bắt buộc | Kiểm gì trước khi mở rộng |
|---|---|
| 2011-04, exact source pins + halo | Peak month; row/memory/disk guard, reconciliation, six snapshots và Trino |
| 2016-04, parser v1.2 | Giữ các record sửa thuộc month/halo, warnings/lineage và quality; đối chiếu đủ 18 ở year profile, không giả tất cả ở tháng 4 |
| 2000-01 | Left UTC source boundary, không tự suy absence là empty month |
| 2023-12 + JMA2023/2024 + USGS overlap | 9h biên JST/UTC, closure/halo/source linking; không đưa 2024 ngoài split vào ML |
| Rerun từng batch | Reuse pin/publication, không duplicate hoặc rewrite partition ngoài scope |

Chỉ một target month/run, một ETL run tại một thời điểm. Closure có thể mở
affected scope; nếu vượt 100k/24-month guard phải dừng và ghi nguyên nhân,
không thu nhỏ closure/truncate records để pass. Probe reader gộp 2011-03/04
đã bị guard chặn; không làm yearly batch mặc định.

Overlay 2024 hiện dùng cho local capture/profile, chưa nối runtime. Trước E2E
phải version/pin combined inventory và BronzeReady receipts đầy đủ. Không
tự đổi environment của stack hoặc lấy synthetic QA thay source thật. Dữ liệu
operational PROVISIONAL không làm ML input.

Disk còn ~9,8 GiB sau captures không là capacity full ETL đã nghiệm thu. Đo
growth từng tầng/peak memory của các E2E trên, lập dự phòng và effort còn lại
rồi mới quyết full rollout. Chưa đo thì không báo ETA và không chạy toàn kỳ.
Không xóa captures/warehouse/volumes để vượt preflight.

MLD-05 cap128 mainshock/100k feature rows vẫn giữ. DAT-03 tiếp theo cần
collection/sharding giữ global pinned Mc, full-assignment proof và strict split,
rồi full225+63-month Gold/Trino → MLI-02 real export. Sau release chính thức,
cả ba người phải verify cùng identity trên máy riêng; pilot receipt không thay
research receipt. EXP-02/03/04/05 và MLI-04/MLQ-01 giữ scope nghiệm thu riêng.

## Test/check đợt này

- 90 Java tests PASS qua hai lượt: regression88 (parser34, integration6,
  quality12, validator5, research profile7, reader probe3, scoped resolver21)
  và inventory2; không full Java suite.
- **12 Python control-plane/evidence tests PASS**, toàn bộ offline/mock; reserve
  tính worst-case 32 MiB/missing ZIP, không dùng size hint có thể drift để cho qua.
- 41 CI policy tests, 10 docs regression, contracts/status/secret-hygiene và
  git diff --check PASS; docs cuối 234 files/2143 links/0 problems. CFG check
  chỉ source/example vì checkout này không có .env; không runtime credential audit.
- Build local `-DskipTests package` PASS; không gọi là test hoặc Docker build.
- Profile/replay thật chỉ đọc ZIP local, không tạo source/Gold acceptance.
- [Bàn giao branch/PR description](./DAT-03_BRANCH_HANDOFF.md) ghi checks/docs
  cuối và commands stage có chọn lọc. Không tự commit/push/merge.
