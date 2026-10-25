# DAT-03 — Full-scale Trino probes và catalog rollout,25/10/2026

Owner HoaiTam, reviewer unassigned; branch `feat/dat-03-research-rollout`.
Theo yêu cầu mới hoàn thiện DAT-03, tiếp tục từ commit0487a83 đã được owner
push; không commit/push/merge ở phiên này. [Contract](../specs/RESEARCH_CATALOG_ROLLOUT.md).

## Capacity SQL — đã vượt gate mới

Docker hoạt động lại với các service start05:50:37Z; agent không restart hoặc
lặp stop4. Read-only preflight đạt no active DagRun/Java/lease/Trino query.
Đây là thay đổi trạng thái quan sát được, không là kết luận root cause của
failed probes cũ. Giữ nguyên [failed evidence](./DAT-03_LIGHT_SESSION.md).

Profile288tháng có4.488.014 accepted JMA records; thêm1.809 USGS response
features gồm halo duplicates tạo upper bound4.489.823, **không** unique count.
105.282×43=4.527.126 stress rows mỗi phía, lớn hơn upper bound. Không lấy probe
32copies/3.369.024rows làm full-scale acceptance. Mọi query vẫn90s/512MB;
Trino profile/container/deployment không đổi trong ba probe sau.

| Attempt tại deployment/staging/dat03-capacity | Phép đo | Actual result |
|---|---|---|
| 52baaf9ac36b49099fc746a4c1e79204 | event32copies, v1 |16/16PASS,302,936s; postflight unchanged |
| e0fe9b46b8bb4007b270784629f800da | event43copies, v2 |16/16PASS,237,479s; postflight unchanged |
| e29f1e8ad41b49259e4d38ca314fba1d | bridge43copies, v2 |16/16PASS,199,856s; postflight unchanged |

V2 bổ sung exact table và overall sequential query-loop budget2400s để cấp
thời gian cho đủ16queries; **không nới limit90s của query đã fail**. Actual
hai probe đều<600s. Không run retry trên unknown query, không lake/lease writes.

| Proof | Request SHA256 | Receipt SHA256 |
|---|---|---|
| event32 | f47709713923d3f0b09272c0e12174c81e46a842d6fef4ca0b96c9e4a4725dba | 23143f273b24a4b9dff9cb89442568c86a1442f20932ac5d5ca423923663eedb |
| event43 | fd006dd1137c4484f0bffc6946a04ac8db3ab84febb523816b1d87e9718614da | 1113f7351d3dedb71da01d7255ea3794ce818ac0a3339c9ba9d7c9715ce42c09 |
| bridge43 | 0bee48878a8642ebc44f9cce27aa7be74375c298e42f517aad8a18b17afd3c92 | 0b2e5e48e6b533ebf3bcad7425d0be4710f00c5103ca774096b6b05ab63f93cf |

Synthetic expansion chỉ engineering capacity SQL, không là research dataset,
full Spark capacity, source coverage hoặc official ML acceptance.

## Catalog thật — checkpoint đầu đã đạt

Deployment staging root: `staging/dat03-catalog/b6db356fb30e46f9a26a419ebacb5393`.
Namespace `iceberg.dat03_catalog_b6db356fb30e46f9a26a419ebacb5393`.
Source cutoff cố định `2026-10-25T06:13:04.747Z`.

- Runner SHA `01f0ab40ab06ee4bceed2924bb47b8276dd84fcdf8d9fe2aecce25075e1f0500`.
- Controller SHA `61a60e4b5f6b8053ccaccb38c4e203733339267e0d9b10d0c3165062713298e1`.
- Request SHA `ad306633ac438b33276cfa3cf5e797482cc78e139410db3385863bbd3c9501fb`.
- Bootstrap37 original captures thành BronzeReady; không tải nguồn lại.
- Session `8c5fdf65afac4ac69c7eeac9e9c09eda`: exit0,138,008s, peak child RSS
  1.266.176KiB; result CHECKPOINTED1/288, no pending month,68reported checks.
 2000-01 có5.418canonical/5.418bridge, six snapshots, Published. Own lease
  release xác nhận; chưa đủ dữ liệu để suy ETA/capacity toàn kỳ.
- Result SHA `f12378a26295d0ae49c230bc29d93625396b13540f303f630249ac672580fafc`;
  execution SHA `747d6e67516877b14670c32268beda5e8df5f4626f16665654539501a655ff3c`.
- Session tiếp theo `7975286c4adb4eec8c4188568a27361d` đã acceptance:
  12tháng mới, tổng13/288 (`2000-01`–`2001-01`), pending=null, exit0,
  timed_out=false,4357,408s, child peak RSS1.420.472KiB. Own lease RELEASED.
  Request/source cutoff/code pins cũ giữ nguyên; không restart/deployed writes.
  Result SHA `dc7258fb6122080603eb85613e0a34a9e33f0ce928141771518fd60f69661e5f`,
  execution SHA `1470e06d93d4f8ddfa75ddd97f6e96ce2553af9441e2946eabc9fdd705e01918`.
  Last checkpoint SHA `348be798017a774b1922fdda11d7f909f6c7a9aa61c23b032b8344b862ed51f0`.
  Không lấy13tháng làm acceptance288 hoặc suy tuyến tính thành ETA toàn kỳ.

| Table | Snapshot của checkpoint2000-01 |
|---|---:|
| event_current |4322056432075520646 |
| event_source_bridge |2559531503479034857 |
| dim_date |4037472708441179252 |
| dim_region |2571049408238038190 |
| dim_magnitude_band |8491985348583396158 |
| dim_depth_band |2029080349348804829 |

## Tests và phần chưa hoàn tất

- Java targeted ResearchCatalogState/GoldOutsideBucket/ResearchUsgsCaptureCheck:
  **22PASS**, JDK21 compile release17, offline Maven. Local package đạt;
  không Docker build hoặc dependency download. Test TempDir canonicalize
  `/var` về real path trên macOS, không nới production symlink guard.
- Python research tests sau bổ sung catalog guards:104PASS ở mốc đầu; thêm
  worker success/failure/timeout và host budget tests, riêng catalog hiện11PASS.
  Những event trong mock tests không là runtime evidence.
- **DAT-03 — Catalog thật:** đang rollout; chưa288publications, chưa final
  coverage/Trino/rerun/resource acceptance trên full catalog.
- **DAT-03 — Official collection export:** chưa có real full-period export,
  collection-aware lifecycle/registry/loader/import acceptance hoặc research kit.
- **DAT-03 — Team handoff:** chưa có ba máy verifier receipts của cùng research
  release; không tạo hộ người nhận. Task giữ **In Progress**.

## Candidate continuation — batch đầu đang nghiệm thu

Để giảm lặp structural scan của25archives và số publication nhỏ, đã bổ sung
batch/cache/opt-in fact verifier theo contract. Không sửa source/data/default
runtime. Candidate mới tại deployment:
`staging/dat03-catalog-continuation/549a3d0aa5f949928403e5e9ee93dc7b`.

- JAR `48e106e19477cdb6cef9b8e4ce3f71d84e952ec7677149fe5bc53f6d81a8d6cc`.
- Controller `f250447bb7e893db0f49c32882c17862e34d10f27520ff2bc2a0fd9248c1681e`.
- Compatibility SHA `e19e418064329333b16efa3431533d328ea343d6f3a36d5b3c4f179744497af7`:
 13.668 original entries identical;7 changed catalog/cache/verifier classes,
 3added classes. Parser/canonical/transform/dependencies không đổi.
- Candidate trước `4ad5bc5525564a06bd6979203f4f8952` chỉ chuẩn bị host, chưa
  chạy và chưa có fact-blocker update; giữ nguyên, không dùng nhầm làm bản mới.
- 38targeted Java tests PASS, gồm state/batch/compatibility/cache, Gold outside/
  fact buckets/publication và operational-policy regression. Python catalog13PASS.
- Trino `EXPLAIN (TYPE VALIDATE)` cho5fact blockers bucket0 trên exact Published
  2000-01 snapshot: cả5exit0/Valid=true. Đây là syntax/type check read-only,
  không scan rows hoặc capacity acceptance. SQL được tạo bằng cùng canonical
  predicate rewrite đã unit-test; không ghi Gold/lease/deployment.
- Sau khi session cũ terminal/release, fixture integration
  FullMonthStateResolverTest21 + RealEtlAdapterTest8 = **29PASS**, JDK21/release17,
  heap768MiB, offline. Không dùng deployment/lake thật cho regression này.
- Candidate đã stage exact5files và chạy session `04ecb00c6c984d75be101ebb6142e7ed`
  từ13checkpoint, max7new months/budget6000s. Actual context target
  `2001-02`–`2001-08`, operation `orc07-30b16e455d1f69cce465d073568967f4dfab99d6`.
  **Đã terminal/acceptance**: total20/288,7new months, pending=null,
  exit0/timed_out=false,1082,979s, child peak RSS1.390.344KiB; own lease RELEASED.
  Cache25full structural scans/27hits; mọi lần đọc vẫn verify actual raw bytes/
  SHA/manifest/inventory và selected-month counts. Count cumulative189.842
  canonical/189.842bridge;148reported checks **của một batch**, không nhân7
  thành số SQL queries giả. Bảy month checkpoints dùng chung Published six pins.
  Result SHA `fdb2fa10523ac79aa102f4029466f6df26460898b0a6d3d2b6ecf93d4425dd0e`,
  execution SHA `d0774baa9ad5743f9b710a302a856e08785835f224bdb8b5b95624010de76cb4`.
  Continuation request SHA `35548638451a038615a9346927e2a89dfd6d9f60d01dd6a398e1d7ae706fd90e`.
- Artifact mtime deltas trên volume của batch đầu: readiness0,068s/bronze0,040s/
  Silver76,049s/Gold257,185s/verify418,938s/publish314,951s. Đây là wall-time
  quan sát phiên này, không internal profiler hoặc ETA tuyến tính toàn kỳ.
  Chưa đo full-scale Spark fingerprint/uniqueness: không suy SQL stress PASS
  hoặc20tháng thành full Spark/export acceptance.

## Collection export — code opt-in, chưa actual full-period acceptance

[Contract mới](../specs/RESEARCH_COLLECTION_EXPORT.md) tách preparation và
official lifecycle; full-parent Iceberg pins, registry bounded lineage/shard,
external checksum loader và result scope bắt buộc. Không đổi default v1.
Python loader6PASS, harness regression46PASS, MLI-02 control-plane26PASS,
Java identity/registry/result contract19PASS. Fixture Spark đang nghiệm thu
bản cập nhật sau thêm parent/schema/config bindings; không ghi Done từ code.
Chưa có full Gold pin288months, official research bundle hoặc three-machine
receipts. Không chạy model/Colab, không cấp scientific approval.

## Continuation v2 —27/288 đã được nghiệm thu

Control folder `staging/dat03-catalog-continuation/bc29aca1e816437884a34eb17de19d29`:
runner `30a3d191be6e5a339c48ca1076341a5058063a2131a07739077132471dc4a880`,
controller `07e0118d4fa64013542054c8977ffa7b95fc09333c640e4ef0818ca9d03bbceb`,
compatibility `8caadf12f11ad068938e3937ecafc1ecfd3ac5a0723f8a274728576dd2b971f9`.
Original13.668 entries vẫn identical ngoài7 reviewed changes/3added; nguồn/
cutoff/parser/canonical/dependencies giữ nguyên. Candidate1840bcdb56464fcbbe26be9d33cb9415
failed ở host overlay khi input JAR đang build, chưa stage/lease/data writes;
giữ artifacts, bổ sung before/after input SHA guard, không sử dụng candidate đó.

Session `721206908a0f47f3a936d6bf63ff9d5b` đã terminal và accepted:

- 27/288 contiguous tháng2000-01–2002-03, thêm7tháng; pending=null.
- Exit0, timed_out=false,1173,357s; peak child RSS1.439.352KiB.
- Result `CHECKPOINTED`, batch policy v2/shuffle16/reserve3600s.
- Result SHA `d18b32c174f568a4fe8b0443fed3472ac19f970395eaeb9ea754dd2944327543`;
  execution SHA `9b790219f21836a093efc919cbd40bf853311b1a2b00a4f1f664821130ce5819`.
- Acceptance `CHECKPOINT_ACCEPTED`, own lease `RELEASED`; research_ready=false,
  official_exported=false. Không full catalog/ML export hoặc team acceptance.
- [Metadata đã đối soát](./DAT-03_CATALOG_CHECKPOINT_027.json) giữ six pins,
  accepted receipt SHA và262.675 canonical/262.675bridge;9,375% catalog.

## Spark capacity — cùng original writer, read-only

Probe runner pin `261336084037bf9be52e9a24a9bd89b6ea82b26e0a3acead0f8ac3622175d970`;
GoldIcebergWriter class SHA `bd97992e949c76a7b8f84521204ca6adab5f2f3b862748c8de256422cc2db934`
khớp original. Namespace bounded cũ,105.282rows x43=4.527.126 mỗi fact table,
vượt upper bound4.489.823; synthetic chỉ engineering stress, không research data.

| Attempt trong staging/dat03-spark-capacity | Actual outcome |
|---|---|
|c69a5702807642cb9139f9f933bf18b0 |event/shuffle1 OOM,exit52,196,862s; giữ failed evidence |
|3a67e545282d4a7d8f8edd73f896832d |event/shuffle16 PASS,325,943s,RSS1.213.728KiB |
|82d30c8302624cf889878a1957e6fda9 |bridge/shuffle16 PASS,216,037s,RSS1.158.312KiB |

Cả hai PASS có count/unique đúng4.527.126, hai lần fingerprint khớp. Giữ
local[1]/heap768MiB/direct256MiB; chỉ shuffle16 opt-in. Không ghi lake/lease,
postflight healthy/quiet và service identity/start/restart unchanged.
Không chứng nhận full ETL joins, real full catalog hoặc ML capacity từ stress.

## Chia đợt theo yêu cầu owner

Ngày25/10 owner yêu cầu commit từng đợt khoảng3h và báo25%/50%/75%/100%
catalog. Giữ <=2h/phiên runtime, mốc72/144/216/288 accepted months; xem
[kế hoạch](../runbooks/DAT-03_BATCH_EXECUTION.md). Hiện27/288=9,375%; không
đánh dấu mốc25% trước khi có acceptance thật. Không tự push/merge.

Trước commit nền:57targeted Java tests PASS (gồm2tests của dataset runtime
đang để local riêng),21Spark collection/export fixture tests PASS,
catalog14/progress4/capacity4/rollout22/MLI-02control26/loader6/harness46/
CI-impact21Python tests PASS. Harness cần loopback; lần sandbox bị chặn bind,
rerun cùng test với quyền loopback đạt46/46, không sửa test để né lỗi.
`make ci-docs-base` PASS; docs248files/2240links0problems, task-status77files
đồng bộ, git diff whitespace đạt. Các con số fixture không là full research acceptance.

## Resume preflight — interpreter cache, trước mọi ETL

Lần resume sau commit nền d96abde dừng trước acquire/worker: Python trước đó
tạo `__pycache__/research-e2e.cpython-313.pyc` trong continuation khiến exact
file-set inspector reject. Read-only check xác nhận lease trống, vẫn27months.
Không tạo failed Java session, không đổi source/continuation five pinned files.

Host inspector sửa để chỉ chấp nhận directory cache không symlink, <=128KiB/
file và tên pyc đúng hai source đã pin; mọi extra file khác vẫn reject.
Worker bootstrap dùng `-B -X pycache_prefix=/dev/null` để không đọc/write cache
cũ, luôn compile đúng source bytes đã hash. Không xóa cache, không thay runner
hoặc nới input SHA guard. Hai regression tests kiểm generated cache/foreign
file/tampered JAR/symlink; resume chỉ sau fresh health/idle/pin checks.

## Control-plane ML export — chỉ component, không runtime acceptance

Trong lúc catalog active, hoàn thiện `ResearchDatasetBuildJob` và
`scripts/research-ml-export.py`, không chạy thêm Spark fixture hoặc ML job
song song trên máy này.12Python mocks PASS,2Java pure config/full288 rejection
tests đã PASS trong targeted57 trước khi mở ETL; CI-impact21 và docs249files/
2245links0problems. Không dùng mock full288/EXPORTED làm research evidence.
[Runbook](../runbooks/DAT-03_RESEARCH_ML_EXPORT.md) mô tả roots/pins/budget,
own lease checkpoint, unknown-outcome stop và final independent handoff gate.

## Guard bổ sung trong khi phiên catalog chạy

Host run/resume kiểm4GiB physical free trước Docker cho cả catalog và ML export;
không dùng logical free trong VM thay host reserve. Các phiên host mới lưu
host wall/free before-after riêng, không retroactively thêm vào phiên cũ.
Catalog mocks hiện19PASS (gồm low-host-disk và success/timeout host observation),
ML export mocks14PASS (thêm boolean count reject và host-disk preflight).
Không đổi active continuation/code pins, restart hoặc xóa artifacts.

Full Airflow discovery477tests lần đầu có20failure/57errors ở QA fixture
paths qua `/var` alias của macOS. Files QA liên quan unchanged từ0487a83;
rerun **cùng code** với TMPDIR=/private/tmp đạt477/477 trong22,229s. Không
nới production path guards hoặc đổi fixture expectations. Những mock log
`pipeline_qa_started` không là DagRun/lake writes thật.
