# DAT-03 — Chạy official ML từ Gold hợp nhất

Đọc [task DAT-03](../task/tasks/DAT-03.md),
[contract](../specs/PREPARED_ML_EXPORT.md),
[Gold acceptance](../evidence/DAT-03_PORTABLE_GOLD_20261115.md) trước.
Controller chỉ orchestration/metadata; Java/Spark xử lý ETL/feature/lifecycle.

## Setup và offline tests

- Cần runtime sẵn healthy: scheduler/PostgreSQL/MinIO/REST/Trino; Java 17/Spark,
  đúng hai Iceberg 1.10.1 JARs và source guard pins của phần 2.
- Không active Java/DagRun/Trino query/foreign lease. Không build/pull/start/
  restart/init/recreate/reset, sửa cron/DAG/credential/ACL hoặc xóa data.
- Host reserve trước prepare >=12 GiB; trước mỗi phiên >=10 GiB; actual reserve
  4 GiB trong phiên. Check này không bảo đảm đủ capacity; cần preflight thật.

```bash
make test-dat03-prepared-ml
make test-research-team PILOT_PYTHON=staging/venvs/dat02/bin/python
./mvnw --batch-mode --no-transfer-progress -pl spark -am -DskipTests package
python3 -B scripts/dat03-prepared-ml.py --help
```

Loader cần `pyarrow==21.0.0` trong venv đã chuẩn bị, không cài global. Component
fixtures không là actual research release. Không chạy Spark fixtures song song
ETL/preflight thật. Package JAR một lần trước prepare, không rebuild trong test.

## Prepare

V2 yêu cầu chạy benchmark synthetic trước, **chỉ khi runtime idle**:

```bash
python3 -B scripts/profile-dat03-40k.py --help
```

`--folder` phải là root UUID mới dưới staging, parent đã tồn tại. Tên script giữ
để tương thích; `--max-candidates 90000` chọn exact profile90k đã được owner duyệt,
default40000 giữ profile cũ. Tool đo nguyên cửa sổ bằng Java 17/Spark, full feature materialization/readback và
rerun; không ghi S3, không là research/scientific approval. Giữ receipt và lấy
external SHA đã nghiệm thu. Không chạy lại cùng folder sau failure; chẩn đoán
nguyên nhân và giữ evidence trước khi quyết định phiên kiểm chứng mới.

`prepare` yêu cầu `--folder` UUID mới, `--evidence` file phần 2 đã commit và
`--evidence-sha256` lấy từ external trusted commit, không từ một bundle tự rehash.
Thêm `--profile` trỏ receipt đo thật và `--profile-sha256` là SHA đã nghiệm thu;
runner hiện tại phải khớp runner SHA trong receipt. Scaling v2 được pin theo đúng
profile40k/90k, không thay các scale/window/Mc hoặc row/shard caps khác.
Kiểm first publication + rerun accepted/byte SHA, copy JAR/controller/scaling vào
operation mới, pin dependencies/guard code. Không gọi Java hoặc ghi lake ở bước này.

Operation **v1 lịch sử** của owner ngày 15/11:
`staging/dat03-prepared-ml/46cead88e7de414daafeaa17a7379cb2/`,
request SHA `f50bf60f671a907fc335e8dc19962ea7874e4ed0ff733950bd2c7fb898c2119e`.
Hai preflight v1 đã dừng sạch với `BLOCKED_RESOURCE`, không ML writes. REPRODUCTION
max 38.854/window; EXTENSION max 83.504/window. Cap 40.000 được duyệt **chưa đủ cho
EXTENSION**. Không chạy build trên operation này hoặc sửa các request/assets v1.
Đây là path/pins thực của máy owner, **không copy sang máy khác rồi gọi ready**.
Runtime root tương ứng dưới `/opt/pipeline/staging/dat03-prepared-ml/`.

V2 metadata đã prepare sau measured40k PASS ngày15/11:
`staging/dat03-prepared-ml/c4bd824ce3a741a78ae3f13ff0019a1f/`, request SHA
`aaeaab957fc0017dedce9f3f58c44c9ac558d066b248b30d09ab088b956b1720`.
[Evidence](../evidence/DAT-03_PREPARED_ML_20261115.md) pin profile/runner/config.
Chưa fresh v2 preflight/build; không gọi operation này ready chỉ vì đã prepare.
Không rebuild/đổi assets rồi tiếp tục cùng request. EXTENSION >40k còn cần quyết
định profile riêng trước khi có thể hoàn tất paired release. **Note40k này là lịch
sử**, đã có owner duyệt và measured90k bên dưới; không sửa request40k cũ.

## Continuation90k ngày15/11

Benchmark `staging/dat03-90k-profile/94acc0aeb7ef4f93bdb581f0d30d82bb/` đã PASS:
90.000rows,205,959giây,peak RSS1.236.388KiB, nguyên window/readback/rerun/null guards.
Receipt SHA `52474ede77429ec7364ad4d952440c3d857d703345fd50cd79c629fb50897ec3`.
Operation mới `staging/dat03-prepared-ml/d02942d588154ca0abb74c63919358ee/`, request
SHA `883bba6e4d7d5b1554e1338c4528f21ac7754dcf84d677a67b8bb71628105dd5`.
Không rebuild/đổi frozen assets hoặc sửa lịch sử40k để dùng90k. Fresh preflight
và official build cần acceptance riêng, không lấy benchmark làm EXPORTED.

## Preflight rồi mới build

Không retry operation v1 để vượt guard. Các lệnh sau dùng đúng operation90k đã
prepare trên máy owner. Chạy tuần tự, kiểm actual terminal acceptance trước
mở phiên tiếp theo; không suy từ command example rằng job đã đạt:

```bash
python3 -B scripts/dat03-prepared-ml.py preflight --folder staging/dat03-prepared-ml/d02942d588154ca0abb74c63919358ee --split REPRODUCTION --seconds 6000
python3 -B scripts/dat03-prepared-ml.py preflight --folder staging/dat03-prepared-ml/d02942d588154ca0abb74c63919358ee --split EXTENSION --seconds 6000
```

- `PREFLIGHT_PASSED`: actual source/Gold/coverage/audit/Mc/window/resource gate đạt,
  không ML writes; metadata có capacity plan, exclusions và counts.
- `BLOCKED_RESOURCE`: không build split đó. Gửi exact blocked-window IDs/counts/
  limit, không sửa report/flags, lấy subset hoặc tăng cap để gọi là full dataset.
- Nonzero/timeout/unknown: giữ lease và logs, chỉ chẩn đoán an toàn. Không chạy
  lại lệnh như retry hoặc release lease bằng tay.

Chỉ khi preflight **của split tương ứng** đã accepted `PREFLIGHT_PASSED` thật mới
build split đó; paired official release vẫn cần cả hai split đạt. Ví dụ cú pháp
(controller từ chối build nếu split chưa có exact preflight PASS):

```bash
python3 -B scripts/dat03-prepared-ml.py build --folder staging/dat03-prepared-ml/d02942d588154ca0abb74c63919358ee --split REPRODUCTION --seconds 6000
```

Nếu `CHECKPOINTED`, process/query idle và receipt/acceptance đúng, cùng lệnh build
có thể tiếp tục prefix: giữ exact own lease/request/runner/config, không đổi code.
Nếu session trước failed/unaccepted, controller chặn; không bypass bằng folder mới.
Chỉ sau DATASET_EXPORTED/final readback/release mới build EXTENSION tương tự.

## Rerun và nhận biết thành công

```bash
python3 -B scripts/dat03-prepared-ml.py rerun --folder staging/dat03-prepared-ml/d02942d588154ca0abb74c63919358ee --split REPRODUCTION --seconds 6000
```

EXTENSION chạy tương tự sau lần export đầu của split đó. Cần:

- Đủ 10/10 step, final_readback DATASET_EXPORTED/quality PASSED/invalid=0,
  exported=true/trusted_handoff_verified=true và counts nonempty.
- Same dataset/collection IDs, mainshock/candidate/registry snapshot IDs,
  trusted/portable manifest và checksum SHA; `reused=true` trên rerun.
- Gold six refs giữ nguyên, services IDs/start/restart không đổi, không active
  Java/DagRun/query; own lease release sau capture. CHECKPOINTED không release.
- Host sessions có intent/runtime report/execution/candidate/acceptance với SHA
  liên kết. Private stdout/stderr 0600, không gửi lên Git/chat hoặc Drive.

Progress ghi step/ordinal trên 10 bước; số SQL trong phase có nhánh runtime nên
không đoán “A/tổng” trước khi biết tổng thực. Audit/window preflight không là
training; không có model fit hoặc scientific approval trong controller này.

## Sau cả hai exports

Actual 15/11: cả REPRODUCTION/EXTENSION đã EXPORTED/rerun/all-shard loader PASS;
311 lifecycle/preflight/rerun SQL + 13 independent readbacks, Gold six pins giữ
nguyên. Paired kit/fresh restore/Drive restore và HoaiTam receipt đạt. Đọc
[hướng dẫn nhận release thật](DAT-03_OFFICIAL_RESEARCH_HANDOFF.md) với exact
ZIP/handoff hashes; không chạy lại exporter để tải dataset cho máy thành viên.
DAT-03 vẫn In Progress vì chưa có receipt thật ThanhTris/rosy179.

Chỉ capture **portable indexed files** và trusted export receipts, không copy
warehouse/source archives. Verify all shards bằng `notebooks/research_collection.py`
với portable manifest/checksum pins từ final_readback (không dùng trusted registry
manifest SHA thay portable SHA). Đóng paired kit theo
[contract](../specs/RESEARCH_TEAM_KIT.md) và [README nhận](DAT-03_RESEARCH_TEAM_README.md).
Giữ ZIP SHA/handoff SHA ngoài kit; không tạo receipt giả cho hai người khác.

`python3 -B scripts/capture-dat03-prepared-ml.py --help` cung cấp capture tool
chỉ đọc cho đúng operation/session/split, external acceptance SHA và output mới
ở ngoài operation. Tool kiểm report/execution/candidate chain,10/10/final trusted
handoff, source lease empty, file allowlist/size/SHA và host reserve trước copy
từng file được index; không copy source/warehouse/private logs. Capture metadata
ghi `semantic_loader_verified=false`; phải chạy loader độc lập sau đó. Không
retry một destination đã partial hoặc tự rehash để thay external pins.

Phần còn thiếu hiện thuộc **DAT-03 — Chuẩn bị catalog nghiên cứu thật và bàn giao
dataset dùng chung**: hai máy ThanhTris/rosy179 tự nhận/verify cùng release và
gửi receipt. Actual exporter/loader/rerun/release đã đạt theo evidence mới;
Gold Published hoặc preflight riêng lẻ vẫn không thay các acceptance này.
