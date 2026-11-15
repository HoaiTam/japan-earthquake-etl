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

`--folder` phải là root UUID mới dưới staging, parent đã tồn tại. Tool đo
40.000 candidates bằng Java 17/Spark, full feature materialization/readback và
rerun; không ghi S3, không là research/scientific approval. Giữ receipt và lấy
external SHA đã nghiệm thu. Không chạy lại cùng folder sau failure; chẩn đoán
nguyên nhân và giữ evidence trước khi quyết định phiên kiểm chứng mới.

`prepare` yêu cầu `--folder` UUID mới, `--evidence` file phần 2 đã commit và
`--evidence-sha256` lấy từ external trusted commit, không từ một bundle tự rehash.
Thêm `--profile` trỏ receipt đo thật và `--profile-sha256` là SHA đã nghiệm thu;
runner hiện tại phải khớp runner SHA trong receipt. Scaling v2 được pin với cap
40.000, không thay các scale/window/Mc hoặc row/shard caps khác.
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
định profile riêng trước khi có thể hoàn tất paired release.

## Preflight rồi mới build

Không retry operation v1 để vượt guard. Các lệnh sau dùng đúng operation v2 đã
prepare trên máy owner; **chưa được chạy trong lần bàn giao này**. Chạy tuần tự;
biết trước EXTENSION có hai cửa sổ vượt 40.000 nên không thể export với cap này:

```bash
python3 -B scripts/dat03-prepared-ml.py preflight --folder staging/dat03-prepared-ml/c4bd824ce3a741a78ae3f13ff0019a1f --split REPRODUCTION --seconds 6000
python3 -B scripts/dat03-prepared-ml.py preflight --folder staging/dat03-prepared-ml/c4bd824ce3a741a78ae3f13ff0019a1f --split EXTENSION --seconds 6000
```

- `PREFLIGHT_PASSED`: actual source/Gold/coverage/audit/Mc/window/resource gate đạt,
  không ML writes; metadata có capacity plan, exclusions và counts.
- `BLOCKED_RESOURCE`: không build split đó. Gửi exact blocked-window IDs/counts/
  limit, không sửa report/flags, lấy subset hoặc tăng cap để gọi là full dataset.
- Nonzero/timeout/unknown: giữ lease và logs, chỉ chẩn đoán an toàn. Không chạy
  lại lệnh như retry hoặc release lease bằng tay.

Chỉ khi preflight **của split tương ứng** đã accepted `PREFLIGHT_PASSED` thật mới
build split đó; paired official release vẫn cần cả hai split đạt. Ví dụ cú pháp
(v2 hiện chưa có preflight PASS nên controller sẽ từ chối build):

```bash
python3 -B scripts/dat03-prepared-ml.py build --folder staging/dat03-prepared-ml/c4bd824ce3a741a78ae3f13ff0019a1f --split REPRODUCTION --seconds 6000
```

Nếu `CHECKPOINTED`, process/query idle và receipt/acceptance đúng, cùng lệnh build
có thể tiếp tục prefix: giữ exact own lease/request/runner/config, không đổi code.
Nếu session trước failed/unaccepted, controller chặn; không bypass bằng folder mới.
Chỉ sau DATASET_EXPORTED/final readback/release mới build EXTENSION tương tự.

## Rerun và nhận biết thành công

```bash
python3 -B scripts/dat03-prepared-ml.py rerun --folder staging/dat03-prepared-ml/c4bd824ce3a741a78ae3f13ff0019a1f --split REPRODUCTION --seconds 6000
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

Chỉ capture **portable indexed files** và trusted export receipts, không copy
warehouse/source archives. Verify all shards bằng `notebooks/research_collection.py`
với portable manifest/checksum pins từ final_readback (không dùng trusted registry
manifest SHA thay portable SHA). Đóng paired kit theo
[contract](../specs/RESEARCH_TEAM_KIT.md) và [README nhận](DAT-03_RESEARCH_TEAM_README.md).
Giữ ZIP SHA/handoff SHA ngoài kit; không tạo receipt giả cho hai người khác.

Phần còn thiếu thuộc **DAT-03 — Chuẩn bị catalog nghiên cứu thật và bàn giao dataset
dùng chung**. Gold Published hoặc preflight PASS chưa là official EXPORTED/release;
task không Done khi còn exporter/loader/rerun/receipts chưa được nghiệm thu thật.
