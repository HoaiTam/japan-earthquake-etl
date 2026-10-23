# ORC-07 — Chạy và kiểm chứng real ETL

## 1. Có gì và dùng khi nào?

[Contract](../specs/REAL_ETL_ADAPTER_CONTRACT.md) mô tả actual phase chain,
source lease, retry và boundary của ORC-01/03 cũ. Không bật daily real chỉ vì
unit tests xanh. Coverage chưa đủ tháng phải fail trước Gold, không hạ gate.

## 2. Kiểm tra offline

Từ repository root, JDK17/21 và Python3 đã chuẩn bị:

```bash
make test-real-etl
make ci-airflow test-ci-workflow
make check-task-status
```

Trên macOS, nếu QA-03 báo `QA03_MATRIX_NOT_BOUNDED` khi unittest tạo file
tạm dưới đường dẫn `/var/...` (symlink đến `/private/var/...`), chạy lại với
`TMPDIR=/private/tmp`, ví dụ `TMPDIR=/private/tmp make ci-airflow`.
Không bỏ kiểm tra symlink của runtime để làm test xanh.

Thành công: unittest OK; Maven BUILD SUCCESS, 0 failure/error/skip. Java kiểm
actual local Iceberg commit/replay, context/upstream/lease/proof corruption và
read-only verification. Python kiểm failure từng phase, timeout/interruption,
snapshot drift, mock rejection, immutable context và DAG graphs.

## 3. Chuẩn bị runtime riêng — không nằm trong test

Foundation MinIO/Airflow/Catalog/Trino phải healthy; không tự reset Docker.
Nếu chưa có, theo lifecycle project: `make docker-build`, sau đó
`make docker-up WAIT_TIMEOUT=600`. Giữ `.env` mạnh đang có, không ghi đè.

```bash
make prepare-real-etl-runtime
```

Lệnh chỉ build image QA `japan-earthquake-etl/real-etl:orc07-java17`; không
start/init/restart foundation. Maven Java17 verify nằm trong build, lần đầu
có thể mất vài phút. Main Airflow image cũng được đóng gói Spark trong Dockerfile
mới; deployment thật dùng workflow docker-build/docker-up riêng của project.
Không tự bật ORC07_DAILY_MODE hoặc sửa credential khi prepare/QA.

## 4. Chuẩn bị public captures đủ tháng

Reuse captures đã có từ ORC-06; không cần tải lại. Nếu chưa có:

```bash
make prepare-full-month
make qa-full-month-live FULL_MONTH_ROOT=staging/orc06-full-month
```

Hai raw files cần là `usgs-2023-09.geojson` và `jma-2023.zip` (full h2023).
Chỉ dùng đúng full-month query + halo; sample nhỏ không đủ coverage.

## 5. Nghiệm thu thật có phạm vi

```bash
make qa-real-etl ORC07_CAPTURE_ROOT=staging/orc06-full-month/captures
```

Có thể thay path bằng root capture ORC-06 cũ. QA chỉ preflight image fingerprint,
capture files và dependency health, rồi chạy one-off container `--no-deps
--pull never` với service `pull_policy: never`; không build/pull/start/init lại foundation.

QA thực hiện:

1. Production Bronze writers upload original public bytes vào MinIO với seed
   run riêng, verify response query/ZIP/SHA/count. Không ghi Gold trong seed.
2. Production `orc_02_daily_sources` DAG chạy historical daily interval cuối
   tháng 09/2023: actual JMA readiness, USGS three-day overlap và same-run
   real ETL tới sáu snapshots, Trino blockers và publication.
3. Replay exact daily operation trong JVMs riêng, không clear task/DagRun:
   receipts/snapshots/publication phải giữ nguyên.
4. Manual `orc_07_real_etl` dùng cùng exact source pins/adapters, namespace QA
   riêng, full-month scope; không sửa namespace/partition sản phẩm.

Mỗi QA run tạo UUID namespace riêng; không auto-delete artifacts/namespace.
Airflow `DAG.test` dùng logical date mới chưa tồn tại và không unpause DAG,
không mark-success; **không gọi đây là scheduled cron run**. Whole-run lease
vẫn chặn source/Gold runs khác. Nếu đang có active run/lease, QA từ chối.
Airflow 3.3.2 có thể hiển thị duration tính từ historical logical date; không
dùng con số đó làm benchmark runtime. Xem wall-clock task logs để đo thời gian.

Thành công terminal: `event=orc07_qa_passed log=staging/orc07-host-.../private.log`.
Chỉ khi container trả exit0, daily/manual actual DAG states và task states đều
success, rerun/six snapshots/publication đạt mới có event này.

Nếu image stale: prepare lại explicit. Nếu QA fail: inspect đúng private log
được in, không copy toàn log lên PR vì có thể có thông tin môi trường. Không
tự release owner, clear DAG, xóa warehouse hoặc đổi SHA cho qua check.

## 6. Kiểm tra trên Airflow, MinIO, Trino

- Airflow: xem mới nhất `orc_02_daily_sources` (8 tasks) và `orc_07_real_etl`
  (5 tasks). `real_etl`/`execute` phải success, final completion gate success;
  SourcesReady một mình không phải Published. Không task nào được mark-success.
- Evidence trong shared staging: `real-etl/last-qa.json` chỉ mục UUID, sau đó
  `real-etl/qa/<UUID>/acceptance.json`. Backend phải
  `MinIO-Iceberg-Trino-Airflow-DAG.test`, status PASSED, scheduled_run false,
  daily/manual published true và rerun_identical true.
- MinIO bucket → configured Silver prefix → `real-etl/<operation>/`: đủ context,
  six detailed phase artifacts và six receipts; SHA/URI/upstream chain khớp.
  `full-month/<operation>/` có coverage registry/receipt/_SUCCESS;
  `bundles/<operation>/` có bốn dataset manifests/Parquet.
- `gold-commits/<operation>/commit.json`: COMMITTED, đủ sáu positive snapshot
  IDs; `published=false` là đúng ở tầng commit.
- `gold-verification/<operation>/publication.json`: actual Trino checks,
  verify_status PASSED, published true, six snapshots khớp commit.
- Trino: lấy **namespace thực tế** từ acceptance, query publication_status.
  Serving view phải là view versioned trong publication report; không query
  raw current tables rồi gọi đó là Published. Xem SQL/readback rules ở
  [GLD-04](../specs/GOLD_TRINO_PUBLICATION.md).

Raw Bronze row count của h2023 là toàn năm, khác Silver selected month và
canonical count. Source links có ACCEPTED/REJECTED/AMBIGUOUS; không trừ tổng
link rows khỏi observations. Read actual reconciliation trong silver receipt.

## 7. Deployment real và recovery

Mặc định `ORC07_DAILY_MODE=disabled`. Muốn bật: deploy Airflow image mới,
đặt request JSON đúng shared staging, pin key/raw-file SHA trong môi trường
và xác nhận policy/coverage đủ scope. Task không nhận executable trong conf.
Manual trigger chỉ nhận bốn conf fields của contract. Không chạy prototype
template zero SHA hoặc file-backed ORC-06 request trực tiếp trên MinIO.

**Không bật current-2026 unattended daily với inventory JMA tới 2023.**
ORC-06 v1 chứng nhận closed month đủ halo; hỗ trợ current month cần version
contract/proof/tests rõ. Nếu chưa có request đủ coverage, để source-only.
Mở rộng current-month-as-of-cutoff ngoài scope ORC-06/ORC-07 v1. QA-01 đã bổ
sung [thiết kế DESIGN_ONLY](../specs/CURRENT_MONTH_AS_OF_CUTOFF.md) ngày 22/10;
sau đó owner yêu cầu [Source/Silver component](./CURRENT_MONTH_SOURCE_SILVER.md)
standalone trên PR #96. Chưa có Gold/publication/daily wiring v2 hoặc task ID/effort
triển khai còn lại riêng. Không dùng
policy JSON thiết kế để bật runtime; không coi v1 đã nghiệm thu là hỗ trợ scope đó.

Source lease owner là exact dag/run; operation path là alias hash. Khi thất
bại, kiểm tra `execution.json`, failure/phase private log, exact Gold journals
và remote Trino statements trước khi retry cùng identity. Chỉ owner đúng có
thể release; metadata cũ Published không chứng minh attempt mới đã dừng.
`failure.json` từ attempt trước được giữ lại; xem `execution.json` và
summary SHA của attempt hiện tại, không dùng failure file cũ thay trạng thái mới.

### Recovery QA sau OOM — không clear hoặc ghi đè snapshot

Spark driver 768m không phải tổng RAM của container: Airflow/Python, Spark JVM,
native buffers và file cache dùng thêm bộ nhớ. Manual QA đã OOM ở limit 2GiB;
scheduler/QA configuration hiện dùng 3GiB, API/dag-processor vẫn 1GiB.
Đây là giới hạn nghiệm thu local, không bảo đảm đủ cho historical 40 năm.

Nếu QA dừng giữa Gold commit, giữ nguyên failed DagRun, own source lease và
các snapshot đã commit. Operator phải xác nhận container/JVM local đã dừng,
không có Trino phase/statement đang chạy và readback journal/snapshot ổn định,
thuộc đúng operation. Không suy điều này chỉ từ task timeout hoặc log im lặng.

Host controller có hai chế độ explicit, không được QA tự gọi:

- `scripts/real-etl.py recover --evidence <UUID> --captures <capture-root>`:
  yêu cầu `recovery-confirmation.json` đúng evidence với state
  `OWN_LOCAL_OOM_STOP_CONFIRMED`, exact owner/operation, các snapshot đã commit,
  daily operation và `trino_phase_started=false`. Chỉ resume khi **lease cũ
  vẫn thuộc exact owner**; không takeover hoặc release run khác. Chạy lại cùng
  operation, so sánh snapshot IDs cũ, release sau Published; giữ failed DagRun
  cũ và nghiệm thu bằng **một manual DagRun mới**, không sửa lịch sử thành success.
- `scripts/real-etl.py verify --evidence <UUID> --captures <capture-root>`:
  chỉ dùng khi acceptance đã PASSED, chạy lại verify/publish exact pinned
  bundles bằng binary đã chuẩn bị; không ingest hoặc ghi Gold snapshots mới.

Các lệnh dùng `python3` từ repository root; `<UUID>` lấy từ actual evidence,
không copy placeholder để chạy. Chúng vẫn preflight image/captures/health và
không build/start/init service. `recover` được giới hạn cho trường hợp local
OOM đã xác nhận trước Trino ở trên; các failure khác cần quy trình điều tra
[QA-04 — Test failure và recovery theo tầng](../task/tasks/QA-04.md), không
tạo file confirmation để bỏ qua việc kiểm tra tiến trình/owner.

[QA-01](../task/tasks/QA-01.md) còn phải nghiệm thu scheduled deployment độc lập;
[QA-02](../task/tasks/QA-02.md)/[QA-03](../task/tasks/QA-03.md)/[QA-04](../task/tasks/QA-04.md)
còn revision/history/failure matrices rộng. [SEC-01](../task/tasks/SEC-01.md)
còn deployed logs/security và ML handoff, không tự đổi Done sau QA này.
