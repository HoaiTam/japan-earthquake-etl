# ORC-05 — Recovery, concurrency và tài nguyên local

## 1. Làm gì / có gì / dùng để làm gì

Profile `orc-05-local-v1` dùng cho một máy local và shared staging volume.
Team vẫn phát triển/test offline song song, nhưng job nặng trên cùng stack
được chạy tuần tự để tránh tranh RAM và ghi đè scope.

| Thành phần | Có gì | Dùng để làm gì |
|---|---|---|
| `runtime_profile.py`, source DAGs | Mutating retries=0, read-only USGS verify retries=1, JMA archive concurrency cap=1 | Retry đúng boundary, không nhân attempt ghi/source download |
| Compose + Java wrappers | LocalExecutor parallelism=1, heap Java source giới hạn, Spark daemon heap, scheduler limit | Giới hạn tài nguyên thực thi thay vì chỉ dựa vào số active runs từng DAG |
| `recovery_qa.py` | Inject failure trước store call, fresh exact readback, contender lease, rerun | Kiểm chứng recovery không xóa Bronze và không đổi count/input |
| `ResourcePilotJob.java` + `resource_pilot.py` | Java parsers → Spark standalone repartition/group/count; cgroup/heap/duration report | Sizing probe có dữ liệu thật, chỉ đọc, không ghi Silver/Gold |
| `staging_maintenance.py` | Preview một exact DAG/run, giữ audit/cache/lease | Chẩn đoán staging an toàn, không chạy TTL delete hoặc takeover |

Python chỉ điều phối/process/metadata; record earthquake được parse bởi Java,
shuffle bởi Spark. Profile không bổ sung DB nghiệp vụ hoặc ML service.

## 2. Runtime profile

| Thành phần | Giá trị / guardrail |
|---|---|
| Airflow LocalExecutor | `parallelism=1`, default `max_active_tasks_per_dag=1`; source/ETL/backfill DAG tối đa 1 run/1 task |
| JMA mapping | `min(JMA_BACKFILL_MAX_CONCURRENCY, 1)`; giá trị requested vẫn validate 1..4; `.env` cũ =2/4 không mở thêm archive slot |
| Source JVM | `SOURCE_RUNNER_HEAP=384m` mặc định; chỉ 128m/192m/256m/384m/512m; cả USGS/JMA/Bronze-reuse wrapper đặt `-Xmx` |
| Scheduler | 1.5 CPU / 2 GiB container: chứa Airflow + task Python + JVM source; không phải 2 GiB heap Java |
| Spark master/worker daemon | `SPARK_DAEMON_MEMORY=256m`; worker limit vẫn 2 CPU / 3 GiB |
| Regular Spark smoke | `.env.example`: driver 512m, executor 1g, worker 1 core; dynamic allocation off, driver result cap64m, shuffle4/default parallelism1 |
| Read-only resource pilot | driver512m, executor512m, 1 core, shuffle4, dynamic allocation off; client limit1 CPU /1 GiB |
| Other services | Giữ baseline Compose hiện có, không tự tăng Trino heap hoặc mở port |

Shaded runner JAR relocate HTTP/Kotlin/JSON/Guava/Commons SDK dependencies vào
`ie212.earthquake.internal.*`; thin Spark JAR và Spark-provided dependencies
không relocate. Runtime pilot phát hiện `NoSuchMethodError` khi MinIO gặp
HTTP dependency cũ của Spark; không bật `userClassPathFirst` để đổi Jackson
của Spark. Chạy lại Bronze recovery và standalone probe là gate compatibility
cho packaged runner, không chỉ unit test thin classpath.

Heap không bằng tổng RSS: cần chỗ cho metaspace/direct buffers/Python/daemon.
Config checker chỉ nhận driver512..1024MiB, executor512..2048MiB và worker1core;
đổi sizing vượt profile cần sửa/review limits và nghiệm thu mới, không chỉ đổi `.env`.
`spark.executor.memoryOverhead` không phải guardrail Spark standalone ở đây;
container limit và heap có headroom là hai lớp riêng. `maxResultSize` không
chặn mọi OOM, chỉ hạn chế kết quả trả về driver.

Tổng **ceilings** các service lớn hơn RAM Docker; không thể bảo đảm tất cả
đạt trần cùng lúc. Không chạy Spark + Trino query lớn + Maven build đồng thời.
Profile là baseline pilot, không production sizing/full lịch sử. Các lệnh
`make -j` không làm target Makefile chạy song song (`.NOTPARALLEL`).

### Triển khai / `.env` cũ

Không ghi đè `.env` để lấy default mới. Có thể chỉnh hai biến Spark thành
512m/1g theo mẫu sau review; `.env` cũ 1g/2g vẫn là cấu hình lớn hơn và **không
được lấy evidence probe512m làm benchmark cho workload đó**. Source heap
mới có default384m kể cả `.env` chưa có biến.

Sau review, khi không có DAG task, source lease hoặc Spark app đang chạy:

```bash
make up-airflow WAIT_TIMEOUT=600
make up-spark WAIT_TIMEOUT=600
docker compose exec -T airflow-scheduler airflow config get-value core parallelism
docker compose exec -T airflow-scheduler airflow config get-value core max_active_tasks_per_dag
```

Hai giá trị phải là1. DAG code bind-mounted cập nhật khi parse, nhưng env,
heap wrappers và container limits phải rebuild/recreate mới áp dụng; không
coi import DagBag là đã deploy scheduler policy. Các target smoke ORC-05
dùng one-off, **không restart/unpause/trigger** service hoặc DAG đang chạy.

## 3. Retry boundary

| Boundary | Automatic retry | Vì sao / cách recover |
|---|---:|---|
| USGS HTTP client | Theo `USGS_HTTP_MAX_ATTEMPTS`, hiện mẫu4 | Retry transport/rate-limit theo Java HTTP policy; không retry payload invalid hoặc đổi query/window |
| Airflow fetch/validate/upload/JMA archive/daily source | 0 | Không nhân HTTP retry với retries toàn task; chưa biết partial side effects thì cần operator đọc receipt |
| USGS verify readback | 1 | Chỉ đọc exact object/checksum; chạy khi lease vẫn held; không upload lại |
| ORC-01 adapters / ORC-03 executor | 0 | Command có thể write/commit; không replay cả chuỗi mù sau partial commit |
| Acquire/release/gates/summary | 0 | Fail closed; all_done release không che upstream failure bởi strict completion leaf |

`USGS_AIRFLOW_TASK_RETRIES` cũ không còn override mutating retries. Retry HTTP
và task retry là hai boundary khác nhau. Timeout/OOM không tự chứng minh
external writer đã dừng hay một commit không thành công.

## 4. Recovery matrix — retry đúng tầng

| Lỗi ở đâu | Giữ/pin gì | Tiếp tục bằng cách gì | Không làm |
|---|---|---|---|
| Fetch/validate Bronze | Context window, request config, staged receipt nếu có | Kiểm tra failed phase, upstream HTTP/structure; rerun đúng operation/window sau khi xác nhận không writer còn sống | Không đổi window hoặc tải 40 năm lại |
| Upload/readback Bronze | Exact raw + manifest URI/SHA/attempt; immutable keys | Object đã BronzeReady: reuse/readback; incomplete attempt: để evidence và dùng attempt hợp lệ theo runner sau kiểm tra | Không overwrite raw/manifest, không tự coi object tồn tại là ready |
| Cache/staging mất | Operation pin và Bronze exact manifests | Ưu tiên `action=reuse` với pins đã xác minh; nếu operation pin mất/không chứng minh scope cũ: dừng, operator đối chiếu và tạo operation mới có scope đã review | Không tự bỏ checksum/gate hoặc suy ra `latest` |
| Silver parse/quality/write | Verified Bronze inputs + exact output partitions + parser/config version | Parse/quality issue: sửa nguyên nhân rồi rerun **cùng scope**; partial dataset chỉ được promote sau manifest/readback đầy đủ | Không xóa Bronze, không xóa toàn bộ Silver hoặc bỏ rejected count |
| Gold commit partial/mất receipt | Baseline snapshots + operation ID + exact required tables/partitions | Đối chiếu từng committed snapshot với operation identity; adapter idempotent xác nhận already committed hoặc chỉ hoàn tất bảng chưa commit | Không replay commit mù, không dùng current/latest làm baseline |
| Trino verification fail | Committed snapshot bundle | Retry verification cùng snapshots sau sửa query/readiness; publish vẫn blocked | Không ingest/commit lại chỉ vì Trino lỗi; không Published trước verify |
| Publish metadata fail | Passed verification receipt + same snapshot bundle | Idempotent publish cùng bundle/report sau đối chiếu trạng thái đã ghi | Không công bố bundle khác hoặc xóa snapshot |
| Disk full/observer failure | Airflow task state, exact persisted operation/journal | Dừng writer; bổ sung dung lượng, đối chiếu atomic state/summary rồi rerun đúng boundary | Không xóa journal để làm summary trông thành công |

Các dòng Silver/Gold là **operator/adapter handoff**, chưa phải fault injection
writer thật; đọc giới hạn task ở mục7. ORC-03 vẫn fail closed khi scoped
baseline/idempotency/outside-scope receipt không đạt. Không dùng graph clear
downstream để vượt quality gate.

### Whole-run lease và Airflow clear

Source DAGs và ORC-03 dùng chung `source-guard/owner.json` + `flock`; owner
là exact `{dag_id, run_id}`. Giữ lease qua toàn run, không chỉ từng task.
Same owner acquire được; contender và owner không khớp assert/release bị chặn.
Parallelism1 điều tiết task, **không thay lease** và không chống external job
không hợp tác. Không tự chạy `spark-submit`, notebook writer hay real ORC-01
adapter vào cùng scope; real ORC-01 command mặc định rỗng, integration writers
phải dùng cùng whole-run ownership trước khi bật (SLV-09/GLD-03/QA-01).

Release all_done có thể đã chạy sau failure. Vì vậy không chỉ clear fetch/
upload/verify: khi lease đã release, clear acquire + đúng task lỗi và các gate/
summary/release/completion phía sau cần thực thi lại, **không clear upstream
write thành công**. JMA chọn đúng mapped archive, không clear cả lịch sử.
ORC-03 task execute bao nhiều inner phases nên không clear để replay partial
commit nếu adapter chưa có reconciliation. Bronze-only reuse dùng operation
cũ/pins cũ, không ingest lại.

### Stale lease / process bị kill

Không có TTL takeover. Tuổi file hoặc DagRun failed không chứng minh process
Java/Spark đã chết. Operator phải ghi evidence: exact owner, Airflow không còn
task owner chạy, Spark không còn app/executor tương ứng, không subprocess/
external writer sống. Nếu chưa chứng minh được: giữ lease, không recover write.
Sau khi quiescent đã được xác nhận, release bằng API hiện có **với exact owner**
(`source_run_guard.lease("release", identity)`), kiểm tra `RELEASED`, sau đó
acquire lại qua DAG/operation đã pin. API vẫn so sánh owner atomically dưới
mutex; không `rm owner.json` hoặc sửa owner tay. Không có CLI auto-force-release.

## 5. Staging cleanup an toàn

Không cleanup lake để phục hồi. Phân loại trước khi bảo trì:

- Bronze/Silver/Gold/Iceberg: immutable/durable business artifacts, không cleanup
  trong ORC-05.
- Operation pins, source result/receipts/cache: phục vụ scope/idempotency/rerun;
  cache không mặc nhiên là disposable sau task success.
- Run summary/state/events: audit, giữ cả failed attempts. Terminal không đủ
  điều kiện xóa vì có thể còn handoff/recovery/reference.
- Temporary Java ZIP: validator/parser dùng path `createTempFile` của chính
  process và cleanup trong `finally` sau đóng stream. Crash leftovers cần
  operator xác nhận exact file, process đã dừng và không có reference; không
  wildcard/TTL, không xóa cả staging volume.

`make maintenance-preview` chỉ đọc **một run đã có**, báo terminal/lease,
không list bucket, delete/move/takeover hoặc tạo summary khi run không tồn tại.
Không có automatic cleanup job được bật. Archive/retention rộng hơn chưa có
task riêng; cần policy/reference index và review trước khi thêm.

```bash
make maintenance-preview RUN_SUMMARY_ROOT=staging/run-summary RUN_SUMMARY_DAG=orc_01_etl_pipeline RUN_SUMMARY_RUN_ID=<run-id-tu-observability-smoke>
docker compose exec -T -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor python -m staging_maintenance --root /opt/pipeline/staging/run-summary/qa --dag-id orc_03_backfill --run-id <run-id-tu-smoke-recovery>
```

## 6. Test theo flow, không ghi lake

```bash
make test-recovery
make test-airflow
make smoke-recovery
make smoke-resource-pilot
```

Foundation master/worker/MinIO phải healthy, không có active app/source lease.
Smoke build image nhưng không restart các service đang chạy. Fixture exact
pins là `airflow/dags/fixtures/orc_03_reuse_sample.json`; thiếu object/checksum
khác thì **fail**, không tự download/fallback scan. Cần pins riêng nếu team
dùng MinIO khác; fixture không tự chứng minh source coverage hiện tại.

Recovery: failure source observer trước store call → FAILED persisted →
contender bị lease chặn → Java fresh readback attempt2 → same-scope rerun
attempt3 → release → BronzeVerified. Không phải scheduler-driven retry,
network outage injection hay fault injection trong Iceberg commit.

Resource pilot: full CRC/structure/count + raw/manifest SHA của exact ZIP/
GeoJSON → USGS parse toàn cửa sổ (<=10000 features) + JMA **first10000 lines**
→ observation schema → Spark standalone repartition4 + aggregate. Raw cap
32MiB/object, request64KiB, không collect whole catalog, không persist Silver.
Counts `parser_considered/rejects/ignored` chỉ thuộc parser probe, **không**
equations quality/dedup/link/Gold ORC-04. Full ZIP readback count không phải
full JMA parse. Cgroup peak bao gồm Python+driver; heap pool peak sum là upper
bound từng pool, không simultaneous RSS. Worker cgroup cần đối chiếu riêng.

Report giữ trong exact `backfill/qa/<run-id>/resource_report.json` và
`run-summary/qa/<hash>/recovery_report.json`; output không chứa credential/raw
payload. [Evidence](../evidence/ORC-05.md) ghi máy, phạm vi, duration/peak và
deployment boundary thật.

## 7. Giới hạn / handoff

- [SLV-09 - Tích hợp và kiểm thử Silver đa nguồn](../task/tasks/SLV-09.md):
  real multi-source adapter/write/readback, resource/failure/recovery của
  dedup/link toàn scope, shared ownership khi ghép real ORC-01.
- [GLD-03 - Ghi Gold Iceberg và commit snapshot](../task/tasks/GLD-03.md):
  partial multi-table commit reconciliation/idempotency thực tế.
- [GLD-04 - Tạo Trino views và verification SQL](../task/tasks/GLD-04.md):
  retry verification/publication pin snapshots và SQL runtime thật.
- [QA-01 - Chạy E2E daily đa nguồn](../task/tasks/QA-01.md): full scheduler
  E2E và đo tài nguyên toàn pipeline; ORC-05 không nghiệm thu Published thay.
- [SEC-01 - Review secret và bề mặt truy cập](../task/tasks/SEC-01.md): review
  quyền volume, endpoints nội bộ/loopback, credentials/log scope; profile không
  chứng minh distributed lock/security production.
- Full40-year sizing và broad retention/archive policy: **chưa có task riêng**;
  không lấy pilot này làm bảo đảm toàn catalog không OOM.

## Tham khảo cấu hình

- [Spark 3.5.9 configuration](https://spark.apache.org/docs/3.5.9/configuration.html)
- [Docker Compose resources](https://docs.docker.com/reference/compose-file/deploy/)
- [Airflow task pools](https://airflow.apache.org/docs/apache-airflow/3.3.2/administration-and-deployment/pools.html)
