# ORC-07 — Real ETL adapter protocol `orc-07-v1`

## Mục đích và ranh giới

Nối exact verified Bronze → ORC-06 full-month Silver → GLD-03 six-table commit
→ read-only Trino blockers → GLD-04 publication. Không thay parser, key,
canonical policy hoặc business schema. Java/Spark làm ETL; Python chỉ điều phối,
đọc/ghi control metadata, gọi executable cố định và quản lý shared source lease.

Protocol này **khác** ORC-01 `1.0` và scoped ORC-03 `orc-03-v1`. Không thêm field
ngầm vào context cũ. ORC-01 mock vẫn `MockComplete/published=false`; executable
real không nhận mock. ORC-03 ingest/reuse/reprocess v1 vẫn hoạt động như trước;
manual real full-month reprocess dùng DAG `orc_07_real_etl`, không giả adapter
scoped v1 đã được cấu hình. Cùng data-plane adapters được daily và manual gọi.

## Owner, identity và request

- Whole-run lease owner là **exact Airflow dag_id/run_id**, giữ cả dấu `:` trong
  run ID. `operation_id = orc07-<40 hex SHA256(dag_id + '|' + run_id)>` là alias
  an toàn cho đường dẫn/Java Silver/Gold journals, không đổi owner thành ingest ID.
- Context pin protocol, owner/operation, mode `real`, trigger daily/manual,
  execution UTC `[start,end)`, source-summary SHA (daily), exact ORC-06 request
  và canonical JSON context SHA. Không chuyển raw data hay credential qua XCom.
- Manual conf chỉ có `request_key`, `request_sha256`, `window_start_utc`,
  `window_end_utc`. Key là slug trỏ đúng `ORC07_STAGING_ROOT/requests/<key>.json`;
  SHA là hash bytes file, không URI latest hoặc arbitrary file/executable.
- ORC-06 request giữ exact input manifest/raw SHA, source pins, namespace,
  target months, baseline registry/commit/SHA và đủ sáu snapshot baselines.
  Context/resolver fields được allowlist trước XCom và kiểm tra lại trong Java.
- Daily request được chọn bằng env `ORC07_DAILY_REQUEST_KEY/SHA256`, không bằng
  dag_run.conf. Sau readiness, thêm exact daily source pins, pin cutoff/processed
  một lần và persist context/selection. Retry không đổi clock/input/baseline.
- Full-window/baseline inputs không bị daily overlap thay thế. ORC-06 tiếp tục
  kiểm JMA replacement, cả tháng cũ/mới và old/new source-link closure.

## Phase chain và bằng chứng thật

| Phase | Data-plane execution | Gate / artifact |
|---|---|---|
| readiness | FullMonthBronzeReader đọc exact new pins | SHA/raw/manifest/source validation, SourcesReady |
| bronze | Readback lại exact pins | BronzeVerified và source counts/lineage |
| silver | FullMonthStateResolver + SilverBundlePublisher | FullMonthReady, COMPLETE_MONTHS, registry/coverage, bốn datasets |
| gold | Re-verify registry/active Bronze/coverage; GoldInputReader + GoldIcebergWriter | Six-table commit journal, readback snapshots/counts; chưa Published |
| verify | GoldPublisher.verify trên exact commit | Trino schemas/counts/identity/business/outside-scope blockers; **không** CREATE/INSERT/view/publication |
| publish | GoldPublisher.publish re-verify exact commit | Versioned serving views, PASSED row, alias và publication journal/readback |

Không hard-code Trino checks=true từ receipt trước đó. `verify_status=PASSED`
chỉ trả sau actual SQL blockers. Gold writer không bị coi là atomic six-table
transaction; mỗi bảng có own commit/readback và resume journal GLD-03.

Mỗi phase nhận protocol/phase/context/exact upstream. Java đối chiếu upstream
với persisted predecessor receipt và artifact SHA. Receipt nhỏ trả artifact
URI/SHA, context/operation/upstream SHA, actual Silver counts, six snapshot IDs
từ Gold/Trino, publication ID/status khi publish. Không trả SQL/raw data/logs
Spark qua XCom. Actual detailed reports giữ trong MinIO.

## Storage và retry

Trong `SILVER_PREFIX`:

    real-etl/<operation>/context.json
    real-etl/<operation>/<phase>.json
    real-etl/<operation>/<phase>-receipt.json
    full-month/<operation>/...       # ORC-06 registry/receipt/marker
    bundles/<operation>/...          # four Silver datasets
    gold-commits/<operation>/...     # GLD-03 identity/commit
    gold-verification/<operation>/publication.json

Control metadata ở `ORC07_STAGING_ROOT/<operation>/`: selection/context,
phase input/receipt, execution.json, summary.json và failure.json. Private
child stdout/stderr có mode 0600; staging không commit Git. Summary pin đủ
receipts/counts/snapshots và verification/publication URIs. ORC-02 observer
chỉ ghi Published sau real completion, không đổi SourcesReady thành Published.

- Same context/operation phải giữ immutable artifact/receipt bytes.
- Rerun trước Gold rechecks current baselines; sau own Gold claim thì dùng
  verified Silver proof + writer journal resume, **không** giả current probe
  vẫn bằng baseline cũ. Writer chịu trách nhiệm chống stale/foreign writes.
- Trước Gold: verify full-month request SHA/marker, Silver readback, mọi active
  Bronze pin và computed registry coverage. Thiếu proof dừng trước table writes.
- Verify/publish luôn đọc exact Gold commit/snapshots; completed publication
  rerun không đưa alias quay về run cũ.
- Runtime failure/timeout/interruption dừng downstream và giữ lease. Summary
  cũ không nhả lease nếu execution hiện tại RUNNING/FAILED hoặc SHA không khớp.
  Chỉ success toàn chuỗi + persisted completion mới release exact owner.
- Không TTL takeover, foreign release, clear DAG, xóa metadata/volume hoặc
  rollback dữ liệu. Outcome JVM/Trino chưa rõ cần operator đối chiếu đúng run.

## Wiring, deployment và tài nguyên

ORC-02 là cron owner duy nhất. Task `real_etl` nằm giữa SourcesReady và release;
manual DAG không schedule. `ORC07_DAILY_MODE=disabled` mặc định giữ source-only,
không fallback mock nếu bật real mà thiếu request/runtime. Bật real chỉ sau
deployment và đủ coverage; không tự sửa `.env` khi chạy tests.

Airflow image đóng gói Spark 3.5.9/Java17, runner JAR và hai Iceberg/AWS runtime
JAR riêng. Mỗi phase dùng `local[1]`, heap 768m, UI off, shuffle 1; scheduler
LocalExecutor và QA container limit **3GiB** (2GiB đã OOM ở manual acceptance),
API/dag-processor giữ baseline 1GiB. Chỉ một mutating task/run và shared cross-DAG source lease.
Không tuyên bố đây là benchmark standalone/historical 40 năm; Spark cluster
profile ORC-05 cũ vẫn giữ. Chuẩn bị/build là bước explicit, QA không tự build/
pull/start/init. QA image riêng không thay tag/restart các services đang chạy.

## Giới hạn cần hiểu đúng

- ORC-06 v1 chỉ COMPLETE_MONTHS cho **tháng đã đóng và đủ halo**. JMA inventory
  hiện tới 2023: không bật unattended current-2026 publication bằng request
  historical và không giả delta chứng minh coverage. Cần version hóa policy/
  proof và tests trước khi hỗ trợ current-month-as-of-cutoff; đó là mở rộng
  ngoài ORC-06/ORC-07 v1, hiện chưa có task riêng cho phiên bản mới.
- ORC-07 QA dùng public full-month 09/2023, actual MinIO/Iceberg/Trino và
  production daily/manual DAGs qua `DAG.test`. Đây không phải scheduled cron
  acceptance hoặc research-period coverage; không clear existing logical dates.
- [QA-01 — Chạy E2E daily đa nguồn](../task/tasks/QA-01.md): nghiệm thu độc lập
  scheduled deployment/traces/counts/publication theo môi trường vận hành.
- [QA-02 — Test rerun, duplicate và late revision](../task/tasks/QA-02.md),
  [QA-03 — Kiểm thử JMA historical backfill](../task/tasks/QA-03.md),
  [QA-04 — Test failure và recovery theo tầng](../task/tasks/QA-04.md): fault,
  revision/history/resume rollout rộng hơn bounded adapter acceptance.
- [SEC-01 — Review secret và bề mặt truy cập](../task/tasks/SEC-01.md): audit
  deployed image/logs và ML handoff riêng; không suy Done từ source tests.

Xem [runbook ORC-07](../runbooks/ORC-07_REAL_ETL.md).
