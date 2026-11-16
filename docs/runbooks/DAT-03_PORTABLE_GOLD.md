# DAT-03 — Chạy Gold từ PREPARED

Đọc [contract](../specs/PREPARED_GOLD_PUBLICATION.md) và
[evidence part1](../evidence/DAT-03_PORTABLE_IMPORT_20261115.md) trước. Chỉ dùng
prepared catalog đầy đủ đã được externally pin; không dùng pilot/partial source.

## Setup và gates

- Runtime hiện có healthy: scheduler, PostgreSQL, MinIO, REST Catalog, Trino.
- Không active Java/DagRun/Trino query, source global lease trống.
- Java17/Spark3.5 + hai Iceberg/AWS JAR1.10.1 đúng pins. Không thay deployed DAG
  runner; job chạy JAR riêng trong scoped staging.
- Namespace được duyệt `iceberg.dat03_unified_20261114` phải rỗng lần đầu.
- Expected input:288 tháng,27 checkpoints,4.488.650canonical/4.489.823bridge,
  trusted SHA trong evidence part1. Footers/file SHA và source physical mapping
  được đọc lại; không tin chỉ một scalar `prepared=true`.
- Disk gate dùng physical host free >=max(10GiB,4GiB reserve +4x prepared payload
  bytes +2GiB overhead), theo dõi reserve4GiB trong phiên. Estimate không là SLA.
- Không tự build/pull/recreate/reset/start thêm service; không xóa data/evidence.

## Offline tests/build

```bash
make test-dat03-gold
make test-dat03-gold-spark
./mvnw --batch-mode --no-transfer-progress -pl spark -am -DskipTests package
```

Spark/Iceberg filesystem fixture target cần JDK17; không cấu hình MinIO/REST.
Host JDK26 có thể compile release17 và chạy pure metadata tests nhưng không thay
actual Java17 writer gate. Không chạy Spark tests song song job thật.

## CLI

```bash
python3 -B scripts/dat03-portable-gold.py --help
```

1. `prepare`: cần `--folder` tên32hex mới trong staging, `--evidence` và external
   `--evidence-sha256` của part1 acceptance đã commit. Kiểm pins, copy prebuilt
   runner/controller, ghi immutable request/assets list; không ETL hoặc Gold write.
2. `preflight`: stage exact files và chạy Java read-only input/source/Trino gate;
   không acquire lease/ghi Gold. Đã staged nhưng failed phải review, không auto retry.
3. `run`: sau preflight accepted, acquire source lease riêng, Spark six-table commit
   và plain-Java Trino verify/publish tuần tự, cùng request/code/cutoff.
4. `rerun`: chỉ sau session run đã accepted; stage accepted pins, chạy lại writer
   readback và read-only publisher, đối chiếu immutable SHA/snapshot/history.
5. `capture`: chỉ cho exact `--session` đã terminal success khi host observer bị
   ngắt. Dùng original host intent, không launch/retry ETL hoặc release foreign owner.

`--seconds` từ600 đến6000. Actual invocation và operation/session/request pins
được ghi trong evidence; không copy path máy owner sang máy khác rồi coi là ready.

## Theo dõi và nhận biết thành công

- Commit có `event=prepared_gold_table_committed ... table=...` đủ6 bảng;
  không coi một bảng thành công là sáu-table atomic commit.
- Trino: `phase=verify query=A/139`, sau đó `phase=publish query=A/41` lần đầu.
  Rerun publish có15 calls (2publication reuse +13post-readbacks), không DDL/DML.
- `portable_gold_session_accepted`, status PUBLISHED, commit/publication SHA,
  six literal snapshot IDs, verify status PASSED và actual query counters.
- Own source lease được release **sau** result/execution SHA và services/idle gates.
- Rerun `reused=true`, cùng commit/publication SHA/six pins, snapshot-history1/table.

`COMMITTED` không là `Published`. Không sử dụng output markers local để skip
Trino blockers. Private logs có thể chứa SQL/SDK details, không copy lên Git/chat.
Terminal log chỉ lọc fixed key-value events và count, không echo raw SQL/env/token.

## Lỗi / giới hạn

### Lỗi EXCEPT DATE/INT sau hai fact

Chỉ ca đã chẩn đoán này dùng `scripts/dat03-portable-gold-recovery.py`; không coi
`recover` là một cách bypass arbitrary failure. CLI `--help` cung cấp bốn mode:

1. `prepare`: `--folder` operation gốc, `--inspection`/`--inspection-sha256`
   của read-only Trino/identity inspection và `--failed-session` chính xác.
   Cần bản failed-runtime đã capture, đúng lỗi DATE/INT, hai own fact snapshots,
   bốn dimension chưa có snapshot, bytecode compatibility và own idle lease.
   Copy runner/scripts vào continuation mới, không sửa assets gốc/ghi Gold.
2. `run`: exact `--continuation`, external `--continuation-sha256`, `--seconds`
   bounded. Stage một lần, assert own lease, tiếp tục commit rồi verify/publish;
   mọi phase ghi execution/result/code pin riêng. Không resubmit khi thất bại.
3. `finalize`: chỉ ghi failure-resolution sau recovery session đã capture và
   accepted thật; cần exact `--session`. Không launch Java/SQL hoặc release lease.
4. `worker`: entry nội bộ đã pin, không phải lệnh vận hành tự do.

Không đổi request/cutoff/context/namespace để né conflict, không chọn snapshot
latest thay partial pins. Bản gốc và continuation giữ song song để audit.
Sau thành công, quay lại `dat03-portable-gold.py rerun`: six snapshots/receipt bytes
phải giữ nguyên. Nếu recovery chưa accepted hoặc resolution sai SHA, rerun chặn.

### Các lỗi khác

- Failure/timeout/unknown: giữ exact request/runner/journals/snapshot/lease/logs;
  không retry mù, clear Airflow/DB, delete warehouse hoặc lấy latest thay pin.
- Optional Docker progress timeout chỉ là observation unavailable. Kiểm worker
  thật; không mở process thứ hai. Capture yêu cầu terminal success thật.
- Partial capture hoặc own lease còn giữ cần chẩn đoán theo exact session; không
  unlink owner bằng tay để chạy tiếp và không coi marker là proof.
- Phần tiếp theo thuộc **DAT-03 — Chuẩn bị catalog nghiên cứu thật và bàn giao
  dataset dùng chung**: official collection adapter/EXPORTED + all-shard loader/
  immutable release + receipts ba máy. Phần2 không hoàn tất MLD-05/MLI-02 gate mới
  bằng cách fake legacy288 checkpoints/FullMonthReady hoặc scalar research flags.
