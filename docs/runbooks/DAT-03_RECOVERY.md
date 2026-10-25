# DAT-03 — Khối 1: chẩn đoán và recovery preflight chỉ đọc

## Mục đích và giới hạn

Không chạy lại `make run-research-e2e` trên retained run: fresh-run entrypoint
cố ý từ chối execution đã tồn tại và namespace đã commit. Khối 1 chỉ kiểm tra
điều kiện phục hồi, **không** resume ETL, publish, rerun hoặc release lease.
Khối 2 của **DAT-03 — Catalog thật và team handoff** mới sở hữu các bước đó.
Task vẫn In Progress; plan READY không là full E2E PASSED hay research approval.

Preflight v1 chỉ nhận trạng thái đã biết: 2000-01/2011-04 completed first/rerun,
2016-04 đã commit nhưng chưa verify/publish, 2023-12 chưa chạy ETL. Các trạng thái
khác (timeout chưa rõ child, phase muộn/journal lạ, publication một phần) phải
chẩn đoán riêng; không đoán checkpoint, không tự sửa metadata để phù hợp.

## Cơ chế

1. Pin byte SHA của request/execution và hai completed progress records từ
   [partial receipt](../evidence/DAT-03_BOUNDED_E2E_PARTIAL.json). Pin riêng
   context 2016-04; không sửa code_version/source_cutoff/baseline trong context.
2. Verify original JAR, inventories, năm JMA captures và USGS capture. Đây là
   hashing bytes/metadata, không parse/transform lại events. Verify completed
   phase/SQL proof offline bằng verifier cũ; đối chiếu live context/receipt/
   artifact SHA chain với journal MinIO. Không biến SQL proof cũ thành query mới.
3. Chỉ assert **own retained lease**, yêu cầu không Java process hoặc queued/
   running DagRun. Không acquire/release/takeover. Kiểm tra bốn service healthy
   trước/sau và reject nếu StartedAt/RestartCount đổi trong phiên.
4. Java runner riêng không tạo SparkSession, không gọi Bronze/Silver/Gold
   writer hay publisher. Chạy **13 metadata SELECT**: sáu current main refs,
   sáu exact snapshot operation/identity và publication_status của operation
   chưa có row. Không chạy lại query EXCEPT/scan toàn bảng đã bị ngắt ở verify.
5. Xuất plan có sáu snapshot pins, original context/commit SHA, runner pin riêng
   và next_phases=[verify,publish]. `recovered`, `acceptance_passed`,
   `research_ready`, `can_release_lease` luôn false. Không tạo final acceptance.

Mỗi attempt có UUID mới dưới staging/dat03-recovery; normal Airflow user tạo
đúng bốn file mới qua stream allowlist/SHA. Không overwrite input/runner/log/
evidence của retained run, không thay deployed runner/mount/DAG/cron. Check lần
hai trên cùng attempt bị từ chối; tạo attempt mới sau khi kiểm tra không còn
process thay vì xóa evidence. Private logs không vào Git hay gửi nhóm.

## Lệnh

Chạy từ checkout chứa bản vá, có JDK17+ và JAR package đúng code. Build **local
Maven**, không Docker image; runtime check không tự build/pull/start/init.

```bash
./mvnw -o --batch-mode --no-transfer-progress -pl spark -am -DskipTests package
make test-research-recovery test-research-e2e
make prepare-research-recovery RESEARCH_DEPLOYMENT_ROOT=/Users/hoaitam/Desktop/Learn/23521394/HK1-Nam-4/IE212/japan-earthquake-etl RESEARCH_RETAINED_ROOT=/Users/hoaitam/Desktop/Learn/23521394/HK1-Nam-4/IE212/japan-earthquake-etl/staging/dat03-e2e/8ca2be1939db4111ae783f6566650fce/runtime
```

Prepare in ra đường dẫn attempt mới. Gán đúng giá trị đó cho biến shell
`dat03_recovery_folder`, sau đó:

```bash
make check-research-recovery RESEARCH_DEPLOYMENT_ROOT=/Users/hoaitam/Desktop/Learn/23521394/HK1-Nam-4/IE212/japan-earthquake-etl RESEARCH_RECOVERY_FOLDER="$dat03_recovery_folder"
```

Thành công: `event=dat03_recovery_ready`, có runtime/plan.json và service-state.json
before==after. Kiểm status READY_FOR_PUBLICATION_RECOVERY, 13 actual==expected,
flags false, quiescence_checked_before_and_after=true. execution.json ở attempt
mới là **preflight execution**, không thay failed execution của run cũ.

Lỗi: giữ attempt và own lease; đọc stderr.private tại máy owner, không đăng raw
log. Không đổi cron, clear DagRun, restart/build hay release lease để làm xanh.
Timeout không chứng minh child đã dừng; lần sau phải kiểm tra `/proc`/DagRun lại.

## Trino transport và chẩn đoán restart

[Statement protocol chính thức](https://trino.io/docs/current/develop/client-protocol.html)
dùng POST để submit và GET nextUri để lấy tiếp kết quả. Client dùng cho cả
readbacks và publication DDL/DML, nên mất response có thể là unknown outcome.
Patch không resubmit POST hoặc tự retry ETL: lỗi transport trả mã
TRINO_QUERY_OUTCOME_UNKNOWN kèm method/page/query_id đã allowlist,
retry=disabled; không in SQL/URI/payload/cause message. JSON lỗi cũng được redaction.
Đây là cải thiện chẩn đoán, **không phải** cách chữa container restart.

Đọc State.StartedAt/FinishedAt/RestartCount/OOMKilled, JVM flags, cgroup
memory.max/current/events và metadata crash/log. Không kết luận OOM từ EOF
hoặc MaxRAMPercentage. Counters hiện tại không tái hiện chắc chắn JVM trước
restart; không tìm thấy heap dump không chứng minh chưa từng OOM.
Không đọc/đăng heap dump, credential hoặc SQL payload của query khác.

## Sau khối 1

**DAT-03 khối 2**: chạy kiểm tra lại ngay trước resume; dùng exact original
context và committed snapshots để verify/publish 2016-04, đối soát/rerun;
tiếp tục case2023-12+h2024/USGS trong cùng run/namespace/source cutoff. Chỉ
nghiệm thu đủ bốn case/SHA chain/Trino/retention mới tạo final acceptance và
release own lease. Preflight không cung cấp lệnh resume để tránh vô tình ghi.
Owner đã yêu cầu thực hiện khối 2 trên PR #100: các lệnh continuation tách riêng
trong [runbook khối 2](./DAT-03_CONTINUATION.md), không thay chế độ chỉ đọc của
preflight hoặc tự chạy continuation khi gọi check-research-recovery.

**DAT-03 khối 3/4/5**: capacity gate/full225+63-month rollout, official
collection-aware MLI-02 export và ba máy receipts vẫn còn. Xem
[runbook collection/E2E](./DAT-03_RESEARCH_COLLECTION_E2E.md).
