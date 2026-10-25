# DAT-03 — Khối 1: chẩn đoán và preflight ngày 25/10/2026

Branch `fix/dat-03-trino-recovery` từ commit DAT-03 `19073bd` trên PR #99.
Task vẫn **In Progress**, assignee HoaiTam, reviewer unassigned. Chưa tự
commit/push/mở PR/merge. [Receipt metadata](./DAT-03_RECOVERY_PREFLIGHT.json)
pin plan/controller/JAR/retained inputs; [runbook](../runbooks/DAT-03_RECOVERY.md).

## Kết quả thật

**READY_FOR_PUBLICATION_RECOVERY**, không RECOVERED hoặc E2E PASSED.
Preflight kết thúc lúc **08:32:48 ngày 25/10/2026 (Asia/Ho_Chi_Minh)**.

- Original request/execution/JAR/inventories/five JMA captures/USGS captures
  và hai completed progress/SHA/SQL proof không đổi. Context/source cutoff
  `2026-10-24T14:31:46.536Z`, code_version gốc và baseline chain được giữ nguyên.
- Live MinIO context/phase receipts/artifact digests khớp retained local files.
  Giữ exact operation `orc07-a24421d9953c4f7f5e41d4d33c10d07af2a7e8f5`.
- **13/13 metadata SELECT đạt**: 6 main refs vẫn là sáu snapshot của commit
  2016-04; 6 snapshot operation/identity khớp; publication_status của operation
  có **0 rows**. Không tồn tại verify/publish receipts/artifacts hoặc publication
  journal của operation. Đây không phải 36 full data/blocker checks mới.
- Không Java process/queued/running DagRun trước và sau preflight. Own retained
  lease vẫn đúng `dat03_bounded_e2e` / `dat03-8ca2be1939db4111ae783f6566650fce`.
  Read-only assertion mở lock cũ bằng rb; không tạo root/lock mới, không release.
- Airflow scheduler/MinIO/Iceberg REST/Trino healthy; StartedAt/RestartCount không
  đổi trong phiên. Worker exit0; chỉ ghi attempt staging mới, không ghi lake.
- Plan giữ recovered=false, acceptance_passed=false, can_release_lease=false,
  research_ready=false. Không replay Bronze/Silver/Gold, không đổi alias/status,
  không chạy tháng2023-12 hoặc tạo final acceptance cho run gốc.

Host attempt nằm trong deployment checkout:
`staging/dat03-recovery/1270d7a7dc734b4ca7a10618ae83a8e2/`.
Full plan ở runtime/plan.json, private stdout/stderr/JAR giữ local/ignored.
Original run ở staging/dat03-e2e/8ca2be1939db4111ae783f6566650fce/runtime/
không bị sửa. Plan SHA trong receipt là external byte pin, không chữ ký nguồn.

## Trino: biết gì và chưa biết gì

Trước phiên bị ngắt, đọc thấy Trino limit3GiB, JVM MaxRAMPercentage80,
OOMKilled=false, cgroup oom/oom_kill=0 và không thấy heap/crash dump trong
metadata đã kiểm. HTTP access-log archives tồn tại nhưng không cung cấp đủ
nguyên nhân restart. Không đọc/phát tán heap dump hoặc payload query khác.

Sau khi tiếp tục, Docker daemon chưa chạy; owner xác nhận tự mở Docker Desktop.
Các service sẵn có trở lại, Trino StartedAt `2026-10-25T01:29:17.226982544Z`,
RestartCount0/OOMKilled=false trong phiên preflight. Không dùng counters mới
để xóa kết luận lịch sử: lỗi EOF ngày24/10 trùng restart thật ở15:09:13Z theo
[partial evidence cũ](./DAT-03_COLLECTION_BOUNDED_E2E.md); nguyên nhân gốc vẫn
**chưa xác định**. Agent không build/pull/start/restart/init/reset service.

Patch Trino client chỉ cải thiện failure handling/redaction: transport loss
báo UNKNOWN kèm safe method/page/query ID, không resubmit POST; malformed JSON/
nextUri không in payload. Không thay memory/query plan hoặc hứa container sẽ
chịu được full EXCEPT/ETL/288 tháng chỉ vì 13 metadata queries đạt.

## Test/check

- Scoped Maven offline/JDK21 target17: **28 PASS**,0 failures/errors/skips:
  TrinoSqlClientTest6, GoldPublicationTest5, ResearchBoundedE2EJobTest3,
  ResearchRecoveryPreflightTest6, RealEtlAdapterTest8. Spark chỉ dùng fixture
  local/loopback, không nguồn thật hoặc Docker lake. Local package đạt.
- Python: recovery13 và existing E2E8 đều PASS. Không cộng lượt lặp CI vào
  unique count. Live entrypoint cũng verify nguyên input/failed execution thật
  và mã JAR khác original runner; không thay pin original để cho code mới chạy.
- `ci-docs-base` đạt (docs/links/task index/contracts/config/CI policy); CFG
  checkout không có .env nên source/example check không là credential audit live.
- Các iteration đầu có lỗi socket sandbox/fixture int-vs-long và canonical
  temp path macOS đã sửa; không tính các lượt lỗi đó là PASS hay runtime evidence.

## Còn lại / giới hạn

**DAT-03 — Catalog nghiên cứu và team handoff, khối 2:** kiểm tra lại trạng thái
ngay trước recovery; chạy verify/publish và rerun/retention2016-04, sau đó case
2023-12+h2024/USGS. Cần controller continuation có pin code recovery riêng,
không sửa original context/code_version hoặc chạy fresh entrypoint mù. Chỉ
full acceptance cả4cases mới được release own lease. Bản vá khối 1 chưa cung
cấp resume command và không cho phép release chỉ vì metadata preflight đạt.

**DAT-03 khối 3/4/5:** capacity/rollout225+63 tháng, official collection-aware
MLI-02 lifecycle/export và receipts ba máy còn thiếu; giữ research_ready=false.
**SEC-01 — Security review:** local scope giữ nguyên; cloud/Drive/Colab chưa
được nghiệm thu nếu sau này migration. Không upload hoặc đổi ACL.
