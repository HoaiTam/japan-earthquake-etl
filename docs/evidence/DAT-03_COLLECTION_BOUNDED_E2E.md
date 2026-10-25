# DAT-03 — Collection preparation và bounded real-source E2E

Tiếp tục PR #99, branch feat/dat-03-research-dataset-handoff từ HEAD258710d.
Task In Progress; assignee HoaiTam, reviewer unassigned. Không tự commit/push/
merge, không chứng nhận full research hoặc receipts ba máy.

Cập nhật25/10: bước3/4 đã được owner commit ở `19073bd`; branch vá riêng
`fix/dat-03-trino-recovery` bổ sung [khối 1 read-only preflight](./DAT-03_RECOVERY_PREFLIGHT.md)
đạt13/13 metadata checks. Các failed/runtime results dưới đây giữ nguyên,
không bị đổi thành full PASSED hoặc coi lease đã release.

## Bước 3 — Component đã kiểm

Collection v1 giữ full parent identity/Gold/split/cutoff, global pinned Mc,
strict-split exclusions và deterministic whole-mainshock assignment. Shard
<=128 mainshocks/100k rows, max64shards/4096 metadata. Default exporter không
được tăng cap. Parent vẫn BUILDING, kết quả RESEARCH_COLLECTION_PREPARED,
research_ready=false và official_exported=false; không official registry/import.

- ResearchCollectionPlanTest: **7 PASS**, gồm249 mainshocks/2shards; row budget
  3x60000 thành3 whole-window shards; omission/duplicate/reassign/oversized/
  typed limits/global Mc/coverage/layout/identity và persisted bigint roundtrip.
- ResearchCollectionJobTest: **2 PASS**, real local Spark+Parquet trên fixture,
  hai mainshocks chia hai shards; verify/readback/rerun/external SHA; corruption
  giữ nguyên không tự sửa. Partial coverage/shard Mc không tạo output.
- Đây là synthetic component QA, **chưa chạy production prepare trên full pin**.
  Các compile/test iteration lỗi đã sửa trước lượt cuối, không tính là PASS.

## Bước 4 — Phiên runtime

Phiên original-source thật **FAILED**, không có final report/acceptance PASSED.
Metadata/pins lưu ở [partial receipt](./DAT-03_BOUNDED_E2E_PARTIAL.json).
Namespace `iceberg.dat03_e2e_8ca2be1939db4111ae783f6566650fce`, run
`dat03-8ca2be1939db4111ae783f6566650fce`. Không đổi deployed runner/DAG/cron,
build/pull/start/init, reset/delete dữ liệu hoặc tự release lease.

| Tháng | Kết quả thật |
|---|---|
| 2000-01 | PASSED first/rerun,5418 canonical events; six snapshots/36 reported Trino checks; SHA chain và readback SQL pin đã verify độc lập |
| 2011-04 | PASSED first/rerun;57044 selected revisions,57043 canonical,1 revision ngoài affected month/halo; Gold cumulative62461; six snapshots/36checks và retention đạt |
| 2016-04 | Silver/Gold COMMITTED, cumulative105282 events; dừng ở verify, chưa Published/rerun acceptance |
| 2023-12+h2024/USGS | Chưa chạy ETL case; captures/Bronze đã được chuẩn bị, không coi là boundary/source-link acceptance |

Hai completed progress records đã kiểm độc lập bằng verifier mới nhất:
input captures/JAR/inventory pins, context/receipt upstream/artifact SHA,
commit bundle/publication ID/six snapshots, actual/expected/SQL exact snapshot,
first==rerun. Không tính36 checks được lưu lại trong reused publication thành
36 freshness SQL mới ở mỗi rerun; các blockers/schema thực vẫn được publisher
truy vấn lại. Hai publications có72 reported checks, không là toàn4cases.

### Lỗi hạ tầng / trạng thái an toàn

Java exit1 ở GoldPublisher.verify → TrinoSqlClient.execute, HTTP2
`java.io.IOException: EOF reached while reading`. Trino container thực sự
finished `2026-10-24T15:09:13.658845584Z`, started lại
`2026-10-24T15:09:14.289355292Z`, restart_count1; hiện healthy. Không có bằng
chứng đủ để kết luận nguyên nhân gốc restart/OOM: inspect hiện báo oom=false,
log trước restart trong khoảng đọc không có output, event history chỉ còn
health-checks. Không sửa client để retry POST mù hoặc đánh dấu verify thành PASS.

Đã xác minh sau lỗi: không Java process trong scheduler container, không
running/queued DagRun. Own lease vẫn giữ đúng dag_id/run_id trên. Không release,
takeover hoặc tự tạo namespace/run khác; các source runs khác sẽ bị lease chặn
cho tới phục hồi/nghiệm thu hoặc operator phê duyệt bước xử lý riêng.
Giữ immutable partial/commit artifacts, chưa tạo final PASSED receipt.

Runtime host copy ở deployment checkout:
`staging/dat03-e2e/8ca2be1939db4111ae783f6566650fce/runtime/`.
Original volume folder cùng UUID dưới `/opt/pipeline/staging/dat03-e2e/`.
Private stdout/stderr/captures/JAR không vào Git. Hai folder prepare trước đó
không chạy lake ETL, được giữ nguyên; không tự cleanup.

### Số đo partial, không full-capacity acceptance

- Wall2260,17s (~37,67 phút), child peak RSS1.367.384KiB (~1,30GiB), driver
  heap768MiB/MaxDirectMemory256MiB/local[1]; Linuxaarch64 Java17.0.19/Spark3.5.9.
- Filesystem free184.323.600.384→184.162.836.480 bytes (giảm160.763.904 bytes).
  Đây là shared Docker-volume filesystem, không phép đo exclusive từng layer;
  không suy tuyến tính thành disk/ETA full288 tháng hoặc export capacity.
- Airflow container limit3GiB; quan sát khi chạy~1,55GiB gồm Airflow+JVM.
- Đây là **failure/partial measurement**, không benchmark hoàn chỉnh4cases,
  không approval full rollout. Cần xác định/ổn định Trino trước khi tiếp tục.

Xem [runbook](../runbooks/DAT-03_RESEARCH_COLLECTION_E2E.md) để lặp lại bằng
identity mới **chỉ khi không có retained lease/execution cần phục hồi**; không
lấy snapshot evidence làm request mới. Owner chọn **giữ nguyên lỗi, bàn giao
trước** sau khi được hỏi; không tiếp tục phục hồi, retry/release hoặc thay
config/service. Cần chỉ đạo mới trước phần runtime tiếp theo.

## Giới hạn còn lại

DAT-03 bước5: full225+63-month rollout/capacity/source proof; bước6: official
collection-aware MLI-02 export/registry/loader/import và release/receipts ba
máy. EXP-02..05 official fit/science và SEC-01 cloud scope chưa được chứng nhận.

## Test/check bàn giao

- Scoped Java regression cuối: **35 PASS**,0 failures/errors/skips: collection
  plan7/job2, E2E guards3, export coverage8/bundle10, strict-split policy5.
  Maven offline/JDK21.0.11 target17; không full Java suite hoặc remote CI.
- Lượt bổ sung trong sandbox lỗi khởi tạo socket Spark (`Operation not
  permitted`) ở ba classes; không tính PASS. Chạy lại chỉ fixture Spark local
  với loopback127.0.0.1 được phép đạt35/35, không phục hồi DAT-03/ghi lake.
- Python control-plane mới **8 PASS** (capture/input pins/tamper/context/
  receipts/SQL snapshot/lifecycle); `ci-docs-base` exit0,233 test executions
  qua13 nhóm, gồm42 CI policy tests. Không cộng các lượt lặp thành unique tests.
- Build package local trước runtime đạt; runner SHA khớp request và copy volume.
  Source/example secret hygiene, docs links/task status/contracts và diff checks
  đạt. Checkout không có .env nên CFG check không là runtime credential audit.
- Partial JSON metadata đã so sánh lại exact retained request/execution/progress
  byte SHAs, publication IDs và sáu snapshot IDs; không sửa evidence lỗi thành PASS.
- Không commit raw/captures/JAR/Parquet/private logs hoặc tự commit/push/merge.
