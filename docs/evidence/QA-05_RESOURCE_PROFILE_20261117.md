# QA-05 — Nghiệm thu resource profile17/11/2026

**Done trong phạm vi profile đại diện.** HoaiTam, reviewer unassigned.
[JSON/pins](./QA-05_RESOURCE_PROFILE_20261117.json),
[cách chạy](../runbooks/QA-05_RESOURCE_PROFILE.md), [task](../task/tasks/QA-05.md).
Branch `test/qa-05-local-resource-profile`, base main `b8884b0`.

## Volume và điều kiện

Hai hard dependencies [QA-01](./QA-01_CURRENT_MONTH_ACCEPTANCE.md) và
[QA-03](./QA-03_RUNTIME_ACCEPTANCE.md) đã Done. Máy8GiB RAM; chỉ năm service
MinIO/REST/Trino/scheduler/PostgreSQL đang chạy healthy. Không bật lại bốn
service cũ đang stopped; exit137 lịch sử không được tính là OOM của phiên này.
Không active Java/DagRun/Trino query hoặc source lease trước/sau phép đo.

Đo trên Gold thật `iceberg.dat03_unified_20261114`, đúng sáu snapshot và SHA
[Published evidence](./DAT-03_PORTABLE_GOLD_20261115.json): 288tháng,
4.488.650canonical /4.489.823bridge. `$files` và `$partitions` đối soát đủ sáu
bảng/580files/672.571.647bytes (~641,4MiB); không download/bung Parquet vào host.
Hai fact mỗi bảng288files/288monthly partitions; bốn dimensions mỗi bảng1file.
Fact event file429.078–4.483.678bytes; bridge345.681–3.865.918bytes.

## Duration và before/after

Lệnh thật: `make profile-qa-05 QA05_OUTPUT=staging/qa05-20261117/profile-01`.
Suite **118,366s**, thấp hơn budget600s; SELECT chậm nhất10,481s kể cả warm-up,
thấp hơn60s/query. Tổng26Trino SELECTs:12metadata+14profile (2warm-up,12measured).

| Workload toàn fact | A: task_concurrency=1, median s | B: task_concurrency=2, median s |
|---|---:|---:|
| Fact summary, đọc magnitude/latitude/depth | 5,048 | 5,069 |
| Aggregate theo tháng | 7,419 | 8,137 |
| Aggregate magnitude/depth bands | 3,962 | 4,015 |

Cùng snapshot, SQL và memory cap256MB. Hai repetitions mỗi query/profile, AB rồi
BA, loại warm-up khỏi median. Counts khớp và result parity đạt theo quy tắc float
aggregation trong runbook. **Không thấy lợi ích tăng concurrency lên2**; giữ1.
Đây là thử nghiệm session-only, không có service configuration change/recreate.
Hai mẫu/profile ít, warm cache/order/host load còn ảnh hưởng; không công bố
speedup toàn pipeline hoặc generalize sang máy khác.

## RAM/CPU/disk và tính bất biến

- 17mẫu Docker stats: Trino sampled peak1.801.738.780bytes (~1,68GiB), CPU217,92%
  (Docker có thể >100% do nhiều core). Đây không phải peak RSS tuyệt đối.
- Engine peak user memory lớn nhất13.438.352bytes (~12,82MiB); spill0bytes ở
  cả14query. Engine reservation khác container RSS/cache; không cộng hai số.
- Host free thấp nhất4.661.215.232bytes (~4,34GiB), giữ trên reserve4GiB.
- Cgroup `oom`/`oom_kill` của đủ năm service0trước/0sau; IDs/start/restarts và
  SHA metadata/pointers/sáu snapshot giữ nguyên. Kết thúc idle và lease trống.
- Không ETL/data writes, thay DAG/cron/credential/ACL, build/pull/start/restart,
  compact files hoặc sửa warehouse/source pins/evidence.

Giữ nguyên query IDs trong raw receipt (prefix `20261116`); đối chiếu host/
container clock sau phiên đều17/11. Không sửa IDs/timestamps để khớp tên evidence
hoặc dùng chúng chứng minh on-time UTC SLA. Duration dùng monotonic timer;
QA-05 này không là audit đồng hồ hoặc scheduler SLA.

## Backfill/Spark — dùng evidence đã accepted, không gọi là chạy mới

[DAT-03 Gold](./DAT-03_PORTABLE_GOLD_20261115.json) đã đo commit825,023s,
whole publication session2.555,570s (~42,6phút), rerun1.484,250s (~24,7phút),
đều trong phiên backfill tối đa2giờ đã dùng ở DAT-03. Không suy ra toàn bộ
288tháng extract/parse hoàn tất trong42,6phút.
[Spark90k component](./DAT-03_OFFICIAL_COLLECTIONS_20261115.json) đã đo205,959s,
peak RSS1.236.388KiB; đây là **synthetic component benchmark**, không phải full
research ETL. QA-05 không rerun ETL hoặc huấn luyện để tạo lại số đã tồn tại.

## Khuyến nghị và cleanup

Giữ Trino concurrency1; một job nặng mỗi lúc, checkpoint theo DAT-03; giữ
one-file/month hiện có. Không compact xuyên288partitions chỉ vì file<1MiB.
Chạy backfill/build offline trước demo, dùng pinned report/fallback để demo.

Theo yêu cầu owner, đã dọn đúng venv `exp01-colab` cũ ở checkout team-setup,
hai `spark/target` cache ở checkout team-setup/task-status-replan, và năm bản
snapshot review tạm trong `/private/tmp`. Tổng logical size khoảng1,03GiB;
free host quan sát trước/sau từ3.973.092KiB lên4.523.012KiB (~537MiB tăng ròng,
APFS shared blocks và hoạt động nền làm khác logical sizes). Cache build/venv
có thể tạo lại bằng source/lockfiles; không có archive phục hồi cache cũ.
Giữ nguyên mọi checkout dirty và staging data/evidence; không xóa whole worktree,
warehouse/source pins, Docker images/volumes. Bản mli-02 vẫn cấp Trino config.

## Kiểm chứng và giới hạn

23offline tests PASS,44CI-policy tests PASS; failure checkpoint/no-retry,
snapshot drift, OOM drift, disk reserve, bounds/pins, file counts và A/B parity
được test. Metadata/current pins và cgroup checks của runtime thật đạt.
Raw receipt/checkpoints giữ ignored, SHA bên JSON; không commit artifact data.

Không chứng nhận full ETL cold-start/40năm, absolute peak memory, scientific
quality hoặc production sizing. **SEC-01 — Review secret và bề mặt truy cập**
vẫn có cloud acceptance riêng; **MLQ-01 — ML E2E** và **DEMO-02 — Rehearsal/release**
chưa hoàn tất, không được đổi Done nhờ profile này. Chưa commit/push/mở PR.
