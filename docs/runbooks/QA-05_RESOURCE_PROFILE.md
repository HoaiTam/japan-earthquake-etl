# QA-05 — Profile dữ liệu lịch sử và tài nguyên local

Owner HoaiTam; [task](../task/tasks/QA-05.md). Đọc
[nghiệm thu QA-01](../evidence/QA-01_CURRENT_MONTH_ACCEPTANCE.md),
[QA-03](../evidence/QA-03_RUNTIME_ACCEPTANCE.md),
[Gold DAT-03](../evidence/DAT-03_PORTABLE_GOLD_20261115.md) trước.

## Mục tiêu và giới hạn

Đo khả năng phục vụ demo trên dữ liệu lịch sử thật đã Published: sáu bảng Gold
của 288 tháng; không tải lại nguồn hoặc chạy lại ETL để đo performance.
Ba workload scan/aggregate toàn bộ 4.488.650 event, hai session profiles và
file/partition metadata. Đây không phải full ETL, cold-start SLA hoặc benchmark
training. Thời gian Spark/backfill đã accepted được dẫn nguồn riêng, không
trộn với thời gian SELECT hiện tại hoặc nhận là phép đo mới.

Cửa sổ của phép đo: mỗi SELECT <=60s, suite <=600s (không bao gồm Docker
startup hoặc ETL). Session query memory <=256MB; không đổi service config.
Đo A/B không bắt buộc candidate nhanh hơn: giữ baseline nếu chưa có lợi ích
đáng tin, không quảng cáo speedup từ hai lần đo.

## Setup và chạy

Python 3.11+ chuẩn, Docker CLI/context `desktop-linux` và deployment hiện có.
Trong scheduler cần boto3/SQLAlchemy như runtime của OPS-01; không tự cài package.
Không cần Maven/Spark build hoặc thêm venv. Năm service healthy:
MinIO, REST Catalog, Trino, scheduler, PostgreSQL. Trước mỗi lượt phải không có
Java/DagRun/Trino query active, source lease trống. Giữ host free >=4GiB trước
và trong phiên; không lấy free trong Docker VM thay host free.

```bash
make test-qa-05
make profile-qa-05 QA05_OUTPUT=staging/qa05-operator-session-01
```

Output bắt buộc là folder mới dưới `staging/`; không ghi đè receipt cũ. Nếu
failed/unknown, đọc checkpoint, xác định nguyên nhân trước khi tạo lượt mới;
không tự retry, clear/mark-success, release lease hoặc khởi động service.

## Quy trình và bằng chứng

1. Verify SHA của evidence Gold đã commit, đủ sáu literal snapshot IDs/counts.
   Đọc REST Catalog: current snapshot phải đúng pin; lưu SHA metadata/pointer.
2. Đọc `$files`/`$partitions`: file bytes/min/max, records, partitions, file
   <1MiB, chặn delete files hoặc count không khớp. Metadata tables dùng current;
   vì vậy yêu cầu identity/current pin trước và sau suite, không giả hỗ trợ
   time travel metadata tables ở mọi phiên bản Trino.
3. Ba truy vấn sinh cố định, không nhận SQL tùy ý: fact summary (đọc magnitude,
   latitude, depth), aggregate theo tháng, aggregate magnitude/depth bands.
   Dữ liệu fact đọc bằng `FOR VERSION AS OF` đúng snapshot, không dùng latest.
4. Warm-up fact summary một lần/profile, không đưa warm-up vào median. Sau đó
   hai repetitions/query/profile, thứ tự AB rồi BA; A=`task_concurrency=1`,
   B=`2`. Cùng `query_max_run_time=60s,query_max_memory=256MB`.
5. Counts phải khớp 4.488.650; so kết quả giữa hai profiles/repetitions. Integer
   counts giữ nguyên; chỉ float aggregates chuẩn hóa 7 significant digits để
   bỏ sai số cộng song song. Đây không phải phép so sánh nhãn/model khoa học.
6. Lưu 14 query checkpoints: 2 warm-ups +12 measured queries, query ID,
   wall/CPU/engine memory/input/spill. Có thêm12 metadata SELECTs, tổng26
   Trino SELECTs. Không có DDL/DML, S3 writes hoặc ML experiment mới.
7. Docker stats sampling: 5s chờ giữa các lần lấy mẫu, cộng latency Docker CLI;
   báo sampled peak, không gọi là peak tuyệt đối/continuously observed RSS.
   Kiểm cgroup `oom`/`oom_kill` cả năm service trước/sau, container IDs/start/
   restart counts và metadata không đổi; runtime idle/lease trống khi kết thúc.

`receipt.json` chỉ xuất khi toàn bộ gate đạt. `intent.json`, `query-XX.json`,
`failed.json` giữ trạng thái thật khi dừng. Không ghi SQL payload, env, credential
hoặc raw Docker inspect vào report. Raw receipt ignored; evidence public chỉ
chứa hashes/counts/metrics, không tài khoản hay private paths.

## Cấu hình khuyến nghị trên máy 8GiB RAM

- Giữ `task.concurrency=1` của profile Trino hiện hành. Candidate2 chưa nhanh
  hơn trên phép đo17/11; không recreate service chỉ để áp dụng config đã có.
- Chạy một batch/query suite nặng tại một thời điểm. Không chạy Spark unit
  suites song song ETL; không tự bật Spark/UI để đo query serving.
- Giữ file sizing hiện tại: hai fact mỗi bảng288files/288partitions, tức một
  file/tháng. File nhỏ theo volume tháng không tự động là lỗi cần compaction;
  không gộp xuyên partition/cắt history khi chưa có benchmark và quyền riêng.
- Full backfill/research build chạy offline trước demo, theo checkpoint/budget
  DAT-03 đã có; không chạy lại288tháng trên sân khấu. Dùng package/fallback của
  [DEMO-01](./DEMO-01_DEMO_PACKAGE.md).
- Giữ disk reserve >=4GiB, giữ warehouse/source pins/evidence. Cache/venv có
  thể tạo lại không phải research artifacts; không xóa cả worktree nếu còn
  ignored evidence/uncommitted changes hoặc Docker bind mounts.

## Nghiệm thu và việc khác

[Actual profile17/11](../evidence/QA-05_RESOURCE_PROFILE_20261117.md) có metrics,
before/after session settings, giới hạn đo và refs backfill/Spark đã accepted.
QA-05 không đóng thay **SEC-01 — Review secret và bề mặt truy cập**,
**MLQ-01 — ML E2E** hoặc **DEMO-02 — Rehearsal/release**.

Tham chiếu chính thức: [Iceberg time travel/metadata tables](https://trino.io/docs/current/connector/iceberg.html),
[task concurrency](https://trino.io/docs/current/admin/properties-task.html).
