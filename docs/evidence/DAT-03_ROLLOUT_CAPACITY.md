# DAT-03 — Rollout preparation / Trino trial, 25/10/2026

Branch `feat/dat-03-research-rollout`, từ main `8ef6c41` sau PR #100 merge.
Owner HoaiTam, reviewer unassigned. **In Progress; khối3/4 chưa hoàn tất**.
Owner chọn máy hiện tại, <=2h/phiên có checkpoint; cho phép backup và recreate
**riêng Trino**, cached image, không build/pull/Airflow/MinIO/credential/data.
[Runbook và cơ chế](../runbooks/DAT-03_RESEARCH_ROLLOUT.md).

Cập nhật sau approval bổ sung: [phiên stop4/restore4](./DAT-03_LIGHT_SESSION.md)
đã hoàn tất,9service healthy/cùng IDs/config; full probe chặn512MB, bucket0–2
đạt/bucket3timeout,12chưa chạy. Không full capacity hoặc catalog/export. Phần
dưới giữ bằng chứng của đợt chỉ chỉnh Trino, không ghi lại failure thành PASS.

## Source / planning đã đạt

- Fresh verify25 JMA original capture/profile pins và lập288 distinct months:
  REPRODUCTION225, EXTENSION63. h2024 là boundary, December2022 cần USGSJan2023
  halo. Planner PLAN_ONLY, không tạo coverage/Gold/research-ready.
- 12 USGS2023 responses đã capture/Java-validate;11 GET mới, December reuse
  original bytes/retrieval. **December thực lấy24/10/2026 14:19:35Z**, không
  đổi retrieved_at thành thời điểm copy25/10. Các tháng khác lấy25/10;
  không tuyên bố đồng nhất source as-of vì chỉ có upper-bound cutoff.
- Tổng1.278.062 bytes,1.809 response features **bao gồm halo duplicates**:
  Jan102/Feb107/Mar110/Apr116/May155/Jun120/Jul118/Aug109/Sep148/Oct313/Nov236/Dec175.
  Đây không phải unique/canonical/ML sample count. Không thêm magnitude floor.
- Local ignored root `staging/dat03-usgs-2023-20261025`, summary
  `summary-b0cab8d6f34efc4e99de8cfdaa03df7e778a668769bdf3dc36d62ed45021d168.json`;
  external summary SHA
  `e891729ce134d493fe3d7442a174f5a8561ada62445e0df839a0db17b54e2d7d`.
  Java runner SHA
  `b0cab8d6f34efc4e99de8cfdaa03df7e778a668769bdf3dc36d62ed45021d168`.
  JAR exact đã giữ riêng ở cùng root, tên `runner-<SHA>.jar`, không phụ thuộc
  target bị build sau. Những bổ sung guard integer overflow có unit test riêng;
  không gán pin build/code mới cho summary đã tạo bằng runner trên.
  BronzeReady/coverage_verified/research_ready/official_exported đều false.

## Capacity trước profile — giữ nguyên failed evidence

Exact bounded namespace `iceberg.dat03_e2e_8ca2be1939db4111ae783f6566650fce`,
outside December2023. So sánh full rows ở snapshot1563218669006267106 và
8023265233980557801, EXCEPT hai chiều. Base105.282 hàng ×32 =3.369.024 hàng
mỗi phía; synthetic expansion không là bộ nghiên cứu.

| Attempt dưới deployment/staging/dat03-capacity | Phép đo thực | Kết quả |
|---|---|---|
| `8feae5aeed954d52b3d68ab6ab78f043` | Full,90s | BLOCKED_TIME, wall99,261s; query20261025_032149_00180_j5h9s |
| `f737b4d89eb349beb8c5d68078b1e455` | Buckets-v1,90s | Chỉ bucket0 chạy rồi BLOCKED_TIME; không phải16 buckets đạt |
| `1ee3aa91ba794ac5961b999ae63b73d8` | Full,300s | CLI exit1; server query20261025_034652_00182_j5h9s FAILED/ABANDONED_TASK |

Probe thứ ba: CLI ghi “Query aborted by user” nhưng không có thao tác cancel
của owner/agent. Server/log xác nhận ABANDONED_TASK: elapsed4,49m, CPU1,70m,
peak424.885.568B; không suy đoán người dùng ngắt hay khẳng định GC/clock là
root cause. Query bucket0: elapsed1,55m, CPU18,27s, peak234.542.648B.
Bốn service trước/sau vẫn same IDs/start/restart0; không OOM ở các phép đo này.
Old SHOW SESSION cũng quá hạn30s, chưa đọc được giá trị thực default.

## Trino-only trial đã triển khai, chưa capacity accepted

Private backup/deployment folder:
`staging/dat03-trino-research-cb606568e1724c5a9b268a7cbfb36ccd` trong deployment.
Giữ before-config, before-JVM, retained Docker/file logs, override, plan và
execution. Không đưa logs/env/credentials vào Git.

- Candidate config SHA `ecb9b6f79fcbfb7649520f99504d63446821e51620a64b562b9121319de65569`.
- Candidate JVM SHA `aa0864b628352343c3be40d3effdb030f0aad8998580774e0532fb631ad0edea`.
- Plan SHA `347029519230543c1ff6324e7e1e77d67ac9378102b34084c3fe057d83be225f`.
- `execution.json` COMPOSE_RETURNED/exit0; apply180s health gate **failed**.
- Trino old `b4c7457d38b8…` → new `21dbe13d7536…`, started04:23:36Z,
  SERVER STARTED04:28:59Z (~322,6s). Healthy muộn, restart0; không sửa lần
  apply deadline thành PASS. New JVM chọn G1 region1MiB và startup log xác nhận
  task.concurrency cấu hình1; chưa đọc lại bằng SQL session thành công.
- Read-only late-health receipt đạt exact4service runtime/security/env/mount
  pins, chỉ Trino khác container ID; scheduler/MinIO/REST unchanged.
  LATE_HEALTH_VERIFIED_NOT_CAPACITY_ACCEPTED; full_capacity_accepted=false.
  Receipt controller pin là code tại lúc chạy, trước phần bổ sung guard/tests;
  không gán SHA code mới cho receipt lịch sử.
  Sau bổ sung guard, read-only rehash backup/candidate pins cũng đạt; old plan
  chưa có Compose-source content hashes, không retroactively chứng nhận chúng.

## Sau trial — đã đo lại, chưa chạy ETL mới

| Kiểm tra | Actual result |
|---|---|
| SHOW SESSION task_concurrency,30s | Query20261025_043406_00000_g4cap FAILED/EXCEEDED_TIME_LIMIT; planning27,35s |
| SELECT1,60s | Query20261025_043558_00001_g4cap FINISHED, returned trino_ready=1; server elapsed56,44s, CPU417ms |
| Full32copy probe90s sau trial | Dừng ROLLOUT_SERVICES_NOT_READY **trước submit SQL/tạo attempt**; không có stress query/receipt mới |
| Health04:38Z | Trino/MinIO/REST healthy; Airflow scheduler và DAG processor unhealthy, healthcheck exit-1 ~30–33s |

Đến khoảng04:49Z hai Airflow service tự healthy lại, same containers, không
có restart/stop hoặc đổi cấu hình của agent. Failed preflight ở trên là mốc
lịch sử, không khẳng định hai service luôn unhealthy.

Sau thay đổi readiness có ý nghĩa, chạy **một** full32copy probe mới, không
retry query unknown. Actual folder `staging/dat03-capacity/10fe81166d554657a88865cdd5c15e63`:

- Query `20261025_045023_00002_g4cap`, server FAILED/EXCEEDED_TIME_LIMIT;
  execution1,50m, elapsed1,56m, CPU29,48s, peak230.585.784B. Không là memory-limit
  failure và không chứng minh root cause. CLI wall116,248s, whole probe117,849s.
- Receipt **BLOCKED_TIME**, postflight PASSED_UNCHANGED: bốn service healthy,
  same IDs/start/restart0, source lease absent, no active DagRun/Java/Trino query.
- Request SHA `ec35d5648ec1a41add5b38e928d89f1b5c749bc799f1d761aed12bed7ea2b316`.
  Receipt SHA `8aaa3022eb60befd13c44e40cdc23ef1d39ed27af5732802b41155b10e336f6d`.
  Không có query retry hoặc ETL tiếp theo; full_capacity_accepted=false.
- Profile trial vẫn gắn vào riêng container Trino, chưa capacity accepted;
  default repo config/Compose giữ nguyên. Không tự rollback/recreate lần hai.

Kiểm thêm buckets-v1 sau full probe: attempt
`staging/dat03-capacity/6666b48002f54520b0c96c4296a37e48`, query
`20261025_045642_00003_g4cap`. **Bucket0 FAILED/EXCEEDED_TIME_LIMIT**, server
elapsed1,51m, CPU816ms, peak18.902.848B; CLI wall125,501s, whole126,990s.
Chỉ1/16 query được chạy và query đó lỗi;0 bucket thành công,15 chưa chạy.
Postflight PASSED_UNCHANGED/no active query, không resubmit/cancel.
Request SHA `f47709713923d3f0b09272c0e12174c81e46a842d6fef4ca0b96c9e4a4725dba`,
receipt SHA `c8b6d51de8c09561eee0e9aad472f1b70212cb74706cc6f7efc0b624547f87cb`.
Không coi chiến lược chia bucket đã đạt capacity hoặc nối vào runtime từ
unit tests. Dừng phép đo; giữ full_capacity_accepted=false.

Host có8GiB RAM, image Trino native arm64 và container aarch64; không có bằng
chứng image x86 chạy giả lập. Snapshot có counters compression/swap tích lũy
và nhiều service CPU cao; counters không phải rate riêng trong probe. Cần đo
tiếp; **không chứng minh** nguyên nhân gốc hoặc profile làm nhanh/chậm hơn.
Không có controlled benchmark
PASS sau trial. Không tự đổi Airflow/Spark/Docker resource settings, không
ngụy tạo capacity acceptance bằng kéo timeout hoặc nới memory guard.
Read-only metadata kiểm active DagRuns=0, source lease absent và active Java
trong scheduler=0. Bốn truy vấn sau trial đều terminal; lần preflight bị chặn
không submit SQL, probe sau readiness là attempt riêng đã ghi bên trên.
Không auto pause/clear/trigger DAG hoặc release lease.

Read-only host sampling trong10,264s khi đo bucket mới: swap-in19.180 pages,
swap-out18.284 pages, page16KiB →29,20/27,83MiB/s. Compression759.461 và
decompression761.917 page operations (có thể lặp trên cùng page, không là RAM
unique). Khác counters tích lũy, đây là **rate trong một khoảng đo thật**, cho
thấy host đang có memory churn. Docker VM báo5.685.809.152 bytes RAM/8CPU,
host8GiB. Không từ một mẫu suy riêng Trino gây áp lực hoặc hứa profile khác
sẽ hết lỗi; chưa có controlled comparison sau khi giảm tải host.
Cuối phiên active DagRuns=0/lease absent, nhưng còn1 DAG unpaused
(`afl_01_smoke`, source khai báo schedule=None); không coi
idle hiện tại là tất cả lịch đã pause, không tự dừng Airflow/Spark hoặc đổi cron.

## Đã triển khai trong code, chưa nghiệm thu full runtime

- Typed/exact planner/capture/capacity guards, immutable request/receipt và
  failure logs; no auto retry/release. Source payload validation bằng Java.
- Opt-in Gold outside-scope full-row16bucket component có unit regression;
  default/operational publisher giữ nguyên. **Chưa nối catalog runtime**, chưa
  chứng minh16queries/full-period capacity bằng dữ liệu thật.
- Không có Bronze/Silver/Gold catalog writes, monthly rollout, new publication,
  collection export hoặc official lifecycle transition trong đợt chuẩn bị này.

## Bước còn thiếu theo task

**DAT-03 — Catalog nghiên cứu thật:** xử lý readiness/capacity trước, sau đó
resumable controller có source/cutoff/JAR pins, <=2h/session, monthly checkpoint,
six snapshots/quality/Trino/retention, đủ225+63 tháng; không tự release foreign
lease hoặc chạy source daily để thay catalog.

**DAT-03 — Official collection export:** versioned collection-aware MLI-02/
registry/loader/import và actual full-period real export vẫn chưa làm. Không
đổi RESEARCH_COLLECTION_PREPARED thành EXPORTED bằng metadata. Khối5 receipts
ba máy ngoài đợt3/4 này; hai thành viên chưa gửi receipt.

Không chứng nhận EXP-02/03/04/05 official experiment, MLI-04/MLQ-01 scientific
report/E2E hoặc SEC-01 cloud bằng kết quả trên.

## Kiểm thử code / docs

- Python `test_research_*.py`: **84 PASS**, gồm capture/planner/capacity,
  immutable failures/no retry, Trino idle/profile/scope/backup và Compose pins.
  Các log STRESS_PROBE_PASSED trong suite là **mock fixture**, không runtime.
- Java21, target17, offline: **31 PASS**, 0 errors/skips: capture9, bucket6,
  default publication5, operational policy5, Trino HTTP protocol6.
- Lần Java đầu trong sandbox có6 socket-bind errors ở HTTP fixture; không
  tính PASS. Chạy lại với quyền loopback cần thiết đạt31/31, không deployment.
- `make ci-docs-base` đạt; task/index77 files đồng bộ, docs243 files/2207 links/
  0 problems, config/example secret hygiene và git diff --check đạt.
- Fresh read-only verify12 actual USGS captures bằng **exact pinned JAR** đạt
  và summary SHA không đổi; không HTTP call, không biến capture thành coverage.
- Full-period Spark/Gold/export/runtime capacity vẫn chưa đạt; local tests
  không thay acceptance này. Không commit/push/merge tự động.
