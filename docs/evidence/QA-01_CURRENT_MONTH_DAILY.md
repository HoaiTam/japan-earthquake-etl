# QA-01 — Daily v2 deployment, 22/10/2026

**Cập nhật hiện hành 23/10:** [scheduled recovery acceptance](./QA-01_CURRENT_MONTH_ACCEPTANCE.md)
đã PASSED, 8/8 task và 42 SQL; QA-01 Done cho operational v2. Mốc 22/10 và
capture failure attempt1 bên dưới giữ làm lịch sử, không giả first-attempt SLA.

## Cập nhật recovery được owner duyệt ngày 23/10/2026

Scheduled `scheduled__2026-10-23T00:15:00+00:00`, operation
`cm1-847b2845f3e2f5ded8e815776e69d9756a65fcfa`, đã failed tại capture lần đầu;
ba task resolve/acquire/JMA success, downstream chưa chạy. Capture bắt đầu
00:39:25Z, task kết thúc 01:40:13Z; máy macOS liên tục sleep/dark-wake trong
khung 07:39–08:40 local. Request đang chờ bị gián đoạn rồi tiếp tục lúc wake;
phase không hoàn tất trong giới hạn 1800 giây. Không kết luận HTTP 200 hoặc
partial objects là source-ready. Năm part receipts còn nguyên, chưa selection
hoặc `_SUCCESS` tại lúc kiểm tra trước recovery.

Owner đã chọn **phục hồi chính scheduled run này để nghiệm thu hôm nay**,
thay lựa chọn cũ giữ nguyên failed run 23/10. Không tạo manual run, không đổi
cron 07:15 hoặc sửa nhãn/type/interval/cutoff/pins. Run 22/10 vẫn failed, không
clear nó. Sau xác nhận active DagRuns=0, scheduler không còn Java writer và
Spark master activeapps=0, dùng Airflow `SerializedDAG.clear` chính thức với
exact `run_id`, chỉ năm task `usgs_bronze`, `sources_ready`, `real_etl`,
`release_source_lease`, `completion_gate`. Dry-run xác nhận đúng năm rows trước
clear. CLI preview theo logical_date trả rỗng nên không dựa vào date filter
để chọn run. Ba upstream success giữ nguyên; lease cùng owner vẫn held, không
release/takeover. Unpause cùng daily để scheduler tiếp tục run đã clear.

Log capture cũ giữ nguyên SHA-256
`6350f99f799b693dccae18ebb8abc08c10deff1677492454706f1f731ae7e7cd`.
Bản vá Python lưu mỗi phase attempt trong thư mục private mới
`phase-attempts/<phase>-<random>/`, không overwrite log cũ. Timeout/nonzero/
invalid JSON/output guard có outcome reason code, không echo argv/payload.
`RETURNED_JSON` chỉ là kết quả subprocess, không thay success receipt/ETL gate.
Không tự retry hoặc giải phóng lease trên timeout; phải kiểm writer trước recovery.
Runner JAR và context pins giữ nguyên; Python runtime được bind mount và kiểm
SHA tại deployment. Dùng `caffeinate -i -t 7200` tạm thời chống idle sleep cho
phiên nghiệm thu; không thay setting macOS vĩnh viễn, không ngăn mọi lid-close.

Kiểm thử bản vá: `make test-qa-01` **75 tests PASS**;
`TMPDIR=/private/tmp make ci-airflow` **450 tests PASS**. Lần chạy full suite
bằng TMPDIR mặc định của macOS fail ở path guards QA-03/QA-04 do ancestor
symlink; chạy lại trên canonical `/private/tmp` đạt, không sửa guard để né lỗi.
Kết quả actual task/collector recovery: **8/8 success và qa-01-v2 PASSED**;
xem [acceptance/pins/SQL](./QA-01_CURRENT_MONTH_ACCEPTANCE.md). Không suy Done
chỉ từ clear hoặc tests; default Make cached-image preflight vẫn chưa đạt,
deployment explicit có [mount/file hash proof riêng](./QA-01_DEPLOYMENT_20261023.json).

## Kết luận tại mốc 22/10 (lịch sử)

**In Progress — chưa nghiệm thu E2E thành công.** Bước 3 implementation đã có:
cùng daily DAG tám task, runtime riêng, Java phase bridge, read-only collector
`qa-01-v2`. Không nâng historical COMPLETE_MONTHS hoặc research/ML eligibility.

Branch `test/qa-01-scheduled-runtime-acceptance`, tiếp tục PR #96 theo owner.
Working tree chưa commit/push lúc deploy; base commit
`7e95eb49d4523010403470bb7a15a1e441fd5689` không đại diện một mình cho binary mới.
Local override pin runner SHA
`cd30d98d09808d1ca617b866c57bcff00bd0aef1f70cefe3fd806146e00aa51d`.

## Deployment và scheduled run thật

- Cached Airflow với readonly prebuilt runner/Iceberg jars; không Docker
  build/pull/init/reset. Metadata Postgres cũ được start lại.
- Airflow API/scheduler/dag-processor/Postgres, MinIO, REST Catalog, Trino healthy;
  DAG import errors `[]`, runner SHA trong container khớp pin.
- Ban đầu daily paused; unpause để scheduler tạo run. Không manual/trigger,
  không SQL tạo/sửa DagRun hoặc mark-success.
- Run `scheduled__2026-10-22T00:15:00+00:00`, type `scheduled`.
- Interval `[2026-10-21T00:15:00Z, 2026-10-22T00:15:00Z)`.
- Start `2026-10-22T11:15:32.461758Z`, end `2026-10-22T11:15:38.130038Z`.
- State **failed**: resolve failed, sáu task upstream_failed, release failed
  khi không có completed result. Tổng tám task. Chưa lease/capture/ETL/publication
  từ run này.

SDK truyền enum `DagRunType.SCHEDULED`; `str(enum)` không là `scheduled`.
Đã normalize `.value` và thêm regression. Probe read-only dùng SDK model từ
chính DB row đã trả đúng run type/interval/empty conf sau vá; không viết control
folder hay chạy lại task. Telemetry outer che inner exception: đây là lỗi
tương thích đã xác nhận và sửa, chưa chứng minh toàn pipeline hết lỗi.

## Lịch và nghiệm thu còn lại

Owner chọn giữ **07:15 Asia/Ho_Chi_Minh**, chờ **23/10/2026** (`00:15Z`), không
đổi cron, clear/retry hoặc sửa state run 22/10. Readback cuối phiên xác nhận
daily `is_paused=false`, `next_dagrun_create_after=2026-10-23T00:15:00Z`,
active runs = 0, source lease không có owner; JMA backfill vẫn paused.
Máy/Docker/stack cần tiếp tục chạy; không recreate chỉ base Compose vì có thể
trả mode về disabled. Follow-up trong cùng chat đã đặt 07:30 mỗi ngày để đọc
kết quả scheduled; sẽ dừng sau khi bàn giao kết quả terminal của phiên nghiệm thu.

Chỉ Done sau scheduled run tám task success, same-run MinIO captures/Silver/Gold,
sáu snapshots/Trino readback và collector v2 PASSED, research/ML flags false.
Sau khi run nghiệm thu kết thúc (success hoặc failed) và không active run,
pause lại theo trạng thái ban đầu; failure giữ nguyên lease/artifacts. Xem
[runbook](../runbooks/CURRENT_MONTH_DAILY.md).

## Kiểm thử

- `make test-qa-01`: 71 tests offline đạt; SDK enum, pin, lease, phase failure,
  collector 42 SQL proofs.
- `CurrentMonthGoldTest`: ba tests Spark/Iceberg local đạt, thêm independent
  audit và publication tamper guard. SQL transport fixture, không Trino live.
- `make test-ci-workflow`: 38 tests đạt; Docker guard: 54 tests đạt.
- `make ci-airflow`: **446 tests đạt**; `./mvnw -pl spark -am test` với JDK21 và
  loopback Spark: **463 tests, 0 failure/error/skip**, BUILD SUCCESS. Lần đầu
  sandbox không cho bind socket, đã chạy lại với quyền loopback; không sửa code
  để né test và không ghi lần sandbox lỗi là PASS.
- `make ci-docs-base`: đạt docs/contracts/security/54 Docker guards/static
  Compose/MinIO/Trino/CI checks; không gọi nguồn thật. `make check-config`,
  `make check-docs` (222 files, 2008 links), `make check-task-status` và
  `git diff --check` đạt. Docs checksum/link count có thể tăng khi bổ sung evidence.

## Phần thiếu có tên

- **QA-01 — Daily E2E:** scheduled current-month acceptance, MinIO → Gold → Trino
  cùng operation thực tế và collector PASSED. Run lỗi 22/10 giữ nguyên.
- **QA-02 / QA-04:** fixtures mới không chứng minh live operational rerun/failure;
  evidence historical không đổi.
- **Chưa có task riêng:** JMA sau baseline/auto deletion/replan vượt budget.

Reviewer `unassigned`, chưa có approval mới.
