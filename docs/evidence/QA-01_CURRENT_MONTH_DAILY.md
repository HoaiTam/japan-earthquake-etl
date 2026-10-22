# QA-01 — Daily v2 deployment, 22/10/2026

## Kết luận hiện tại

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
