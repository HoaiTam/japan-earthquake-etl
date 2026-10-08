---
task_id: "ORC-02"
status: "Done"
week: 4
block: "F - Điều phối"
workstream: "Scheduling"
scope: "Core"
priority: "P1"
effort_hours: 4
assignee: "HoaiTam"
reviewer: "unassigned"
dependencies: ["ORC-01"]
---

# ORC-02 - Cấu hình lịch và readiness cho hai nguồn

## Mục đích

Dùng để hai nguồn có nhịp cập nhật khác nhau nhưng vẫn vào cùng pipeline có kiểm soát.

## Phạm vi công việc

Chốt daily schedule, timezone, overlap USGS, JMA catalog check, max active runs và điều kiện không chạy trùng daily/backfill.

## Thành phần cần có

- **Đầu vào và contract:** [ORC-01](./ORC-01.md)
- **Phần triển khai:** Chốt daily schedule, timezone, overlap USGS, JMA catalog check, max active runs và điều kiện không chạy trùng daily/backfill.
- **Kết quả bàn giao:** Runtime schedule profile và readiness sensors/checks.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- Runtime schedule profile và readiness sensors/checks.

## Tiêu chí hoàn thành

- [x] Daily run ổn định.
- [x] JMA chỉ kích hoạt năm thay đổi.
- [x] timezone/data interval đúng.

## Hard dependency

- [ORC-01](./ORC-01.md)

## Cách triển khai và phối hợp

### Phân công tuần 4 mở rộng

- **Owner / effort:** HoaiTam, 4h Core; đưa từ tuần 5 lên khối vận hành của [kế hoạch tuần 4](../WEEK_4_PARALLEL_PLAN.md).
- **Dependency / kết quả:** ORC-01 đã Done; ORC-02 đã có profile, readiness source thật, data-interval fixtures và live evidence; không thay nghiệm thu toàn chuỗi Gold.
- **Làm gì / có gì:** USGS UTC daily window/three-day revision overlap, JMA changed-year readiness, timezone điều phối, max-active-runs và daily/backfill collision guards.
- **Dùng để làm gì:** Hai nguồn có nhịp khác nhau vào cùng ETL mà không chạy chồng hoặc xử lý sai ngày.
- **Handoff / nghiệm thu:** Profile/ready checks cho DAG và ORC-03/05; kiểm tra UTC/JST/host boundary, no-change JMA và daily/backfill contention. Daily ổn định cần evidence runtime phù hợp, không chỉ cron string/mock success.

Có thể chuẩn bị interface, fixture, mock, test plan và tài liệu trước khi toàn bộ upstream chạy thật. Chỉ được chuyển sang `Done` khi hard dependency đã đạt và acceptance criteria được kiểm tra trên output phù hợp.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới từ `main`: `feat/orc-02-cau-hinh-lich-va-readiness`.
3. Triển khai đúng phạm vi; dùng fixture nhỏ, xác định được và không phụ thuộc mạng cho unit test.
4. Chạy test/check phù hợp, đối chiếu acceptance criteria và cập nhật tài liệu nếu contract hoặc hành vi thay đổi.
5. Cập nhật `status`, `assignee`, `reviewer` và Evidence ngay trong file này khi mở PR hoặc hoàn tất review.

## Ranh giới

- Không tự mở rộng sang deliverable của task khác.
- Không đổi contract upstream trong PR implementation mà không cập nhật task contract liên quan và có review.
- Không commit secret, credential, payload nhạy cảm, data dump lớn hoặc artifact build không cần thiết.
- Không đánh dấu `Done` nếu chưa có evidence kiểm tra được.

## Theo dõi

- **Trạng thái:** Done
- **Assignee:** HoaiTam
- **Reviewer:** unassigned; khuyến nghị review độc lập cho P1, chưa có approval và không chặn Done theo workflow hiện hành.
- **Evidence / PR:** [Evidence ORC-02](../../evidence/ORC-02.md), [runtime metadata](../../evidence/ORC-02-runtime.json); chưa mở PR.
- **Kỹ năng phù hợp:** Airflow scheduling, config

## Kết quả triển khai — 2026-10-08

- Branch `feat/orc-02-cau-hinh-lich-va-readiness`, từ `origin/main` `f5a62d3`
  (PR #43 ORC-01). [Runbook](../../specs/SOURCE_SCHEDULE_AND_READINESS.md)
  giải thích thành phần, cấu hình, flow, test và handoff.
- Daily DAG chạy 07:15 Asia/Ho_Chi_Minh = 00:15 UTC; explicit timetable,
  catchup=false, max_active_runs=1. Profile chỉ một DAG sở hữu cron.
- USGS resolve target ngày UTC trước interval end, overlap 3 ngày, pin scope
  qua retry. Java fetch/validate/upload/verify; không dry-run giả Ready.
- JMA probe inventory exact watchlist `[1997,2000,2023]`, HEAD + cache SHA và
  MinIO readback; chỉ ingest segment cần cập nhật/bootstrap/checksum audit.
  Năm 1997 verify đủ 2 segments. Không đổi bytes thì reuse exact publication.
- Persistent whole-run lease giữa daily, USGS và JMA backfill; foreign
  cleanup không xóa holder. Strict completion gate không che lỗi upstream.
- `make check` đạt 167 Java + 89 Airflow Python tests. Live Airflow 3.3.2 đạt
  scheduled + hai fixed-window runs (mỗi run 7 success tasks) và controlled
  backfill contention; reports SourcesReady/verified, **published=false**.

### Giới hạn / handoff

- Docker build bị timeout mạng; live QA dùng clean-tested release-17 JAR copy
  vào existing Airflow containers, runtime Java17. Chưa xác nhận image build
  mới; container recreate từ image cũ sẽ mất JAR cập nhật. Cần chạy lại
  `make smoke-source-readiness` khi mạng ổn; **chưa có task riêng** cho lỗi
  mạng Docker Build. Chi tiết tại evidence, không nhận full build là passed.
- [ORC-03 - Implement backfill và reprocessing](./ORC-03.md): historical
  dispatcher/affected partitions và reprocess exact inputs.
- [ORC-04 - Chuẩn hóa logging và run summary](./ORC-04.md): summary toàn ETL;
  hiện report chỉ source gate.
- [ORC-05 - Chốt recovery, concurrency và tài nguyên](./ORC-05.md): stale
  lease recovery và tài nguyên Spark/Iceberg/ML; chưa distributed locking.
- [SLV-09 - Tích hợp và kiểm thử Silver đa nguồn](./SLV-09.md),
  [GLD-03 - Ghi Gold Iceberg và commit snapshot](./GLD-03.md),
  [GLD-04 - Tạo Trino views và verification SQL](./GLD-04.md),
  [QA-01 - Chạy E2E daily đa nguồn](./QA-01.md): adapters và output downstream
  thật, committed/verified Gold Published; ORC-02 không tự trigger mock ETL.
- [QA-05 - Profile dữ liệu lịch sử và tài nguyên local](./QA-05.md): sizing
  full history; 3-year watchlist là pilot, không toàn bộ training coverage.

## Checklist bàn giao

- [x] Deliverable đã có trong repository hoặc môi trường demo.
- [x] Acceptance criteria đã được kiểm tra.
- [x] Test tự động đạt hoặc có evidence thủ công có thể lặp lại.
- [x] Tài liệu/contract đã cập nhật nếu schema, flow, cấu hình hoặc hành vi thay đổi.
- [x] Không chứa secret, dữ liệu nhạy cảm hoặc file build không cần thiết.
- [ ] Reviewer độc lập cho P1 (khuyến nghị, chưa xác nhận; không chặn Done).

## Tài liệu liên quan

- [Lịch nguồn và readiness](../../specs/SOURCE_SCHEDULE_AND_READINESS.md)
- [Evidence ORC-02](../../evidence/ORC-02.md)
- [Kế hoạch tuần 4 mở rộng](../WEEK_4_PARALLEL_PLAN.md)
- [Kế hoạch 8 tuần](../README.md)
- [Các khối công việc](../WORK_BLOCKS.md)
- [Baseline MVP, KPI và Definition of Done](../../specs/MVP_SCOPE_KPI_AND_DOD.md)
