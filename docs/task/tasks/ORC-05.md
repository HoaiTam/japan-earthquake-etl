---
task_id: "ORC-05"
status: "Done"
week: 4
block: "F - Điều phối"
workstream: "Recovery and resources"
scope: "Core"
priority: "P1"
effort_hours: 4
assignee: "HoaiTam"
reviewer: "unassigned"
dependencies: ["ORC-01"]
---

# ORC-05 - Chốt recovery, concurrency và tài nguyên

## Mục đích

Dùng để nhiều thành viên chạy task/backfill linh hoạt mà không tranh tài nguyên hoặc phá dữ liệu của nhau.

## Phạm vi công việc

Cấu hình retry boundary, Spark resources, task concurrency, staging cleanup an toàn và hướng xử lý lỗi ở từng tầng.

## Thành phần cần có

- **Đầu vào và contract:** [ORC-01](./ORC-01.md)
- **Phần triển khai:** Cấu hình retry boundary, Spark resources, task concurrency, staging cleanup an toàn và hướng xử lý lỗi ở từng tầng.
- **Kết quả bàn giao:** Runtime profile, recovery matrix và operator notes.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- Runtime profile, recovery matrix và operator notes.

## Tiêu chí hoàn thành

- [x] Máy local không OOM ở data đại diện: exact Bronze readback hai nguồn, JMA first10000 lines + USGS16 features qua Java/Spark; driver/worker OOM counters=0.
- [x] Retry đúng tầng: mutating task retries=0, USGS read-only verify retries=1, HTTP retry riêng trong Java; controlled recovery source phase giữ pins/count.
- [x] Không cần xóa dữ liệu để phục hồi: lease contention/rerun và maintenance preview chỉ giữ/read metadata, không xóa bucket/volume/warehouse.

## Hard dependency

- [ORC-01](./ORC-01.md)

## Cách triển khai và phối hợp

### Phân công tuần 4 mở rộng

- **Owner / effort:** HoaiTam, 4h Core; đưa từ tuần 5 lên [kế hoạch tuần 4](../WEEK_4_PARALLEL_PLAN.md).
- **Đã triển khai:** Retry/resource/concurrency profile và recovery matrix theo ORC-01; [evidence](../../evidence/ORC-05.md) ghi hardware, measured pilot và deployment boundary thực tế.
- **Làm gì / có gì:** Spark resources, limits theo layer, daily/backfill contention, retry boundary và scoped staging maintenance, không xóa bucket/volume/warehouse để recover.
- **Dùng để làm gì:** Team test/backfill linh hoạt trên local, tránh OOM và ghi đè output người khác.
- **Handoff / nghiệm thu:** Ghi cấu hình máy/data scope/runtime measurement và recovery run có kiểm soát; profile/endpoints/permissions/log scope cho SEC-01. Data đại diện chỉ theo pilot coverage, không nhận là benchmark full 40 năm.

Có thể chuẩn bị interface, fixture, mock, test plan và tài liệu trước khi toàn bộ upstream chạy thật. Chỉ được chuyển sang `Done` khi hard dependency đã đạt và acceptance criteria được kiểm tra trên output phù hợp.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Branch thực hiện theo yêu cầu nền GitHub mới nhất: `feat/orc-05-recovery-resource-profile`, từ merge PR #47 (`752cce27`); topology main/PR ghi trong evidence.
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
- **Reviewer:** unassigned; chưa có review độc lập, không tự ghi approval.
- **Evidence / PR:** [ORC-05 evidence](../../evidence/ORC-05.md), [runtime report](../../evidence/ORC-05-runtime.json); chưa mở PR/commit/push.
- **Kỹ năng phù hợp:** Airflow recovery, Docker, Spark

## Checklist bàn giao

- [x] Deliverable đã có trong repository hoặc môi trường demo.
- [x] Acceptance criteria đã được kiểm tra trong phạm vi local/pilot.
- [x] Test tự động đạt và có evidence runtime lặp lại.
- [x] Tài liệu/contract đã cập nhật nếu schema, flow, cấu hình hoặc hành vi thay đổi.
- [x] Không chứa secret, dữ liệu nhạy cảm hoặc file build không cần thiết.
- [ ] P0/P1 có reviewer khác assignee xác nhận.

## Tài liệu liên quan

- [Recovery/resource profile và operator notes](../../specs/RECOVERY_AND_RESOURCES.md):
  retry matrix từng tầng, heap/concurrency caps, `.env` cũ/deploy mới, safe
  staging preview, stale lease quiescence và partial commit reconciliation.
- **Giới hạn nghiệm thu:** Không benchmark full40năm, không fault-inject writer
  Silver/Gold thật, không scheduler E2E Published. [SLV-09 - Tích hợp và kiểm thử
  Silver đa nguồn](./SLV-09.md), [GLD-03 - Ghi Gold Iceberg và commit snapshot](./GLD-03.md),
  [GLD-04 - Tạo Trino views và verification SQL](./GLD-04.md) và
  [QA-01 - Chạy E2E daily đa nguồn](./QA-01.md) thực hiện integration/recovery
  writer/snapshot thật. [SEC-01 - Review secret và bề mặt truy cập](./SEC-01.md)
  nhận handoff profile/permissions/endpoints/log scope. Broad retention chưa có task riêng.

- [ORC-03 backfill/reprocessing handoff](../../specs/BACKFILL_AND_REPROCESSING.md):
  single-task/shared whole-run lease, immutable operation scope và fail-closed
  baseline/idempotency protocol; ORC-05 chốt local retry/resource/whole-run lease
  và matrix stale-lease/cache-loss/partial commit; writer integration thật theo
  các task handoff bên trên, không coi scoped adapter doubles là commit evidence.

- [Kế hoạch tuần 4 mở rộng](../WEEK_4_PARALLEL_PLAN.md)
- [Kế hoạch 8 tuần](../README.md)
- [Các khối công việc](../WORK_BLOCKS.md)
- [Baseline MVP, KPI và Definition of Done](../../specs/MVP_SCOPE_KPI_AND_DOD.md)
