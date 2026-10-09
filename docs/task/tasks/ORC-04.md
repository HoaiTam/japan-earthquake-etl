---
task_id: "ORC-04"
status: "Done"
week: 4
block: "F - Điều phối"
workstream: "Observability"
scope: "Core"
priority: "P1"
effort_hours: 4
assignee: "HoaiTam"
reviewer: "unassigned"
dependencies: ["ORC-01"]
---

# ORC-04 - Chuẩn hóa logging và run summary

## Mục đích

Dùng để theo dõi pipeline, điều tra lỗi và cung cấp freshness/evidence cho demo.

## Phạm vi công việc

Ghi input, fetched, parsed, valid, rejected, duplicate, linked, published, snapshot và duration theo run/source.

## Thành phần cần có

- **Đầu vào và contract:** [ORC-01](./ORC-01.md)
- **Phần triển khai:** Ghi input, fetched, parsed, valid, rejected, duplicate, linked, published, snapshot và duration theo run/source.
- **Kết quả bàn giao:** Structured logs, metrics và run summary cuối DAG.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- Structured logs, metrics và run summary cuối DAG.

## Tiêu chí hoàn thành

- [x] Count các tầng đối soát được từ run_id.
- [x] Không log secret/payload lớn.
- [x] Lỗi có reason rõ.

## Hard dependency

- [ORC-01](./ORC-01.md)

## Cách triển khai và phối hợp

### Phân công tuần 4 mở rộng

- **Owner / effort:** HoaiTam, 4h Core; đưa từ tuần 5 lên [kế hoạch tuần 4](../WEEK_4_PARALLEL_PLAN.md).
- **Làm trước:** Structured log/summary serializer và count fixtures theo ORC-01 phase I/O; không chờ network để viết tests.
- **Làm gì / có gì:** Run/source/release/snapshot context, fetched/parsed/valid/rejected/duplicate/superseded/linked/current counts, durations và status/reasons.
- **Dùng để làm gì:** Đối soát và điều tra failure mà không log full payload hoặc credential.
- **Nghiệm thu / handoff:** Runtime summary trace về exact manifests/snapshots, counts có định nghĩa không trộn history/current hoặc link/event grain; secret/redaction tests và failed/empty/rerun summaries. Không suy Published chỉ từ task exit code hoặc mock marker.

Có thể chuẩn bị interface, fixture, mock, test plan và tài liệu trước khi toàn bộ upstream chạy thật. Chỉ được chuyển sang `Done` khi hard dependency đã đạt và acceptance criteria được kiểm tra trên output phù hợp.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Branch `feat/orc-04-structured-run-summary`; lần cập nhật theo yêu cầu user
   rebase lên PR #46 head `97f9ac5`, không tiếp tục dùng nền main `ff4f460`.
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
- **Reviewer:** unassigned — chưa có reviewer độc lập; khuyến nghị review P1, không tự ghi approval.
- **Evidence / PR:** [ORC-04 evidence](../../evidence/ORC-04.md), [runtime metadata](../../evidence/ORC-04-runtime.json); [PR #47](https://github.com/HoaiTam/japan-earthquake-etl/pull/47) trên nền [PR #46](https://github.com/HoaiTam/japan-earthquake-etl/pull/46).
- **Kỹ năng phù hợp:** Logging, metrics, Airflow

## Checklist bàn giao

- [x] Deliverable đã có trong repository hoặc môi trường demo.
- [x] Acceptance criteria đã được kiểm tra.
- [x] Test tự động đạt hoặc có evidence thủ công có thể lặp lại.
- [x] Tài liệu/contract đã cập nhật nếu schema, flow, cấu hình hoặc hành vi thay đổi.
- [x] Không chứa secret, dữ liệu nhạy cảm hoặc file build không cần thiết.
- [ ] P0/P1 có reviewer khác assignee xác nhận.

## Đã triển khai và cách dùng

- `run_observability.py`: structured phase/final/failure logs, journal và atomic
  summary theo DAG/run ID; context hash, source/release/SHA/snapshot, duration,
  safe reasons, count gauges không cộng dồn khi retry/rerun.
- ORC-01/02/03 nối observer vào task bodies/final gates; DAG callback ghi failure
  fallback, không thêm success leaf che lỗi. Source-only giữ SourcesReady,
  mock giữ MockComplete, cả hai published=false.
- ORC-03 real run bind parent run/operation/scope, source lease/readback,
  optional scoped six-phase ETL, execution receipt hash và cleanup/final gate.
  Preview không tạo telemetry; ingest/reuse giữ BronzeVerified/published=false.
- Optional receipt extension `orc-04-v1` khóa input/parsed/quality/dedup/current/
  linked-membership/Gold-event equations, primary reject reasons, exact scope;
  legacy adapter không có count vẫn giữ null/not_reported, không tự điền zero.
- `make test-observability`, `make observability-smoke`, `make smoke-observability`
  và các target đọc summary theo exact DAG/run ID. Chi tiết what/how/why tại
  [Run observability contract](../../specs/RUN_OBSERVABILITY_CONTRACT.md).

Done cho deliverable **telemetry runtime và handoff contract**: equations
full-layer bằng fixture, runtime Airflow API/graph, saved real Bronze metadata
và fresh Java readback/rerun Bronze ORC-03. Không phải Done cho E2E business
reconciliation/Gold Published.
**SLV-06 - Deduplicate và xử lý revision trong từng nguồn**, **SLV-07 - Liên
kết observation và chọn canonical event**, **SLV-09 - Tích hợp và kiểm thử
Silver đa nguồn**, **GLD-03 - Ghi Gold Iceberg và commit snapshot**, **GLD-04 -
Tạo Trino views và verification SQL** phải emit real business counts/freshness.
**QA-01 - Chạy E2E daily đa nguồn** nghiệm thu scheduler ETL thật;
ORC-03 dispatcher đã được nối trên nền PR #46;
**ORC-05 - Chốt recovery, concurrency và tài nguyên** xử lý abandoned run/
recovery policy. Không chuyển các task đó thành Done từ evidence ORC-04.

## Tài liệu liên quan

- [ORC-03 backfill/reprocessing handoff](../../specs/BACKFILL_AND_REPROCESSING.md):
  summary hiện có operation/scope hash, exact Bronze pins, published boolean;
  ORC-04 bổ sung count/reason/duration và reconciliation, không suy count từ số manifest.

- [Run observability contract và runbook](../../specs/RUN_OBSERVABILITY_CONTRACT.md)
- [Kế hoạch tuần 4 mở rộng](../WEEK_4_PARALLEL_PLAN.md)
- [Quality và observability](../../DATA_QUALITY_AND_OBSERVABILITY.md)
- [Kế hoạch 8 tuần](../README.md)
- [Các khối công việc](../WORK_BLOCKS.md)
- [Baseline MVP, KPI và Definition of Done](../../specs/MVP_SCOPE_KPI_AND_DOD.md)
