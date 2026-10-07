---
task_id: "ORC-04"
status: "Backlog"
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

- [ ] Count các tầng đối soát được từ run_id.
- [ ] không log secret/payload lớn.
- [ ] lỗi có reason rõ.

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
2. Tạo branch mới từ `main`: `feat/orc-04-chuan-hoa-logging-va-run`.
3. Triển khai đúng phạm vi; dùng fixture nhỏ, xác định được và không phụ thuộc mạng cho unit test.
4. Chạy test/check phù hợp, đối chiếu acceptance criteria và cập nhật tài liệu nếu contract hoặc hành vi thay đổi.
5. Cập nhật `status`, `assignee`, `reviewer` và Evidence ngay trong file này khi mở PR hoặc hoàn tất review.

## Ranh giới

- Không tự mở rộng sang deliverable của task khác.
- Không đổi contract upstream trong PR implementation mà không cập nhật task contract liên quan và có review.
- Không commit secret, credential, payload nhạy cảm, data dump lớn hoặc artifact build không cần thiết.
- Không đánh dấu `Done` nếu chưa có evidence kiểm tra được.

## Theo dõi

- **Trạng thái:** Backlog
- **Assignee:** HoaiTam
- **Reviewer:** Chưa ghi lại
- **Evidence / PR:** Chưa có
- **Kỹ năng phù hợp:** Logging, metrics, Airflow

## Checklist bàn giao

- [ ] Deliverable đã có trong repository hoặc môi trường demo.
- [ ] Acceptance criteria đã được kiểm tra.
- [ ] Test tự động đạt hoặc có evidence thủ công có thể lặp lại.
- [ ] Tài liệu/contract đã cập nhật nếu schema, flow, cấu hình hoặc hành vi thay đổi.
- [ ] Không chứa secret, dữ liệu nhạy cảm hoặc file build không cần thiết.
- [ ] P0/P1 có reviewer khác assignee xác nhận.

## Tài liệu liên quan

- [Kế hoạch tuần 4 mở rộng](../WEEK_4_PARALLEL_PLAN.md)
- [Quality và observability](../../DATA_QUALITY_AND_OBSERVABILITY.md)
- [Kế hoạch 8 tuần](../README.md)
- [Các khối công việc](../WORK_BLOCKS.md)
- [Baseline MVP, KPI và Definition of Done](../../specs/MVP_SCOPE_KPI_AND_DOD.md)
