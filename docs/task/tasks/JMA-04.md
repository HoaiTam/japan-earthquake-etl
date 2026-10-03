---
task_id: "JMA-04"
status: "Backlog"
week: 3
block: "C - JMA Bronze"
workstream: "Backfill orchestration"
scope: "Core"
priority: "P0"
effort_hours: 5
assignee: "unassigned"
reviewer: "unassigned"
dependencies: ["JMA-02", "JMA-03"]
---

# JMA-04 - Tạo workflow ingest JMA theo năm

## Mục đích

Dùng để nạp 40 năm theo batch nhỏ, kiểm soát tài nguyên và sửa riêng partition bị JMA cập nhật.

## Phạm vi công việc

Nhận danh sách năm hoặc khoảng năm, preview phạm vi, chạy có giới hạn song song, tái sử dụng archive hợp lệ và ghi trạng thái từng năm.

## Thành phần cần có

- **Đầu vào và contract:** [JMA-02](./JMA-02.md), [JMA-03](./JMA-03.md)
- **Phần triển khai:** Nhận danh sách năm hoặc khoảng năm, preview phạm vi, chạy có giới hạn song song, tái sử dụng archive hợp lệ và ghi trạng thái từng năm.
- **Kết quả bàn giao:** Airflow JMA backfill task group và year-level run summary.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- Airflow JMA backfill task group và year-level run summary.

## Tiêu chí hoàn thành

- [ ] Có thể chạy lại một năm độc lập.
- [ ] không tải lại file không đổi.
- [ ] failure không để trạng thái nửa hoàn tất.

## Hard dependency

- [JMA-02](./JMA-02.md)
- [JMA-03](./JMA-03.md)

## Cách triển khai và phối hợp

Có thể chuẩn bị interface, fixture, mock, test plan và tài liệu trước khi toàn bộ upstream chạy thật. Chỉ được chuyển sang `Done` khi hard dependency đã đạt và acceptance criteria được kiểm tra trên output phù hợp.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới từ `main`: `feat/jma-04-tao-workflow-ingest-jma-theo`.
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
- **Assignee:** Chưa ghi lại
- **Reviewer:** Chưa ghi lại
- **Evidence / PR:** Chưa có
- **Kỹ năng phù hợp:** Airflow backfill, idempotency

## Checklist bàn giao

- [ ] Deliverable đã có trong repository hoặc môi trường demo.
- [ ] Acceptance criteria đã được kiểm tra.
- [ ] Test tự động đạt hoặc có evidence thủ công có thể lặp lại.
- [ ] Tài liệu/contract đã cập nhật nếu schema, flow, cấu hình hoặc hành vi thay đổi.
- [ ] Không chứa secret, dữ liệu nhạy cảm hoặc file build không cần thiết.
- [ ] P0/P1 có reviewer khác assignee xác nhận.

## Tài liệu liên quan

- [Kế hoạch 8 tuần](../README.md)
- [Các khối công việc](../WORK_BLOCKS.md)
- [Baseline MVP, KPI và Definition of Done](../../specs/MVP_SCOPE_KPI_AND_DOD.md)
