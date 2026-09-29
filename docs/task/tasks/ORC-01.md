---
task_id: "ORC-01"
status: "Backlog"
week: 5
block: "F - Điều phối"
workstream: "DAG skeleton"
scope: "Core"
priority: "P0"
effort_hours: 4
assignee: "unassigned"
reviewer: "unassigned"
dependencies: ["CON-02", "CON-03"]
---

# ORC-01 - Hoàn thiện DAG end-to-end theo contract

## Mục đích

Dùng làm khung tích hợp sớm; nhóm Airflow không phải chờ toàn bộ code xử lý hoàn thành.

## Phạm vi công việc

Nối source readiness, Bronze, Silver, Gold, verify và publish; truyền run context; cho phép dùng mock/fixture trong lúc block nguồn chưa xong.

## Thành phần cần có

- **Đầu vào và contract:** [CON-02](./CON-02.md), [CON-03](./CON-03.md)
- **Phần triển khai:** Nối source readiness, Bronze, Silver, Gold, verify và publish; truyền run context; cho phép dùng mock/fixture trong lúc block nguồn chưa xong.
- **Kết quả bàn giao:** DAG chính với task groups và dependency gates.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- DAG chính với task groups và dependency gates.

## Tiêu chí hoàn thành

- [ ] Failure dừng đúng tầng.
- [ ] task group có input/output contract rõ.
- [ ] có thể test DAG structure không cần mạng.

## Hard dependency

- [CON-02](./CON-02.md)
- [CON-03](./CON-03.md)

## Cách triển khai và phối hợp

Có thể chuẩn bị interface, fixture, mock, test plan và tài liệu trước khi toàn bộ upstream chạy thật. Chỉ được chuyển sang `Done` khi hard dependency đã đạt và acceptance criteria được kiểm tra trên output phù hợp.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới từ `main`: `feat/orc-01-hoan-thien-dag-end-to`.
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
- **Kỹ năng phù hợp:** Airflow, orchestration

## Checklist bàn giao

- [ ] Deliverable đã có trong repository hoặc môi trường demo.
- [ ] Acceptance criteria đã được kiểm tra.
- [ ] Test tự động đạt hoặc có evidence thủ công có thể lặp lại.
- [ ] Tài liệu/contract đã cập nhật nếu schema, flow, cấu hình hoặc hành vi thay đổi.
- [ ] Không chứa secret, dữ liệu nhạy cảm hoặc file build không cần thiết.
- [ ] P0/P1 có reviewer khác assignee xác nhận.

## Tài liệu liên quan

- [Kế hoạch 6 tuần](../README.md)
- [Các khối công việc](../WORK_BLOCKS.md)
- [Baseline MVP, KPI và Definition of Done](../../specs/MVP_SCOPE_KPI_AND_DOD.md)
