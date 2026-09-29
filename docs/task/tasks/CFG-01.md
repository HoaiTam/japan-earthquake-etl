---
task_id: "CFG-01"
status: "Done"
week: 1
block: "Foundation"
workstream: "Configuration"
scope: "Core"
priority: "P0"
effort_hours: 3
assignee: "unassigned"
reviewer: "unassigned"
dependencies: ["REP-01"]
---

# CFG-01 - Thiết lập cấu hình và secret hygiene

## Mục đích

Chuẩn hóa cấu hình runtime và ngăn credential hoặc secret thật đi vào Git.

## Phạm vi công việc

Tạo .gitignore, .env.example, quy ước biến môi trường và kiểm tra không commit secret.

## Thành phần cần có

- **Đầu vào và contract:** [REP-01](./REP-01.md)
- **Phần triển khai:** Tạo .gitignore, .env.example, quy ước biến môi trường và kiểm tra không commit secret.
- **Kết quả bàn giao:** .gitignore, .env.example và bảng biến cấu hình.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- .gitignore, .env.example và bảng biến cấu hình.

## Tiêu chí hoàn thành

- [x] Clone mới có thể tạo .env.
- [x] secret thật không xuất hiện trong Git.

## Hard dependency

- [REP-01](./REP-01.md)

## Cách triển khai và phối hợp

Có thể chuẩn bị interface, fixture, mock, test plan và tài liệu trước khi toàn bộ upstream chạy thật. Chỉ được chuyển sang `Done` khi hard dependency đã đạt và acceptance criteria được kiểm tra trên output phù hợp.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới từ `main`: `feat/cfg-01-thiet-lap-cau-hinh-va`.
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
- **Assignee:** Chưa ghi lại
- **Reviewer:** Chưa ghi lại
- **Evidence / PR:** [PR #5](https://github.com/HoaiTam/japan-earthquake-etl/pull/5)
- **Kỹ năng phù hợp:** Git, bảo mật cấu hình

## Checklist bàn giao

- [x] Deliverable đã có trong repository hoặc môi trường demo.
- [x] Acceptance criteria đã được kiểm tra.
- [x] Test tự động đạt hoặc có evidence thủ công có thể lặp lại.
- [x] Tài liệu/contract đã cập nhật nếu schema, flow, cấu hình hoặc hành vi thay đổi.
- [x] Không chứa secret, dữ liệu nhạy cảm hoặc file build không cần thiết.
- [x] P0/P1 có reviewer khác assignee xác nhận.

## Tài liệu liên quan

- [Kế hoạch 6 tuần](../README.md)
- [Các khối công việc](../WORK_BLOCKS.md)
- [Baseline MVP, KPI và Definition of Done](../../specs/MVP_SCOPE_KPI_AND_DOD.md)
