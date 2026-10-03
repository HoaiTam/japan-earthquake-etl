---
task_id: "DEMO-01"
status: "Backlog"
week: 8
block: "K - QA & Release"
workstream: "Demo"
scope: "Core"
priority: "P0"
effort_hours: 4
assignee: "unassigned"
reviewer: "unassigned"
dependencies: ["QA-01", "MLQ-01", "MLI-04"]
---

# DEMO-01 - Chuẩn bị dữ liệu và kịch bản demo

## Mục đích

Dùng để trình bày rõ giá trị của từng tầng dữ liệu và giảm rủi ro phụ thuộc mạng khi demo.

## Phạm vi công việc

Chọn daily/JMA evidence đại diện và một dataset/experiment nhỏ; chuẩn bị SQL, static report, ảnh/log, fixture/bundle fallback và thứ tự trình bày từ source đến HDBSCAN.

## Thành phần cần có

- **Đầu vào và contract:** [QA-01](./QA-01.md), [MLQ-01](./MLQ-01.md), [MLI-04](./MLI-04.md)
- **Phần triển khai:** Trình bày Bronze/Silver/Gold ngắn gọn rồi tập trung dataset manifest, feature, comparison, import gate và ML report.
- **Kết quả bàn giao:** Demo package và preflight checklist.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- Demo package và preflight checklist.

## Tiêu chí hoàn thành

- [ ] Demo có fallback mạng/Drive/Colab và không phụ thuộc Power BI.
- [ ] mọi con số có run_id/query evidence.
- [ ] không cần chạy lại 40 năm trên sân khấu.
- [ ] Nêu rõ output là aftershock candidate, không phải dự đoán/quan hệ nhân quả.

## Hard dependency

- [QA-01](./QA-01.md)
- [MLQ-01](./MLQ-01.md)
- [MLI-04](./MLI-04.md)

## Cách triển khai và phối hợp

Có thể chuẩn bị interface, fixture, mock, test plan và tài liệu trước khi toàn bộ upstream chạy thật. Chỉ được chuyển sang `Done` khi hard dependency đã đạt và acceptance criteria được kiểm tra trên output phù hợp.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới từ `main`: `feat/demo-01-chuan-bi-du-lieu-va`.
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
- **Kỹ năng phù hợp:** Demo, communication, QA

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
