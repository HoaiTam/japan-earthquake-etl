---
task_id: "DEMO-02"
status: "Backlog"
week: 8
block: "K - QA & Release"
workstream: "Release"
scope: "Core"
priority: "P0"
effort_hours: 6
assignee: "unassigned"
reviewer: "unassigned"
dependencies: ["QA-01", "QA-02", "QA-04", "MLQ-01", "SEC-01", "DOC-01", "DEMO-01"]
---

# DEMO-02 - Rehearsal và tạo release candidate

## Mục đích

Dùng làm cổng bàn giao cuối; Stretch không đạt không chặn release nếu toàn bộ Core đã đạt.

## Phạm vi công việc

Chạy rehearsal 15-20 phút, sửa blocker, đóng known issues, xác nhận checklist Core và gắn tag/commit demo sau phê duyệt.

## Thành phần cần có

- **Đầu vào và contract:** [QA-01](./QA-01.md), [QA-02](./QA-02.md), [QA-04](./QA-04.md), [MLQ-01](./MLQ-01.md), [SEC-01](./SEC-01.md), [DOC-01](./DOC-01.md), [DEMO-01](./DEMO-01.md)
- **Phần triển khai:** Chạy rehearsal 15-20 phút, sửa blocker, đóng known issues, xác nhận checklist Core và gắn tag/commit demo sau phê duyệt.
- **Kết quả bàn giao:** Release candidate, tag đề xuất và biên bản rehearsal.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- Release candidate, tag đề xuất và biên bản rehearsal.

## Tiêu chí hoàn thành

- [ ] Ba thành viên thực hiện được phần của mình.
- [ ] ETL E2E, ML E2E/reproducibility, recovery, security, docs và demo checklist đạt.

## Hard dependency

- [QA-01](./QA-01.md)
- [QA-02](./QA-02.md)
- [QA-04](./QA-04.md)
- [MLQ-01](./MLQ-01.md)
- [SEC-01](./SEC-01.md)
- [DOC-01](./DOC-01.md)
- [DEMO-01](./DEMO-01.md)

## Cách triển khai và phối hợp

Có thể chuẩn bị interface, fixture, mock, test plan và tài liệu trước khi toàn bộ upstream chạy thật. Chỉ được chuyển sang `Done` khi hard dependency đã đạt và acceptance criteria được kiểm tra trên output phù hợp.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới từ `main`: `feat/demo-02-rehearsal-va-tao-release-candidate`.
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
- **Kỹ năng phù hợp:** Release management, presentation

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
