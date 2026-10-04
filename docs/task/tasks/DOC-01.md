---
task_id: "DOC-01"
status: "Ready"
week: 6
block: "K - QA & Release"
workstream: "Documentation"
scope: "Core"
priority: "P0"
effort_hours: 4
assignee: "unassigned"
reviewer: "unassigned"
dependencies: []
---

# DOC-01 - Cập nhật tài liệu theo từng block

## Mục đích

Dùng để tài liệu được cập nhật liên tục theo PR; final review ở tuần 6 nhưng có thể làm song song từ đầu.

## Phạm vi công việc

Đồng bộ kiến trúc USGS/JMA và HDBSCAN; cập nhật source/data layers, canonicalization, Gold-to-ML flow, Colab handoff, experiment/import lifecycle, runbook và known issues.

## Thành phần cần có

- **Đầu vào và contract:** Không có hard dependency.
- **Phần triển khai:** Đồng bộ ETL + HDBSCAN end-to-end, thuật ngữ candidate, hai DAG ML, static report và ranh giới không prediction/causal.
- **Kết quả bàn giao:** Bộ docs khớp hệ thống thực tế và link nội bộ hợp lệ.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- Bộ docs khớp hệ thống thực tế và link nội bộ hợp lệ.

## Tiêu chí hoàn thành

- [ ] Thành viên mới hiểu làm gì, gồm gì, dùng để làm gì và chạy được theo runbook.
- [ ] không còn mô tả mâu thuẫn.
- [ ] Thành viên mới tái lập được một dataset/experiment nhỏ mà không cần credential trong notebook.

## Hard dependency

- Không có.

## Cách triển khai và phối hợp

Có thể bắt đầu ngay. Chốt output contract hoặc fixture nhỏ trước để các task tiêu thụ có thể làm song song.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới từ `main`: `docs/doc-01-cap-nhat-tai-lieu-theo`.
3. Triển khai đúng phạm vi; dùng fixture nhỏ, xác định được và không phụ thuộc mạng cho unit test.
4. Chạy test/check phù hợp, đối chiếu acceptance criteria và cập nhật tài liệu nếu contract hoặc hành vi thay đổi.
5. Cập nhật `status`, `assignee`, `reviewer` và Evidence ngay trong file này khi mở PR hoặc hoàn tất review.

## Ranh giới

- Không tự mở rộng sang deliverable của task khác.
- Không đổi contract upstream trong PR implementation mà không cập nhật task contract liên quan và có review.
- Không commit secret, credential, payload nhạy cảm, data dump lớn hoặc artifact build không cần thiết.
- Không đánh dấu `Done` nếu chưa có evidence kiểm tra được.

## Theo dõi

- **Trạng thái:** Ready
- **Assignee:** Chưa ghi lại
- **Reviewer:** Chưa ghi lại
- **Evidence / PR:** Chưa có
- **Kỹ năng phù hợp:** Technical writing, architecture

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
