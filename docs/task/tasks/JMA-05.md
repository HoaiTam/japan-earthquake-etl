---
task_id: "JMA-05"
status: "Backlog"
week: 4
block: "C - JMA Bronze"
workstream: "QA"
scope: "Core"
priority: "P0"
effort_hours: 4
assignee: "HoaiTam"
reviewer: "unassigned"
dependencies: ["JMA-04"]
---

# JMA-05 - Kiểm thử JMA đến Bronze

## Mục đích

Dùng làm mốc BronzeReady của JMA trước khi chạy parser và historical backfill lớn.

## Phạm vi công việc

Test archive hợp lệ, ZIP hỏng, sai record length, file thay đổi, resume và integration run cho một vài năm đại diện.

## Thành phần cần có

- **Đầu vào và contract:** [JMA-04](./JMA-04.md)
- **Phần triển khai:** Test archive hợp lệ, ZIP hỏng, sai record length, file thay đổi, resume và integration run cho một vài năm đại diện.
- **Kết quả bàn giao:** JMA unit/integration tests và evidence theo year/run_id.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- JMA unit/integration tests và evidence theo year/run_id.

## Tiêu chí hoàn thành

- [ ] Test offline dùng fixture.
- [ ] manifest, checksum, version và record count đối soát được.

## Hard dependency

- [JMA-04](./JMA-04.md)

## Cách triển khai và phối hợp

### Phân công tuần 4

- **Owner / effort:** HoaiTam, 4h Core; xem [kế hoạch tuần 4](../WEEK_4_PARALLEL_PLAN.md).
- **Nhóm:** [Mở đường — làm trước](../WEEK_4_PREP_GROUP.md), bước 2/3 sau JMA-04; bàn giao input thật cho ba khối triển khai.
- **Làm trước:** Test plan, ZIP/checksum/failure/resume fixtures và readback harness; hiện Backlog vì chưa bắt đầu. Khi làm phần chuẩn bị được phép, ghi In Progress và dependency JMA-04 còn chờ.
- **Làm gì / có gì:** Kiểm thử workflow JMA-04 với archive hợp lệ/hỏng, revision, retry/rerun; report year/segment/release/run, manifest/raw checksum/count và readback.
- **Dùng để làm gì:** Gate JMA BronzeReady thật trước Silver và historical backfill lớn, không chỉ xác nhận validator mock.
- **Input / handoff:** Dùng sample JMA DAT-01 và exact manifest do workflow/writer tạo; bổ sung archive 2000 thuộc reproduction cho ML pilot, ghi metadata/evidence riêng, không đổi catalog DAT-01 đang khóa hai entry. Bàn giao BronzeReady evidence cho SLV-09; không đổi STAGED_SOURCE thành Ready chỉ bằng sửa catalog metadata.
- **Nghiệm thu:** Chỉ Done khi JMA-04 đã đạt và run thật/readback/rerun đối soát được trên các năm đại diện 2000, 2023 và 1997 (hai segment riêng). Không bịa count archive 2000; test offline bao phủ case lỗi, full 40 năm không thuộc task QA này. Một năm reproduction chỉ là pilot, chưa phải dataset/Mc đầy đủ giai đoạn nghiên cứu.

Đây là integration gate. Có thể chuẩn bị test plan, fixture và harness trước, nhưng chỉ được chuyển sang `Done` sau khi toàn bộ hard dependency cung cấp output thật và báo cáo đối soát đạt.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới từ `main`: `feat/jma-05-kiem-thu-jma-en-bronze`.
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
- **Kỹ năng phù hợp:** Fixture testing, Airflow, MinIO

## Checklist bàn giao

- [ ] Deliverable đã có trong repository hoặc môi trường demo.
- [ ] Acceptance criteria đã được kiểm tra.
- [ ] Test tự động đạt hoặc có evidence thủ công có thể lặp lại.
- [ ] Tài liệu/contract đã cập nhật nếu schema, flow, cấu hình hoặc hành vi thay đổi.
- [ ] Không chứa secret, dữ liệu nhạy cảm hoặc file build không cần thiết.
- [ ] P0/P1 có reviewer khác assignee xác nhận.

## Tài liệu liên quan

- [Phân công tuần 4](../WEEK_4_PARALLEL_PLAN.md)
- [Nhóm mở đường tuần 4](../WEEK_4_PREP_GROUP.md)
- [Shared real samples](../../specs/SHARED_REAL_SAMPLE_DATA.md)
- [JMA writer/handoff](../../specs/JMA_BRONZE_WRITER_CONTRACT.md)
- [Kế hoạch 8 tuần](../README.md)
- [Các khối công việc](../WORK_BLOCKS.md)
- [Baseline MVP, KPI và Definition of Done](../../specs/MVP_SCOPE_KPI_AND_DOD.md)
