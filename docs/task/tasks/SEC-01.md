---
task_id: "SEC-01"
status: "In Progress"
week: 4
block: "K - QA & Release"
workstream: "Security"
scope: "Core"
priority: "P1"
effort_hours: 4
assignee: "HoaiTam"
reviewer: "unassigned"
dependencies: ["ORC-05"]
---

# SEC-01 - Review secret và bề mặt truy cập

## Mục đích

Dùng để bảo đảm source credentials và storage không bị lộ khi nhóm chia sẻ repository/demo.

## Phạm vi công việc

Kiểm tra env, Git history, logs, default credentials, exposed ports, DSN, Drive handoff và bucket/prefix permissions cho ETL lẫn ML artifacts.

## Thành phần cần có

- **Đầu vào và contract:** [ORC-05](./ORC-05.md)
- **Phần triển khai:** Kiểm tra env, Git history, logs, default credentials, exposed ports, DSN và bucket permissions cho toàn bộ block mới.
- **Kết quả bàn giao:** Security checklist và issue fixes.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- Security checklist và issue fixes.

## Tiêu chí hoàn thành

- [ ] Không có secret trong repo/log.
- [x] chỉ port cần thiết được expose.
- [x] sample config không chứa credential thật.
- [ ] Colab không có MinIO/pipeline credential và result bundle không rò local account path.

## Hard dependency

- [ORC-05](./ORC-05.md)

## Cách triển khai và phối hợp

### Phân công tuần 4 mở rộng

- **Owner / effort:** Trang, 4h Core; đưa từ tuần 6 lên [kế hoạch tuần 4](../WEEK_4_PARALLEL_PLAN.md), đang In Progress sau khi ORC-05 đã Done.
- **Làm trước:** Checklist/static tests và leak-pattern fixtures, không in credential thật hoặc đọc account/resource ngoài project scope.
- **Làm gì / có gì:** Audit config/Git/logs/ports/prefix permissions, default credential/DSN và ML bundle path/token boundary; issue/fix report có scope.
- **Dùng để làm gì:** ETL/runtime evidence và result fixtures chia sẻ được mà không lộ secret hoặc local account path.
- **Handoff / nghiệm thu:** Sau ORC-05 nhận profile/endpoints/permissions/log scope thật, chạy checks và ghi findings/fix evidence; không tự public port hoặc dùng root credential để vượt permission. Thành phần notebook/export chưa tồn tại ghi rõ chưa kiểm tra, không xác nhận trước các artifacts tương lai; acceptance chưa đủ thì giữ chưa Done và review lại khi downstream được thêm.

### Bổ sung audit runtime sau PR #72

- **Owner hiện tại:** HoaiTam theo yêu cầu nhận task; Trang là owner của mốc tuần 4 ở trên. Reviewer vẫn `unassigned`, không suy ra approval từ việc merge PR cũ.
- **Làm gì / có gì:** Scanner source/history/log redacted; kiểm tra config local, env/role và port thật của 11 services; probe quyền MinIO với key UUID riêng; đổi bootstrap/smoke sang stdin cho password; chặn catalog bootstrap INFO của Trino.
- **Dùng để làm gì:** Có report an toàn để chia sẻ, xác định finding thật và phân biệt code fix với cấu hình đã deploy. Không build/start/init trong các lệnh audit/test.
- **Đã nghiệm thu:** Source/history không còn finding chưa triage trong phạm vi scanner; CFG local đạt; 4 port loopback đúng; root credential không nằm ở consumer; 15 permission checks thật đạt. Hai object QA Silver/warehouse đã được xóa đúng key, Bronze QA giữ lại vì DeleteObject bị cấm.
- **Cập nhật 22/10/2026:** Trino đã mount logging fix read-only; đã đọc đủ retained logs của 11 containers bằng streaming, không dùng tail/since hoặc xóa log. Current scan source/history/logs không có finding chưa triage. Các URI test chỉ được triage sau review exact content/hash, không exempt key/token/credential thật.
- **Handoff local:** gate `sec-01-handoff-v1` đã kiểm 5 artifacts/23 files thật của DAT-02/EXP-01/MLD-05/MLI-03, gồm ZIP, notebook, exported dataset và results; checksum/marker/decoded Parquet scan đạt. Đây không là nghiệm thu tài khoản/permission Google Drive hoặc session Colab ngoài project.
- **Còn chặn Done:** Spark master/worker đang dừng; fresh permission probes chưa chạy. Owner chọn giữ stack và chờ QA-01 scheduled 23/10 07:15 rồi mới nghiệm thu SEC-01. Không reuse 15 probe checks lịch sử làm evidence hiện tại, không tự start Spark/restart Airflow hoặc đổi lịch QA-01. Owner xác nhận chưa dùng Colab/Drive, chỉ Jupyter local; acceptance Colab giữ chưa xác nhận, không tự tick từ local handoff scan.
- **Cách chạy / đọc kết quả:** [Checklist SEC-01](../../security/SEC-01-CHECKLIST.md), [evidence runtime](../../evidence/SEC-01_RUNTIME.md). Exit 1 là findings/error; 2 là coverage chưa đủ, không được coi là Done.

Có thể chuẩn bị interface, fixture, mock, test plan và tài liệu trước khi toàn bộ upstream chạy thật. Chỉ được chuyển sang `Done` khi hard dependency đã đạt và acceptance criteria được kiểm tra trên output phù hợp.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới từ `main`: `chore/sec-01-review-secret-va-be-mat`.
3. Triển khai đúng phạm vi; dùng fixture nhỏ, xác định được và không phụ thuộc mạng cho unit test.
4. Chạy test/check phù hợp, đối chiếu acceptance criteria và cập nhật tài liệu nếu contract hoặc hành vi thay đổi.
5. Cập nhật `status`, `assignee`, `reviewer` và Evidence ngay trong file này khi mở PR hoặc hoàn tất review.

## Ranh giới

- Không tự mở rộng sang deliverable của task khác.
- Không đổi contract upstream trong PR implementation mà không cập nhật task contract liên quan và có review.
- Không commit secret, credential, payload nhạy cảm, data dump lớn hoặc artifact build không cần thiết.
- Không đánh dấu `Done` nếu chưa có evidence kiểm tra được.

## Theo dõi

- **Trạng thái:** In Progress
- **Assignee:** HoaiTam
- **Reviewer:** unassigned — chưa có approval độc lập
- **Evidence / PR:** [Audit 22/10 và kế hoạch nghiệm thu](../../evidence/SEC-01_20261022.md), [report redacted](../../evidence/SEC-01-20261022.json); [checklist](../../security/SEC-01-CHECKLIST.md). [Runtime audit trước đây](../../evidence/SEC-01_RUNTIME.md), PR #67/#56 là mốc lịch sử. Bản vá ở branch `fix/sec-01-ml-handoff-audit`, worktree riêng từ main `2f040c4`, không đổi checkout đang bind PR #96. Chưa commit/push/mở PR.
- **Kỹ năng phù hợp:** Security, Docker, Git

## Checklist bàn giao

- [x] Deliverable đã có trong repository hoặc môi trường demo.
- [ ] Acceptance criteria đã được kiểm tra.
- [x] Test tự động đạt hoặc có evidence thủ công có thể lặp lại.
- [x] Tài liệu/contract đã cập nhật nếu schema, flow, cấu hình hoặc hành vi thay đổi.
- [ ] Không chứa secret, dữ liệu nhạy cảm hoặc file build không cần thiết.
- [ ] P0/P1 có reviewer khác assignee xác nhận.

Reviewer độc lập được khuyến nghị, không phải blocker Done theo AGENTS.md.
Acceptance runtime/permission mới và Colab boundary còn thiếu là blocker thực tế;
local artifact scan không chứng nhận remote account. Không ghi approval giả.

## Tài liệu liên quan

- [Kế hoạch tuần 4 mở rộng](../WEEK_4_PARALLEL_PLAN.md)
- [Configuration và secret hygiene](../../specs/CONFIGURATION_AND_SECRETS.md)
- [Kế hoạch 8 tuần](../README.md)
- [Các khối công việc](../WORK_BLOCKS.md)
- [Baseline MVP, KPI và Definition of Done](../../specs/MVP_SCOPE_KPI_AND_DOD.md)
