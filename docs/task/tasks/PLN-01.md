---
task_id: "PLN-01"
status: "Done"
week: 1
block: "Foundation"
workstream: "Planning"
scope: "Core"
priority: "P0"
effort_hours: 4
assignee: "HoaiTam"
reviewer: "unassigned"
dependencies: []
---

# PLN-01 - Chốt scope, KPI và Definition of Done

## Mục đích

Cố định ranh giới MVP, KPI và Definition of Done để cả nhóm dùng cùng tiêu chuẩn khi triển khai và review.

## Phạm vi công việc

Rà tài liệu hiện tại, chốt phạm vi MVP, phần Stretch, KPI và tiêu chí bàn giao. Bản cập nhật phải đưa quy trình nhận diện chuỗi dư chấn bằng Window/DBSCAN/HDBSCAN vào Core, chuyển Power BI thành phần tùy chọn và giữ rõ giới hạn đây là clustering hồi cứu, không phải dự đoán động đất.

## Thành phần cần có

- **Đầu vào và contract:** Không có hard dependency.
- **Phần triển khai:** Cập nhật baseline theo luồng Gold snapshot → ML dataset → Colab experiment → validate/import → Iceberg `ml.*` → static report; phân loại lại Power BI và KPI nghiệm thu.
- **Kết quả bàn giao:** Biên bản scope, đường găng, capacity 8 tuần và checklist DoD HDBSCAN được cập nhật trong docs.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- Biên bản scope và checklist DoD được cập nhật trong docs.

## Tiêu chí hoàn thành

- [x] Phương án HDBSCAN, dataset/experiment lifecycle và ranh giới không dự đoán được đưa vào baseline chính thức.
- [x] Core/Stretch, đường găng, capacity 8 tuần và Definition of Done được đồng bộ với task index.
- [x] KPI bao phủ ETL, ML dataset/artifact, scientific evaluation và report/import gate mà không dùng accuracy giả khi thiếu ground truth.
- [x] Baseline có static check lặp lại được; reviewer độc lập được khuyến nghị nhưng không chặn `Done`.

## Hard dependency

- Không có.

## Cách triển khai và phối hợp

Có thể bắt đầu ngay. Chốt output contract hoặc fixture nhỏ trước để các task tiêu thụ có thể làm song song.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới từ `main`: `docs/pln-01-hdbscan-scope-update`.
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
- **Reviewer:** Chưa ghi lại
- **Evidence / PR:** Hoàn tất trên branch `docs/pln-01-hdbscan-scope-update`; chưa commit/push hoặc mở PR. Baseline cũ tại [PR #3](https://github.com/HoaiTam/japan-earthquake-etl/pull/3) được giữ làm mốc lịch sử nhưng đã bị baseline HDBSCAN hiện hành thay thế.
- **Evidence kiểm thử:** `./scripts/check-mvp-baseline.sh` kiểm tra scope 8 tuần, HDBSCAN Core, Power BI Stretch, research split/grain/feature/artifact gate và trạng thái task/roadmap; `git diff --check` kiểm tra lỗi whitespace.
- **Kỹ năng phù hợp:** Phân tích yêu cầu, tài liệu

## Checklist bàn giao

- [x] Deliverable cập nhật đã có trong repository hoặc môi trường demo.
- [x] Acceptance criteria mới đã được kiểm tra.
- [x] Test/check tài liệu đạt hoặc có evidence thủ công có thể lặp lại.
- [x] Baseline, roadmap và task downstream đã đồng bộ.
- [x] Không chứa secret, dữ liệu nhạy cảm hoặc file build không cần thiết.
- [ ] Reviewer độc lập xác nhận baseline HDBSCAN cập nhật (khuyến nghị, không chặn `Done`).

## Điều chỉnh phân công tuần 5 — 2026-10-14

- Branch `docs/pln-01-week-5-parallel-plan` từ `origin/main` `4d3ac3d`
  (PR #71). Assignee cập nhật HoaiTam; reviewer unassigned, chưa có approval.
- [Kế hoạch tuần 5](../WEEK_5_PARALLEL_PLAN.md): 12 Core/70h, chia
  HoaiTam 22h / ThanhTris 25h / rosy179 23h. Budget nghiệm thu tồn 4h,
  review/tích hợp/dự phòng 22h; tổng 96h đề xuất, không cam kết capacity thực tế.
- Thêm ORC-06/07 16h cho full-month state và real adapters; backlog
  75 task/380h, Core 67/Stretch 8. Không đổi scientific scope/schema/DoD,
  không triển khai runtime trong phiên lập kế hoạch.
- Giữ FND-01/ORC-02 Review, SEC-01 In Progress; MLD-02/03 vẫn
  rosy179, reviewer HoaiTam. Pilot chưa đủ coverage không được nâng research-ready.
- Evidence: `make check-week-5-plan check-week-3-plan check-task-status`
  đạt; `make test-week-5-plan` đạt 6 regressions; `make ci-docs` và
  `git diff --check` đạt. Chỉ static/docs/metadata tests, không evidence runtime.
  Task planning Done, các task được giao chưa tự Done. Chưa commit/push/mở PR.

## Điều chỉnh Jupyter-first — 2026-10-14

- Theo yêu cầu người dùng, bổ sung [DAT-02](./DAT-02.md), owner HoaiTam,
  Core/P0/6h, Backlog do MLD-04 chưa Done. Chưa tạo dữ liệu hoặc notebook thật.
- Branch mới `docs/pln-01-jupyter-pilot-first` từ `origin/main` `4d3ac3d`;
  giữ nguyên các thay đổi kế hoạch tuần 5 chưa commit của phiên trước.
- EXP-01 local harness dùng DAT-02 trước; EXP-02 test Jupyter và lưu local
  PASSED report rồi mới chạy cùng code/bundle trên Colab. Official dataset/
  result/import gates giữ nguyên; pilot không được EXPORTED/CANDIDATE.
- Cập nhật MLD-04/05, MLI-03, DOC-01, roadmap/specs và AGENTS; không đổi
  MLD-02/03 rosy179/reviewer HoaiTam hoặc nhận upstream Done cần update.
- QA-04 giữ owner/scope/4h và dời tuần 6; tuần 5 12 Core/72h (24/25/23),
  +4h budget tồn +20h review/tích hợp =96h. Backlog 76 task/386h,
  Core68/Stretch8; 364+16+6h, không tăng effort task cũ.
- Evidence: check-week-5-plan/check-task-status/check-week-3-plan đạt;
  test-week-5-plan đạt 11 regressions, make ci-docs và git diff --check đạt. Chỉ kiểm tra kế
  hoạch/docs, không chứng minh dataset/feature/Jupyter/Colab runtime đã chạy.
- Planning Done; DAT-02/EXP implementation chưa Done; chưa commit/push/mở PR.

## Tài liệu liên quan — Jupyter-first

- [Kế hoạch 8 tuần](../README.md)
- [Jupyter pilot workflow](../../specs/JUPYTER_PILOT_WORKFLOW.md)
- [Roadmap HDBSCAN](../HDBSCAN_WORKSTREAM.md)
- [Các khối công việc](../WORK_BLOCKS.md)
- [Baseline MVP, KPI và Definition of Done](../../specs/MVP_SCOPE_KPI_AND_DOD.md)
