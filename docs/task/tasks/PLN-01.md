---
task_id: "PLN-01"
status: "Done"
week: 1
block: "Foundation"
workstream: "Planning"
scope: "Core"
priority: "P0"
effort_hours: 4
assignee: "unassigned"
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
- **Assignee:** Chưa ghi lại
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

## Tài liệu liên quan

- [Kế hoạch 8 tuần](../README.md)
- [Roadmap HDBSCAN](../HDBSCAN_WORKSTREAM.md)
- [Các khối công việc](../WORK_BLOCKS.md)
- [Baseline MVP, KPI và Definition of Done](../../specs/MVP_SCOPE_KPI_AND_DOD.md)
