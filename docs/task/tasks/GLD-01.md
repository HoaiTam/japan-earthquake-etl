---
task_id: "GLD-01"
status: "Ready"
week: 4
block: "E - Gold & Serving"
workstream: "Analytics model"
scope: "Core"
priority: "P0"
effort_hours: 6
assignee: "Trang"
reviewer: "unassigned"
dependencies: ["CON-03", "CON-04"]
---

# GLD-01 - Xây canonical event, dimensions và bands

## Mục đích

Dùng làm dataset canonical duy nhất cho phân tích và ML, tránh double count JMA–USGS hoặc để consumer tự lặp lại logic nguồn.

## Phạm vi công việc

Tạo `gold.earthquake_event_current` với canonical key, time/location, magnitude/depth/type, JMA catalog era, quality/source coverage và provenance bridge. Dimensions/bands chỉ giữ phần cần cho kiểm chứng; dashboard aggregates thuộc `GLD-02` Stretch.

## Thành phần cần có

- **Đầu vào và contract:** [CON-03](./CON-03.md), [CON-04](./CON-04.md)
- **Phần triển khai:** Tạo event current/bridge đủ field cho filter ML: natural event, study area, primary JMA, `UNIFIED`, quality status và source lineage.
- **Kết quả bàn giao:** Gold event transformation, dimension/band mapping và tests.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- Gold event transformation, dimension/band mapping và tests.

## Tiêu chí hoàn thành

- [ ] Canonical key duy nhất.
- [ ] null policy đúng.
- [ ] giá trị biên band không chồng lấn.
- [ ] count giải thích được từ Silver.
- [ ] Các field bắt buộc cho MLD audit/filter có giá trị và provenance đúng contract.

## Hard dependency

- [CON-03](./CON-03.md)
- [CON-04](./CON-04.md)

## Cách triển khai và phối hợp

### Phân công tuần 4

- **Owner / effort:** Trang, 6h Core; xem [kế hoạch tuần 4](../WEEK_4_PARALLEL_PLAN.md).
- **Bắt đầu ngay:** CON-03/CON-04 đã Done; dựng current-observation/membership fixture theo contract, không chờ Silver thật để test transformation.
- **Làm gì / có gì:** `gold.event_current`, source bridge và dimension/band mapping; natural/ROI view `gold.earthquake_event_current`; giữ JMA era/source/quality và field-level provenance đủ ML audit/filter.
- **Dùng để làm gì:** Dataset canonical duy nhất không double count; chọn field theo primary/source rules, không average/coalesce hai nguồn tùy ý.
- **Handoff:** Gold outputs theo [CON-03](../../specs/SILVER_GOLD_DATA_MODEL.md) cho GLD-03; input thật lấy từ SLV-09 SilverReady và membership SLV-07. Không sinh canonical ID hoặc matching lần hai.
- **Nghiệm thu:** Unique canonical, bridge không nhân fact, null/bands/Unknown/Offshore và ML filter fields có tests. Task transformation có thể đạt acceptance bằng fixture; không nhận Gold thật Published trước GLD-03/04.

Có thể chuẩn bị interface, fixture, mock, test plan và tài liệu trước khi toàn bộ upstream chạy thật. Chỉ được chuyển sang `Done` khi hard dependency đã đạt và acceptance criteria được kiểm tra trên output phù hợp.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới từ `main`: `feat/gld-01-xay-canonical-event-dimensions-va`.
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
- **Assignee:** Trang
- **Reviewer:** Chưa ghi lại
- **Evidence / PR:** Chưa có
- **Kỹ năng phù hợp:** Spark SQL, analytics modeling

## Checklist bàn giao

- [ ] Deliverable đã có trong repository hoặc môi trường demo.
- [ ] Acceptance criteria đã được kiểm tra.
- [ ] Test tự động đạt hoặc có evidence thủ công có thể lặp lại.
- [ ] Tài liệu/contract đã cập nhật nếu schema, flow, cấu hình hoặc hành vi thay đổi.
- [ ] Không chứa secret, dữ liệu nhạy cảm hoặc file build không cần thiết.
- [ ] P0/P1 có reviewer khác assignee xác nhận.

## Tài liệu liên quan

- [Phân công tuần 4](../WEEK_4_PARALLEL_PLAN.md)
- [Silver/Gold logical model](../../specs/SILVER_GOLD_DATA_MODEL.md)
- [Kế hoạch 8 tuần](../README.md)
- [Các khối công việc](../WORK_BLOCKS.md)
- [Baseline MVP, KPI và Definition of Done](../../specs/MVP_SCOPE_KPI_AND_DOD.md)
