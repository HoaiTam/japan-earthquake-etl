# Kế hoạch task 6 tuần

Backlog chi tiết và khu vực để thành viên pick task nằm trong file [JAPAN_EARTHQUAKE_ETL_TASKS.xlsx](./JAPAN_EARTHQUAKE_ETL_TASKS.xlsx).

## 1. Giả định lập kế hoạch

- Nhóm có 3 thành viên.
- Thời gian thực hiện là 6 tuần.
- Capacity tham khảo là 15 giờ/người/tuần, tương đương 270 giờ toàn nhóm.
- Assignee và reviewer được để nhóm tự chọn trong Excel.
- Mỗi task chỉ có một assignee chính; reviewer phải là người khác assignee.
- Task `Core` cần hoàn thành để đạt tiêu chí phiên bản đầu tiên.
- Task `Stretch` chỉ thực hiện sau khi các dependency Core ổn định.
- Không bao gồm thiết kế database/DDL chi tiết hoặc Data Science/Generative AI.

Nếu capacity thực tế khác 15 giờ/người/tuần, nhóm cập nhật ô capacity trong sheet `Tổng quan` rồi cân bằng lại task theo effort.

## 2. Mục tiêu theo tuần

| Tuần | Mục tiêu | Exit criteria | Effort kế hoạch |
|---:|---|---|---:|
| 1 | Foundation và môi trường local | Các service nền chạy, health check đạt, có smoke checklist | 45 giờ |
| 2 | Extract và Bronze | Lấy được USGS GeoJSON, lưu Bronze cùng metadata/checksum | 45 giờ |
| 3 | Silver pipeline | Parse, chuẩn hóa, validation, dedup và ghi Silver Parquet | 45 giờ |
| 4 | Gold, Iceberg và Trino | Commit được Gold snapshot và query/verify bằng Trino | 45 giờ |
| 5 | Orchestration hoàn chỉnh và Power BI | DAG hỗ trợ retry/backfill; ba trang dashboard hoạt động | 45 giờ |
| 6 | QA, vận hành và demo | E2E, rerun, reconciliation, runbook và demo được xác nhận | 45 giờ |

Tổng effort gồm cả task Stretch là **270 giờ**. Hai task spatial enrichment ở tuần 4 có thể hoãn nếu ảnh hưởng đường găng.

## 3. Quy tắc pick task

1. Điền tên ba thành viên ở sheet `Tổng quan` trước khi pick.
2. Pick task `P0` trước, sau đó đến `P1`; chỉ pick `P2/Stretch` khi đường găng ổn định.
3. Không bắt đầu task nếu dependency chưa hoàn tất, trừ khi chỉ chuẩn bị fixture/test plan.
4. Mỗi người giữ tổng effort toàn dự án trong khoảng cân bằng; workbook tự tính tải theo người và theo tuần.
5. Không để một người giữ toàn bộ kiến thức của một chuỗi quan trọng. Người review nên thuộc stream khác khi có thể.
6. Khi bắt đầu, chuyển trạng thái sang `In Progress`. Khi mở PR chuyển sang `Review`; chỉ dùng `Done` sau khi đạt tiêu chí hoàn thành và có evidence.
7. Ghi link PR, commit, ảnh hoặc log xác nhận vào cột `Evidence / PR`.

## 4. Definition of Done chung

Một task chỉ được xem là `Done` khi:

- Deliverable đã có trong repository hoặc môi trường demo.
- Tiêu chí hoàn thành của task đã được kiểm tra.
- Test liên quan đạt; nếu chưa có test tự động phải có bằng chứng thủ công.
- Không chứa secret, dữ liệu nhạy cảm hoặc file build không cần thiết.
- Tài liệu/contract được cập nhật nếu hành vi, cấu hình hoặc schema logic thay đổi.
- Có reviewer khác assignee xác nhận đối với task P0/P1.

## 5. Trường trong workbook

| Trường | Ý nghĩa |
|---|---|
| `Task ID` | Mã ổn định dùng trong branch, commit và trao đổi |
| `Tuần` | Tuần mục tiêu từ 1 đến 6 |
| `Scope` | `Core` hoặc `Stretch` |
| `Priority` | `P0`, `P1` hoặc `P2` |
| `Dependency` | Task phải hoàn tất trước |
| `Effort (h)` | Ước lượng giờ công của assignee chính |
| `Assignee` | Người chịu trách nhiệm chính, do nhóm pick |
| `Reviewer` | Người kiểm tra, khác assignee |
| `Status` | `Backlog`, `Ready`, `In Progress`, `Review`, `Blocked`, `Done` |
| `Evidence / PR` | Link hoặc mô tả bằng chứng hoàn thành |

## 6. Điều chỉnh kế hoạch

- Nếu một task lệch effort trên 50%, cập nhật estimate còn lại và cân bằng task chưa bắt đầu.
- Nếu task P0 trễ, ưu tiên hỗ trợ đường găng trước task Stretch.
- Không giảm test, idempotency hoặc data quality để giữ đúng lịch.
- Nếu spatial enrichment chưa ổn định cuối tuần 4, giữ tọa độ và phân loại `Unknown/Offshore` theo phạm vi tối thiểu, sau đó chuyển enrichment chi tiết thành Stretch.
- Cuối mỗi tuần, nhóm review workload, dependency và exit criteria trong 20–30 phút.

