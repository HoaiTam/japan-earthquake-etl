# Kế hoạch task 6 tuần

Backlog được lưu hoàn toàn bằng Markdown. Mỗi task có một file độc lập trong [danh mục task](./tasks/README.md); phạm vi, thành phần và mục đích của từng khối được giải thích tại [Các khối công việc sau Foundation](./WORK_BLOCKS.md).

## 1. Giả định lập kế hoạch

- Nhóm có 3 thành viên.
- Thời gian thực hiện là 6 tuần.
- Capacity tham khảo là 15 giờ/người/tuần, tương đương 270 giờ toàn nhóm.
- Assignee, reviewer, trạng thái và evidence được cập nhật trực tiếp trong file của từng task.
- Mỗi task chỉ có một assignee chính; reviewer phải là người khác assignee.
- Task `Core` cần hoàn thành để đạt tiêu chí phiên bản đầu tiên.
- Task `Stretch` chỉ thực hiện sau khi các dependency Core ổn định.
- Có thiết kế logical data model cho Bronze, Silver, Gold, Iceberg và Trino; không xây thêm business database quan hệ hoặc DDL vật lý ngoài phạm vi lakehouse.
- Dữ liệu động đất đến từ hai nguồn: USGS cho luồng cập nhật hằng ngày và JMA cho kho lịch sử 40 năm có version.
- Không bao gồm Data Science/Generative AI.

Nếu capacity thực tế khác 15 giờ/người/tuần, nhóm dùng trường `effort_hours` trong từng file task và bảng chỉ mục để cân bằng lại các task chưa bắt đầu.

## 2. Mục tiêu theo tuần

| Tuần | Mục tiêu | Exit criteria | Effort kế hoạch |
|---:|---|---|---:|
| 1 | Foundation và môi trường local | Các service nền chạy, health check đạt, có smoke checklist | 45 giờ |
| 2 | Contracts đa nguồn và USGS Bronze | Contract chung được chốt; USGS raw, manifest và smoke test Bronze đạt | 45 giờ |
| 3 | JMA Bronze và parser hai nguồn | Archive JMA theo năm vào Bronze; USGS/JMA parse về cùng Silver contract | 45 giờ |
| 4 | Silver publish, canonical event và Gold | Silver đối soát/idempotent; Gold snapshot commit và verify bằng Trino | 45 giờ |
| 5 | Orchestration và Power BI | DAG retry/backfill hoạt động; Power BI đọc Trino và KPI khớp SQL | 45 giờ |
| 6 | QA, vận hành, tài liệu và demo | E2E, revision/backfill, recovery, security, docs và demo được xác nhận | 45 giờ |

Tổng effort gồm cả task Stretch là **270 giờ**. `QA-05` và `OPS-01` là Stretch, có thể hoãn nếu ảnh hưởng đường găng Core.

## 3. Khối công việc sau Foundation

Các task được chia thành tám khối để nhóm có thể giao nguyên một phạm vi cho một người hoặc tách task nhỏ theo kỹ năng:

| Khối | Task | Phạm vi | Mục đích |
|---|---|---|---|
| A - Hợp đồng dữ liệu | `CON-01..04` | Phạm vi nguồn, Bronze contract, Silver/Gold model và fixture | Tạo giao diện chung trước khi code để các khối khác làm song song |
| B - USGS Bronze | `USG-01..05` | Request, HTTP client, raw writer, Airflow và tests | Cung cấp dữ liệu cập nhật hằng ngày có thể audit |
| C - JMA Bronze | `JMA-01..05` | Inventory 40 năm, downloader, versioning, Bronze và backfill | Cung cấp lịch sử JMA bất biến theo catalog release |
| D - Silver đa nguồn | `SLV-01..09` | Parser, normalize, lineage, quality, dedup, source linking và Parquet | Tạo observation chuẩn và tránh double count hai nguồn |
| E - Gold & Serving | `GLD-01..04` | Canonical event, aggregates, Iceberg snapshot và Trino views | Tạo lớp bảng/SQL ổn định cho analytics |
| F - Điều phối | `ORC-01..05` | DAG, schedule, backfill, observability và recovery | Ghép các khối theo contract và vận hành an toàn |
| G - Power BI | `BI-01..05` | ODBC, semantic model, dashboard, refresh và reconciliation | Trình bày KPI từ Trino mà không đọc object trực tiếp |
| H - QA & Release | `QA/SEC/OPS/DOC/DEMO` | E2E, revision, recovery, security, docs và release | Xác nhận tích hợp và bàn giao sản phẩm |

Chi tiết “làm phần gì”, “có những gì” và “dùng để làm gì” nằm trong [file riêng của từng task](./tasks/README.md) và trong [WORK_BLOCKS.md](./WORK_BLOCKS.md).

## 4. Quy tắc pick task

1. Mở [danh mục task](./tasks/README.md), chọn task `Ready` hoặc `Backlog` có hard dependency đã đạt.
2. Pick task `P0` trước, sau đó đến `P1`; chỉ pick `P2/Stretch` khi đường găng ổn định.
3. Mục `Hard dependency` trong file task chỉ ghi **hard dependency**. Nếu contract và fixture của task đã có, thành viên được bắt đầu code/test bằng fixture hoặc mock mà không phải chờ toàn bộ upstream chạy thật.
4. Mỗi người tự cộng `effort_hours` của các task đã nhận và giữ tải theo tuần trong khoảng cân bằng.
5. Không để một người giữ toàn bộ kiến thức của một chuỗi quan trọng. Người review nên thuộc stream khác khi có thể.
6. Khi bắt đầu, cập nhật metadata và mục `Theo dõi` trong file task sang `In Progress`, điền assignee và đồng bộ dòng tương ứng trong chỉ mục.
7. Khi mở PR chuyển sang `Review`; chỉ dùng `Done` sau khi đạt tiêu chí hoàn thành và có evidence.
8. Ghi link PR, commit, ảnh hoặc log xác nhận vào mục `Evidence / PR` của file task.

## 5. Definition of Done chung

Một task chỉ được xem là `Done` khi:

- Deliverable đã có trong repository hoặc môi trường demo.
- Tiêu chí hoàn thành của task đã được kiểm tra.
- Test liên quan đạt; nếu chưa có test tự động phải có bằng chứng thủ công.
- Không chứa secret, dữ liệu nhạy cảm hoặc file build không cần thiết.
- Tài liệu/contract được cập nhật nếu hành vi, cấu hình hoặc schema logic thay đổi.
- Có reviewer khác assignee xác nhận đối với task P0/P1.

## 6. Cấu trúc của mỗi file task

| Trường/phần | Ý nghĩa |
|---|---|
| `Task ID` | Mã ổn định dùng trong branch, commit và trao đổi |
| `Tuần` | Tuần mục tiêu từ 1 đến 6 |
| `Khối` | Foundation hoặc khối A-H để chia ownership và review |
| `Scope` | `Core` hoặc `Stretch` |
| `Priority` | `P0`, `P1` hoặc `P2` |
| `Hard dependency` | Task phải hoàn tất trước; mỗi mã có link đến file nguồn |
| `effort_hours` | Ước lượng giờ công của assignee chính |
| `Assignee` | Người chịu trách nhiệm chính, do nhóm pick |
| `Reviewer` | Người kiểm tra, khác assignee |
| `Status` | `Backlog`, `Ready`, `In Progress`, `Review`, `Blocked`, `Done` |
| `Evidence / PR` | Link hoặc mô tả bằng chứng hoàn thành |
| `Mục đích` | Task tạo giá trị gì cho pipeline |
| `Phạm vi` và `Thành phần cần có` | Task làm phần gì và phải có những đầu ra nào |
| `Tiêu chí hoàn thành` | Checklist cụ thể để quyết định task có thể chuyển `Done` hay chưa |
| `Cách triển khai và phối hợp` | Phần có thể làm song song, cách dùng fixture/mock và integration gate |
| `Ranh giới` | Những thay đổi không được tự mở rộng trong task |

## 7. Giảm phụ thuộc và làm song song

- Block B và C triển khai song song sau khi `CON-01` chốt phạm vi nguồn.
- Parser `SLV-02` và `SLV-03` triển khai song song bằng fixture sau `CON-03`/`CON-04`; không cần chờ downloader hoàn chỉnh.
- Gold, Airflow và Power BI có thể phát triển với Silver fixture, mock task hoặc sample Trino view theo contract.
- Chỉ các integration gate `USG-05`, `JMA-05`, `SLV-09`, `GLD-04`, `BI-05` và `QA-01` mới chờ output chạy thật của nhiều khối.
- Nếu dependency chưa sẵn sàng, assignee vẫn có thể hoàn thành test plan, fixture, interface, query hoặc dashboard mock; không tự đổi contract đã chốt.

## 8. Điều chỉnh kế hoạch

- Nếu một task lệch effort trên 50%, cập nhật estimate còn lại và cân bằng task chưa bắt đầu.
- Nếu task P0 trễ, ưu tiên hỗ trợ đường găng trước task Stretch.
- Không giảm test, idempotency hoặc data quality để giữ đúng lịch.
- Nếu mapping region chưa ổn định cuối tuần 4, giữ tọa độ thật và `Unknown/Offshore`; không làm mất event hoặc chặn Gold Core.
- Cuối mỗi tuần, nhóm review workload, dependency và exit criteria trong 20–30 phút.

## 9. Baseline PLN-01

Phạm vi MVP, KPI, đường găng và Definition of Done dùng chung được tập hợp tại [Baseline phạm vi MVP, KPI và Definition of Done](../specs/MVP_SCOPE_KPI_AND_DOD.md).

`PLN-01` đã hoàn tất qua [PR #3](https://github.com/HoaiTam/japan-earthquake-etl/pull/3). Các quyết định kỹ thuật đã được giao cho task downstream không phải là lý do mở rộng phạm vi hoặc bỏ qua dependency.
