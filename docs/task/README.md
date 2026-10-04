# Kế hoạch task 8 tuần

Backlog được lưu hoàn toàn bằng Markdown. Mỗi task có một file độc lập trong
[danh mục task](./tasks/README.md); phạm vi, thành phần và mục đích của từng
khối được giải thích tại [Các khối công việc sau Foundation](./WORK_BLOCKS.md).
Phân công gần nhất nằm tại [Kế hoạch tuần 3 — ba luồng không chờ
nhau](./WEEK_3_PARALLEL_PLAN.md).

## 1. Giả định lập kế hoạch

- Nhóm có 3 thành viên.
- Thời gian thực hiện là 8 tuần.
- Capacity tham khảo là 15 giờ/người/tuần, tương đương 360 giờ toàn nhóm trong
  8 tuần. Backlog có 364 giờ task vì 12 giờ data-readiness được người điều phối
  làm trước tuần 3; 352 giờ còn lại nằm trong capacity 8 tuần.
- Assignee, reviewer, trạng thái và evidence được cập nhật trực tiếp trong file của từng task.
- Mỗi task chỉ có một assignee chính; nếu có reviewer thì reviewer phải là người khác assignee.
- Task `Core` cần hoàn thành để đạt tiêu chí phiên bản đầu tiên.
- Task `Stretch` chỉ thực hiện sau khi các dependency Core ổn định.
- Có thiết kế logical data model cho Bronze, Silver, Gold và namespace `ml` trên Iceberg/Trino; không xây thêm business database quan hệ hoặc serving copy ngoài lakehouse.
- Dữ liệu động đất đến từ hai nguồn: USGS cho luồng cập nhật hằng ngày và JMA cho kho lịch sử 40 năm có version.
- Core bao gồm quy trình clustering hồi cứu Window/DBSCAN/HDBSCAN từ Gold đến namespace `ml`; không bao gồm dự đoán động đất, cảnh báo thời gian thực hoặc Generative AI.

Nếu capacity thực tế khác 15 giờ/người/tuần, nhóm dùng trường `effort_hours` trong từng file task và bảng chỉ mục để cân bằng lại các task chưa bắt đầu.

## 2. Mục tiêu theo tuần

| Tuần | Mục tiêu | Exit criteria | Effort kế hoạch |
|---:|---|---|---:|
| 1 | Foundation và môi trường local | Các service nền chạy, health check đạt, có smoke checklist | 45 giờ |
| 2 | Contracts đa nguồn và USGS Bronze | Contract chung được chốt; USGS raw, manifest và smoke test Bronze đạt | 45 giờ |
| Trước tuần 3 | Data readiness do người điều phối thực hiện | Live USGS Bronze, inventory JMA và catalog hai sample thật được khóa | 12 giờ |
| 3 | Ba luồng JMA/Silver độc lập | JMA downloader/writer, hai parser, resolver, quality, lineage và Silver writer đạt unit acceptance; integration bằng hai sample nhỏ | 41 giờ task + 4 giờ review |
| 4 | JMA orchestration, Silver integration và Gold | JMA backfill/QA, Silver dedup/link/E2E và Gold snapshot/verification tiếp tục theo dependency | 44 giờ |
| 5 | Điều phối ETL và chuẩn bị ML dataset | DAG ETL retry/backfill hoạt động; Gold snapshot được pin; audit/Mc/mainshock/window bắt đầu | 45 giờ |
| 6 | Feature và baseline experiment | Feature 4-D được validate/export; notebook chạy Window/DBSCAN/HDBSCAN global | 45 giờ |
| 7 | Evaluation và ML import | Adaptive/out-of-period, stability/Omori, ETL QA và import gate được kiểm tra | 44 giờ |
| 8 | Tích hợp, QA, tài liệu và demo | ML tables/report, E2E, recovery, demo và Power BI Stretch được xác nhận | 43 giờ |

Tổng effort gồm cả task Stretch là **364 giờ**. Sau 12 giờ pre-week, kế hoạch
còn **352 giờ task** trong 8 tuần; 8 giờ capacity không gắn task, trong đó 4 giờ
được giữ cho review/integration tuần 3. `GLD-02`, `BI-01..05`, `QA-05` và `OPS-01` là Stretch, có
thể hoãn nếu ảnh hưởng đường găng Core.

## 3. Khối công việc sau Foundation

Các task được chia thành mười một khối để nhóm có thể giao nguyên một phạm vi cho một người hoặc tách task nhỏ theo kỹ năng:

| Khối | Task | Phạm vi | Mục đích |
|---|---|---|---|
| A - Hợp đồng dữ liệu | `CON-01..04`, `DAT-01` | Phạm vi nguồn, Bronze contract, Silver/Gold/ML model, fixture và catalog sample thật | Tạo giao diện/input chung trước khi code để các khối khác làm song song |
| B - USGS Bronze | `USG-01..06` | Request, HTTP client, raw writer, Airflow, tests và live MinIO runner | Cung cấp dữ liệu cập nhật hằng ngày có thể audit và một manifest thật để tích hợp |
| C - JMA Bronze | `JMA-01..05` | Inventory 40 năm, downloader, versioning, Bronze và backfill | Cung cấp lịch sử JMA bất biến theo catalog release |
| D - Silver đa nguồn | `SLV-01..09` | Parser, normalize, lineage, quality, dedup, source linking và Parquet | Tạo observation chuẩn và tránh double count hai nguồn |
| E - Gold & Serving | `GLD-01..04` | Canonical event, Iceberg snapshot, Trino verification; dashboard aggregate là Stretch | Tạo lớp bảng/SQL ổn định cho ML và analytics |
| F - Điều phối ETL | `ORC-01..05` | DAG, schedule, backfill, observability và recovery đến Gold | Ghép các tầng dữ liệu và vận hành an toàn |
| G - ML Dataset | `MLD-01..05` | Pin snapshot, audit/Mc, mainshock window, feature và export | Tạo input bất biến, có lineage cho mọi thuật toán |
| H - Experiment | `EXP-01..05` | Colab, Window/DBSCAN/HDBSCAN, adaptive, stability và extension | So sánh thuật toán có thể tái lập, không tuyên bố causal |
| I - ML Integration | `MLI-01..04`, `MLQ-01` | Bundle contract, hai DAG, Iceberg `ml.*`, Trino/static report và E2E | Đưa kết quả external về lakehouse qua quality gate |
| J - Power BI tùy chọn | `BI-01..05` | ODBC, semantic model, dashboard, refresh và reconciliation | Phần trình bày mở rộng khi đường găng ML ổn định |
| K - QA & Release | `QA/SEC/OPS/DOC/DEMO` | ETL E2E, revision, recovery, security, docs và release | Xác nhận tích hợp và bàn giao sản phẩm |

Chi tiết “làm phần gì”, “có những gì” và “dùng để làm gì” nằm trong [file riêng của từng task](./tasks/README.md) và trong [WORK_BLOCKS.md](./WORK_BLOCKS.md).

## 4. Quy tắc pick task

1. Mở [danh mục task](./tasks/README.md), chọn task `Ready` hoặc `Backlog` có hard dependency đã đạt.
2. Pick task `P0` trước, sau đó đến `P1`; chỉ pick `P2/Stretch` khi đường găng ổn định.
3. Mục `Hard dependency` trong file task chỉ ghi **hard dependency**. Nếu contract và fixture của task đã có, thành viên được bắt đầu code/test bằng fixture hoặc mock mà không phải chờ toàn bộ upstream chạy thật.
4. Mỗi người tự cộng `effort_hours` của các task đã nhận và giữ tải theo tuần trong khoảng cân bằng.
5. Không để một người giữ toàn bộ kiến thức của một chuỗi quan trọng. Người review nên thuộc stream khác khi có thể.
6. Khi bắt đầu, cập nhật metadata và mục `Theo dõi` trong file task sang `In Progress`, điền assignee và đồng bộ dòng tương ứng trong chỉ mục.
7. Dùng `Needs Update` khi task đã hoàn tất theo baseline cũ nhưng contract mới làm output chưa còn đủ. Dùng `Review` khi công việc còn chờ kiểm tra để hoàn tất. Khi deliverable, tiêu chí hoàn thành, test/check, evidence và tài liệu liên quan đã đạt, tự động chuyển sang `Done` trước khi bàn giao; việc chưa có reviewer không chặn trạng thái này.
8. Ghi link PR, commit, ảnh hoặc log xác nhận vào mục `Evidence / PR` của file task.

## 5. Definition of Done chung

Một task chỉ được xem là `Done` khi:

- Deliverable đã có trong repository hoặc môi trường demo.
- Tiêu chí hoàn thành của task đã được kiểm tra.
- Test liên quan đạt; nếu chưa có test tự động phải có bằng chứng thủ công.
- Không chứa secret, dữ liệu nhạy cảm hoặc file build không cần thiết.
- Tài liệu/contract được cập nhật nếu hành vi, cấu hình hoặc schema logic thay đổi.
- Reviewer khác assignee được khuyến nghị đối với task P0/P1 nhưng không phải điều kiện chặn `Done`.

## 6. Cấu trúc của mỗi file task

| Trường/phần | Ý nghĩa |
|---|---|
| `Task ID` | Mã ổn định dùng trong branch, commit và trao đổi |
| `Tuần` | Tuần mục tiêu từ 1 đến 8 |
| `Khối` | Foundation hoặc khối A-K để chia ownership và review |
| `Scope` | `Core` hoặc `Stretch` |
| `Priority` | `P0`, `P1` hoặc `P2` |
| `Hard dependency` | Task phải hoàn tất trước; mỗi mã có link đến file nguồn |
| `effort_hours` | Ước lượng giờ công của assignee chính |
| `Assignee` | Người chịu trách nhiệm chính, do nhóm pick |
| `Reviewer` | Người kiểm tra, khác assignee; có thể để `unassigned` và không chặn `Done` |
| `Status` | `Backlog`, `Ready`, `In Progress`, `Review`, `Blocked`, `Needs Update`, `Done` |
| `Evidence / PR` | Link hoặc mô tả bằng chứng hoàn thành |
| `Mục đích` | Task tạo giá trị gì cho pipeline |
| `Phạm vi` và `Thành phần cần có` | Task làm phần gì và phải có những đầu ra nào |
| `Tiêu chí hoàn thành` | Checklist cụ thể để quyết định task có thể chuyển `Done` hay chưa |
| `Cách triển khai và phối hợp` | Phần có thể làm song song, cách dùng fixture/mock và integration gate |
| `Ranh giới` | Những thay đổi không được tự mở rộng trong task |

## 7. Giảm phụ thuộc và làm song song

- Block B và C triển khai song song sau khi `CON-01` chốt phạm vi nguồn.
- `USG-06`, `JMA-01` và `DAT-01` khóa hai sample thật trước tuần 3; sample chỉ
  dùng cho integration, còn unit test tiếp tục dùng fixture xác định.
- Parser `SLV-02` và `SLV-03` triển khai song song bằng fixture sau `CON-03`/`CON-04`; không cần chờ downloader hoàn chỉnh.
- Gold và Airflow ETL có thể phát triển với Silver fixture/mock; notebook và result validator có thể phát triển bằng feature/result bundle nhỏ trước khi Gold thật sẵn sàng.
- `MLD-01/02`, `EXP-01` và `MLI-01` khóa contract/fixture độc lập để ba luồng Dataset, Experiment và Import làm song song.
- `USG-05` là gate bằng fixture/mock; `USG-06` là gate chạy thật USGS → MinIO.
  Các integration gate đa khối còn lại là `JMA-05`, `SLV-09`, `GLD-04`,
  `QA-01` và `MLQ-01`.
- Nếu dependency chưa sẵn sàng, assignee vẫn có thể hoàn thành test plan, fixture, interface, query hoặc dashboard mock; không tự đổi contract đã chốt.

Ba luồng, handoff contract và checklist kiểm tra cuối tuần được chốt trong
[kế hoạch tuần 3](./WEEK_3_PARALLEL_PLAN.md). Mỗi task vẫn dùng branch/PR riêng;
không gộp cả một luồng vào một branch.

## 8. Điều chỉnh kế hoạch

- Nếu một task lệch effort trên 50%, cập nhật estimate còn lại và cân bằng task chưa bắt đầu.
- Nếu task P0 trễ, ưu tiên hỗ trợ đường găng trước task Stretch.
- Không giảm test, idempotency hoặc data quality để giữ đúng lịch.
- Nếu mapping region chưa ổn định cuối tuần 4, giữ tọa độ thật và `Unknown/Offshore`; không làm mất event hoặc chặn Gold Core.
- Cuối mỗi tuần, nhóm review workload, dependency và exit criteria trong 20–30 phút.

## 9. Baseline PLN-01

Phạm vi MVP, KPI, đường găng và Definition of Done dùng chung được tập hợp tại [Baseline phạm vi MVP, KPI và Definition of Done](../specs/MVP_SCOPE_KPI_AND_DOD.md).

`PLN-01` đã cập nhật baseline HDBSCAN: quy trình Gold snapshot → ML dataset →
Window/DBSCAN/HDBSCAN → validate/import → Iceberg `ml.*` → static report là
Core; Power BI là Stretch. [Roadmap HDBSCAN](./HDBSCAN_WORKSTREAM.md), baseline
và task riêng phải được dùng cùng nhau; implementation không tự suy diễn thêm
ngoài contract đã được owner task chốt. [PR #3](https://github.com/HoaiTam/japan-earthquake-etl/pull/3)
chỉ còn là mốc lịch sử của baseline cũ.
