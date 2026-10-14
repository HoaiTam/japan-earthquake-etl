# Kế hoạch task 8 tuần

Backlog được lưu hoàn toàn bằng Markdown. Mỗi task có một file độc lập trong
[danh mục task](./tasks/README.md); phạm vi, thành phần và mục đích của từng
khối được giải thích tại [Các khối công việc sau Foundation](./WORK_BLOCKS.md).
Phân công gần nhất nằm tại [Kế hoạch tuần 5](./WEEK_5_PARALLEL_PLAN.md),
19–25/10/2026, gồm nhóm nghiệm thu tồn và ba khối song song.
[Kế hoạch tuần 4](./WEEK_4_PARALLEL_PLAN.md) và [nhóm mở đường](./WEEK_4_PREP_GROUP.md)
giữ lại làm mốc lịch sử. [Kế hoạch tuần 3](./WEEK_3_PARALLEL_PLAN.md) giữ lại để đối chiếu
handoff/evidence.

## 1. Giả định lập kế hoạch

- Nhóm có 3 thành viên.
- Thời gian thực hiện là 8 tuần.
- Capacity sơ bộ là 15 giờ/người/tuần, tương đương 360 giờ toàn nhóm trong
  8 tuần. Baseline cũ 364h; tuần 5 thêm ORC-06/07 16h và DAT-02 6h nên hiện **386h**.
  12h data-readiness trước tuần 3, còn **374h** task trong 8 tuần; vượt capacity
  sơ bộ, không coi toàn backlog vừa mức 15h/người.
- Theo yêu cầu tăng khối lượng, riêng tuần 4 dùng phương án **89h Core + 7h
  review/tích hợp**, khoảng **32h/người**. Đây là tải đề xuất, không phải capacity
  thực tế đã xác nhận; không ép 18 task vào mức 15h cũ. Giữ nguyên effort/scope/
  hard dependency, chuyển lịch ORC-01..05, MLD-01..03, MLI-01 và SEC-01 lên tuần 4.
- Tuần 5 dùng **72h Core + 4h nghiệm thu tồn + 20h review/tích hợp/dự phòng**,
  tổng 96h, khoảng 32h/người. Không cộng lại effort cũ của carryover; nếu budget
  tồn vượt 4h, replan QA-03/EXP-02 thay vì hạ gate. Đây vẫn là capacity đề xuất.
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
| 4 | Nhóm mở đường + ETL/Gold/ML pilot mở rộng | JMA Bronze thật, DAG/run context, SilverReady, Gold Published, pinned dataset/audit/candidate pilot và security evidence | 89 giờ Core + 7 giờ review/tích hợp; GLD-02 Stretch 4h ngoài cam kết |
| 5 | Nối ETL thật, feature/export, baseline algorithms và import | Full-month proof, real adapters, QA/recovery; feature/export và experiment chỉ nghiệm thu khi audit/coverage/lifecycle hợp lệ | 72h Core + 4h nghiệm thu tồn + 20h review/tích hợp/dự phòng |
| 6 | Chốt docs và phần tuần 5 chưa đủ gate; lên lịch evaluation/report | Không bỏ coverage/quality gate; tiếp tục adaptive/evaluation theo handoff tuần 5 | 12h task gắn tuần 6, gồm QA-04 dời từ tuần 5; lịch tiếp theo chốt sau review tuần 5 |
| 7 | Adaptive/evaluation và extension | Stability/Omori, out-of-period và coverage đủ trước scientific review | 22h task đang gắn tuần 7; replan sau tuần 5 |
| 8 | ML report/E2E, release và Stretch | Report/import lineage, demo và release evidence; BI không chặn Core | 44h task đang gắn tuần 8, gồm BI-02 đã dời |

Tổng effort gồm Stretch là **386 giờ** = baseline **364h** + ORC-06/07 **16h** + DAT-02 **6h**.
Baseline lịch sử còn **352 giờ task** trong 8 tuần sau pre-week; hiện tại
còn **374 giờ task** trong 8 tuần (386 − 12). “Còn” ở đây là phân bổ lịch,
không phải remaining effort của các task đã Done. Giữ tuần 3/4 làm mốc lịch sử.
Tuần 5 kéo 54h task cũ từ tuần 6/7/8 lên; ORC-06/07 thêm 16h, DAT-02 thêm 6h và QA-04 4h dời sang tuần 6
= 72h tuần 5. BI-02 5h dời sang tuần 8. Tổng giờ theo metadata (tách USG-06/JMA-01/DAT-01 ra pre-week):
45/45/41/93/72/12/22/44h + pre-week 12h
= 386h. Review/tích hợp là budget dự phòng, không cộng vào effort task.
Lịch tuần 6–8 phải chốt lại theo remaining work/coverage sau tuần 5; số giờ
metadata không đồng nghĩa capacity cam kết. GLD-02, BI-01..05, QA-05 và OPS-01
vẫn Stretch, không chiếm budget đường găng Core.

## 3. Khối công việc sau Foundation

Các task được chia thành mười một khối để nhóm có thể giao nguyên một phạm vi cho một người hoặc tách task nhỏ theo kỹ năng:

| Khối | Task | Phạm vi | Mục đích |
|---|---|---|---|
| A - Hợp đồng dữ liệu | `CON-01..04`, `DAT-01..02` | Phạm vi nguồn, Bronze contract, Silver/Gold/ML model, fixture và catalog sample thật | Tạo giao diện/input chung trước khi code để các khối khác làm song song |
| B - USGS Bronze | `USG-01..06` | Request, HTTP client, raw writer, Airflow, tests và live MinIO runner | Cung cấp dữ liệu cập nhật hằng ngày có thể audit và một manifest thật để tích hợp |
| C - JMA Bronze | `JMA-01..05` | Inventory 40 năm, downloader, versioning, Bronze và backfill | Cung cấp lịch sử JMA bất biến theo catalog release |
| D - Silver đa nguồn | `SLV-01..09` | Parser, normalize, lineage, quality, dedup, source linking và Parquet | Tạo observation chuẩn và tránh double count hai nguồn |
| E - Gold & Serving | `GLD-01..04` | Canonical event, Iceberg snapshot, Trino verification; dashboard aggregate là Stretch | Tạo lớp bảng/SQL ổn định cho ML và analytics |
| F - Điều phối ETL | `ORC-01..07` | DAG, schedule, backfill, full-month state, real adapters, observability và recovery đến Gold | Ghép các tầng dữ liệu và vận hành an toàn |
| G - ML Dataset | `MLD-01..05` | Pin snapshot, audit/Mc, mainshock window, feature và export | Tạo input bất biến, có lineage cho mọi thuật toán |
| H - Experiment | `EXP-01..05` | Jupyter local trước → Colab sau, Window/DBSCAN/HDBSCAN, adaptive, stability và extension | So sánh thuật toán có thể tái lập, không tuyên bố causal |
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
không gộp cả một luồng vào một branch. [Phân công tuần 4 mở rộng](./WEEK_4_PARALLEL_PLAN.md)
tách nhóm mở đường JMA-04/JMA-05/ORC-01 (13h HoaiTam làm trước), rồi ba khối
vận hành ETL, Silver + ML audit/candidate, Gold + ML identity/contract/security.
Tổng task mỗi người là 30/29/30h, đã tính nhóm mở đường; chỉ gate thật chờ
upstream, fixture/mock/test plan có thể chuẩn bị ngay. Không đổi dependency để
nhận notebook/export chưa có là đã Done.

[Phân công tuần 5](./WEEK_5_PARALLEL_PLAN.md) thay lịch hiện hành: HoaiTam
DAT-02/ORC-07/MLI-02/QA-01; ThanhTris ORC-06/MLI-03/QA-02/03; rosy179
MLD-04/05/EXP-01/02. Các task đều có phần handoff tuần 5 trong file riêng.
Hard dependency chỉ chặn nghiệm thu thật; mọi luồng được bắt đầu bằng fixtures.
DAT-02 tạo development pilot bundle riêng, không đổi parent BUILDING/research_ready=false.
EXP-01/02 test Jupyter trước; local PASSED rồi mới migrate Colab. Official
research vẫn cần MLD-05 EXPORTED và import gates. [Workflow](../specs/JUPYTER_PILOT_WORKFLOW.md)
nêu coverage/type/markers/ownership; QA-04 giữ scope và dời tuần 6.

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
