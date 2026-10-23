# Tuần 5 — ETL thật và Jupyter pilot trước Colab

## Phân công hiện hành — đồng bộ 22/10/2026

Áp dụng cho **19–25/10/2026**, theo bảng phân công gần nhất HoaiTam gửi.
Phần này thay roster/capacity ở các mục lịch sử 1–6 bên dưới. Không sửa lại
evidence của các lần nghiệm thu cũ hoặc coi merge/CI là scientific approval.

### Bảng 11 task giao việc trong tuần

| Người làm | Task | Làm gì / output | Effort | Trạng thái tại 22/10 |
|---|---|---|---:|---|
| HoaiTam | [ORC-07](./tasks/ORC-07.md) | Nối real ETL adapters đến Gold Published | 8h | Done |
| HoaiTam | [QA-01](./tasks/QA-01.md) | Nghiệm thu daily scheduled E2E và đối soát từng tầng | 5h | In Progress |
| HoaiTam | [DOC-01](./tasks/DOC-01.md) | Đồng bộ kiến trúc, runbook, Jupyter → Colab và task tracking | 4h | Done |
| ThanhTris | [MLI-03](./tasks/MLI-03.md) | Validate/import kết quả ML, không nhận development pilot | 7h | Done |
| ThanhTris | [QA-02](./tasks/QA-02.md) | Rerun, duplicate và late revision | 5h | Done |
| ThanhTris | [QA-03](./tasks/QA-03.md) | Historical backfill và báo coverage có phạm vi | 5h | Done |
| ThanhTris | [QA-04](./tasks/QA-04.md) | Failure/recovery các tầng ETL và ML import | 4h | Done |
| rosy179 | [EXP-01](./tasks/EXP-01.md) | Jupyter harness, loader/writer và môi trường chung | 5h | Done |
| rosy179 | [EXP-02](./tasks/EXP-02.md) | Window/DBSCAN/HDBSCAN global; local trước Colab | 7h | In Progress |
| rosy179 | [MLI-02](./tasks/MLI-02.md) | Build dataset DAG đến validated export bundle | 5h | In Progress |
| rosy179 | [EXP-03](./tasks/EXP-03.md) | Adaptive HDBSCAN và multi-sequence resolution | 6h | In Progress |

Tổng bảng trên **11 task/61h**, phân bổ **17/21/23h**. MLI-02 chuyển từ
HoaiTam sang rosy179; DOC-01/QA-04/EXP-03 được kéo từ tuần 6/6/7 về tuần 5.
Chỉ đổi lịch/owner, giữ nguyên effort, scope, acceptance và hard dependencies.
Owner/branch trong evidence trước ngày đồng bộ là thông tin lịch sử, không
phải owner hiện hành. Reviewer unassigned không được tự tick approval.

### Bốn task mở đường — không đếm lại vào bảng 11 task

| Task | Owner hiện hành | Effort | Trạng thái / handoff |
|---|---|---:|---|
| [ORC-06](./tasks/ORC-06.md) | HoaiTam | 8h | Done — full-month state/coverage cho ORC-07 |
| [DAT-02](./tasks/DAT-02.md) | HoaiTam | 6h | Done — development-only Jupyter pilot |
| [MLD-04](./tasks/MLD-04.md) | rosy179 | 6h | Done — shared feature 4-D |
| [MLD-05](./tasks/MLD-05.md) | rosy179 | 5h | Done — component validation/export; không chứng minh catalog thật toàn kỳ |

Nhóm mở đường **4 task/25h**. Ownership giữ theo task hiện hành, không đổi
assignee chỉ vì người điều phối đã hỗ trợ triển khai. Tính đủ metadata tuần 5:

| Assignee | Bảng giao việc | Mở đường | Tổng effort task |
|---|---:|---:|---:|
| HoaiTam | 17h | 14h | **31h** |
| ThanhTris | 21h | 0h | **21h** |
| rosy179 | 23h | 11h | **34h** |
| Tổng | 61h | 25h | **86h / 15 task Core** |

Không phải 15 task mới hoặc 86h còn lại: các task Done giữ effort kế hoạch
để truy vết, không giao làm lại. Tổng backlog vẫn **76 task/386h**. Lịch mới
dịch 14h từ tuần 6/7 về tuần 5; tuần 6 còn 4h, tuần 7 còn 16h theo metadata.
Giữ budget tồn 4h + review/tích hợp 20h của kế hoạch cũ thì tổng tham khảo là
**110h**, không còn 96h/32h mỗi người; chưa phải capacity đã được xác nhận.
Giữ buffer cũ theo người cho ra 39/28/43h. Không tự tăng effort hoặc giảm gate
để ép vào budget; điều phối phải cân bằng remaining work thực tế.

### Các gate còn thiếu và thứ tự tiếp tục

- **MLI-02 — Build dataset DAG:** rosy179 nối real MLD-05 exporter vào runner,
  kiểm DAG/registry/retry/handoff thật. Mock/offline PASS không hoàn tất task.
- **QA-01 — Daily E2E:** HoaiTam nghiệm thu scheduled deployment có eligible
  input, counts, six snapshots/publication và KPI SQL. Current-month/as-of-cutoff
  và JMA sau 2023 chưa có task contract riêng; không lấy request lịch sử giả daily.
- **EXP-02 — Baselines:** local fresh-kernel pilot đã đạt; còn Colab parity và
  official EXPORTED dataset run. Không nâng DAT-02 PARTIAL thành research-ready.
- **EXP-03 — Adaptive/multi-sequence:** còn reproduction run/fresh-kernel
  evidence và dependency EXP-02; isolated_exec/fallback không mở Colab gate.
- **SEC-01 — Review bảo mật:** Done cho Docker/Jupyter local owner duyệt ngày
  23/10, có [runtime/15 permission checks/handoff evidence riêng](../evidence/SEC-01_LOCAL_ACCEPTANCE_20261023.md),
  không nhận Done từ docs sync hoặc QA-01. Colab/Drive chưa triển khai, mở lại
  SEC-01 trước migration; không thuộc 15 task Core tuần 5.
- Các task đã Done dùng [task index](./tasks/README.md) và evidence riêng để
  đọc phạm vi. QA-02/03/04 là runtime acceptance có giới hạn; MLD-05 là synthetic
  component QA + credential-free loader, không thay official ML whole-flow.

Checks: `make check-week-5-plan check-task-status test-week-5-plan check-docs`;
[biên bản đồng bộ](../evidence/DOC-01_TRACKING_SYNC.md).
Planning check phải giữ 15 task/86h và owner 31/21/34; không đổi state để làm
planning test xanh. Xem [known issues hiện hành](../KNOWN_ISSUES.md).

## Lịch sử kế hoạch — các mục 1–6 tại mốc 14–16/10/2026

Các số 12 task/72h, owner MLI-02 HoaiTam và lịch QA-04 tuần 6/DOC-01 tuần 6/
EXP-03 tuần 7 bên dưới được giữ để truy vết mốc cũ, **không còn là phân công
hiện hành**. Phần đồng bộ 22/10 ở trên có ưu tiên; trạng thái/evidence chính
thức vẫn nằm trong từng file task. Không sửa artifact nghiệm thu lịch sử.

## 1. Mốc lập kế hoạch và mục tiêu

- Lập ngày **2026-10-14**, từ `origin/main` **4d3ac3d**, PR #71 đã merge.
  GitHub không có PR mở tại thời điểm kiểm tra. Đây là trạng thái tại mốc đó,
  không phải cam kết trạng thái GitHub sẽ giữ nguyên cả tuần.
- “Tuần tiếp theo” được hiểu là **19–25/10/2026**, tuần 5 của kế hoạch dự án.
- Dùng ba assignee GitHub **HoaiTam / ThanhTris / rosy179** cho task mới.
  Đây là phân công đề xuất; không tự suy ra rosy179 là tên Trang trong kế hoạch
  cũ. SEC-01 đã được HoaiTam nhận để hoàn thiện audit (mốc tuần 4 owner Trang
  giữ làm lịch sử); MLD-02/03 giữ rosy179, reviewer HoaiTam.
- Mục tiêu: kết nối các component ETL đã có thành một run thật có phạm vi,
  làm feature 4-D/export, build/import DAG và ba thuật toán baseline;
  **Jupyter pilot trước → local test đạt → Colab sau** theo yêu cầu mới.
  **Không tuyên bố MVP hoặc dataset toàn kỳ nghiên cứu đã hoàn tất.**

Đã có component smoke Bronze/Silver/Gold, MLD-01..03 và MLI-01. ORC-01 chỉ
nghiệm thu skeleton; ORC-03 có dispatcher/adapter boundary, chưa tự chứng minh
daily đến Published. Gold writer đòi `COMPLETE_MONTHS`; sample 16 dòng hoặc
USGS overlap 3 ngày không đủ để thay toàn tháng.

Bổ sung hai task triển khai để QA-01 không phải vừa viết pipeline vừa nghiệm thu:

- [ORC-06 — Resolve affected months và dựng full-month Silver state](./tasks/ORC-06.md), 8h.
- [ORC-07 — Kết nối real ETL adapters đến Gold Published](./tasks/ORC-07.md), 8h.

Bổ sung [DAT-02 — Dataset pilot nhỏ để test Jupyter](./tasks/DAT-02.md),
HoaiTam, 6h; [workflow](../specs/JUPYTER_PILOT_WORKFLOW.md) khóa development-only
bundle và local-to-Colab gate. Backlog **73 → 76 task**, **364 → 386h**,
Core **65 → 68**, Stretch giữ 8: 16h ORC + 6h DAT mới, không tăng effort task
cũ hoặc tính lại giờ tuần 4. Không đổi schema/grain/lifecycle hay phạm vi nghiên cứu của baseline.

## 2. Capacity và ba khối giao việc

Giữ mức tăng tốc tham khảo **32h/người**, tổng 96h, như tuần 4. Nếu thực tế chỉ
15h/người, chỉ nhận nhóm ưu tiên đầu tiên; không ép cả bảng vào 45h toàn nhóm.

| Assignee | Task tuần 5 | Effort task mới | Tồn tuần trước | Review/tích hợp/dự phòng | Tổng budget |
|---|---|---:|---:|---:|---:|
| HoaiTam | DAT-02, ORC-06, ORC-07, MLI-02, QA-01 | 32h | tối đa 4h điều phối/nghiệm thu còn lại | 4h | 40h |
| ThanhTris | MLI-03, QA-02, QA-03 | 17h | không tính lại effort cũ | 7h | 24h |
| rosy179 | MLD-04, MLD-05, EXP-01, EXP-02 | 23h | không tính lại effort cũ | 9h | 32h |
| **Tổng** | **12 task Core** | **72h** | **4h** | **20h** | **96h** |

Cập nhật 2026-10-15: HoaiTam nhận triển khai ORC-06 (8h) thay ThanhTris.
Phân bổ task 24/25/23h ban đầu chuyển thành 32/17/23h; tổng task/effort không
đổi. Nếu giữ nguyên buffer thì budget thành 40/24/32h, **không còn cân bằng
32h/người**; cần replan capacity trước khi cam kết cả tuần. Đây không phải
thêm 8h mới vào backlog. Chưa đổi assignee MLD-05 trong PR ORC-06 này.

4h là budget dự kiến cho phần còn lại, **không phải estimate hoàn thành cả
FND-01/ORC-02/SEC-01**; owner phải báo remaining effort sau preflight. Security
owner hiện tại HoaiTam nghiệm thu SEC-01; reviewer vẫn unassigned, không tự ghi approval.
20h gồm review chéo, tích hợp, CI/network/resource issues và cập nhật runbook,
không phải task code mới. Quá budget thì chuyển QA-03/EXP-02 sang tuần kế tiếp;
giữ nguyên acceptance criteria và trạng thái thực tế.
DAT-02 thêm 6h và QA-04 4h dời tuần 6: tuần 5 70 + 6 − 4 = 72h;
recovery scope/effort không bị giảm, chỉ đổi lịch để ưu tiên kiểm tra hướng ML local.

### Khối A — HoaiTam: pilot dữ liệu, điều phối và nghiệm thu

| Task | Làm gì / có gì | Dùng để làm gì / bàn giao | Bắt đầu ngay / gate nghiệm thu |
|---|---|---|---|
| [ORC-06](./tasks/ORC-06.md), 8h | Nhận triển khai từ ThanhTris: resolver state đủ tháng và coverage registry | Bàn giao full-month Silver cho ORC-07 | Ba dependency đã Done; vẫn phải có bounded real evidence |
| [ORC-07](./tasks/ORC-07.md), 8h | Đóng gói real phase runner; nối exact Bronze, full-month Silver, Gold commit, Trino verify, publish và receipts/lease | Một DAG thật kết thúc Published, không dùng mock hoặc nối các smoke namespace rời | Contract tests/mocks ngay; real gate cần ORC-02 và ORC-06 |
| [MLI-02](./tasks/MLI-02.md), 5h | DAG pin → audit/Mc → candidates → features → export; retry/summaries, manual bundle handoff | Build dataset độc lập daily ETL, không trigger Colab tự động | Graph/mock ngay; Done cần MLD-05 đúng lifecycle |
| [QA-01](./tasks/QA-01.md), 5h | Script/checklist một daily interval đến Gold Published, count/snapshot/lineage report | Nghiệm thu độc lập các adapters, chỉ ra đúng phase nếu fail | Test plan ngay; chạy thật sau ORC-07, không chỉ mock graph |
| [DAT-02](./tasks/DAT-02.md), 6h | Chọn 1–3 mainshock, pins/coverage, shared features và pilot Parquet/checksums; Jupyter inspection | Dataset nhỏ development-only cho EXP-01/02, không official EXPORTED | Selection/schema fixtures ngay; real feature gate cần MLD-04, không đợi MLD-05 |

### Khối B — ThanhTris: state dữ liệu và ML import

| Task | Làm gì / có gì | Dùng để làm gì / bàn giao | Bắt đầu ngay / gate nghiệm thu |
|---|---|---|---|
| [MLI-03](./tasks/MLI-03.md), 7h | Result validator/import DAG, Iceberg tables, idempotency và Trino readback | Bundle đúng được CANDIDATE; bundle sai không serving, không sửa Gold | Dùng fixtures MLI-01 ngay, không chờ Colab; live dùng QA namespace pin dataset thật |
| [QA-02](./tasks/QA-02.md), 5h | Rerun, duplicate, late revision USGS/JMA; scope/count/snapshot diff | Chứng minh revision chọn đúng, Bronze history còn và tháng ngoài scope không đổi | Chuẩn bị fixtures ngay; whole-pipeline acceptance sau ORC-07 |
| [QA-03](./tasks/QA-03.md), 5h | Tập năm/release đại diện, resume/changed archive, count/resource/coverage report | Đánh giá khả năng mở rộng lịch sử và ghi chính xác phần coverage còn thiếu | Lập matrix ngay; integration sau ORC-07; không bắt buộc tải 40 năm trong task 5h |

### Khối C — rosy179: feature/export và thí nghiệm baseline

| Task | Làm gì / có gì | Dùng để làm gì / bàn giao | Bắt đầu ngay / gate nghiệm thu |
|---|---|---|---|
| [MLD-04](./tasks/MLD-04.md), 6h | Relative km/time, scaling 4-D, finite/zero-self/resource tests | Tạo vector thống nhất cho mọi thuật toán, giữ candidate grain/lineage | Candidate fixtures MLD-03 ngay; readback feature/count trên pinned input |
| [MLD-05](./tasks/MLD-05.md), 5h | Validate/commit snapshot, Parquet parts + manifest/checksums, rerun | Bundle độc lập cho Colab, không credential và không trộn attempts | Layout/validator trước; lifecycle gate sau MLD-04 + audit đủ điều kiện |
| [EXP-01](./tasks/EXP-01.md), 5h | Shared module + Jupyter notebook, locked environment; development/official loader/writer tách biệt | Local harness chạy lại từ đầu; Colab adapter dùng cùng code, không notebook state ẩn | Fixture trước; Done local DAT-02 inspection/loader; Colab model smoke chờ EXP-02 local gate |
| [EXP-02](./tasks/EXP-02.md), 7h | Ba baseline local, plots/noise/roles/count/runtime và local-smoke-report; migrate Colab sau PASSED | Kiểm tra hướng kỹ thuật trước cloud; official output sau MLD-05 theo MLI-01 | Local cần DAT-02/EXP-01; final Done cần official export + Colab parity; pilot PASSED không là scientific approval |

Status trong file task là trạng thái thực thi, không phải lịch giao việc:
Ở mốc lập kế hoạch, ORC-06/MLD-04/MLI-03 Ready vì hard dependencies đã Done; task còn chờ gate
giữ Backlog nhưng được làm phần fixture/interface. Chỉ đổi In Progress khi
bắt đầu triển khai, Done khi đủ evidence; chưa điền reviewer/approval giả.

Cập nhật 2026-10-15: HoaiTam yêu cầu triển khai thêm dependency MLD-04 rồi
DAT-02. MLD-04 có feature/readback, DAT-02 có pilot JMA/inspection local;
assignee vẫn rosy179/HoaiTam theo task, reviewer unassigned. Xem task/evidence
hiện hành và [runbook pilot](../runbooks/DAT-02_JUPYTER_PILOT.md).
MLD-05/EXP-01 có thể nhận handoff; không suy EXP-02 hoặc Colab đã hoàn tất.
Không thêm task/effort hoặc tự tái phân budget từ phần nghiệm thu sớm này.

Cập nhật 2026-10-16: ORC-06 Done, có resolver/coverage/receipt và bounded
file-backed full-month QA 09/2023, xem [evidence](../evidence/ORC-06.md).
ORC-07 Done sau bounded daily/manual production DAGs qua DAG.test:
MinIO → full-month Silver → sáu Gold snapshots → actual Trino → Published;
same-operation replay và owned partial-commit recovery đạt.
Xem [evidence](../evidence/ORC-07.md). QA-01 vẫn cần nghiệm thu scheduled
deployment độc lập; không suy QA-01/02/03/04 Done từ adapter acceptance.
Tháng đang mở không được ORC-06 v1 chứng nhận COMPLETE_MONTHS.

## 3. Nhóm HoaiTam làm trước — không chặn mọi người viết code

1. [FND-01 — Tạo smoke checklist cho môi trường](./tasks/FND-01.md):
   **Done** sau nghiệm thu lifecycle mới ngày 2026-10-15: cold/warm startup,
   hai foundation smoke, hai USGS live smoke và data/container readback đạt.
   [Evidence](../evidence/FND-01_LIVE_ACCEPTANCE.md). Không cần làm lại prep này
   nếu source/config/runtime không đổi; budget tuần không tự phân bổ lại.
2. [ORC-02 — Cấu hình lịch và readiness cho hai nguồn](./tasks/ORC-02.md):
   **Done** sau nghiệm thu 2026-10-15: runner packaged/fresh, scheduled/first/rerun/contention
   đạt và pause/lease được xác nhận lại. Không cần clear run lỗi vì current
   interval chưa có failed run trước test. [Evidence](../evidence/ORC-02_LIVE_ACCEPTANCE.md).
3. [SEC-01 — Review secret và bề mặt truy cập](./tasks/SEC-01.md):
   **Done — local** sau audit 23/10: source/history/retained logs/runtime và
   actual pinned notebook/export/result đạt, fresh 15 permission checks đạt.
   Xem [evidence](../evidence/SEC-01_LOCAL_ACCEPTANCE_20261023.md); không chứng nhận
   Colab/Drive chưa triển khai, phải mở lại SEC-01 trước migration. QA-01 failed
   và source lease được giữ nguyên, không lấy SEC-01 thay E2E acceptance.
4. Chốt exact source manifests/SHA, baseline Gold snapshots, namespace QA,
   fixture feature/result và resource slots trong biên bản handoff.
5. DAT-02 chọn fixed mainshock IDs/input pins/coverage, khóa pilot manifest/layout
   cho rosy179 viết loader. Nhận MLD-04 feature fixture khi có; không tự normalize
   Bronze trong Jupyter. 6h này đã tính vào khối A, không tính hai lần là prep.

Đây là **nghiệm thu tồn và chốt giao diện**, không chạy lại mọi build trong
mỗi smoke. Dùng quy trình Docker hiện tại: build/start chủ động một lần nếu cần,
test chỉ preflight và dùng services đang chạy. Không clear DAG, bỏ lease,
đổi .env hoặc xóa lake để làm xanh test. Phiên lập kế hoạch này chưa chạy live;
không tự đổi ba task tồn sang Done. Cập nhật nghiệm thu 2026-10-15 chỉ đóng
ORC-02 bằng live evidence riêng, không thay budget/assignee các task tuần 5
hoặc nghiệm thu FND-01/SEC-01 từ kết quả source-only.

## 4. Làm song song và thứ tự tích hợp

- Ngày 1: cả ba người có thể code/test offline. ThanhTris bắt đầu MLI-03 với
  MLI-01 fixtures; rosy179 bắt đầu MLD-04 + export fixture layout; HoaiTam
  nhận ORC-06, DAT-02 selection/schema + ORC-07 protocol/MLI-02 graph. Chốt version/checksum fixtures trước
  khi consumer dùng; không đổi contract ngầm.
- Ngày 2–3: handoff full-month scope + pinned baseline; feature/schema/parts
  fixture; valid/invalid result fixture. Consumer vẫn dùng version đã chốt,
  không phải chờ PR upstream merge để viết test.
- Ngày 4–5: tích hợp lần lượt
  **ORC-06 → ORC-07 → QA-01/02/03** và
  **MLD-04 → DAT-02 → EXP-01 → EXP-02 Jupyter local PASSED → Colab pilot**.
  Nhánh official **MLD-04 → MLD-05 → MLI-02/EXP-02 research** giữ lifecycle gates.
  MLI-03 nghiệm thu bằng bundle hợp lệ/lỗi có candidate lineage đã pin;
  output EXP-02 là integration bổ sung, không hard dependency viết importer.
  QA-04 sang tuần 6, dùng output ORC-07/MLI-03 cho recovery thật.
- Cuối tuần: review 20–30 phút, đối soát task file/index/PR/evidence,
  tính remaining effort; không lấy số PR merged làm số task Done.

**Gates thật vẫn phải chờ:** chỉ ORC-07 có full-month input và source readiness
thật mới nối Published; chỉ official dataset qua audit/candidate/feature/export
hợp lệ mới chạy official experiment/import. DAT-02 development pilot tách riêng,
được local test trước research export, không đổi parent lifecycle. “Không ai chờ ai” áp dụng khi phát triển bằng fixture,
không có nghĩa bỏ integration gate.

## 5. Coverage nghiên cứu và việc chưa cam kết tuần 5

Pilot MLD-02/03 hiện vẫn BUILDING/`research_ready=false`; slice nhỏ có selection
bias, không đại diện reproduction 2000–2018 hoặc extension 2018–2023.
MLD-05 phải kiểm tra coverage/audit eligibility; **không đổi pilot thành
VALIDATED/EXPORTED để chạy notebook cho kịp lịch**. Nếu chưa đủ, bàn giao
implementation/fixture evidence, giữ task chưa Done và ghi blocker cụ thể.
QA-03 báo tập năm còn thiếu + kế hoạch chạy batch trước khi nhận dataset
nghiên cứu đủ; không gán acceptance 40 năm cho một smoke vài năm.
DAT-02 có thể xuất development copy có coverage PARTIAL và research_ready=false
để debug kỹ thuật trên Jupyter; không đổi window rule, không dùng nó kết luận
khoa học. EXP-02 local report PASSED mới cho Colab pilot với cùng code/bytes.
Official Colab research vẫn chờ MLD-05 EXPORTED.

- [QA-04 — Test failure và recovery theo tầng](./tasks/QA-04.md):
  dời nghiệm thu toàn tầng sang tuần 6, owner ThanhTris/4h theo phân công hiện tại;
  đã Done với [runtime acceptance 10/10 ngày 21/10/2026](../evidence/QA-04_RUNTIME_ACCEPTANCE.md).
- [EXP-03 — HDBSCAN adaptive và multi-sequence](./tasks/EXP-03.md),
  [EXP-04 — Scientific evaluation](./tasks/EXP-04.md),
  [EXP-05 — Out-of-period extension](./tasks/EXP-05.md):
  chưa cam kết adaptive/stability/Omori/extension hoàn tất tuần 5.
- [MLI-04 — Trino views và static report](./tasks/MLI-04.md):
  chờ MLI-03/EXP-04; tuần này chỉ chuẩn bị query/template nếu dư budget.
- [MLQ-01 — E2E Gold snapshot đến ML report](./tasks/MLQ-01.md):
  chưa nghiệm thu toàn ML pipeline khi các gates trên chưa đủ.
- [DOC-01 — Cập nhật tài liệu theo từng block](./tasks/DOC-01.md):
  mọi PR tuần 5 phải cập nhật runbook/limitations; final docs task vẫn tuần 6,
  không nhận Done trước khả năng tái lập dataset/experiment.
- GLD-02/BI-01..05/OPS-01/QA-05 vẫn Stretch. BI-02 dời tuần 8,
  không chiếm week-5 Core budget.

## 6. Checklist review cuối tuần

Kiểm tra kế hoạch offline hiện có: `make check-week-5-plan`,
`make check-task-status`, `make test-week-5-plan` (13 regression cases, gồm EOF và owner ORC-06) và
`make ci-docs`. Các checks này không build/start Docker, không lấy dữ liệu,
không chứng minh runtime/research acceptance. Khi đổi roster/task/effort chính
thức, cập nhật plan/metadata/index và planning checks/tests cùng PR.

- [ ] Mỗi task có branch/PR riêng, assignee đúng, status metadata/Theo dõi/index đồng bộ.
- [x] ORC-06 proof đủ tháng; moved event có cả tháng cũ/mới; incomplete bị chặn;
      bounded file-backed acceptance, không thay ORC-07/QA-01 whole-pipeline gate.
- [x] ORC-07 có bounded daily/manual Airflow DAG.test → exact Bronze → Silver →
      đủ Gold snapshots → Trino report → publication cùng identity; mock
      `published=false`, replay identical và partial-commit resume bảo toàn snapshots.
- [ ] QA-01 nghiệm thu E2E scheduled deployment độc lập, traces/counts/publication;
      không dùng bounded adapter acceptance thay cho QA-01.
- [x] QA-02 có runtime rerun/revision/retention evidence 8/8, sáu publication
      và Trino readback đạt ngày 21/10/2026; xem [nghiệm thu](../evidence/QA-02_RUNTIME_ACCEPTANCE.md).
- [x] QA-03 historical backfill: archive public đại diện, resume/changed-release/resource/retention 6/6,
      sáu publication và Trino readback đạt ngày 21/10/2026; xem [nghiệm thu/capacity](../evidence/QA-03_RUNTIME_ACCEPTANCE.md).
      Coverage bounded, không chứng minh toàn kỳ 40 năm hoặc research-ready.
- [x] QA-04 hoàn tất nghiệm thu failure/recovery 10/10, tám Gold publication và
      ML CANDIDATE pinned readback; [evidence](../evidence/QA-04_RUNTIME_ACCEPTANCE.md).
      Vẫn giữ roster/effort/scope tuần 6; synthetic QA không thay official ML E2E.
- [ ] MLD-04/05 count/grain/feature/checksum/lifecycle đúng; research coverage không bị nói quá.
- [ ] DAT-02 pilot pins/schema/count/checksum/coverage và development-only flags đúng;
      Jupyter inspection run-all đạt, không official lifecycle/import.
- [ ] EXP-01/02 local cùng pilot/config có version; PASSED trước Colab, noise/failure
      không bị ẩn; official result vẫn cần MLD-05 EXPORTED.
- [x] MLI-03 rejects bundle lỗi trước serving, CANDIDATE không tự APPROVED — [runtime component + 9-task Airflow evidence, 19/10/2026](../evidence/MLI-03_RUNTIME_ACCEPTANCE.md); synthetic QA không thay official export/scientific approval.
- [ ] Docs/checks/CI đạt; carryover chưa có evidence giữ đúng Review/In Progress.
- [ ] Nếu runtime/coverage trễ: báo blocker và remaining effort, replan tuần 6 thay vì bỏ test.
