# Tuần 5 — ETL thật và Jupyter pilot trước Colab

## 1. Mốc lập kế hoạch và mục tiêu

- Lập ngày **2026-10-14**, từ `origin/main` **4d3ac3d**, PR #71 đã merge.
  GitHub không có PR mở tại thời điểm kiểm tra. Đây là trạng thái tại mốc đó,
  không phải cam kết trạng thái GitHub sẽ giữ nguyên cả tuần.
- “Tuần tiếp theo” được hiểu là **19–25/10/2026**, tuần 5 của kế hoạch dự án.
- Dùng ba assignee GitHub **HoaiTam / ThanhTris / rosy179** cho task mới.
  Đây là phân công đề xuất; không tự suy ra rosy179 là tên Trang trong kế hoạch
  cũ. SEC-01 giữ owner Trang; MLD-02/03 giữ rosy179, reviewer HoaiTam.
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
| HoaiTam | DAT-02, ORC-07, MLI-02, QA-01 | 24h | tối đa 4h điều phối/nghiệm thu còn lại | 4h | 32h |
| ThanhTris | ORC-06, MLI-03, QA-02, QA-03 | 25h | không tính lại effort cũ | 7h | 32h |
| rosy179 | MLD-04, MLD-05, EXP-01, EXP-02 | 23h | không tính lại effort cũ | 9h | 32h |
| **Tổng** | **12 task Core** | **72h** | **4h** | **20h** | **96h** |

4h là budget dự kiến cho phần còn lại, **không phải estimate hoàn thành cả
FND-01/ORC-02/SEC-01**; owner phải báo remaining effort sau preflight. Security
owner Trang xác nhận SEC-01; HoaiTam phối hợp, không tự nhận approval thay owner.
20h gồm review chéo, tích hợp, CI/network/resource issues và cập nhật runbook,
không phải task code mới. Quá budget thì chuyển QA-03/EXP-02 sang tuần kế tiếp;
giữ nguyên acceptance criteria và trạng thái thực tế.
DAT-02 thêm 6h và QA-04 4h dời tuần 6: tuần 5 70 + 6 − 4 = 72h;
recovery scope/effort không bị giảm, chỉ đổi lịch để ưu tiên kiểm tra hướng ML local.

### Khối A — HoaiTam: pilot dữ liệu, điều phối và nghiệm thu

| Task | Làm gì / có gì | Dùng để làm gì / bàn giao | Bắt đầu ngay / gate nghiệm thu |
|---|---|---|---|
| [ORC-07](./tasks/ORC-07.md), 8h | Đóng gói real phase runner; nối exact Bronze, full-month Silver, Gold commit, Trino verify, publish và receipts/lease | Một DAG thật kết thúc Published, không dùng mock hoặc nối các smoke namespace rời | Contract tests/mocks ngay; real gate cần ORC-02 và ORC-06 |
| [MLI-02](./tasks/MLI-02.md), 5h | DAG pin → audit/Mc → candidates → features → export; retry/summaries, manual bundle handoff | Build dataset độc lập daily ETL, không trigger Colab tự động | Graph/mock ngay; Done cần MLD-05 đúng lifecycle |
| [QA-01](./tasks/QA-01.md), 5h | Script/checklist một daily interval đến Gold Published, count/snapshot/lineage report | Nghiệm thu độc lập các adapters, chỉ ra đúng phase nếu fail | Test plan ngay; chạy thật sau ORC-07, không chỉ mock graph |
| [DAT-02](./tasks/DAT-02.md), 6h | Chọn 1–3 mainshock, pins/coverage, shared features và pilot Parquet/checksums; Jupyter inspection | Dataset nhỏ development-only cho EXP-01/02, không official EXPORTED | Selection/schema fixtures ngay; real feature gate cần MLD-04, không đợi MLD-05 |

### Khối B — ThanhTris: state dữ liệu và ML import

| Task | Làm gì / có gì | Dùng để làm gì / bàn giao | Bắt đầu ngay / gate nghiệm thu |
|---|---|---|---|
| [ORC-06](./tasks/ORC-06.md), 8h | Pin baseline/input coverage; resolve tháng cũ/mới; dựng current state đủ tháng, checksum/count proof | Handoff `COMPLETE_MONTHS` cho Gold; không mất event ngoài overlap | Fixtures từ SLV/GLD đã Done; real full-month readback là gate |
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
ORC-06/MLD-04/MLI-03 Ready vì hard dependencies đã Done; task còn chờ gate
giữ Backlog nhưng được làm phần fixture/interface. Chỉ đổi In Progress khi
bắt đầu triển khai, Done khi đủ evidence; chưa điền reviewer/approval giả.

## 3. Nhóm HoaiTam làm trước — không chặn mọi người viết code

1. [FND-01 — Tạo smoke checklist cho môi trường](./tasks/FND-01.md):
   xác nhận lifecycle mới, startup/health/smoke thật. Docs hiện **Review**.
2. [ORC-02 — Cấu hình lịch và readiness cho hai nguồn](./tasks/ORC-02.md):
   **Done** sau nghiệm thu 2026-10-15: runner packaged/fresh, scheduled/first/rerun/contention
   đạt và pause/lease được xác nhận lại. Không cần clear run lỗi vì current
   interval chưa có failed run trước test. [Evidence](../evidence/ORC-02_LIVE_ACCEPTANCE.md).
3. [SEC-01 — Review secret và bề mặt truy cập](./tasks/SEC-01.md):
   cùng owner Trang rà checklist/log/ports/artifact boundary; docs còn
   **In Progress**. Merge #67 không tự chứng minh mọi acceptance đã đạt.
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

- Ngày 1: cả ba người có thể code/test offline. ThanhTris bắt đầu ORC-06 +
  MLI-01 fixtures; rosy179 bắt đầu MLD-04 + export fixture layout; HoaiTam
  bắt đầu DAT-02 selection/schema + ORC-07 protocol/MLI-02 graph. Chốt version/checksum fixtures trước
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
  dời nghiệm thu toàn tầng sang tuần 6, owner HoaiTam/4h giữ nguyên.
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
`make check-task-status`, `make test-week-5-plan` (12 regression cases, gồm EOF của file mới) và
`make ci-docs`. Các checks này không build/start Docker, không lấy dữ liệu,
không chứng minh runtime/research acceptance. Khi đổi roster/task/effort chính
thức, cập nhật plan/metadata/index và planning checks/tests cùng PR.

- [ ] Mỗi task có branch/PR riêng, assignee đúng, status metadata/Theo dõi/index đồng bộ.
- [ ] ORC-06 proof đủ tháng; moved event có cả tháng cũ/mới; incomplete bị chặn.
- [ ] ORC-07/QA-01 có Airflow run → exact Bronze → Silver → đủ Gold snapshots →
      Trino report → publication cùng identity; mock `published=false`.
- [ ] QA-02/03 có rerun/revision/resume evidence, không mất dữ liệu ngoài scope;
      QA-04 ghi rõ kế hoạch nghiệm thu recovery tuần 6.
- [ ] MLD-04/05 count/grain/feature/checksum/lifecycle đúng; research coverage không bị nói quá.
- [ ] DAT-02 pilot pins/schema/count/checksum/coverage và development-only flags đúng;
      Jupyter inspection run-all đạt, không official lifecycle/import.
- [ ] EXP-01/02 local cùng pilot/config có version; PASSED trước Colab, noise/failure
      không bị ẩn; official result vẫn cần MLD-05 EXPORTED.
- [ ] MLI-03 rejects bundle lỗi trước serving, CANDIDATE không tự APPROVED.
- [ ] Docs/checks/CI đạt; carryover chưa có evidence giữ đúng Review/In Progress.
- [ ] Nếu runtime/coverage trễ: báo blocker và remaining effort, replan tuần 6 thay vì bỏ test.
