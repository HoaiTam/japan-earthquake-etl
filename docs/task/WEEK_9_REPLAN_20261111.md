# Phân công hiện hành — tuần9, 16–22/11/2026

Chốt lại ngày **11/11/2026**, từ main `35582a0`. Chuẩn bị ngay trong
11–15/11, mục tiêu nghiệm thu tuần9. Thay roster tuần7 cho phần việc còn lại;
giữ lịch sử cũ. Bản kế hoạch tuần8–9 trong worktree docs cũ chưa commit không
phải nguồn trạng thái của lần này; không ghi đè/chuyển các edits cũ vào branch này.

## 1. Trạng thái đã rà soát

**Cập nhật12/11:63/81 Done (77,8%),5 Needs Update,12 Backlog,1 In Progress (DAT-03).**
DAT-03 đã tiếp tục [metadata audit](../evidence/DAT-03_LINEAGE_FRONTIER_20261112.md);
chưa official integration/export. Quyết định11/11 bên dưới giữ làm lịch sử.
Core:63/73 Done; Stretch:0/8 Done. Còn18task =10Core+8Stretch; không đổi scope
để khớp deadline. Số task không phải tỷ lệ thời gian hoặc chất lượng khoa học.
[Đối soát PR và quyết định từng task](../evidence/TASK_STATUS_AUDIT_20261111.md).

- COL-03 Done: #117 đã merge và phạm vi pilot đã review đạt.
- DAT-03, EXP-02/03/04, MLI-04: đổi từ In Progress sang Needs Update theo yêu cầu
  owner cho phần implementation đã có nhưng còn thiếu acceptance. Không coi
  task từng Done toàn bộ. Bắt đầu làm lại phần thiếu thì chuyển In Progress.
- SEC-01 giữ Needs Update; #119 còn Draft. Không merge PR thay nghiệm thu cloud.
- Backlog không đổi thành In Progress chỉ vì đã có assignee/lịch.
- Reviewer không tự gán; mỗi task vẫn giữ hard dependencies và effort lịch sử.

## 2. Ba khối, mỗi người6task

Giờ bên dưới là **budget dự kiến phần còn làm**, không phải runtime máy hoặc
cam kết hoàn tất full-data. Mỗi người khoảng28–32h gồm review/buffer. DAT-03 có
runtime/capacity/lineage chưa chốt: nếu vượt budget phải báo lại, không hạ gate.

### Khối1 — Dữ liệu, bảo mật và release

Owner: **HoaiTam**.

| Task | Status | Làm phần gì / dùng để làm gì | Bàn giao / nghiệm thu |
|---|---|---|---|
| [DAT-03](./tasks/DAT-03.md) | In Progress | Lineage → catalog tích hợp → official export. Đối chiếu bytes/SHA tầng tiếp theo của Trí/Trang; không chạy lại ETL đã accepted. | Catalog thống nhất có boundary/reconciliation, Gold/Trino pins; REPRODUCTION/EXTENSION EXPORTED, loader/rerun và receipts ba máy. |
| [SEC-01](./tasks/SEC-01.md) | Needs Update | Bảo mật Drive/Colab. Tiếp tục PR #119; scan exact notebooks/output đã có, kiểm ACL và xác nhận roster riêng. | Hết finding/pending trong scope; actual session/recipient evidence ba người, không suy quyền nhận từ CI hoặc tên file. |
| [MLQ-01](./tasks/MLQ-01.md) | Backlog | Nghiệm thu ML E2E. Viết test plan, negative fixtures và reconciliation queries ngay. | Sau hard dependencies: truy vết Gold → dataset → experiment → import/report; reject bundle lỗi và rerun không duplicate. |
| [OPS-01](./tasks/OPS-01.md) | Backlog | Backup/restore. Inventory metadata và chuẩn bị restore procedure trong namespace thử. | Restore đọc được Gold và ML lineage/registry; backup chia sẻ không chứa secret, không restore đè deployment gốc. |
| [QA-05](./tasks/QA-05.md) | Backlog | Profile tài nguyên. Chọn volume đại diện, định nghĩa phép đo lúc runtime rảnh. | Duration/RAM/disk/query và before/after nếu tối ưu; run không OOM và đạt cửa sổ vận hành đã chốt. |
| [DEMO-02](./tasks/DEMO-02.md) | Backlog | Rehearsal/release candidate. Chuẩn bị checklist, phân vai và phương án demo offline. | Sau hard dependencies: ba người rehearsal 15–20 phút, chốt blocker/known limits; tag/publish cần quyền riêng. |

Budget tham khảo: DAT-03 12h, SEC-01 4h, MLQ-01 4h, OPS-01 3h, QA-05 3h, DEMO-02 2h; buffer4h.

### Khối2 — Baseline, extension và dashboard

Owner: **rosy179**.

| Task | Status | Làm phần gì / dùng để làm gì | Bàn giao / nghiệm thu |
|---|---|---|---|
| [EXP-02](./tasks/EXP-02.md) | Needs Update | Baseline official. Tái dùng code/local–Colab parity đã merge #114; khóa runner/config theo release. | WINDOW/DBSCAN/HDBSCAN global per-mainshock trên REPRODUCTION EXPORTED; artifact/pins/counts/environment thật, giữ failures. |
| [EXP-05](./tasks/EXP-05.md) | Backlog | Extension 2018–2023. Chuẩn bị runner/comparison schema bằng fixture, chưa tune extension. | Sau EXP-04 freeze: bốn thuật toán trên EXTENSION, cùng rule đã khóa; report reproduction-vs-extension có reasons/drift/sample size. |
| [BI-02](./tasks/BI-02.md) | Backlog | Semantic model/KPI. Thiết kế date table/relationships/DAX theo CON-03 và sample schema từ BI-01. | Model/KPI khớp Gold và SQL cùng filter; null không ép 0, không double count. |
| [BI-03](./tasks/BI-03.md) | Backlog | Dashboard tổng quan. Chuẩn bị layout bằng sample/fixture trong lúc model chưa xong. | Sau BI-02: trang Overview hoạt động, số liệu/units/freshness truy về query/pins thật. |
| [BI-04](./tasks/BI-04.md) | Backlog | Dashboard chi tiết. Chuẩn bị map/depth/magnitude và filter layout theo schema thống nhất. | Sau BI-02: trang không gian/độ sâu–magnitude đúng units, giữ Offshore/Unknown và filter context. |
| [BI-05](./tasks/BI-05.md) | Backlog | Refresh/SQL parity BI. Chuẩn bị query kiểm chứng và refresh checklist. | Sau BI-01/02: refresh chỉ dữ liệu Published, KPI khớp Trino cùng filters/pins; actual Power BI evidence. |

Budget tham khảo: EXP-02 8h, EXP-05 5h, BI-02 4h, BI-03 3h, BI-04 3h, BI-05 3h; buffer4h.

### Khối3 — Adaptive, đánh giá và serving

Owner: **ThanhTris**.

| Task | Status | Làm phần gì / dùng để làm gì | Bàn giao / nghiệm thu |
|---|---|---|---|
| [EXP-03](./tasks/EXP-03.md) | Needs Update | Adaptive và multi-sequence. Tái dùng #113/#116; bổ sung actual Colab notebook/output/session cho SEC-01 sau local gate. | Sau EXP-02 official: adaptive REPRODUCTION bundle, raw/resolved membership, ambiguous/nested counts; khóa config trước extension. |
| [EXP-04](./tasks/EXP-04.md) | Needs Update | Đánh giá khoa học. Tái dùng evaluation harness/pilot #121 và COL-03 #117; chuẩn bị evaluator/config. | Sau EXP-02/03 official: DBCV/noise/Omori/stability với sample/failure/N/A reasons; freeze rule trước EXP-05, không kết luận từ pilot. |
| [MLI-04](./tasks/MLI-04.md) | Needs Update | Trino/static report official. Tái dùng views/report/tests đã merge #124, không viết lại template. | Sau EXP-04 và actual import: đọc đúng dataset/run/snapshot, đối soát SQL với report; chưa đủ evidence phải PENDING. |
| [GLD-02](./tasks/GLD-02.md) | Backlog | Aggregate dashboard. Thiết kế aggregate theo fact/date/region/bands và fixture tests. | Aggregate khớp fact cùng filters, không double count từ source bridge; không làm hard dependency giả cho ML. |
| [BI-01](./tasks/BI-01.md) | Backlog | Power BI/Trino connectivity. Xác nhận máy Windows/Power BI/ODBC phù hợp; bàn giao sample schema sớm cho BI-02. | Typed Power Query Import từ Trino chạy thật; freshness/pins và runbook không credential, không đọc Parquet trực tiếp. |
| [DEMO-01](./tasks/DEMO-01.md) | Backlog | Kịch bản demo. Soạn luồng source → lakehouse → ML → report và fallback có nhãn rõ. | Sau QA-01/MLQ-01/MLI-04: demo package với exact runs/query/pins, không chạy lại toàn catalog trên sân khấu. |

Budget tham khảo: EXP-03 6h, EXP-04 6h, MLI-04 4h, GLD-02 3h, BI-01 4h, DEMO-01 3h; buffer4h.

## 3. Nhóm mở đường và giảm thời gian chờ

1. **HoaiTam — DAT-03:** đọc gói lineage Trí, xác minh trusted SHA; chốt danh sách
   bytes còn thiếu của hai máy. Coverage288 tháng rời không thay integrated Gold.
   Capacity/profile trước import; checkpoint từng batch, không ETL replay mù.
2. **ThanhTris / rosy179:** cấp đúng original metadata/payload tham chiếu theo
   requirements, không gửi credential. Ba máy tự verify cùng official release;
   receipt Colab không thay receipt máy riêng, không tạo hộ.
3. **SEC-01 làm ngay:** HoaiTam audit artifacts sẵn có; Trang gửi actual notebook/
   output đã có, Trí bổ sung actual Colab session sau local gate. Owner xác nhận
   roster ở kênh riêng, không tự đổi ACL. Không phải đợi DAT-03 Done mới audit pilot.
4. **ThanhTris — BI-01:** xác nhận máy/Power BI/ODBC và gửi schema sample sớm;
   **rosy179 — BI-02** thiết kế model/DAX bằng contract/sample ngay. Layout BI-03/04
   làm trước; nghiệm thu cần model/actual data. GLD-02 không chặn ML.
5. **MLQ-01/DEMO-01/02:** chuẩn bị plan/fallback ngay, chưa nghiệm thu khi hard
   dependency chưa đạt. OPS-01 namespace thử và QA-05 đo lúc runtime rảnh.

## 4. Lịch và cổng nghiệm thu

| Mốc | Kết quả cần có | Nếu chưa đủ |
|---|---|---|
| 11–15/11 | Lineage gaps, dataset readiness/ETA; actual Colab evidence, BI environment/schema | Gửi danh sách thiếu theo owner; tiếp tục fixture/runner/report, không nhận pilot là official |
| 16–18/11 | DAT-03 official release/receipts; reproduction baseline/adaptive/evaluation theo gate | Điều chỉnh ETA theo actual capacity, không tự bỏ windows hoặc sửa lifecycle |
| 19–20/11 | Freeze → extension → import/report/Trino parity; security, restore/profile và BI checks | Giữ trạng thái thật và nêu task blocker; không lấy SQL fixture làm live acceptance |
| 21–22/11 | E2E, demo/rehearsal, docs và release checklist | Chưa đủ18task thì không tuyên bố toàn project Done |

Official chain giữ nguyên:
**DAT-03/MLD-05 → EXP-02 → EXP-03 → EXP-04 → EXP-05**.
MLI-03 component đã Done: tái dùng importer, không viết importer mới hoặc nhập
pilot vào official `ml.*`. MLI-04 cần EXP-04 và actual import/readback;
MLQ-01 cần QA-01/MLI-02/MLI-04/EXP-05; DEMO-01 và DEMO-02 giữ hard gates riêng.
Chuẩn bị độc lập được phép, nghiệm thu không được bỏ dependency.

## 5. Setup và hướng dẫn agent

1. Đọc AGENTS, task riêng và Hard dependency; tạo branch task từ main mới nhất.
   Đừng mang lại các commits đã merge #117/#124 hoặc dùng branch stale để làm mới.
   SEC-01 nếu tiếp tục #119 thì kiểm nhánh đó với main, không tạo PR chứa lại code.
2. [Onboarding local](../runbooks/DOC-01_TEAM_ONBOARDING.md),
   [setup Jupyter/Colab](../runbooks/WEEK_7_COLAB_SETUP.md),
   [dataset dùng chung](../runbooks/DAT-03_SHARED_DATASET.md),
   [official export](../runbooks/DAT-03_RESEARCH_ML_EXPORT.md).
   Pin release/config/code và checksum; local gate trước cloud, không dùng pilot
   279rows/2mainshock để mở official lifecycle. Dataset readonly, output riêng.
3. Chạy `make help` để chọn lệnh có thật. Không tự build/start Docker để chạy
   test offline; không chạy Spark fixture suites song song ETL thật. Vượt
   capacity hoặc lỗi không xác định: giữ checkpoint, chẩn đoán rồi báo owner.
4. Power BI cần môi trường/driver thật tương thích và endpoint Trino được phép;
   thiếu môi trường thì báo ngay, SQL-only/screenshots không thay Power BI acceptance.
5. Bàn giao source/tests, docs và evidence nhỏ gồm dataset/run/config/SHA,
   counts/reasons/runtime. Không commit ZIP/Parquet/.env/token/private logs.
   Chạy test theo impact, `make check-docs`, `make check-task-status`, `git diff --check`.
6. Không tự bật lịch/automation, đổi cron/ACL/credentials, cleanup warehouse hoặc
   release lease của run khác. Lịch này không tự cấp quyền ETL/upload/deploy/tag.
7. Khi thực sự nhận làm: Needs Update/Backlog → In Progress, đồng bộ task/index;
   đủ acceptance → Done. Không giữ Needs Update cho một phiên đang thực hiện.

**COL-03 không giao lại:** đã Done, chỉ bảo trì nếu upstream thay contract.
DOC-01 giữ Done và bảo trì theo từng PR, không tạo task mới hoặc sửa effort cũ.

