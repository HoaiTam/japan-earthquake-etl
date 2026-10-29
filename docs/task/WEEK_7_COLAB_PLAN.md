# Tuần 7 — Thực nghiệm pilot trên Google Colab

Lịch: **02–08/11/2026**, Asia/Ho_Chi_Minh; chốt phân công ngày 29/10/2026.
Đây là kế hoạch hiện hành, thay phân công tuần 5 cho các đầu việc bên dưới.
Không đổi lịch sử nghiệm thu hoặc tự cho rằng task đã được thực hiện.

## 1. Mục tiêu và đầu vào

Cả ba người dùng **cùng pilot JMA thật trên Drive**, chạy notebook trên Colab
và có bảng so sánh Window/DBSCAN/HDBSCAN global/adaptive ban đầu. Đây là
clustering hồi cứu theo từng mainshock, không dự đoán động đất hoặc mô hình sinh.

- Pilot: **279 dòng ở grain mainshock/candidate, 2 mainshock**, không phải
  279 động đất độc lập. Một window self-only; coverage PARTIAL, khoảng 14 ngày.
- Giữ `DEVELOPMENT_ONLY`, `research_ready=false`; không import kết quả pilot
  vào official `ml.*`, không phát `_SUCCESS.json` hoặc scientific approval.
- [Setup đầy đủ](../runbooks/WEEK_7_COLAB_SETUP.md): link tải, pins, local gate,
  Drive, thư viện, loader, lệnh chạy, lưu output và xử lý lỗi.
- Local baseline đã có bằng chứng fresh kernel trong
  [DOC-01 reproduction](../evidence/DOC-01-reproduction.json). Đây không phải
  Colab evidence; nếu không nhận được exact local smoke report gắn đúng
  pilot/config/code thì tự chạy lại local, không tạo JSON PASSED bằng tay.

## 2. Cách đọc status

`New`/`Needs Update` trong cột **loại việc tuần này** mô tả nguồn công việc,
không thay enum status repository. `New` bắt đầu `Backlog`; task cũ chưa làm
vẫn `Backlog`, không gọi là `Needs Update` một cách sai nghĩa.
`In Progress` chỉ khi thực sự bắt đầu, `Done` chỉ khi đủ acceptance của toàn task.

EXP-01 mở lại `Needs Update` cho setup cloud thật; local acceptance cũ vẫn giữ.
DOC-01 giữ nghiệm thu tài liệu; phân công không tự chứng minh cloud đã chạy.

## 3. Phân công để chụp/gửi nhóm

Giờ bên dưới là **ước lượng phần còn làm tuần này**, không cộng lại toàn bộ
`effort_hours` lịch sử. Dự kiến 18h/người gồm review/buffer; không là SLA.

| Người / khối | Task | Loại việc tuần này | Status lúc giao | Làm gì / dùng để làm gì | Giờ |
|---|---|---|---|---|---:|
| HoaiTam — 1 | [DAT-03](./tasks/DAT-03.md) | Cũ — Needs Update phần handoff | In Progress | Chốt release pilot/pins; nhận receipt riêng của ba người | 3 |
| HoaiTam — 1 | [EXP-01](./tasks/EXP-01.md) | Cũ — Needs Update cloud setup | Needs Update | Chuẩn hóa setup CPU, lock, gate và loader Colab dùng chung | 4 |
| HoaiTam — 1 | [COL-01](./tasks/COL-01.md) | New | Backlog | EDA, grain, null/trùng và coverage; giải thích đúng input | 3 |
| HoaiTam — 1 | [DOC-01](./tasks/DOC-01.md) | Cũ — bổ sung hướng dẫn | Done — scope tài liệu hiện có | Cập nhật lệnh/expected output theo các PR cloud thực tế | 3 |
| HoaiTam — 1 | [SEC-01](./tasks/SEC-01.md) | Cũ — Needs Update cloud audit | Needs Update | Kiểm quyền nhận, notebook/session và output không chứa secret | 3 |
| rosy179 — 2 | [EXP-02](./tasks/EXP-02.md) | Cũ — hoàn thiện Colab parity | In Progress | Chạy ba baseline cùng input/config; so với local | 6 |
| rosy179 — 2 | [COL-02](./tasks/COL-02.md) | New | Backlog | Sweep tham số nhỏ có version, không sửa feature gốc | 4 |
| rosy179 — 2 | [COL-03](./tasks/COL-03.md) | New | Backlog | Bảng metrics baseline/plots và lý do N/A/failure | 4 |
| ThanhTris — 3 | [EXP-03](./tasks/EXP-03.md) | Cũ — chuyển owner, hoàn thiện | In Progress | Adaptive/multi-sequence; fresh kernel và output thật | 5 |
| ThanhTris — 3 | [COL-04](./tasks/COL-04.md) | New | Backlog | Test ít mẫu/noise/overlap/tie và kiểm rerun | 3 |
| ThanhTris — 3 | [EXP-04](./tasks/EXP-04.md) | Cũ — chưa làm, bắt đầu phần pilot | Backlog | Stability/physical-consistency harness, chưa scientific conclusion | 4 |
| ThanhTris — 3 | [MLI-04](./tasks/MLI-04.md) | Cũ — chưa làm, chuẩn bị template | Backlog | Report tĩnh development; SQL tests dùng fixture riêng | 3 |

- HoaiTam: 16h công việc + 2h review/buffer.
- rosy179: 14h công việc + 4h review/buffer.
- ThanhTris: 15h công việc + 3h review/buffer.
- **12 task được giao**: 8 task cũ + 4 task COL mới; EXP-04 chỉ có một owner
  là ThanhTris. rosy179 cung cấp baseline metrics qua COL-03, không cùng sửa
  evaluation module để tránh xung đột. Reviewer vẫn `unassigned` đến khi có
  người thật nhận review; không ghi approval trước.

## 4. Hướng dẫn khối 1 — HoaiTam

1. Đọc DAT-03/EXP-01/SEC-01 và setup chung. Chốt pilot ZIP/handoff/feature SHA,
   commit code và config SHA; không chọn partial catalog ZIP làm feature bundle.
2. Mỗi người tự tải/verify. Receipt Colab ghi nhận session Colab của người đó;
   **không thay receipt từ máy riêng** còn yêu cầu trong DAT-03. Không tạo hộ.
3. EXP-01 bổ sung bootstrap notebook/adapter dùng lại shared modules; reject
   path sai trước fit, không synthetic fallback khi chạy pilot thật. Ghi Python/
   packages thực tế, CPU và local report nguồn; xử lý conflict có version.
4. COL-01 kiểm dữ liệu và viết giải thích units/scaling/missing coverage.
5. SEC-01 kiểm quyền đúng folder/release/output và actual Colab session; mount
   Drive có quyền rộng, không mô tả là sandbox chỉ đọc folder nhóm. Không tự
   đổi ACL, public link, đọc tài khoản ngoài project hoặc lưu token vào notebook.
6. DOC-01 cập nhật hướng dẫn theo implementation cloud mới; ghi việc nào đã
   thử/việc nào chưa. Không làm lại 288 tháng DAT-03 trong budget pilot tuần này.

**Bàn giao:** bootstrap/EDA notebook sạch, setup/environment report, pins,
receipt thật từng người, checklist bảo mật và docs. Những gì chưa đủ vẫn giữ
DAT-03 In Progress/SEC-01 Needs Update; thiếu receipt thì nêu đúng người.

## 5. Hướng dẫn khối 2 — rosy179

1. Bắt đầu ngay từ loader/code/config trên main đã merge và cùng ZIP pilot;
   không đợi notebook EDA của HoaiTam. Cài setup chung, kiểm local gate.
2. EXP-02 chạy WINDOW/DBSCAN/HDBSCAN_GLOBAL **per-mainshock**, dùng cấu hình
   mặc định trước. Giữ một output directory mới cho từng algorithm/run.
3. Đối soát 279 input grains/2 summaries mỗi algorithm, roles, noise và
   sequence memberships; so local–Colab theo tolerance đã công bố. Cluster ID
   có thể đổi nhãn; so tập membership thay vì raw số label.
4. COL-02 chạy sweep giới hạn và ghi config SHA/version riêng; không ghi đè
   default config hoặc chọn tham số theo extension/nhìn plot. Không gọi kết
   quả pilot là cấu hình tốt nhất cho catalog đầy đủ.
5. COL-03 tạo bảng/plots từ artifacts, không nhập số tay. Window không có
   DBCV theo semantics density thì N/A + reason; runtime Colab chỉ là số đo
   của session, không benchmark công bằng nếu hardware khác.

**Bàn giao:** notebook baseline/sweep, parity report, config matrix, memberships/
summaries/manifests, metrics và plots. EXP-02 còn official acceptance cần
DAT-03/MLD-05 EXPORTED, nên Colab pilot đạt chưa đủ chuyển toàn task Done.

## 6. Hướng dẫn khối 3 — ThanhTris

1. Đọc EXP-03 và resolution rules hiện có; tiếp nhận code đã merge từ rosy179,
   không viết pipeline adaptive khác. Bắt đầu fixture/unit tests ngay, không
   đợi khối 2. Run Colab adaptive trên pilot thật sau local baseline gate và
   fresh-kernel smoke adaptive bằng exact code/input (xem setup).
2. EXP-03 chạy adaptive, giữ raw memberships theo mỗi window; báo before/after
   unique candidate counts, tie/ambiguous/nested evidence. Noise hợp lệ không
   được chữa bằng chọn cluster gần mainshock nhất.
3. COL-04 bổ sung test biên bằng fixture nhỏ có nhãn fixture rõ. Không gọi
   synthetic fixture là real-pilot run hoặc sửa bundle pilot để tạo tình huống.
4. EXP-04 làm stability harness/metric contracts từ fixtures trước; khi có
   baseline artifacts cùng pilot/config thì ghép so sánh thật. Giữ các window
   thất bại trong mẫu số; Omori thiếu mẫu báo N/A, không ép fit.
5. MLI-04 chuẩn bị static-report template tách development/official. Pilot
   report không query production hoặc import vào `ml.*`; SQL smoke chỉ trên
   fixtures có nhãn rõ. Official query/count-vs-Trino còn là acceptance sau.

**Bàn giao:** adaptive notebook fresh kernel, boundary tests, stability report,
static-report source/output mẫu. EXP-03/04/MLI-04 chỉ Done khi đủ scope gốc;
fixture/template/pilot không thay nghiên cứu chính thức hoặc SQL readback thật.

## 7. Trình tự tuần và giảm phụ thuộc

- **Ngày 1:** cả ba tự setup/verify cùng pins. HoaiTam kiểm môi trường/ACL;
  rosy179 chạy default baseline; ThanhTris chạy adaptive fixtures/local smoke.
- **Ngày 2–3:** ba nhánh làm độc lập. Chỉ thống nhất giao diện bằng manifest,
  không cùng ghi một notebook/config hoặc output directory.
- **Ngày 4–5:** rosy179 gửi baseline artifacts + checksum; ThanhTris đọc exact
  artifacts để so sánh, HoaiTam review setup/receipts và cập nhật runbook.
- **Cuối tuần:** cả ba tự Restart session → Run all, demo 5–10 phút/khối,
  kiểm failure/noise/N/A và bàn giao PR + evidence có thể tái lập.

Mỗi task một branch/PR nhỏ từ main mới nhất; không push trực tiếp main.
Không bắt buộc mở Docker, Airflow hoặc chạy lại ETL để fit pilot.
Local gate cũ bị thiếu hoặc code/model/config đã đổi: rerun local cho scope đó;
không bỏ gate bằng cách tự gán environment flag/report PASSED trong Colab.

## 8. Giao diện artifact và nghiệm thu

- Dùng writer/schema hiện có: `memberships.parquet`, `sequence_summary.parquet`,
  `experiment_config.json`, `experiment_metrics.json`, `pilot_result_manifest.json`.
- Kèm pilot ID, feature/checksum-index SHA, full commit, config SHA, Python/
  package versions, recipient, runtime kind, UTC timestamp, seed nếu áp dụng.
- Hướng dẫn setup có lệnh thật; file notebook/module của task mới bên trên
  là **deliverable phải triển khai**, chưa tồn tại và không giả lệnh Make.
- Output lưu `outputs/<recipient>/<task>/<session-id>/` trên Drive sau verify;
  input giữ nguyên. Không sync/delete folder nguồn hoặc dùng chung run ID.
- Git chỉ source notebook sạch/config/tests/evidence metadata nhỏ, không đưa
  ZIP/data/token/raw private logs vào PR. Notebook chia sẻ có thể chứa outputs:
  xóa output nhạy cảm trước khi chia sẻ hoặc commit.

Bốn task COL mới có tổng estimate **14h**; backlog thành **81 task/416h**,
Core 73/Stretch 8. Đây là tổng estimate lịch sử + task mới, **không phải** giờ
còn làm toàn project. Budget tuần **54h** tính phần còn làm và buffer riêng,
không cộng đôi 14h COL vào 54h.
