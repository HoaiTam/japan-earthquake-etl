# Kế hoạch tuần 4 mở rộng — nhóm mở đường và ba khối triển khai

Phân công này thay bản 8 task Core trước đó theo yêu cầu tăng khối lượng tuần 4.
Baseline code là `main` sau PR #38 (`a73e599`, 2026-10-07). Đây là kế hoạch,
không phải evidence implementation hoặc nghiệm thu đã đạt.

## 1. Tổng tải và điều chỉnh lịch

- **18 task Core / 89 giờ task**, thêm **7 giờ review/tích hợp = 96 giờ** dự kiến.
- Tải cả tuần: HoaiTam **30h**, ThanhTris **29h**, Trang **30h** task; sau buffer
  mỗi người khoảng **32h**. Đây là phương án tăng tốc, không còn giới hạn 15h
  của kế hoạch sơ bộ. Nếu capacity thực tế thấp hơn, ưu tiên nhóm mở đường và
  Core ETL/Gold; không giảm test/acceptance hoặc nhận mock là evidence thật.
- **13h nhóm mở đường đã nằm trong 30h của HoaiTam**, không tính ngoài tuần
  hoặc cộng thêm một lần vào capacity.
- Giữ nguyên effort từng task, 73 task / 364h toàn backlog. Đưa ORC-01..05,
  MLD-01..03, MLI-01 và SEC-01 lên `week: 4`, đồng bộ task index. Tuần 4 hiện
  có 19 task / 93h nếu cộng GLD-02 Stretch 4h; Stretch không nằm trong cam kết
  89h Core + 7h buffer. Những tuần sau được phân lại sau review tuần 4.
- Chưa kéo EXP-01 vào cam kết Done: hard dependency MLD-05 chưa nằm trong
  tuần này. DOC-01 cũng giữ tuần 6 vì acceptance tái lập dataset/experiment
  không thể được nhận là đạt chỉ nhờ cập nhật docs ETL. Không thêm task hình
  thức hoặc hạ gate để tăng số lượng.

## 2. Nhóm HoaiTam làm trước — 3 task / 13h

Đọc [nhóm mở đường tuần 4](./WEEK_4_PREP_GROUP.md) để có hướng dẫn riêng.

| Thứ tự | Task | Effort | Output mở đường |
|---:|---|---:|---|
| 1 | [JMA-04](./tasks/JMA-04.md) | 5h | Workflow năm/segment, preview, resume và immutable Bronze handoff |
| 2 | [JMA-05](./tasks/JMA-05.md) | 4h | JMA BronzeReady thật, readback/rerun, manifest/SHA/release/count; sample trong reproduction và extension |
| 3 | [ORC-01](./tasks/ORC-01.md) | 4h | Khung DAG ETL, task-group I/O/run context và failure gates bằng contract/mock |

Thứ tự gợi ý **JMA-04 → JMA-05 → ORC-01**; ORC-01 phụ thuộc CON-02/03 đã
Done nên có thể chuẩn bị song song. HoaiTam làm nhóm này trước để bàn giao
input/context rõ rồi chia ba khối bên dưới. Không cần chờ SLV/Gold hoàn tất để
test cấu trúc ORC-01, nhưng mock DAG không phải evidence pipeline Published.

Không đưa SLV-09 vào nhóm mở đường: task đó còn chờ SLV-06/07. Không yêu cầu
HoaiTam làm hết Silver/Gold trước khi hai người còn lại bắt đầu.

## 3. Ba khối sau khi bàn giao

| Thành viên | Task còn lại sau nhóm mở đường | Giờ còn lại | Nhóm mở đường | Tổng task tuần | Buffer | Tổng dự kiến |
|---|---|---:|---:|---:|---:|---:|
| HoaiTam | ORC-02, ORC-03, ORC-04, ORC-05 | 17h | 13h | 30h | 2h | 32h |
| ThanhTris | SLV-06, SLV-07, SLV-09, MLD-02, MLD-03 | 29h | 0h | 29h | 3h | 32h |
| Trang | GLD-01, GLD-03, GLD-04, MLD-01, MLI-01, SEC-01 | 30h | 0h | 30h | 2h | 32h |

### Khối A — HoaiTam: điều phối và vận hành ETL

| Task | Giờ | Làm gì / có gì | Dùng để làm gì |
|---|---:|---|---|
| [ORC-02](./tasks/ORC-02.md) | 4h | Schedule/readiness profile, UTC interval, timezone điều phối, USGS revision overlap và JMA change check | Hai nguồn có nhịp khác nhau nhưng không chạy trùng daily/backfill |
| [ORC-03](./tasks/ORC-03.md) | 5h | Preview UTC/year/release scope, Bronze reuse, retry/reprocess parameters và tests | Chạy lại đúng phạm vi, không duplicate hoặc sửa partition ngoài scope |
| [ORC-04](./tasks/ORC-04.md) | 4h | Structured logs/counts/reasons/duration và summary theo run/source/snapshot | Điều tra lỗi và đối soát toàn ETL mà không in raw/secret |
| [ORC-05](./tasks/ORC-05.md) | 4h | Spark resource/concurrency profile, recovery matrix và retry boundary | Test/backfill trên local không tranh tài nguyên hoặc cần xóa dữ liệu để phục hồi |

ORC-02..05 đều chờ ORC-01 để hoàn tất; sau nhóm mở đường có thể phát triển
độc lập bằng phase/summary mocks. Runtime nghiệm thu vẫn cần adapters và
output thật Silver/Gold, không coi skeleton là daily pipeline chạy thành công.

### Khối B — ThanhTris: Silver current và ML audit/candidate

| Task | Giờ | Làm gì / có gì | Dùng để làm gì |
|---|---:|---|---|
| [SLV-06](./tasks/SLV-06.md) | 5h | Source-local dedup/revision, tie-break, history/current flags và counts | Một current revision/source key, giữ lịch sử và không dedup xuyên nguồn |
| [SLV-07](./tasks/SLV-07.md) | 6h | Versioned candidate matching, reasons/confidence, link/membership và stable canonical ID | Không double count USGS–JMA; ambiguous/unmatched không bị mất |
| [SLV-09](./tasks/SLV-09.md) | 4h | Hai nguồn thật qua parser/quality/dedup/link/publish, readback và reconciliation report | Gate SilverReady cho Gold, không chỉ unit test từng component |
| [MLD-02](./tasks/MLD-02.md) | 7h | Snapshot input audit, histogram/Mc estimator có version, exclusion counts/reasons và pilot | Input nghiên cứu có completeness evidence, không tune bằng extension hoặc xóa Gold |
| [MLD-03](./tasks/MLD-03.md) | 7h | Mainshock/window resolver, range join, candidate grain và resource guards | Giảm catalog thành cửa sổ theo mainshock trước feature/HDBSCAN, chưa gắn nhãn dư chấn |

SLV-06/07 đã đủ dependency để viết unit ngay. MLD-02/03 chuẩn bị histogram,
Gold/candidate fixture và config trước; chỉ chạy/chốt audit thật sau MLD-01
của Trang. Counts của sample 2000/2023 là pilot theo coverage đã ghi, không
được trình bày như Mc/kết quả nghiên cứu đủ toàn reproduction period.

### Khối C — Trang: Gold snapshot, ML identity/contract và security

| Task | Giờ | Làm gì / có gì | Dùng để làm gì |
|---|---:|---|---|
| [GLD-01](./tasks/GLD-01.md) | 6h | Canonical event current/bridge, natural/ROI view, dimensions/bands và provenance | Gold không double count, đủ JMA/source/era/quality fields cho ML |
| [GLD-03](./tasks/GLD-03.md) | 6h | Iceberg schema/write affected scope, commit metadata/snapshot và failure/rerun tests | Snapshot bền vững, pin/time-travel được, không publish bộ output dở |
| [GLD-04](./tasks/GLD-04.md) | 4h | Trino views/queries, blocker report và verify đúng committed snapshot | Gate Gold Published độc lập với Spark, không query nhầm latest |
| [MLD-01](./tasks/MLD-01.md) | 5h | Published snapshot resolver, deterministic dataset ID và reproduction/extension manifests | Audit/candidate cùng đọc snapshot bất biến, không resolve lại current khi rerun |
| [MLI-01](./tasks/MLI-01.md) | 5h | Machine-readable result schema/lifecycle, success/invalid fixtures và validator interface | Khóa giao diện Colab/import sớm; không cần model thật và không tự approve result |
| [SEC-01](./tasks/SEC-01.md) | 4h | Review config/history/logs/ports/prefix permissions và ML artifact leak checks | Không lộ secret/path khi chia sẻ code, runtime evidence hoặc bundle |

GLD-01/MLI-01 có thể làm ngay với contract/fixture. MLD-01 dùng snapshot
metadata giả lập trước, nhưng chỉ Done khi GLD-04 cung cấp snapshot thật.
SEC-01 chuẩn bị checklist trước và chờ ORC-05 để nghiệm thu; thành phần ML
chưa tồn tại phải ghi rõ chưa kiểm tra, không xác nhận an toàn cho notebook
hoặc export tương lai. Nếu acceptance security chưa đạt thì giữ task chưa Done.

**GLD-02 — Trang, 4h Stretch:** aggregate dashboard; chỉ pick sau Core và khi
có capacity bổ sung thực tế. Không lấy 7h buffer để làm BI, không chặn ML.

## 4. Dependency và handoff: chỉ gate thật phải chờ

```text
Nhóm mở đường: JMA-04 → JMA-05 ; ORC-01 → ORC-02/03/04/05
                                  ORC-05 → SEC-01
JMA-05 + component tuần 3 + SLV-06 → SLV-07 → SLV-09 SilverReady
  → GLD-01 input thật → GLD-03 commit → GLD-04 verify/Published
  → MLD-01 pin → MLD-02 audit/Mc → MLD-03 candidate windows
MLI-01 độc lập bằng CON-03/schema fixture, không cần model thật.
```

Đây là thứ tự ghép dữ liệu thật, không phải thứ tự bắt đầu toàn bộ task.
Không xóa hard dependency để tạo cảm giác độc lập. Tại PR phân công có 6
task đã Ready khi lập kế hoạch: JMA-04, SLV-06, SLV-07, GLD-01, ORC-01, MLI-01.
Trạng thái hiện tại theo file task/index: JMA-04 đã Done implementation/offline,
JMA-05 đã Done live QA/readback/rerun, có [input evidence](../evidence/JMA-05.md).
Các task chờ gate còn Backlog; khi thực sự làm fixture/mock/test plan được phép thì ghi In Progress
và dependency còn chờ. Chỉ Done khi acceptance/dependency/tests/docs/evidence
đạt; chưa có reviewer không cho phép bịa approval.

| Producer → consumer | Handoff / ranh giới |
|---|---|
| JMA-04/05 → SLV-09 | Exact manifest/raw key, year/segment/release/SHA/count/run; 1997 hai segment, raw ZIP nguyên bản |
| ORC-01 → ORC-02..05 / các adapters | Run context, phase I/O, dry-run vs real-mode boundary; mocks không mở production publish gate |
| SLV-06/07 → SLV-09/GLD-01 | History/current, link/membership, stable canonical IDs/model version, reconciliation; không canonicalize lần hai |
| SLV-09 → GLD-01/03 | Exact verified Silver output/current/member datasets, report và scope; không wildcard/latest |
| GLD-03/04 → MLD-01 | Table identity, committed/verified snapshot ID, publication metadata và coverage; commit không tự Published |
| MLD-01 → MLD-02/03 | Pinned snapshot + dataset identity/config/split/cutoff; không query current lại hoặc dùng extension tune reproduction |
| MLD-02 → MLD-03 | Versioned Mc/audit report, eligible input và exclusion counts; pilot scope phải được ghi |
| MLI-01 → EXP/MLI downstream | Schema/fixture/checksum/grain/lifecycle đúng CON-03, không triển khai model/import DAG trong task contract |
| ORC-05 → SEC-01 | Runtime resources/recovery profile, endpoints/permissions và logs có scope; không public credential |

Rủi ro baseline phải xử lý trong PR của owner, không sửa ngầm shared contract:

- JMA-04 bổ sung HTTP GET status/type cho result/state downloader. State JMA-02
  cũ thiếu metadata cần GET lại; Range 206 đối soát full archive size, không bịa
  HTTP 200. Workflow/offline acceptance đã có; live evidence ở JMA-05.
- SLV-01 nhận manifest local path: stage exact manifest từ store; không dùng
  `Path.of("s3://...")`. SLV-08 hiện chỉ ghi observations/rejects, không giả
  định link/membership đã persist đủ cho Gold.
- Nhiều bảng Iceberg không mặc nhiên commit atomically; GLD-03/04 phải chốt
  phạm vi bảng/snapshot được verify và không publish một bộ output dở.
- Sample DAT-01 chỉ có JMA 2023 (extension). JMA-05 bổ sung archive 2000 thuộc
  reproduction; catalog DAT-01 vẫn giữ hai entry đã khóa, sample bổ sung ghi
  metadata/evidence riêng trong JMA-05, không sửa checksum/identity cũ.
- MLD-01..03 chưa tạo feature/export: manifest giữ BUILDING cho tới khi gate
  MLD-04/05 đạt. Không fake VALIDATED/EXPORTED hoặc coi window là aftershock.

## 5. Lịch làm, review và nghiệm thu

1. **Mở đường:** HoaiTam làm 3 task của nhóm chuẩn bị, verify catalog USGS/JMA,
   khóa metadata samples và phase I/O. Nếu hai người bắt đầu sớm, họ vẫn viết
   fixture/unit GLD-01, SLV-06/07, MLI-01 mà không phải chờ data thật.
2. **Nửa đầu tuần:** HoaiTam làm ORC-02..05; ThanhTris hoàn tất SLV-06/07 rồi
   ghép SLV-09; Trang làm GLD-01/03, MLI-01 và chuẩn bị GLD-04/MLD-01. Mỗi
   task có branch/PR riêng, merge component đã test, không gộp cả khối.
3. **Nửa sau tuần:** Trang verify/pin Gold; ThanhTris chạy audit/Mc/candidate
   pilot; HoaiTam tích hợp adapters ETL và retry/reprocess; Trang review
   security sau ORC-05. Gate upstream trễ thì gate downstream vẫn chưa Done,
   chuyển sang fixture/mock/issue report, không chạy full 40 năm để che thiếu test.

Buffer review/tích hợp: HoaiTam 2h (review ML/security), ThanhTris 3h (review
Gold/SQL/orchestration), Trang 2h (review Silver). Đây là đề xuất, không phải
reviewer đã nhận hoặc approval; giữ `reviewer: unassigned` tới khi có xác nhận.
Buffer đã bao gồm recovery/hỗ trợ, không cộng thêm vào effort task QA.

Lệnh đã có tại repo root: `make test-contracts`, `make test`, `make check`,
`git diff --check`; operator có `.env` hợp lệ/service chạy dùng
`make verify-samples` để verify DAT-01. Runtime command ingest/dedup/Gold/ML
business của các task chưa triển khai phải do owner bổ sung vào Makefile và
runbook; không dùng smoke hạ tầng làm evidence pipeline Published.

### Checklist cuối tuần

- [ ] Nhóm mở đường bàn giao đủ manifests/checksums/coverage và phase I/O.
- [ ] Task đã đạt có evidence riêng và metadata/Theo dõi/index khớp; task chưa
  đạt giữ đúng status. 18 task là kế hoạch, không phải 18 task Done.
- [ ] Source → Bronze → Silver → Gold verify → pinned dataset/candidate pilot
  trace được run/release/snapshot/model versions bằng dữ liệu thật có phạm vi.
- [ ] Counts đối soát riêng parser/quality rejects, exact duplicate, superseded
  history/current, linked/canonical, ML eligible/excluded/mainshock/candidate.
- [ ] Rerun/revision không duplicate logic hoặc sửa partition ngoài scope;
  Bronze cũ bất biến, failure commit/verify không thành Published.
- [ ] Snapshot pin không trôi; mainshock depth/magnitude/grain/window guards
  đúng CON-03; pilot Mc không được trình bày như audit toàn catalog.
- [ ] Spark Java 17, MinIO/Catalog/Trino runtime có evidence; unit/mock không
  được gọi là runtime smoke. Security issues được ghi/fix có scope.
- [ ] MLI-01 fixtures/lifecycle đúng contract; không fake notebook/model,
  feature export, experiment result hoặc approval chưa có.
- [ ] Không commit raw lớn/credential/.env/build artifact; GLD-02 còn Backlog
  nếu chưa làm. Chốt lại workload và lịch tuần 5/6 theo kết quả thực tế.

## 6. Tài liệu cần đọc

- [Nhóm mở đường](./WEEK_4_PREP_GROUP.md), [task index](./tasks/README.md),
  [các khối công việc](./WORK_BLOCKS.md), [roadmap HDBSCAN](./HDBSCAN_WORKSTREAM.md).
- [Baseline MVP/DoD](../specs/MVP_SCOPE_KPI_AND_DOD.md),
  [Silver/Gold contract](../specs/SILVER_GOLD_DATA_MODEL.md),
  [ML logical model](../specs/ML_DATA_MODEL.md).
- [JMA writer/handoff](../specs/JMA_BRONZE_WRITER_CONTRACT.md),
  [shared samples](../specs/SHARED_REAL_SAMPLE_DATA.md),
  [fixtures](../../tests/fixtures/README.md),
  [Git workflow](../conventions_and_workflow/GIT_WORKFLOW.md).
