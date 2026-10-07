---
task_id: "JMA-05"
status: "Done"
week: 4
block: "C - JMA Bronze"
workstream: "QA"
scope: "Core"
priority: "P0"
effort_hours: 4
assignee: "HoaiTam"
reviewer: "unassigned"
dependencies: ["JMA-04"]
---

# JMA-05 - Kiểm thử JMA đến Bronze

## Mục đích

Dùng làm mốc BronzeReady của JMA trước khi chạy parser và historical backfill lớn.

## Phạm vi công việc

Test archive hợp lệ, ZIP hỏng, sai record length, file thay đổi, resume và integration run cho một vài năm đại diện.

## Thành phần cần có

- **Đầu vào và contract:** [JMA-04](./JMA-04.md)
- **Phần triển khai:** Test archive hợp lệ, ZIP hỏng, sai record length, file thay đổi, resume và integration run cho một vài năm đại diện.
- **Kết quả bàn giao:** JMA unit/integration tests và evidence theo year/run_id.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- JMA unit/integration tests và evidence theo year/run_id.
- [QA runbook](../../specs/JMA_BRONZE_QA.md), [evidence live](../../evidence/JMA-05.md)
  và [exact readback report](../../evidence/JMA-05-live-readback.json).
- Makefile `test-jma-qa` (offline) và `smoke-jma-live` (DAG/Java17/MinIO thật).

## Tiêu chí hoàn thành

- [x] Test offline dùng fixture.
- [x] manifest, checksum, version và record count đối soát được.

## Hard dependency

- [JMA-04](./JMA-04.md)

## Cách triển khai và phối hợp

### Phân công tuần 4

- **Owner / effort:** HoaiTam, 4h Core; xem [kế hoạch tuần 4](../WEEK_4_PARALLEL_PLAN.md).
- **Nhóm:** [Mở đường — làm trước](../WEEK_4_PREP_GROUP.md), bước 2/3 sau JMA-04; bàn giao input thật cho ba khối triển khai.
- **Đã triển khai:** JMA-04 Done; test plan/error suite, readback harness và live preview/ingest/rerun 4 archive đã đạt. Không dùng mock để thay gate live.
- **Làm gì / có gì:** Kiểm thử workflow JMA-04 với archive hợp lệ/hỏng, revision, retry/rerun; report year/segment/release/run, manifest/raw checksum/count và readback.
- **Dùng để làm gì:** Gate JMA BronzeReady thật trước Silver và historical backfill lớn, không chỉ xác nhận validator mock.
- **Input / handoff:** Dùng sample JMA DAT-01 và exact manifest do workflow/writer tạo; bổ sung archive 2000 thuộc reproduction cho ML pilot, ghi metadata/evidence riêng, không đổi catalog DAT-01 đang khóa hai entry. Bàn giao BronzeReady evidence cho SLV-09; không đổi STAGED_SOURCE thành Ready chỉ bằng sửa catalog metadata.
- **Nghiệm thu:** Chỉ Done khi JMA-04 đã đạt và run thật/readback/rerun đối soát được trên các năm đại diện 2000, 2023 và 1997 (hai segment riêng). Không bịa count archive 2000; test offline bao phủ case lỗi, full 40 năm không thuộc task QA này. Một năm reproduction chỉ là pilot, chưa phải dataset/Mc đầy đủ giai đoạn nghiên cứu.

Đây là integration gate. Có thể chuẩn bị test plan, fixture và harness trước, nhưng chỉ được chuyển sang `Done` sau khi toàn bộ hard dependency cung cấp output thật và báo cáo đối soát đạt.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới từ `main`: `feat/jma-05-kiem-thu-jma-en-bronze`.
3. Triển khai đúng phạm vi; dùng fixture nhỏ, xác định được và không phụ thuộc mạng cho unit test.
4. Chạy test/check phù hợp, đối chiếu acceptance criteria và cập nhật tài liệu nếu contract hoặc hành vi thay đổi.
5. Cập nhật `status`, `assignee`, `reviewer` và Evidence ngay trong file này khi mở PR hoặc hoàn tất review.

## Ranh giới

- Không tự mở rộng sang deliverable của task khác.
- Không đổi contract upstream trong PR implementation mà không cập nhật task contract liên quan và có review.
- Không commit secret, credential, payload nhạy cảm, data dump lớn hoặc artifact build không cần thiết.
- Không đánh dấu `Done` nếu chưa có evidence kiểm tra được.

## Theo dõi

- **Trạng thái:** Done
- **Assignee:** HoaiTam
- **Reviewer:** Chưa ghi lại
- **Branch / baseline:** `feat/jma-05-bronze-integration-qa` từ `main` `7e8d9d3`
  (merge PR #41); chưa commit/push/mở PR. `.env` và `.metals/` giữ nguyên.
- **Offline evidence 2026-10-07:** `make test-jma-qa` đạt 46/46 Java +
  26/26 Python tests; full `make check` đạt 162/162 Java + 41/41 Airflow,
  73 task status và tất cả static contract/config/Compose/foundation checks.
  Host JDK 21.0.11, compiler `--release 17`; image build/QA thật Java17.0.19.
- **Live evidence:** `make verify-samples` đạt hai DAT-01 identities;
  `make smoke-jma-live` đạt preview/first/rerun, lần cuối evidence ID
  `20261007T085218Z-06a45f424022`. Cả 4 mapped tasks + summary/gate success;
  readback/rerun SHA raw/manifest, release, bytes/count và original publication
  identity khớp; download/publication reuse=true, pause state đã khôi phục.
  Xem [evidence/handoff](../../evidence/JMA-05.md).
- **Counts thật:** 1997 jan-sep 39,951; oct-dec 16,284; 2000 109,967;
  2023 257,020 structural records. DAT-01 vẫn hai entries, JMA STAGED_SOURCE;
  BronzeReady là writer manifest mới, không sửa sample metadata.
- **Giới hạn / task tiếp theo:** [SLV-09 - Tích hợp và kiểm thử Silver đa nguồn](./SLV-09.md)
  còn ghép parser/quality/dedup/link/SilverReady thật; [QA-03 - Kiểm thử JMA
  historical backfill](./QA-03.md) còn historical flow rộng hơn; [MLD-02 - Audit
  Gold và xác định magnitude of completeness](./MLD-02.md) còn Gold audit/Mc
  theo coverage. Sample 2000 chỉ pilot, không full research dataset. Revision/
  resume/corruption test offline; không phá nguồn/object live. Reviewer độc
  lập chưa ghi nhận, giữ `unassigned`.
- **Kỹ năng phù hợp:** Fixture testing, Airflow, MinIO

## Checklist bàn giao

- [x] Deliverable đã có trong repository hoặc môi trường demo.
- [x] Acceptance criteria đã được kiểm tra.
- [x] Test tự động đạt hoặc có evidence thủ công có thể lặp lại.
- [x] Tài liệu/contract đã cập nhật nếu schema, flow, cấu hình hoặc hành vi thay đổi.
- [x] Không chứa secret, dữ liệu nhạy cảm hoặc file build không cần thiết.
- [ ] P0/P1 có reviewer khác assignee xác nhận.

## Tài liệu liên quan

- [JMA Bronze QA](../../specs/JMA_BRONZE_QA.md)
- [Live evidence / exact input handoff](../../evidence/JMA-05.md)

- [Phân công tuần 4](../WEEK_4_PARALLEL_PLAN.md)
- [Nhóm mở đường tuần 4](../WEEK_4_PREP_GROUP.md)
- [Shared real samples](../../specs/SHARED_REAL_SAMPLE_DATA.md)
- [JMA writer/handoff](../../specs/JMA_BRONZE_WRITER_CONTRACT.md)
- [Kế hoạch 8 tuần](../README.md)
- [Các khối công việc](../WORK_BLOCKS.md)
- [Baseline MVP, KPI và Definition of Done](../../specs/MVP_SCOPE_KPI_AND_DOD.md)
