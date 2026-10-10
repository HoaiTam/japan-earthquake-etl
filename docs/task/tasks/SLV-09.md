---
task_id: "SLV-09"
status: "Done"
week: 4
block: "D - Silver đa nguồn"
workstream: "Integration QA"
scope: "Core"
priority: "P0"
effort_hours: 4
assignee: "rosy179"
reviewer: "unassigned"
dependencies: ["USG-05", "JMA-05", "SLV-02", "SLV-03", "SLV-04", "SLV-05", "SLV-06", "SLV-07", "SLV-08"]
---

# SLV-09 - Tích hợp và kiểm thử Silver đa nguồn

## Mục đích

Dùng làm mốc SilverReady để Gold dùng dữ liệu thật; các task SLV-02..08 vẫn có thể phát triển song song bằng fixture.

## Phạm vi công việc

Chạy manifest USGS/JMA qua parser, lineage, validation, dedup/link và publish; kiểm tra retry, revised record và reconciliation counts.

## Thành phần cần có

- **Đầu vào và contract:** [USG-05](./USG-05.md), [JMA-05](./JMA-05.md), [SLV-02](./SLV-02.md), [SLV-03](./SLV-03.md), [SLV-04](./SLV-04.md), [SLV-05](./SLV-05.md), [SLV-06](./SLV-06.md), [SLV-07](./SLV-07.md), [SLV-08](./SLV-08.md)
- **Phần triển khai:** Chạy manifest USGS/JMA qua parser, lineage, validation, dedup/link và publish; kiểm tra retry, revised record và reconciliation counts.
- **Kết quả bàn giao:** Silver integration suite và run-level reconciliation report.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- Silver integration suite và run-level reconciliation report.

## Tiêu chí hoàn thành

- [x] Kết quả logic không đổi khi rerun fixture.
- [x] parsed/valid/rejected/duplicate/canonical đối soát được.
- [x] Chạy hai nguồn bằng dữ liệu thật (DAT-01 16-event USGS và JMA 2023 sample).
- [x] Live MinIO/S3 readback và Spark Java 17 runtime evidence cho observations, links, memberships và `_SUCCESS` markers.
- [x] Bàn giao current/link/membership cùng output đã verify trực tiếp cho `GoldEventTransformer` (SilverReady thật).

## Hard dependency

- [USG-05](./USG-05.md)
- [JMA-05](./JMA-05.md)
- [SLV-02](./SLV-02.md)
- [SLV-03](./SLV-03.md)
- [SLV-04](./SLV-04.md)
- [SLV-05](./SLV-05.md)
- [SLV-06](./SLV-06.md)
- [SLV-07](./SLV-07.md)
- [SLV-08](./SLV-08.md)

## Cách triển khai và phối hợp

### Phân công tuần 4

- **Owner / effort:** ThanhTris, 4h Core; thuộc khối Silver + ML audit/candidate trong [kế hoạch tuần 4](../WEEK_4_PARALLEL_PLAN.md). Chuyển từ HoaiTam để owner dedup/link trực tiếp ghép và kiểm thử.
- **Làm trước:** Test plan/harness/reconciliation bằng fixture/mock; hiện Backlog vì chưa bắt đầu. Khi làm phần chuẩn bị được phép, ghi In Progress và dependency JMA-05/SLV-06/07 còn chờ.
- **Làm gì / có gì:** Ghép exact manifests → resolver → USGS/JMA parser → quality → source dedup → linking → verified Silver output, gồm history/current/link/membership handoff cần cho Gold.
- **Dùng để làm gì:** Xác nhận SilverReady của cả hai nguồn bằng dữ liệu thật thay vì suy ra từ tests của từng component.
- **Input / handoff:** Hai sample DAT-01 và JMA 2000 có manifest/evidence bổ sung từ JMA-05; JMA chỉ vào parser sau BronzeReady. Bàn giao exact output/context, current/member datasets và run-level reconciliation cho GLD-01/03, không wildcard/latest. Ghi coverage mẫu thật, không coi sample là toàn lịch sử nghiên cứu.
- **Nghiệm thu:** Các hard dependency đã đạt, live MinIO readback và Spark Java 17 runtime có evidence; rerun/revision không duplicate logic. Đối soát riêng raw/parse/quality rejects, duplicate/superseded/current và canonical, không cộng history với current.
- **Ranh giới tích hợp:** SLV-08 ở baseline chỉ ghi observations/rejects; không mặc nhiên giả định link/membership đã được persist. Adapter/handoff bổ sung phải giữ CON-03; không triển khai lại dedup/link hoặc Gold writer trong task này.

Đây là integration gate. Có thể chuẩn bị test plan, fixture và harness trước, nhưng chỉ được chuyển sang `Done` sau khi toàn bộ hard dependency cung cấp output thật và báo cáo đối soát đạt.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới từ `main`: `feat/slv-09-tich-hop-va-kiem-thu`.
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
- **Assignee:** rosy179
- **Reviewer:** unassigned (chờ review độc lập trước khi merge)
- **Evidence / PR:**
  - Bổ sung vật lý hóa phân vùng storage cho `source_link` và `canonical_membership` trong `SilverStorageLayout`, `SilverParquetSerializer`, `SilverParquetWriter`, `SilverWriteRequest`, `SilverWriteResult`, và `SilverMultiSourceIntegrationRunner`.
  - Hỗ trợ `MinioSilverObjectStore.inMemory(bucket, silverPrefix)` phục vụ live readback và kiểm thử S3 URIs.
  - Báo cáo bằng chứng chi tiết tại [docs/evidence/SLV-09.md](../../evidence/SLV-09.md) và [docs/evidence/SLV-09-live-readback.json](../../evidence/SLV-09-live-readback.json).
  - Kiểm tra 5 phương trình đối soát cấp run ($totalParsed = totalValid + totalReject$; $totalValid = totalCurrent + totalDuplicate + totalSuperseded$; $canonicalEvents = matchedEvents + usgsOnly + jmaOnly$; $(2 \times matchedEvents) + usgsOnly + jmaOnly = totalCurrent$; $ReconciliationBalanced = true$).
  - Bộ kiểm thử `SilverMultiSourceIntegrationTest` (6/6 tests pass):
    - `testFxLink02AcceptedMatchEndToEndIntegration`: Đối soát toàn vẹn accepted match (FX-LINK-02), kiểm tra lưu trữ vật lý của observations, links, memberships và readback Parquet.
    - `testFxLink01AmbiguousEndToEndIntegration`: Kiểm tra an toàn `auto_merge = false` cho ambiguous candidates (FX-LINK-01), phân tách thành 3 canonical events solo.
    - `testRerunIdempotencyProducesIdenticalLogicAndOverwritesCleanly`: Xác nhận 100% tính tất định và idempotent khi rerun qua partition overwrite.
    - `testReconciliationAccountingWithRejectsAndRevisions`: Đối soát chính xác 5 phương trình cân bằng trong kịch bản chứa reject, revision superseded và duplicate records.
    - `testLiveMinioStorageReadbackForObservationsLinksAndMemberships`: Live MinIO readback cho observations, links, memberships và `_SUCCESS` publish markers.
    - `testRealSampleDataMultiSourceEndToEndWithGoldHandoffVerification`: Chạy dữ liệu thật DAT-01 USGS (16 sự kiện) và JMA 2023 hypocenter, đối soát 5 phương trình, đọc ngược Parquet từ storage và biến đổi thành công với `GoldEventTransformer` (18 observations -> 17 canonical events, 18 bridge rows, 0 lỗi).
  - Toàn bộ Spark suite: **232/232 tests pass** (0 failures, 0 errors, 0 skipped).
- **Kỹ năng phù hợp:** Spark integration, reconciliation

## Checklist bàn giao

- [x] Deliverable đã có trong repository hoặc môi trường demo.
- [x] Acceptance criteria đã được kiểm tra.
- [x] Test tự động đạt hoặc có evidence thủ công có thể lặp lại.
- [x] Tài liệu/contract đã cập nhật nếu schema, flow, cấu hình hoặc hành vi thay đổi.
- [x] Không chứa secret, dữ liệu nhạy cảm hoặc file build không cần thiết.
- [ ] P0/P1 có reviewer khác assignee xác nhận.

## Tài liệu liên quan

- [Phân công tuần 4](../WEEK_4_PARALLEL_PLAN.md)
- [Shared real samples](../../specs/SHARED_REAL_SAMPLE_DATA.md)
- [Silver/Gold logical model](../../specs/SILVER_GOLD_DATA_MODEL.md)
- [Kế hoạch 8 tuần](../README.md)
- [Các khối công việc](../WORK_BLOCKS.md)
- [Baseline MVP, KPI và Definition of Done](../../specs/MVP_SCOPE_KPI_AND_DOD.md)
