# Nhóm mở đường tuần 4 — HoaiTam làm trước

Nhóm này tập hợp task có sẵn, không tạo mã task mới hoặc thay acceptance criteria.
Làm trước để team có JMA Bronze thật, sample identity và khung run context/DAG.
Chi tiết phân chia phần còn lại ở [kế hoạch tuần 4 mở rộng](./WEEK_4_PARALLEL_PLAN.md).

## 1. Danh sách và thứ tự

| Thứ tự | Task | Owner | Effort | Có những gì / dùng để làm gì |
|---:|---|---|---:|---|
| 1 | [JMA-04](./tasks/JMA-04.md) | HoaiTam | 5h | Workflow year/segment, preview, concurrency/resume, nối downloader/writer và summary để ingest có phạm vi |
| 2 | [JMA-05](./tasks/JMA-05.md) | HoaiTam | 4h | Offline error suite và live Bronze manifest/readback/rerun theo release để bàn giao input thật cho Silver |
| 3 | [ORC-01](./tasks/ORC-01.md) | HoaiTam | 4h | Khung DAG ETL, phase input/output/run context và failure gates để các adapter Silver/Gold ghép cùng giao diện |

Tổng **13h**, đã tính trong 30h task cả tuần của HoaiTam. JMA-05 chờ JMA-04;
ORC-01 chỉ phụ thuộc CON-02/03 đã Done nên có thể chuẩn bị mock/fixture trước.
Mỗi task vẫn tạo branch/PR riêng, không dùng một branch cho cả nhóm.

## 2. Làm gì trong từng task?

### JMA-04 — workflow ingest, không tải cả 40 năm

- Resolve danh sách năm/range thành exact inventory entries; 1997 là hai segment.
- Preview source/archive/release và output scope trước khi ghi, giới hạn tải/concurrency.
- Nối JMA-02 download với JMA-03 validation/immutable raw + manifest-last writer.
- Giữ HTTP transport metadata thật; state downloader không tự đủ metadata writer.
- Reuse archive đã verify, retry riêng năm/segment lỗi và report status/reason từng entry.

### JMA-05 — evidence và dữ liệu để mọi người test

- Verify hai sample DAT-01 còn đọc được: USGS BronzeReady 16 event và JMA 2023
  STAGED_SOURCE. Không đổi status JMA bằng sửa catalog; chạy writer để có
  BronzeReady manifest mới, giữ nguyên raw bytes/checksum nguồn.
- Chạy workflow có phạm vi với các năm đại diện **2000, 2023, 1997** (1997 hai
  segment). 2000 thuộc reproduction period; 2023 thuộc extension; 1997 kiểm
  tra boundary era/segmentation. Không tải full 40 năm.
- Sample 2000 chưa có trong DAT-01: ghi URI/year/release/SHA/size/count/readback
  từ run thật vào evidence JMA-05. Không bịa expected count và không thêm entry
  vào catalog DAT-01 đang khóa hai samples/checksums.
- Test checksum/ZIP/record length/revision/resume/rerun bằng fixture/mock; với
  live run đọc lại exact object/manifest và ghi metadata nhỏ, không commit raw.
- Bàn giao một report có thể lặp lại với run/attempt, input manifest/raw keys,
  SHA, release, structural count, coverage, readback và reuse status.

Dữ liệu này phục vụ integration/pilot. Một năm reproduction không đại diện
đầy đủ 2000–2018; Mc/candidate pilot phải ghi coverage và limitation, không
được gọi là đã có training dataset hoặc kết quả HDBSCAN toàn giai đoạn.

### ORC-01 — khung tích hợp, không giả lập pipeline đã Published

- Khóa phase input/output và run context theo CON-02/03: exact input,
  run/window/processing date, backfill flag, output scope và config version.
- DAG source readiness → Bronze → Silver → Gold → verify → publish; unit test
  cấu trúc/failure propagation bằng adapters mock, không cần code Gold chạy thật.
- Phân biệt dry-run/mock và real-mode; mock success không mở production publish gate.
- Bàn giao task-group interface/fixture cho ORC-02..05 và owners Silver/Gold;
  runtime command/adapter cụ thể được nối khi implementation upstream sẵn sàng.
- Không đưa Colab/ML experiment vào daily critical path; build/import ML là DAG khác.

## 3. Điều kiện bàn giao nhóm mở đường

- [ ] JMA-04/05 và ORC-01 đạt acceptance riêng, tests/docs/evidence và status/index đồng bộ.
- [ ] JMA sample có BronzeReady manifest thật, raw SHA/readback/rerun verified.
- [ ] USGS sample vẫn truy vết được bằng exact manifest; lỗi sample/readback được ghi rõ.
- [ ] Sample reproduction 2000 có metadata/coverage thật; 1997 giữ hai segment.
- [ ] Phase/run-context fixtures đủ cho các owners viết/test adapter độc lập.
- [ ] Ghi rõ ORC-01 đã kiểm thử skeleton/mock, chưa gọi toàn ETL Published.
- [ ] Không chứa credential/raw lớn và không chạy/xóa full lake/warehouse.

Nhóm này mở đường cho data và orchestration, không thay SilverReady/Gold
Published. Không đưa SLV-09/GLD-04 vào đây vì còn chờ dedup/link/writer.
ThanhTris/Trang có thể viết SLV-06/07, GLD-01, MLI-01 bằng fixture trong lúc
HoaiTam làm nhóm chuẩn bị; không yêu cầu mọi người chờ 13h mới bắt đầu.

## 4. Kiểm tra và tài liệu

Lệnh hiện có: `make test-contracts`, `make test`, `make check`,
`git diff --check`; operator có cấu hình/service hợp lệ dùng
`make verify-samples` cho DAT-01. Commands business mới phải được owner bổ
sung vào Makefile/runbook khi triển khai, không copy lệnh chưa tồn tại.

- [JMA writer/handoff](../specs/JMA_BRONZE_WRITER_CONTRACT.md)
- [Shared real samples](../specs/SHARED_REAL_SAMPLE_DATA.md)
- [Bronze contract](../specs/BRONZE_STORAGE_CONTRACT.md)
- [Silver/Gold model](../specs/SILVER_GOLD_DATA_MODEL.md)
- [ML logical model](../specs/ML_DATA_MODEL.md)
