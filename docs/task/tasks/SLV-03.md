---
task_id: "SLV-03"
status: "Done"
week: 3
block: "D - Silver đa nguồn"
workstream: "JMA parser"
scope: "Core"
priority: "P0"
effort_hours: 6
assignee: "HoaiTam"
reviewer: "unassigned"
dependencies: ["CON-03", "CON-04"]
---

# SLV-03 - Parse và chuẩn hóa JMA fixed-width

## Mục đích

Dùng để chuyển archive JMA thành observation chuẩn độc lập với downloader và USGS parser.

## Phạm vi công việc

Đọc record 96-byte; parse JST, độ/phút tọa độ, depth, magnitude/type, intensity, tsunami, region, record source và determination flag.

## Thành phần cần có

- **Đầu vào và contract:** [CON-03](./CON-03.md), [CON-04](./CON-04.md)
- **Phần triển khai:** Đọc record 96-byte; parse JST, độ/phút tọa độ, depth, magnitude/type, intensity, tsunami, region, record source và determination flag.
- **Kết quả bàn giao:** Spark Java JMA parser, code mapping và field-level tests.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- Spark Java JMA parser, code mapping và field-level tests.
- [JMA Silver parser](../../specs/JMA_SILVER_PARSER.md): giải thích thành phần, byte offsets, numeric/code mapping, lineage, quality handoff và cách test.

## Tiêu chí hoàn thành

- [x] Test biên cột và ký tự thiếu.
- [x] JST sang UTC đúng.
- [x] giữ raw flags và catalog_release để audit.

## Hard dependency

- [CON-03](./CON-03.md)
- [CON-04](./CON-04.md)

## Cách triển khai và phối hợp

Có thể chuẩn bị interface, fixture, mock, test plan và tài liệu trước khi toàn bộ upstream chạy thật. Chỉ được chuyển sang `Done` khi hard dependency đã đạt và acceptance criteria được kiểm tra trên output phù hợp.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới từ `main`: `feat/slv-03-jma-fixed-width-parser`.
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
- **Branch / baseline:** `feat/slv-03-jma-fixed-width-parser` từ main `bb3b892` (sau merge JMA-03 / PR #37); CON-03 và CON-04 đã Done. Chưa commit/push/mở PR cho SLV-03. Giữ nguyên `.env`, `.metals/` và quy tắc PR description đã thêm vào `AGENTS.md` ở lần bàn giao trước.
- **Deliverable:** `JmaFixedWidthParser`, `JmaParseContext`, `JmaParseResult`, `JmaCodeMapping`, `JmaNativeFields`; dùng model/schema/key SLV-02/SLV-04 hiện có. Thêm overload quality nhận cả JMA observations/rejects, không đổi policy/schema upstream.
- **Evidence 2026-10-07:** `make test-jma-parser` đạt **36/36 tests** (30 field/parser tests, 6 integration tests); full `make check` đạt **140/140 Java tests**, **15/15 Airflow tests**, checker **73 task** và toàn bộ contract/foundation/USG-06 static checks. Build bằng JDK 21.0.11, compiler `--release 17`; không gọi nguồn thật hoặc ghi MinIO thật. Spark Dataset adapters đã compile/Row-schema checked, chưa chạy Spark cluster/JDK 17 runtime.
- **Acceptance evidence:** Biên cột 01..96, required/optional blank, 95/97 byte/CRLF/non-ASCII, implied/explicit decimal, depth-slice/trailing fractional blank, encoded negative magnitude, degree/minute/sign/ROI, JST → UTC không phụ thuộc host, UTC partition và mốc era 1997 đều có assertion. Native agency/intensity/determination case, hai magnitude, region/code flags và release giữ cho audit; raw record hash tính trên 96 bytes trước normalize.
- **Integration evidence:** JMA-03 ZIP → SLV-01 exact input → parser → SLV-05 → SLV-08 Parquet readback giữ fields/count/lineage, rerun ổn định. Mixed/all-invalid parser input được đối soát và block checked publication trước mọi storage write; lỗi staging SHA/length hoặc wrong member/context dừng parser, không trả partial success.
- **Handoff / safety:** [Parser contract](../../specs/JMA_SILVER_PARSER.md), Spark README, quality guide và Makefile được cập nhật; `make help`, local Markdown link check và `git diff --check` đạt. Không sửa shared fixtures, inventory, Silver schema/key hoặc dependencies; không có migration/xóa dữ liệu bắt buộc. Reviewer độc lập chưa được ghi nhận, giữ `reviewer: unassigned`; native metadata ngoài các cột Silver là in-memory, audit bền vững qua Bronze locator/hash. DAG/live QA/dedup/linking/E2E tiếp tục ở các task tương ứng.
- **Kỹ năng phù hợp:** Spark SQL, Java, fixed-width

## Checklist bàn giao

- [x] Deliverable đã có trong repository hoặc môi trường demo.
- [x] Acceptance criteria đã được kiểm tra.
- [x] Test tự động đạt hoặc có evidence thủ công có thể lặp lại.
- [x] Tài liệu/contract đã cập nhật nếu schema, flow, cấu hình hoặc hành vi thay đổi.
- [x] Không chứa secret, dữ liệu nhạy cảm hoặc file build không cần thiết.
- [ ] P0/P1 có reviewer khác assignee xác nhận.

## Tài liệu liên quan

- [JMA Silver parser](../../specs/JMA_SILVER_PARSER.md)
- [CON-03 Silver/Gold model](../../specs/SILVER_GOLD_DATA_MODEL.md)
- [Shared fixtures CON-04](../../../tests/fixtures/README.md)
- [JMA inventory/source format](../../specs/JMA_ARCHIVE_INVENTORY.md)
- [JMA Bronze writer](../../specs/JMA_BRONZE_WRITER_CONTRACT.md)
- [Silver quality validation](../../specs/SILVER_QUALITY_VALIDATION.md)
- [Kế hoạch 8 tuần](../README.md)
- [Các khối công việc](../WORK_BLOCKS.md)
- [Baseline MVP, KPI và Definition of Done](../../specs/MVP_SCOPE_KPI_AND_DOD.md)
