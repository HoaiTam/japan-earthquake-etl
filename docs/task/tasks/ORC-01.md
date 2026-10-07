---
task_id: "ORC-01"
status: "Done"
week: 4
block: "F - Điều phối"
workstream: "DAG skeleton"
scope: "Core"
priority: "P0"
effort_hours: 4
assignee: "HoaiTam"
reviewer: "unassigned"
dependencies: ["CON-02", "CON-03"]
---

# ORC-01 - Hoàn thiện DAG ETL đến Gold theo contract

## Mục đích

Dùng làm khung tích hợp sớm; nhóm Airflow không phải chờ toàn bộ code xử lý hoàn thành.

## Phạm vi công việc

Nối source readiness, Bronze, Silver, Gold, verify và publish; truyền run context; cho phép dùng mock/fixture trong lúc block nguồn chưa xong. DAG kết thúc ở Gold Published và không chờ Colab.

## Thành phần cần có

- **Đầu vào và contract:** [CON-02](./CON-02.md), [CON-03](./CON-03.md)
- **Phần triển khai:** Nối source readiness, Bronze, Silver, Gold, verify và publish; truyền run context; cho phép dùng mock/fixture trong lúc block nguồn chưa xong.
- **Kết quả bàn giao:** DAG chính với task groups và dependency gates.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- DAG chính với task groups và dependency gates.
- Implementation: `airflow/dags/orc_01_etl_pipeline.py`, helper
  `etl_pipeline_runtime.py`, fixture `fixtures/orc_01_mock_v1.json`.
- [Phase/run-context contract và runbook](../../specs/ETL_ORCHESTRATION_CONTRACT.md),
  `make test-orchestration`, `make etl-preview`, `make etl-mock`.

## Tiêu chí hoàn thành

- [x] Failure dừng đúng tầng.
- [x] task group có input/output contract rõ.
- [x] có thể test DAG structure không cần mạng.

## Hard dependency

- [CON-02](./CON-02.md)
- [CON-03](./CON-03.md)

## Cách triển khai và phối hợp

### Phân công tuần 4 mở rộng — nhóm mở đường

- **Owner / effort:** HoaiTam, 4h Core; đưa từ tuần 5 lên tuần 4, bước 3/3 của [nhóm mở đường](../WEEK_4_PREP_GROUP.md).
- **Bắt đầu ngay:** CON-02/03 đã Done. Test DAG structure/phase I/O/run context bằng contract và mock; không chờ Silver/Gold runtime để viết skeleton.
- **Làm gì / có gì:** Task groups source readiness → Bronze → Silver → Gold → verify → publish, exact input/output scope, UTC interval/backfill context và failure propagation.
- **Dùng để làm gì:** Giao diện chung cho các adapter và ORC-02..05, tránh mỗi người tự đặt runner/summary protocol.
- **Handoff:** Versioned phase/run-context fixture và mock adapter boundary cho các owners; runtime command/adapter thật được nối khi component sẵn sàng. Mock/dry-run thành công không mở real-mode Published gate.
- **Nghiệm thu / ranh giới:** Acceptance skeleton đạt unit/static tests, docs chỉ rõ mock/runtime giới hạn; không gọi đây là evidence toàn ETL Published, không đặt Colab trong daily critical path. 4h này đã nằm trong 13h mở đường và 30h task tuần của HoaiTam.

Có thể chuẩn bị interface, fixture, mock, test plan và tài liệu trước khi toàn bộ upstream chạy thật. Chỉ được chuyển sang `Done` khi hard dependency đã đạt và acceptance criteria được kiểm tra trên output phù hợp.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới từ `main`: `feat/orc-01-etl-dag-contract`.
3. Triển khai đúng phạm vi; dùng fixture nhỏ, xác định được và không phụ thuộc mạng cho unit test.
4. Chạy test/check phù hợp, đối chiếu acceptance criteria và cập nhật tài liệu nếu contract hoặc hành vi thay đổi.
5. Cập nhật `status`, `assignee`, `reviewer` và Evidence ngay trong file này khi mở PR hoặc hoàn tất review.

## Ranh giới

- [MLI-02 - Tạo Airflow DAG build ML dataset](./MLI-02.md) và
  [MLI-03 - Validate/import kết quả và commit bảng ML Iceberg](./MLI-03.md)
  chưa triển khai DAG build/import trong task này; không đặt external Colab
  step vào daily critical path.
- Done ở đây là nghiệm thu skeleton/mock; real adapters và daily ETL
  Published còn thiếu theo [bảng task handoff](../../specs/ETL_ORCHESTRATION_CONTRACT.md#7-handoff-giới-hạn-và-task-còn-thiếu).
- Không tự mở rộng sang deliverable của task khác.
- Không đổi contract upstream trong PR implementation mà không cập nhật task contract liên quan và có review.
- Không commit secret, credential, payload nhạy cảm, data dump lớn hoặc artifact build không cần thiết.
- Không đánh dấu `Done` nếu chưa có evidence kiểm tra được.

## Theo dõi

- **Trạng thái:** Done
- **Assignee:** HoaiTam
- **Reviewer:** unassigned; chưa có approval, review độc lập được khuyến nghị.
- **Evidence / PR:** [ORC-01 evidence](../../evidence/ORC-01.md); branch
  `feat/orc-01-etl-dag-contract`, chưa commit/push/mở PR tự động.
- **Kỹ năng phù hợp:** Airflow, orchestration

## Checklist bàn giao

- [x] Deliverable đã có trong repository hoặc môi trường demo.
- [x] Acceptance criteria đã được kiểm tra.
- [x] Test tự động đạt hoặc có evidence thủ công có thể lặp lại.
- [x] Tài liệu/contract đã cập nhật nếu schema, flow, cấu hình hoặc hành vi thay đổi.
- [x] Không chứa secret, dữ liệu nhạy cảm hoặc file build không cần thiết.
- [ ] Reviewer độc lập P0 được khuyến nghị; chưa có approval, không chặn Done theo quy trình hiện hành.

## Tài liệu liên quan

- [ETL orchestration contract](../../specs/ETL_ORCHESTRATION_CONTRACT.md)
- [Evidence ORC-01](../../evidence/ORC-01.md)
- [Nhóm mở đường tuần 4](../WEEK_4_PREP_GROUP.md)
- [Kế hoạch tuần 4 mở rộng](../WEEK_4_PARALLEL_PLAN.md)
- [Bronze contract](../../specs/BRONZE_STORAGE_CONTRACT.md)
- [Silver/Gold model](../../specs/SILVER_GOLD_DATA_MODEL.md)
- [Kế hoạch 8 tuần](../README.md)
- [Các khối công việc](../WORK_BLOCKS.md)
- [Baseline MVP, KPI và Definition of Done](../../specs/MVP_SCOPE_KPI_AND_DOD.md)
