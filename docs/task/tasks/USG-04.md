---
task_id: "USG-04"
status: "Done"
week: 2
block: "B - USGS Bronze"
workstream: "Airflow integration"
scope: "Core"
priority: "P0"
effort_hours: 4
assignee: "unassigned"
reviewer: "unassigned"
dependencies: ["USG-02", "USG-03"]
---

# USG-04 - Tích hợp USGS ingest vào Airflow

## Mục đích

Dùng để vận hành daily USGS độc lập; block Silver chỉ cần nhận manifest đã verify.

## Phạm vi công việc

Tạo task resolve interval, fetch, validate, upload và verify; truyền run context thống nhất và chặn downstream khi Bronze chưa sẵn sàng.

## Thành phần cần có

- **Đầu vào và contract:** [USG-02](./USG-02.md), [USG-03](./USG-03.md)
- **Phần triển khai:** Tạo task resolve interval, fetch, validate, upload và verify; truyền run context thống nhất và chặn downstream khi Bronze chưa sẵn sàng.
- **Kết quả bàn giao:** DAG `usg_04_usgs_ingest`, task group resolve/fetch/
  validate/upload/verify, Bronze publish gate, runner protocol và run summary.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- USGS task group/DAG và run summary cơ bản.

## Tiêu chí hoàn thành

- [x] Task group và các phase hiển thị rõ trong Airflow UI.
- [x] Retry giữ cùng `logical_run_key` và logical window đã resolve.
- [x] Silver chỉ được nối sau `bronze_ready_gate`; verify thất bại chặn gate.

## Hard dependency

- [USG-02](./USG-02.md)
- [USG-03](./USG-03.md)

## Cách triển khai và phối hợp

Có thể chuẩn bị interface, fixture, mock, test plan và tài liệu trước khi toàn bộ upstream chạy thật. Chỉ được chuyển sang `Done` khi hard dependency đã đạt và acceptance criteria được kiểm tra trên output phù hợp.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới: `feat/usg-04-airflow-ingest`.
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
- **Assignee:** Chưa ghi lại
- **Reviewer:** Chưa ghi lại
- **Evidence / PR:** Chưa mở PR; evidence cục bộ bên dưới.
- **Kỹ năng phù hợp:** Airflow, Python/Java integration

## Checklist bàn giao

- [x] Deliverable đã có trong repository hoặc môi trường demo.
- [x] Acceptance criteria đã được kiểm tra.
- [x] Test tự động đạt hoặc có evidence thủ công có thể lặp lại.
- [x] Tài liệu/contract đã cập nhật nếu schema, flow, cấu hình hoặc hành vi thay đổi.
- [x] Không chứa secret, dữ liệu nhạy cảm hoặc file build không cần thiết.
- [ ] Reviewer độc lập đã xác nhận (khuyến nghị, không chặn `Done`).

## Tài liệu liên quan

- [Kế hoạch 8 tuần](../README.md)
- [Các khối công việc](../WORK_BLOCKS.md)
- [Baseline MVP, KPI và Definition of Done](../../specs/MVP_SCOPE_KPI_AND_DOD.md)
- [Airflow local contract](../../specs/AIRFLOW_LOCAL.md)
- [USGS Airflow ingest contract](../../specs/USGS_AIRFLOW_INGEST_CONTRACT.md)

## Evidence

- `python3 -m unittest discover -s airflow/tests -p 'test_*.py'` — đạt: 13
  tests, gồm DAG contract, interval seed/overlap, dry-run, runner boundary và
  publish gate.
- `./scripts/check-airflow.sh` — đạt; Compose Airflow contract và USG-04 static
  markers hợp lệ.
- `./scripts/check-foundation.sh` — đạt; upstream Maven 20 tests, Airflow,
  Compose, MinIO, Spark và query contracts đều pass.
- `node /private/tmp/con03_validate_links.mjs` — đạt: 102 Markdown files,
  623 local links.
- Secret-pattern scan và `git diff --check` — đạt.
