---
task_id: "SLV-05"
status: "Done"
week: 3
block: "D - Silver đa nguồn"
workstream: "Quality"
scope: "Core"
priority: "P0"
effort_hours: 5
assignee: "HoaiTam"
reviewer: "unassigned"
dependencies: ["CON-03", "CON-04"]
---

# SLV-05 - Áp dụng validation và reject metrics

## Mục đích

Dùng để ngăn dữ liệu lỗi vào analytics mà không xóa bằng chứng raw.

## Phạm vi công việc

Kiểm tra required fields, parseability, tọa độ, thời gian, range, JMA quality flags; tổng hợp reason count theo source và run.

## Thành phần cần có

- **Đầu vào và contract:** [CON-03](./CON-03.md), [CON-04](./CON-04.md)
- **Phần triển khai:** Kiểm tra required fields, parseability, tọa độ, thời gian, range, JMA quality flags; tổng hợp reason count theo source và run.
- **Kết quả bàn giao:** Silver validator, reject dataset và quality summary.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- Silver validator, reject dataset và quality summary.

## Tiêu chí hoàn thành

- [x] valid + rejected = parsed.
- [x] blocker dừng publish.
- [x] rejected vẫn truy vết được về Bronze.

## Hard dependency

- [CON-03](./CON-03.md)
- [CON-04](./CON-04.md)

## Cách triển khai và phối hợp

Có thể chuẩn bị interface, fixture, mock, test plan và tài liệu trước khi toàn bộ upstream chạy thật. Chỉ được chuyển sang `Done` khi hard dependency đã đạt và acceptance criteria được kiểm tra trên output phù hợp.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới từ `main`: `feat/slv-05-ap-dung-validation-va-reject`.
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
- **Evidence / PR gốc:** [PR #32](https://github.com/HoaiTam/japan-earthquake-etl/pull/32). Sau merge vào main `4f906ae`, clean build bị 6 compilation errors do API quality không khớp model của SLV-02; claim test trước tích hợp không đủ làm evidence cho main này.
- **Hotfix / PR:** [PR #36](https://github.com/HoaiTam/japan-earthquake-etl/pull/36), branch `fix/slv-05-shared-model-integration`, bắt đầu từ [PR #35](https://github.com/HoaiTam/japan-earthquake-etl/pull/35) head `d92dee7`, tích hợp main `4f906ae` cùng hotfix tại commit `572918c`. Assignee của lần sửa là `HoaiTam`; giữ reviewer `unassigned`. Không thay đổi JMA-03 đang dở.
- **Đồng bộ sau PR #35:** Main đã nhận SLV-08 tại `40710d4`; commit `1b6334c` giải quyết conflict trên GitHub và giữ nguyên code hotfix. Bản này sửa lại metadata assignee về `HoaiTam` và index SLV-03 về `Ready` cho khớp file task; không khôi phục API quality cũ hoặc đánh dấu JMA parser đã hoàn tất khi chưa có evidence.
- **Kiểm thử lại sau conflict 2026-10-07:** Full `make check` đạt sau đồng bộ `1b6334c` và sửa metadata/index: **83/83 Java tests**, **15/15 Airflow tests**, checker **73 task** và toàn bộ static checks. Chạy bằng JDK 21.0.11 (`--release 17`) với quyền mở HTTP mock trên localhost; sandbox không cho bind cổng khiến 6 HTTP tests báo `Operation not permitted` ở lần chạy đầu, không phải compilation errors. Không chạy runtime smoke hoặc ghi dữ liệu nguồn thật.
- **Evidence 2026-10-07:** `make test` và `make package-java` đạt; sau bổ sung case đối soát mixed rejects, `make check` chạy clean Maven verify và đạt **83/83 Java tests** (quality/integration **12/12**), **15/15 Airflow tests**, toàn bộ contract/foundation/USG-06 static checks. Build bằng JDK **21.0.11**, compiler target `--release 17`; chưa xác nhận smoke runtime JDK 17 vì Docker daemon đang tắt.
- **Acceptance evidence:** Parser → quality → SLV-08 ghi Parquet thật trên file store, verify row count/SHA-256; blocked/mismatched gate không tạo staging/output/manifest/marker; parser rejects và quality rejects giữ đủ lineage, `valid + rejected = parsed`; null/negative depth/warning/duplicate không bị sửa hoặc lọc mất.
- **Tracking regression:** `make check-task-status` đạt cho **73 task**; cùng checker chạy trên snapshot main `4f906ae` fail đúng `SLV-03 metadata=Ready but index=Done`. Index đã giữ SLV-03 `Ready` và SLV-05 `Done`. `sh -n`, `git diff --check`, `git diff --cached --check` đạt; configuration/secret hygiene check đạt.
- **Kỹ năng phù hợp:** Data quality, Spark

## Checklist bàn giao

- [x] Deliverable đã có trong repository hoặc môi trường demo.
- [x] Acceptance criteria đã được kiểm tra.
- [x] Test tự động đạt hoặc có evidence thủ công có thể lặp lại.
- [x] Tài liệu/contract đã cập nhật nếu schema, flow, cấu hình hoặc hành vi thay đổi.
- [x] Không chứa secret, dữ liệu nhạy cảm hoặc file build không cần thiết.
- [ ] P0/P1 có reviewer khác assignee xác nhận.

## Tài liệu liên quan

- [Silver quality validation và tích hợp parser/writer](../../specs/SILVER_QUALITY_VALIDATION.md)
- [Logical data model Silver/Gold](../../specs/SILVER_GOLD_DATA_MODEL.md)
- [Kế hoạch 8 tuần](../README.md)
- [Các khối công việc](../WORK_BLOCKS.md)
- [Baseline MVP, KPI và Definition of Done](../../specs/MVP_SCOPE_KPI_AND_DOD.md)
