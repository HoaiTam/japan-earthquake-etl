---
task_id: "SPK-01"
status: "Done"
week: 1
block: "Foundation"
workstream: "Compute"
scope: "Core"
priority: "P0"
effort_hours: 6
assignee: "HoaiTam"
reviewer: "unassigned"
dependencies: ["CMP-01", "REP-01"]
---

# SPK-01 - Cấu hình Spark standalone và Java build

## Mục đích

Cung cấp runtime Spark và quy trình build Java có thể lặp lại cho các job xử lý dữ liệu.

## Phạm vi công việc

Thêm Spark master/worker/client, Maven wrapper và job Hello World.

## Thành phần cần có

- **Đầu vào và contract:** [CMP-01](./CMP-01.md), [REP-01](./REP-01.md)
- **Phần triển khai:** Thêm Spark master/worker/client, Maven wrapper và job Hello World.
- **Kết quả bàn giao:** Spark services, pom.xml và JAR smoke test.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- Spark services, pom.xml và JAR smoke test.

## Tiêu chí hoàn thành

- [x] spark-submit chạy trên worker và trả exit code 0.
- [x] Spark/Airflow Java builder có fixture/inventory trước Maven verify;
  preflight và regression test phát hiện thiếu đầu vào.
- [x] Hotfix được kiểm chứng bằng Maven Java 17 trong container và Spark runtime.

## Hard dependency

- [CMP-01](./CMP-01.md)
- [REP-01](./REP-01.md)

## Cách triển khai và phối hợp

Có thể chuẩn bị interface, fixture, mock, test plan và tài liệu trước khi toàn bộ upstream chạy thật. Chỉ được chuyển sang `Done` khi hard dependency đã đạt và acceptance criteria được kiểm tra trên output phù hợp.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới từ `main`: `feat/spk-01-cau-hinh-spark-standalone-va`.
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
- **Reviewer:** unassigned; khuyến nghị review độc lập hotfix, chưa có approval.
- **Evidence / PR:** [PR #9](https://github.com/HoaiTam/japan-earthquake-etl/pull/9)
- **Hotfix 2026-10-08:** Branch `fix/spk-01-docker-build-inputs` từ `origin/main`
  tại `bfe8a35` (PR #44). Bổ sung fixture/inventory vào Java builder Spark và
  preflight chống thiếu đầu vào; giữ nguyên Maven `clean verify`.
- **Evidence hotfix:** [SPK-01 Docker build inputs](../../evidence/SPK-01_DOCKER_BUILD_INPUTS.md).
  12 regression test, 167 Java + 89 Airflow test; `make build` retry đạt,
  Maven Java 17 offline chạy mới đạt, worker `ALIVE`/spark-submit exit `0`.
  Evidence ghi riêng timeout Docker Hub của script smoke và cách kiểm chứng
  runtime trên image đã build; không coi lỗi registry là lỗi Maven.
- **Kỹ năng phù hợp:** Spark, Java, Maven, Docker

## Checklist bàn giao

- [x] Deliverable đã có trong repository hoặc môi trường demo.
- [x] Acceptance criteria đã được kiểm tra.
- [x] Test tự động đạt hoặc có evidence thủ công có thể lặp lại.
- [x] Tài liệu/contract đã cập nhật nếu schema, flow, cấu hình hoặc hành vi thay đổi.
- [x] Không chứa secret, dữ liệu nhạy cảm hoặc file build không cần thiết.
- [ ] Reviewer độc lập xác nhận hotfix (khuyến nghị; reviewer hiện `unassigned`).

## Tài liệu liên quan

- [Kế hoạch 8 tuần](../README.md)
- [Các khối công việc](../WORK_BLOCKS.md)
- [Baseline MVP, KPI và Definition of Done](../../specs/MVP_SCOPE_KPI_AND_DOD.md)
- [Spark standalone và Java build](../../specs/SPARK_STANDALONE.md)
