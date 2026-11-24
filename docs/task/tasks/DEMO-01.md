---
task_id: "DEMO-01"
status: "In Progress"
week: 8
block: "K - QA & Release"
workstream: "Demo"
scope: "Core"
priority: "P0"
effort_hours: 4
assignee: "ThanhTris"
reviewer: "unassigned"
dependencies: ["QA-01", "MLQ-01", "MLI-04"]
---

# DEMO-01 - Chuẩn bị dữ liệu và kịch bản demo

## Giao việc hiện hành — 11/11/2026

- **Owner:** ThanhTris; **status:** In Progress.
- **Làm phần gì / dùng để làm gì:** Kịch bản demo.
- **Bắt đầu độc lập:** Soạn luồng source → lakehouse → ML → report và fallback có nhãn rõ.
- **Đầu ra và điều kiện Done:** Sau QA-01/MLQ-01/MLI-04: demo package với exact runs/query/pins, không chạy lại toàn catalog trên sân khấu.
- **Lịch:** chuẩn bị 11–15/11, nghiệm thu tuần9 16–22/11; xem
  [kế hoạch và setup](../WEEK_9_REPLAN_20261111.md). Giữ hard dependencies,
  scope/effort lịch sử; các mục tuần cũ dưới đây là lịch sử.

## Mục đích

Dùng để trình bày rõ giá trị của từng tầng dữ liệu và giảm rủi ro phụ thuộc mạng khi demo.

## Phạm vi công việc

Chọn daily/JMA evidence đại diện và một dataset/experiment nhỏ; chuẩn bị SQL, static report, ảnh/log, fixture/bundle fallback và thứ tự trình bày từ source đến HDBSCAN.

## Thành phần cần có

- **Đầu vào và contract:** [QA-01](./QA-01.md), [MLQ-01](./MLQ-01.md), [MLI-04](./MLI-04.md)
- **Phần triển khai:** Trình bày Bronze/Silver/Gold ngắn gọn rồi tập trung dataset manifest, feature, comparison, import gate và ML report.
- **Kết quả bàn giao:** Demo package và preflight checklist.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- Demo package và preflight checklist.

## Tiêu chí hoàn thành

- [x] Demo có fallback mạng/Drive/Colab và không phụ thuộc Power BI.
- [ ] mọi con số có run_id/query evidence.
- [x] không cần chạy lại 40 năm trên sân khấu.
- [x] Nêu rõ output là aftershock candidate, không phải dự đoán/quan hệ nhân quả.

## Hard dependency

- [QA-01](./QA-01.md)
- [MLQ-01](./MLQ-01.md)
- [MLI-04](./MLI-04.md)

## Cách triển khai và phối hợp

Có thể chuẩn bị interface, fixture, mock, test plan và tài liệu trước khi toàn bộ upstream chạy thật. Chỉ được chuyển sang `Done` khi hard dependency đã đạt và acceptance criteria được kiểm tra trên output phù hợp.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới từ `main`: `feat/demo-01-chuan-bi-du-lieu-va`.
3. Triển khai đúng phạm vi; dùng fixture nhỏ, xác định được và không phụ thuộc mạng cho unit test.
4. Chạy test/check phù hợp, đối chiếu acceptance criteria và cập nhật tài liệu nếu contract hoặc hành vi thay đổi.
5. Cập nhật `status`, `assignee`, `reviewer` và Evidence ngay trong file này khi mở PR hoặc hoàn tất review.

## Ranh giới

- Không tự mở rộng sang deliverable của task khác.
- Không đổi contract upstream trong PR implementation mà không cập nhật task contract liên quan và có review.
- Không commit secret, credential, payload nhạy cảm, data dump lớn hoặc artifact build không cần thiết.
- Không đánh dấu `Done` nếu chưa có evidence kiểm tra được.

## Theo dõi

- **Trạng thái:** In Progress
- **Assignee:** ThanhTris
- **Reviewer:** unassigned
- **Evidence / PR:** Đã chuẩn bị phần độc lập trên nhánh `feat/demo-01-chuan-bi-du-lieu-va` ([PR #135](https://github.com/HoaiTam/japan-earthquake-etl/pull/135)): manifest ghim 11 bằng chứng đại diện (`QA-01`..`QA-04`, `ORC-07`, `DAT-03` v4 release, `DAT-02`, `EXP-02`..`EXP-04`, `MLI-04`) tại [demo_package_manifest_v1.json](../../../config/demo/demo_package_manifest_v1.json), kịch bản trình bày 15–20 phút + 4 phương án fallback tại [DEMO-01_DEMO_PACKAGE.md](../../runbooks/DEMO-01_DEMO_PACKAGE.md), công cụ preflight fail-closed [demo-preflight.py](../../../scripts/demo-preflight.py) và unit tests [test_demo_preflight.py](../../../tests/test_demo_preflight.py). Giữ `In Progress` chờ nghiệm thu official của [EXP-02](./EXP-02.md)..[EXP-04](./EXP-04.md), [MLI-04](./MLI-04.md) và [MLQ-01](./MLQ-01.md) để chốt exact official runs/query/pins.
- **Bàn giao v2:** [Manifest demo](../../../config/demo/demo_package_manifest_v2.json) ghim EXP-02/03/04 và EXP-05 official. Fixture kiểm preflight đòi đủ bốn thuật toán đại diện và receipt import EXTENSION theo pin (`algorithm_name=WINDOW`, từ chối chọn đơn lẻ adaptive). Preflight xác minh thêm fingerprint từ marker EXP-05, một dòng registry đọc lại, digest cohort EXP-03 trong evidence MLI-04, và bằng chứng chi tiết của receipt MLQ-01 (`trace` với `query_id`/`lineage_check=PASS` cho 4 run REPRODUCTION + 1 EXTENSION WINDOW, `reject_verification` không tạo `CANDIDATE`, `rerun_verification` trước/sau giữ nguyên snapshot và counts). Cổng MLI-04 và MLQ-01 vẫn PENDING. [Review #143 (shard EXTENSION)](https://github.com/HoaiTam/japan-earthquake-etl/pull/143#issuecomment-7102538242), [Review #143 (bằng chứng receipt MLQ-01)](https://github.com/HoaiTam/japan-earthquake-etl/pull/143#issuecomment-7127604628).
- **Kỹ năng phù hợp:** Demo, communication, QA

## Checklist bàn giao

- [ ] Deliverable đã có trong repository hoặc môi trường demo.
- [ ] Acceptance criteria đã được kiểm tra.
- [ ] Test tự động đạt hoặc có evidence thủ công có thể lặp lại.
- [ ] Tài liệu/contract đã cập nhật nếu schema, flow, cấu hình hoặc hành vi thay đổi.
- [ ] Không chứa secret, dữ liệu nhạy cảm hoặc file build không cần thiết.
- [ ] P0/P1 có reviewer khác assignee xác nhận.

## Tài liệu liên quan

- [Kế hoạch 8 tuần](../README.md)
- [Các khối công việc](../WORK_BLOCKS.md)
- [Baseline MVP, KPI và Definition of Done](../../specs/MVP_SCOPE_KPI_AND_DOD.md)
