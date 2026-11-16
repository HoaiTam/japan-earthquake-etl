---
task_id: "OPS-01"
status: "Done"
week: 6
block: "K - QA & Release"
workstream: "Backup and restore"
scope: "Stretch"
priority: "P2"
effort_hours: 4
assignee: "HoaiTam"
reviewer: "unassigned"
dependencies: ["GLD-03", "ORC-05"]
---

# OPS-01 - Smoke test backup và restore metadata

## Giao việc hiện hành — 11/11/2026

**17/11 — Done cho smoke đại diện**, không phải full disaster recovery.
[Actual evidence](../../evidence/OPS-01_BACKUP_RESTORE_20261117.md) và
[runbook/Make commands](../../runbooks/OPS-01_BACKUP_RESTORE.md): backup13bảng,
77objects/14artifacts, hai Bronze manifests gốc, toàn graph metadata/Avro/
Parquet/Puffin; restore namespace/prefix/DB riêng. Trino13signatures +13rerun
+4ML lineage joins đạt; PostgreSQL schema71tables/531columns khớp. Source pins
và5services unchanged, lease empty/runtime idle, không ETL/build/restart/ACL.
47offline regression tests/43CI policy tests đạt. HoaiTam, reviewer unassigned;
owner duyệt commit/push/tạo PR và merge17/11 sau khi checks đạt.
DAT-03 closure tách PR riêng, không sửa dataset.

- **Owner:** HoaiTam; **status:** Done.
- **Làm phần gì / dùng để làm gì:** Backup/restore.
- **Bắt đầu độc lập:** Inventory metadata và chuẩn bị restore procedure trong namespace thử.
- **Đầu ra và điều kiện Done:** Restore đọc được Gold và ML lineage/registry; backup chia sẻ không chứa secret, không restore đè deployment gốc.
- **Lịch:** chuẩn bị 11–15/11, nghiệm thu tuần9 16–22/11; xem
  [kế hoạch và setup](../WEEK_9_REPLAN_20261111.md). Giữ hard dependencies,
  scope/effort lịch sử; các mục tuần cũ dưới đây là lịch sử.

## Mục đích

Dùng để tăng khả năng phục hồi; có thể hoãn nếu thời gian chỉ đủ hoàn thành MVP Core.

## Phạm vi công việc

Backup manifest Bronze/Gold/ML quan trọng, Iceberg catalog metadata, experiment artifact registry và Airflow metadata cần thiết; restore trong môi trường thử.

## Thành phần cần có

- **Đầu vào và contract:** [GLD-03](./GLD-03.md), [ORC-05](./ORC-05.md)
- **Phần triển khai:** Backup manifest Bronze/Gold quan trọng, Iceberg catalog metadata và Airflow metadata cần thiết; restore trong môi trường thử.
- **Kết quả bàn giao:** Backup manifest và restore procedure đã thử.
- **Kiểm thử và evidence:** Kiểm tra từng acceptance criterion, lưu lệnh chạy/log/report có thể lặp lại và cập nhật mục Evidence bên dưới.

## Deliverable

- Backup manifest và restore procedure đã thử.

## Tiêu chí hoàn thành

- [x] Restore đọc được Gold snapshot/query kiểm chứng:7Gold/publication tables,
  snapshot/count/full-row checksum giữ nguyên; data/metadata đọc từ prefix mới.
- [x] Restore truy vết được dataset/experiment manifest và bảng `ml.*` liên quan:
  6ML component tables, exact original manifests/registry,4join checks đạt.
- [x] Backup không chứa secret chia sẻ: decoded Avro/Parquet/Puffin/JSON/text
  và exact configured secrets đã kiểm; artifact/ZIP private, không tự upload.

## Hard dependency

- [GLD-03](./GLD-03.md)
- [ORC-05](./ORC-05.md)

## Cách triển khai và phối hợp

Có thể chuẩn bị interface, fixture, mock, test plan và tài liệu trước khi toàn bộ upstream chạy thật. Chỉ được chuyển sang `Done` khi hard dependency đã đạt và acceptance criteria được kiểm tra trên output phù hợp.

1. Xác nhận hard dependency và đọc contract/tài liệu liên quan.
2. Tạo branch mới từ `main`: `feat/ops-01-smoke-test-backup-va-restore`.
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
- **Reviewer:** unassigned
- **Evidence / PR:** [Runtime + tests17/11](../../evidence/OPS-01_BACKUP_RESTORE_20261117.md), [machine-readable](../../evidence/OPS-01_BACKUP_RESTORE_20261117.json); chưa mở PR.
- **Kỹ năng phù hợp:** Backup, MinIO, operations

## Checklist bàn giao

- [x] Deliverable đã có trong repository hoặc môi trường demo.
- [x] Acceptance criteria đã được kiểm tra trên13bảng đại diện.
- [x] Test tự động đạt và có actual restore/readback evidence lặp lại được.
- [x] Runbook, Make commands, profile và metadata task/index đã cập nhật.
- [x] Không commit ZIP/data/secret/private logs/build artifacts.
- Reviewer độc lập chưa có (`unassigned`); task P2, không tự tạo approval.

## Giới hạn còn lại

- Không phải backup full Bronze/Silver/warehouse/catalog volume hoặc Airflow
  history/credentials. Full disaster recovery **chưa có task riêng**.
- [SEC-01 — Review secret và bề mặt truy cập](./SEC-01.md) còn cloud ACL/roster/
  session acceptance; OPS-01 không đóng task đó.
- [MLQ-01 — Chạy E2E Gold snapshot đến ML report](./MLQ-01.md) và
  [MLI-04 — Tạo Trino views và static report](./MLI-04.md) còn official
  research/report gates. Synthetic ML restore không thay nghiệm thu đó.

## Tài liệu liên quan

- [Kế hoạch 8 tuần](../README.md)
- [Các khối công việc](../WORK_BLOCKS.md)
- [Baseline MVP, KPI và Definition of Done](../../specs/MVP_SCOPE_KPI_AND_DOD.md)
