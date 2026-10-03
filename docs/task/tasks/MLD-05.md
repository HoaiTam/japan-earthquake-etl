---
task_id: "MLD-05"
status: "Backlog"
week: 6
block: "G - ML Dataset"
workstream: "Feature validation and export"
scope: "Core"
priority: "P0"
effort_hours: 5
assignee: "unassigned"
reviewer: "unassigned"
dependencies: ["MLD-04"]
---

# MLD-05 - Validate feature snapshot và export Parquet bundle

## Mục đích

Tạo bundle độc lập, nhỏ gọn và kiểm chứng được để Colab đọc đúng dataset mà
không sao chép physical files trong Iceberg warehouse.

## Phạm vi công việc

- Chạy quality gate grain, mainshock presence, `Mc`, finite feature và hard limit.
- Commit feature snapshot rồi materialize export theo `dataset_id`.
- Ghi `features-part-*.parquet`, `dataset_manifest.json`, `checksums.sha256` và count/schema summary.
- Hỗ trợ stage/upload thủ công hoặc adapter Drive riêng; không đặt credential MinIO trong notebook.

## Deliverable

- Feature validator và deterministic export job.
- Export layout/manifest/checksum contract cùng fixture bundle nhỏ.
- Runbook verify/download/upload và cleanup staging có phạm vi.

## Tiêu chí hoàn thành

- [ ] Bundle chỉ chứa cột cần cho clustering và lineage.
- [ ] Checksum/count/schema khớp feature snapshot đã pin.
- [ ] Rerun cùng dataset không trộn part từ export khác.
- [ ] Colab có thể verify bundle mà không cần MinIO credential.
- [ ] Không copy trực tiếp physical files của Iceberg warehouse.

## Hard dependency

- [MLD-04](./MLD-04.md)

## Cách làm song song

Bundle layout/validator và feature fixture có thể khóa sớm để `EXP-01` phát
triển độc lập; integration export dùng snapshot thật khi `MLD-04` hoàn tất.

## Ranh giới

- Không chạy model hoặc ghi membership trong task này.
- Không ép một file Parquet duy nhất nếu volume lớn.
- Không commit data export lớn, Drive token hoặc credential vào repository.

## Theo dõi

- **Trạng thái:** Backlog
- **Assignee:** Chưa ghi lại
- **Reviewer:** Chưa ghi lại
- **Evidence / PR:** Chưa có
- **Kỹ năng phù hợp:** Parquet, checksums, data export, Spark

## Checklist bàn giao

- [ ] Deliverable và test/evidence tồn tại trong repository.
- [ ] Acceptance criteria đã được kiểm tra.
- [ ] Contract/docs đã cập nhật và không chứa secret/build artifact.
- [ ] Reviewer độc lập được khuyến nghị cho P0.

## Tài liệu liên quan

- [Roadmap HDBSCAN](../HDBSCAN_WORKSTREAM.md)
- [MLD-04](./MLD-04.md)
