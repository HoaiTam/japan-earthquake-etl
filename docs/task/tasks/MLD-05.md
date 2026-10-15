---
task_id: "MLD-05"
status: "Ready"
week: 5
block: "G - ML Dataset"
workstream: "Feature validation and export"
scope: "Core"
priority: "P0"
effort_hours: 5
assignee: "rosy179"
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

## Phân công tuần 5

- **Owner / effort:** rosy179, 5h Core; [kế hoạch và handoff](../WEEK_5_PARALLEL_PLAN.md).
- **Làm gì / có gì:** Validate/commit feature snapshot; export Parquet parts, dataset_manifest.json và checksums.sha256, marker/lifecycle sau readback.
- **Dùng để làm gì / output:** Bundle độc lập để EXP-01/MLI-02 dùng, schema/key/checksum/count/version rõ, không credential hoặc copy Iceberg metadata.
- **Bắt đầu ngay / gate:** Layout/invalid-bundle fixtures làm ngay. Done cần MLD-04, audit/coverage eligibility và export readback; pilot BUILDING/research_ready=false không được tự nâng EXPORTED.
- **Theo dõi:** Phân công không có nghĩa đã triển khai; chỉ đổi In Progress khi bắt đầu, Done khi đủ acceptance/evidence. Reviewer hiện unassigned, chưa có approval.

## Điều chỉnh Jupyter-first — 2026-10-14

Task này vẫn sở hữu official VALIDATED → EXPORTED bundle/lifecycle. DAT-02 chỉ export development pilot riêng để EXP-01/02 test Jupyter trước, không yêu cầu MLD-05 Done và không bypass audit/coverage. Official bundle phải chạy local loader/checksum smoke trước handoff Colab; Colab model gate do EXP-02 sở hữu, không tạo dependency vòng cho exporter.

- [DAT-02](./DAT-02.md)
- [Workflow Jupyter → Colab](../../specs/JUPYTER_PILOT_WORKFLOW.md)

## Theo dõi

- **Trạng thái:** Ready
- **Assignee:** rosy179
- **Reviewer:** Chưa ghi lại
- **Evidence / PR:** Chưa có
- **Kỹ năng phù hợp:** Parquet, checksums, data export, Spark

## Checklist bàn giao

- [ ] Deliverable và test/evidence tồn tại trong repository.
- [ ] Acceptance criteria đã được kiểm tra.
- [ ] Contract/docs đã cập nhật và không chứa secret/build artifact.
- [ ] Reviewer độc lập được khuyến nghị cho P0.

## Tài liệu liên quan

### Handoff MLD-04/DAT-02 — 2026-10-15

MLD-04 đã có `SpacetimeFeatureEngine`, typed global/adaptive config, gate
invalid reason/count và Parquet readback. Xem [runbook feature](../../specs/SPACETIME_FEATURES.md).
Task này sẵn sàng triển khai nhưng chưa Done: local feature snapshot và DAT-02
development pilot không phải official VALIDATED/EXPORTED. Không lấy coverage
PARTIAL của nguồn 14 ngày làm acceptance nghiên cứu; không sửa source BUILDING
chỉ để mở official experiment. Assignee/reviewer giữ nguyên, chưa có approval.

- [Roadmap HDBSCAN](../HDBSCAN_WORKSTREAM.md)
- [MLD-04](./MLD-04.md)
