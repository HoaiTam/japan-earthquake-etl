---
task_id: "MLD-01"
status: "In Progress"
week: 4
block: "G - ML Dataset"
workstream: "Dataset identity"
scope: "Core"
priority: "P0"
effort_hours: 5
assignee: "Trang"
reviewer: "unassigned"
dependencies: ["CON-03", "GLD-04"]
---

# MLD-01 - Pin Gold snapshot và tạo dataset manifest

## Mục đích

Cố định đúng Gold input để mọi thuật toán đọc cùng dữ liệu và experiment cũ
vẫn tái lập được khi Gold có revision/snapshot mới.

## Phạm vi công việc

- Resolve một snapshot đã `Published` của `gold.earthquake_event_current`.
- Sinh `dataset_id` bất biến từ period/config/version, không dùng timestamp ngẫu nhiên làm identity duy nhất.
- Ghi `ml.dataset_manifest` với snapshot ID, cutoff, filter JSON, feature/config version, code version và trạng thái lifecycle.
- Tách manifest cho reproduction `2000-01-01..2018-09-30` và extension `2018-10-01..2023-12-31`.

## Deliverable

- Spark/SQL logic resolve snapshot và materialize dataset manifest.
- Schema/test fixture cho manifest reproduction và extension.
- Contract trạng thái tối thiểu `BUILDING`, `VALIDATED`, `EXPORTED`, `REJECTED`.

## Tiêu chí hoàn thành

- [ ] Cùng snapshot/config tạo cùng identity logic hoặc bị phát hiện là rerun, không tạo dataset mơ hồ.
- [ ] Manifest giữ `gold_snapshot_id`, cutoff, filter, feature/config/code version và timestamp UTC.
- [ ] Dataset không resolve từ bảng `current` lần nữa sau khi đã pin snapshot.
- [ ] Snapshot chưa Published hoặc filter period sai bị reject trước khi build candidate.

## Hard dependency

- [CON-03](./CON-03.md)
- [GLD-04](./GLD-04.md)

## Cách làm song song

### Phân công tuần 4 mở rộng

- **Owner / effort:** Trang, 5h Core; đưa từ tuần 5 lên khối Gold + ML identity trong [kế hoạch tuần 4](../WEEK_4_PARALLEL_PLAN.md).
- **Làm trước:** Snapshot/publication metadata mock, canonical identity payload và period-boundary tests theo CON-03; hiện Backlog vì GLD-04 chưa Done.
- **Làm gì / có gì:** Resolve exact Published Gold snapshot, dataset ID/manifest cho reproduction và extension, lineage/filter/config/cutoff và rerun conflict detection.
- **Dùng để làm gì:** MLD-02/03 đọc cùng snapshot/config, không bị current table trôi giữa audit và candidate build.
- **Handoff / nghiệm thu:** Sau GLD-04 verify thật, pin snapshot và bàn giao manifest/coverage cho ThanhTris. Dữ liệu pilot 2000/2023 không phải toàn research period. Không fake VALIDATED/EXPORTED khi feature/export chưa đạt; manifest có thể BUILDING còn implementation identity task đạt acceptance riêng.

Có thể hoàn thành schema, identity rule và unit test bằng snapshot metadata giả
lập trước khi Gold thật sẵn sàng. Integration evidence với snapshot thật mới
chặn trạng thái `Done`.

## Ranh giới

- Không estimate `Mc`, tạo mainshock/window hoặc feature trong task này.
- Không query snapshot mới âm thầm khi rerun cùng `dataset_id`.
- Không ghi credential, data dump hoặc physical Iceberg file path vào manifest.

## Theo dõi

- **Trạng thái:** In Progress
- **Assignee:** Trang
- **Reviewer:** Chưa ghi lại
- **Evidence / PR:** 5 DatasetIdentityPlanTest tests passed; metadata-only schema/identity fixtures, no live resolver/materialization. GLD-04 Published snapshot evidence still required. Reviewer unassigned.
- **Kỹ năng phù hợp:** Iceberg snapshots, Spark SQL, data lineage

## Checklist bàn giao

- [ ] Deliverable và test/evidence tồn tại trong repository.
- [ ] Acceptance criteria đã được kiểm tra.
- [ ] Contract/docs đã cập nhật và không chứa secret/build artifact.
- [ ] Reviewer độc lập được khuyến nghị cho P0.

## Tài liệu liên quan

- [Kế hoạch tuần 4 mở rộng](../WEEK_4_PARALLEL_PLAN.md)
- [Roadmap HDBSCAN](../HDBSCAN_WORKSTREAM.md)
- [Logical data model](../../specs/SILVER_GOLD_DATA_MODEL.md)
- [ML logical data model](../../specs/ML_DATA_MODEL.md)
- [GLD-04](./GLD-04.md)
