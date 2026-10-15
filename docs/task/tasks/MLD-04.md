---
task_id: "MLD-04"
status: "Done"
week: 5
block: "G - ML Dataset"
workstream: "Spacetime features"
scope: "Core"
priority: "P0"
effort_hours: 6
assignee: "rosy179"
reviewer: "unassigned"
dependencies: ["MLD-03"]
---

# MLD-04 - Tạo và scale feature không-thời gian 4-D

## Mục đích

Biến candidate sang feature có đơn vị đúng và scale có version để time không
áp đảo distance, đồng thời bảo toàn dấu PRE/POST quanh mainshock.

## Phạm vi công việc

- Tính `dx_km`, `dy_km`, `dz_km`, `distance_3d_km`, `delta_time_hours` tương đối với mainshock.
- Đổi latitude/longitude degree sang local kilomet; `lat0` đổi radian trước `cos`.
- Tạo `[x_scaled,y_scaled,z_scaled,time_scaled]` cho global và adaptive scaling.
- Lưu scale values/version, kiểm tra finite/tolerance và mainshock vector `[0,0,0,0]`.

Baseline formula:

```text
dx_km = 111.32 * cos(radians(mainshock_lat)) * (candidate_lon - mainshock_lon)
dy_km = 110.57 * (candidate_lat - mainshock_lat)
dz_km = candidate_depth_km - mainshock_depth_km
distance_3d_km = sqrt(dx_km^2 + dy_km^2 + dz_km^2)

x_scaled = dx_km / R_scale
y_scaled = dy_km / R_scale
z_scaled = dz_km / Z_scale
time_scaled = delta_time_hours / T_scale
```

## Deliverable

- Spark feature transformation và config resolver global/adaptive.
- Feature schema/tests cho đơn vị, timezone, sign, boundary và NaN/Infinity.
- Candidate snapshot hoàn chỉnh sẵn sàng cho export.

## Tiêu chí hoàn thành

- [x] Không đưa latitude/longitude degree trực tiếp vào Euclidean clustering.
- [x] Mainshock vector bằng zero trong tolerance và `delta_time_hours` giữ đúng dấu.
- [x] Mọi feature hữu hạn; invalid rows có reason/count thay vì bị drop im lặng.
- [x] Global/adaptive scale có version, giá trị thực tế lưu cùng dataset.
- [x] Magnitude không nằm trong vector baseline 4-D.

## Hard dependency

- [MLD-03](./MLD-03.md)

## Cách làm song song

Formula và unit tests có thể triển khai từ fixture tọa độ nhỏ; chỉ integration
snapshot cần candidate output thật.

## Ranh giới

- Không fit DBSCAN/HDBSCAN trong Spark task này.
- Không thay local approximation bằng projection mới mà không version hóa.
- Không tune scale bằng extension output.

## Phân công tuần 5

- **Owner / effort:** rosy179, 6h Core; [kế hoạch và handoff](../WEEK_5_PARALLEL_PLAN.md).
- **Làm gì / có gì:** Tạo feature km/giờ theo mainshock, scale vector 4-D có version; fixtures zero-self, PRE/POST/time tie, null/NaN/scale invalid.
- **Dùng để làm gì / output:** Feature snapshot/schema/count summary cho MLD-05 và fixture nhỏ cho EXP-01; không thêm magnitude vào vector hoặc đổi candidate grain.
- **Bắt đầu ngay / gate:** Code bằng MLD-03 fixtures ngay. Readback feature trên exact candidate input, finite/zero-self/resource checks đạt; không suy pilot đã research_ready.
- **Theo dõi:** Phân công không có nghĩa đã triển khai; chỉ đổi In Progress khi bắt đầu, Done khi đủ acceptance/evidence. Reviewer hiện unassigned, chưa có approval.

## Điều chỉnh Jupyter-first — 2026-10-14

DAT-02 dùng chung Java/Spark feature implementation/config và small fixture của task này để tạo pilot; không viết lại coordinate/scaling trong notebook. Bàn giao feature schema/version/test cases sớm; DAT-02 không là hard dependency của MLD-04.

- [DAT-02](./DAT-02.md)
- [Workflow Jupyter → Colab](../../specs/JUPYTER_PILOT_WORKFLOW.md)

## Theo dõi

- **Trạng thái:** Done
- **Assignee:** rosy179
- **Reviewer:** unassigned
- **Evidence / PR:** Hoàn tất 2026-10-15, bắt đầu branch `feat/mld-04-spacetime-features` từ main `884c422` (PR #75); bản cuối mang cùng DAT-02 trên `feat/dat-02-jupyter-pilot-dataset`, chưa commit/push. [Feature readback](../../evidence/MLD-04-feature-readback.json): global/adaptive đều 279 rows, 0 invalid, Parquet readback; 12 feature tests, gồm ba JVM độc lập byte-identical writes; full Java suite 363 tests đạt. SQL chỉ SELECT, parent vẫn BUILDING. Không có approval reviewer.
- **Kỹ năng phù hợp:** Geodesy basics, Spark transformations, numerical testing

## Checklist bàn giao

- [x] Deliverable và test/evidence tồn tại trong repository.
- [x] Acceptance criteria đã được kiểm tra.
- [x] Contract/docs đã cập nhật và không chứa secret/build artifact.
- [ ] Reviewer độc lập được khuyến nghị cho P0.

## Tài liệu liên quan

- [Roadmap HDBSCAN](../HDBSCAN_WORKSTREAM.md)
- [MLD-03](./MLD-03.md)
- [Feature implementation/runbook](../../specs/SPACETIME_FEATURES.md)
