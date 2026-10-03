---
task_id: "MLD-04"
status: "Backlog"
week: 6
block: "G - ML Dataset"
workstream: "Spacetime features"
scope: "Core"
priority: "P0"
effort_hours: 6
assignee: "unassigned"
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

- [ ] Không đưa latitude/longitude degree trực tiếp vào Euclidean clustering.
- [ ] Mainshock vector bằng zero trong tolerance và `delta_time_hours` giữ đúng dấu.
- [ ] Mọi feature hữu hạn; invalid rows có reason/count thay vì bị drop im lặng.
- [ ] Global/adaptive scale có version, giá trị thực tế lưu cùng dataset.
- [ ] Magnitude không nằm trong vector baseline 4-D.

## Hard dependency

- [MLD-03](./MLD-03.md)

## Cách làm song song

Formula và unit tests có thể triển khai từ fixture tọa độ nhỏ; chỉ integration
snapshot cần candidate output thật.

## Ranh giới

- Không fit DBSCAN/HDBSCAN trong Spark task này.
- Không thay local approximation bằng projection mới mà không version hóa.
- Không tune scale bằng extension output.

## Theo dõi

- **Trạng thái:** Backlog
- **Assignee:** Chưa ghi lại
- **Reviewer:** Chưa ghi lại
- **Evidence / PR:** Chưa có
- **Kỹ năng phù hợp:** Geodesy basics, Spark transformations, numerical testing

## Checklist bàn giao

- [ ] Deliverable và test/evidence tồn tại trong repository.
- [ ] Acceptance criteria đã được kiểm tra.
- [ ] Contract/docs đã cập nhật và không chứa secret/build artifact.
- [ ] Reviewer độc lập được khuyến nghị cho P0.

## Tài liệu liên quan

- [Roadmap HDBSCAN](../HDBSCAN_WORKSTREAM.md)
- [MLD-03](./MLD-03.md)
