---
task_id: "MLD-02"
status: "Backlog"
week: 5
block: "G - ML Dataset"
workstream: "Input audit and completeness"
scope: "Core"
priority: "P0"
effort_hours: 7
assignee: "unassigned"
reviewer: "unassigned"
dependencies: ["MLD-01"]
---

# MLD-02 - Audit Gold và xác định magnitude of completeness

## Mục đích

Loại bias do detection threshold/catalog change trước khi đếm sequence và
version hóa `Mc` để sensitivity experiment có input giải thích được.

## Phạm vi công việc

- Audit uniqueness, finite coordinate/depth/magnitude, natural event, study area, JMA primary và `UNIFIED` era.
- Thống kê count theo năm, depth bin, magnitude bin và phát hiện biến động quanh catalog/network change.
- Implement phương pháp `Mc` pilot (ít nhất maximum curvature) theo period/region/depth group hoặc một ngưỡng bảo thủ có evidence.
- Ghi `mc_method`, `mc_value`, `mc_region_version`, audit counts và reason codes vào manifest/output audit.

## Deliverable

- Gold input audit job/report và reject/exclusion reason summary.
- `Mc` estimator/config có version và fixture histogram xác định được.
- Giá trị trung tâm cùng hai giá trị lân cận cho sensitivity, không hard-code không giải thích.

## Tiêu chí hoàn thành

- [ ] Không xóa event khỏi Gold; chỉ exclude khỏi dataset với reason code.
- [ ] Có reason tối thiểu cho missing depth/magnitude, ngoài period, dưới completeness và source không comparable.
- [ ] `Mc` có method/version/evidence và được áp dụng nhất quán trong cùng dataset.
- [ ] Audit counts đối soát được với snapshot manifest và không dùng dữ liệu extension để tune reproduction.

## Hard dependency

- [MLD-01](./MLD-01.md)

## Cách làm song song

Estimator và audit rules có thể phát triển bằng histogram/Gold fixture trước;
chỉ bước chốt `Mc` và counts cần snapshot thật từ `MLD-01`.

## Ranh giới

- Không gọi event dưới `Mc` là invalid source record.
- Không sao chép mặc định `Mc=2` mà không chạy audit trên dữ liệu project.
- Không chọn tham số từ extension period.

## Theo dõi

- **Trạng thái:** Backlog
- **Assignee:** Chưa ghi lại
- **Reviewer:** Chưa ghi lại
- **Evidence / PR:** Chưa có
- **Kỹ năng phù hợp:** Spark SQL, catalog completeness, data quality

## Checklist bàn giao

- [ ] Deliverable và test/evidence tồn tại trong repository.
- [ ] Acceptance criteria đã được kiểm tra.
- [ ] Contract/docs đã cập nhật và không chứa secret/build artifact.
- [ ] Reviewer độc lập được khuyến nghị cho P0.

## Tài liệu liên quan

- [Roadmap HDBSCAN](../HDBSCAN_WORKSTREAM.md)
- [MLD-01](./MLD-01.md)

