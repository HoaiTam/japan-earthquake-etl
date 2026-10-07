---
task_id: "MLD-02"
status: "Backlog"
week: 4
block: "G - ML Dataset"
workstream: "Input audit and completeness"
scope: "Core"
priority: "P0"
effort_hours: 7
assignee: "ThanhTris"
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

### Phân công tuần 4 mở rộng

- **Owner / effort:** ThanhTris, 7h Core; đưa từ tuần 5 lên khối Silver + ML audit/candidate trong [kế hoạch tuần 4](../WEEK_4_PARALLEL_PLAN.md).
- **Làm trước:** Gold/histogram fixtures, exclusion rules và versioned estimator/config; không chờ snapshot thật để viết unit, không chốt Mc giả làm evidence.
- **Làm gì / có gì:** Audit natural/ROI/primary-JMA/UNIFIED/finite fields, completeness pilot, central/sensitivity Mc values và reason/count report; không xóa event khỏi Gold.
- **Dùng để làm gì:** Candidate filter có completeness evidence và period/source coverage rõ, không tune reproduction bằng extension data.
- **Handoff / nghiệm thu:** Chờ MLD-01 của Trang pin snapshot thật, ghi audit/Mc report theo dataset/split/group và bàn giao eligible input/config cho MLD-03. JMA 2000 là reproduction pilot, JMA 2023 là extension; một năm không chứng minh Mc đại diện toàn 2000–2018. Nếu sample chưa đủ chốt estimator thì ghi limitation/giữ task chưa đạt, không hard-code Mc=2.

Estimator và audit rules có thể phát triển bằng histogram/Gold fixture trước;
chỉ bước chốt `Mc` và counts cần snapshot thật từ `MLD-01`.

## Ranh giới

- Không gọi event dưới `Mc` là invalid source record.
- Không sao chép mặc định `Mc=2` mà không chạy audit trên dữ liệu project.
- Không chọn tham số từ extension period.

## Theo dõi

- **Trạng thái:** Backlog
- **Assignee:** ThanhTris
- **Reviewer:** Chưa ghi lại
- **Evidence / PR:** Chưa có
- **Kỹ năng phù hợp:** Spark SQL, catalog completeness, data quality

## Checklist bàn giao

- [ ] Deliverable và test/evidence tồn tại trong repository.
- [ ] Acceptance criteria đã được kiểm tra.
- [ ] Contract/docs đã cập nhật và không chứa secret/build artifact.
- [ ] Reviewer độc lập được khuyến nghị cho P0.

## Tài liệu liên quan

- [Kế hoạch tuần 4 mở rộng](../WEEK_4_PARALLEL_PLAN.md)
- [ML logical model](../../specs/ML_DATA_MODEL.md)
- [Roadmap HDBSCAN](../HDBSCAN_WORKSTREAM.md)
- [MLD-01](./MLD-01.md)
