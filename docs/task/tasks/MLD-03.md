---
task_id: "MLD-03"
status: "Needs Update"
week: 4
block: "G - ML Dataset"
workstream: "Mainshock and candidate windows"
scope: "Core"
priority: "P0"
effort_hours: 7
assignee: "HoaiTam"
reviewer: "unassigned"
dependencies: ["MLD-01", "MLD-02"]
---

# MLD-03 - Chọn mainshock và tạo candidate windows

## Mục đích

Giảm catalog thành các cửa sổ rộng theo từng mainshock để giữ background/noise
cho clustering nhưng tránh full cross join toàn Nhật Bản.

## Phạm vi công việc

- Chọn mainshock baseline: depth `50–200 km`, magnitude `>5.5`, trong period của dataset.
- Version hóa magnitude-dependent pre/post window và search radius theo rule Gardner–Knopoff/Uhrhammer mở rộng.
- Equi-join bucket UTC-day/vĩ độ/kinh độ → time/bounding-box residual → distance phẳng cục bộ; giữ candidate `magnitude >= Mc`.
- Ghi mainshock snapshot, candidate grain và metadata count/resource guard; chưa gắn nhãn aftershock.

## Deliverable

- `ml.mainshock_candidate_snapshot` và phần window của `ml.sequence_candidate_snapshot`.
- Window/config resolver có version, Spark join logic và unit/integration tests.
- Count theo window, nested-mainshock fields và hard-limit behavior.

## Tiêu chí hoàn thành

- [x] Grain `(dataset_id, mainshock_event_id, candidate_event_id)` là duy nhất.
- [x] Mỗi window chứa chính mainshock đúng một lần và giữ candidate PRE/POST.
- [x] Physical plan có hash/sort-merge bucket join, không có CartesianProduct/BroadcastNestedLoopJoin, kể cả khi tắt broadcast.
- [x] Event có thể ở nhiều window mà không bị coi là duplicate sai.
- [x] Window vượt hard limit bị fail/flag có reason, không âm thầm truncate.
- [x] Invariants kiểm tra phân tán; giữ timestamp microseconds; Parquet lỗi phải fail, không fallback JSON.
- [ ] Nghiệm thu input Published JMA được pin qua MLD-01, audit/Mc thật có scope từ MLD-02; lưu evidence tái lập.

## Hard dependency

- [MLD-01](./MLD-01.md)
- [MLD-02](./MLD-02.md)

## Cách làm song song

### Phân công tuần 4 mở rộng

- **Owner / effort:** HoaiTam, 7h Core; đưa từ tuần 5 lên [kế hoạch tuần 4](../WEEK_4_PARALLEL_PLAN.md), sau MLD-01/02 cho integration thật.
- **Làm trước:** Mainshock/window resolver, bucket-join và export tests bằng Gold/candidate fixture, Mc fixture có version; phần fixture được phép, không thay evidence integration thật.
- **Làm gì / có gì:** Mainshock depth 50–200 km inclusive/magnitude >5.5, versioned pre/post/radius rule, time/bounding-box/distance prefilter, unique window grain và hard-limit reasons/counts.
- **Dùng để làm gì:** Cửa sổ candidate cho MLD-04/05, không full cross join catalog và không coi event ở nhiều windows là duplicate sai.
- **Handoff / nghiệm thu:** Pin input qua MLD-01, dùng Mc thật có scope từ MLD-02; test physical plan, PRE/POST/mainshock presence và no silent truncate. Output mới là window stage của candidate, không giả feature vector/export hoặc aftershock labels đã có; dataset giữ BUILDING tới gate feature/export.

Window resolver/range-join tests có thể dùng candidate fixture và một `Mc` giả
lập có version; integration chỉ thay input bằng output thật của `MLD-02`.

## Phần cần update để nghiệm thu

[MLD-01 - Pin Gold snapshot và tạo dataset manifest](./MLD-01.md) đã Done trên main.
[MLD-02 - Audit Gold và xác định magnitude of completeness](./MLD-02.md) còn Needs Update:
thiếu evidence audit/Mc thật trên Published JMA. Khi upstream đạt gate, chạy lại
MLD-03 với dataset_id/snapshot đã pin và Mc được chấp nhận (không dùng central
Mc khi estimator không reliable), đối soát count/PRE/POST/export và lưu evidence.
Bản vá hiện tại có thể tích hợp code/fixture, nhưng chưa nghiệm thu dataset thực.

## Ranh giới

- Window là prefilter/baseline, không phải nhãn dư chấn.
- Không hard-code một radius/duration cho mọi magnitude mà không có config version.
- Không tự loại nested mainshock; giữ field để giải quyết downstream.

## Theo dõi

- **Trạng thái:** Needs Update
- **Assignee:** HoaiTam
- **Reviewer:** unassigned; chưa có review độc lập
- **Evidence / PR:** Bản vá trực tiếp [PR #61](https://github.com/HoaiTam/japan-earthquake-etl/pull/61), gồm 6 resolver, 6 engine, 1 audit→window fixture integration và 8 regression tests; kết quả chạy nằm trong evidence, không coi fixture là Published JMA. Xem [Evidence MLD-03](../../evidence/MLD-03.md) và [Đặc tả kỹ thuật](../../specs/MAINSHOCK_AND_CANDIDATE_WINDOWS.md).
- **Kỹ năng phù hợp:** Spark range join, geospatial filtering, test design

## Checklist bàn giao

- [x] Deliverable và test/evidence tồn tại trong repository.
- [x] Acceptance kỹ thuật đã có test tự động.
- [ ] Acceptance integration thật qua MLD-02 chưa đạt; không chuyển Done dựa vào fixture.
- [x] Contract/docs đã cập nhật và không chứa secret/build artifact.
- [ ] Reviewer độc lập được khuyến nghị cho P0.

## Tài liệu liên quan

- [Kế hoạch tuần 4 mở rộng](../WEEK_4_PARALLEL_PLAN.md)
- [ML logical model](../../specs/ML_DATA_MODEL.md)
- [Roadmap HDBSCAN](../HDBSCAN_WORKSTREAM.md)
- [Đặc tả Mainshock và candidate windows](../../specs/MAINSHOCK_AND_CANDIDATE_WINDOWS.md)
- [Evidence MLD-03](../../evidence/MLD-03.md)
- [MLD-02](./MLD-02.md)
- [MLD-04](./MLD-04.md)
