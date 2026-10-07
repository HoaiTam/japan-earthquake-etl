---
task_id: "MLD-03"
status: "Backlog"
week: 4
block: "G - ML Dataset"
workstream: "Mainshock and candidate windows"
scope: "Core"
priority: "P0"
effort_hours: 7
assignee: "ThanhTris"
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
- Range join theo time → bounding box → distance chính xác; giữ candidate `magnitude >= Mc`.
- Ghi mainshock snapshot, candidate grain và metadata count/resource guard; chưa gắn nhãn aftershock.

## Deliverable

- `ml.mainshock_candidate_snapshot` và phần window của `ml.sequence_candidate_snapshot`.
- Window/config resolver có version, Spark join logic và unit/integration tests.
- Count theo window, nested-mainshock fields và hard-limit behavior.

## Tiêu chí hoàn thành

- [ ] Grain `(dataset_id, mainshock_event_id, candidate_event_id)` là duy nhất.
- [ ] Mỗi window chứa chính mainshock đúng một lần và giữ candidate PRE/POST.
- [ ] Không cross join mainshock với toàn catalog; physical plan/test chứng minh có prefilter.
- [ ] Event có thể ở nhiều window mà không bị coi là duplicate sai.
- [ ] Window vượt hard limit bị fail/flag có reason, không âm thầm truncate.

## Hard dependency

- [MLD-01](./MLD-01.md)
- [MLD-02](./MLD-02.md)

## Cách làm song song

### Phân công tuần 4 mở rộng

- **Owner / effort:** ThanhTris, 7h Core; đưa từ tuần 5 lên [kế hoạch tuần 4](../WEEK_4_PARALLEL_PLAN.md), sau MLD-01/02 cho integration thật.
- **Làm trước:** Mainshock/window resolver và range-join tests bằng Gold/candidate fixture, versioned Mc mock; hiện Backlog, chỉ prepare parts được phép.
- **Làm gì / có gì:** Mainshock depth 50–200 km inclusive/magnitude >5.5, versioned pre/post/radius rule, time/bounding-box/distance prefilter, unique window grain và hard-limit reasons/counts.
- **Dùng để làm gì:** Cửa sổ candidate cho MLD-04/05, không full cross join catalog và không coi event ở nhiều windows là duplicate sai.
- **Handoff / nghiệm thu:** Pin input qua MLD-01, dùng Mc thật có scope từ MLD-02; test physical plan, PRE/POST/mainshock presence và no silent truncate. Output mới là window stage của candidate, không giả feature vector/export hoặc aftershock labels đã có; dataset giữ BUILDING tới gate feature/export.

Window resolver/range-join tests có thể dùng candidate fixture và một `Mc` giả
lập có version; integration chỉ thay input bằng output thật của `MLD-02`.

## Ranh giới

- Window là prefilter/baseline, không phải nhãn dư chấn.
- Không hard-code một radius/duration cho mọi magnitude mà không có config version.
- Không tự loại nested mainshock; giữ field để giải quyết downstream.

## Theo dõi

- **Trạng thái:** Backlog
- **Assignee:** ThanhTris
- **Reviewer:** Chưa ghi lại
- **Evidence / PR:** Chưa có
- **Kỹ năng phù hợp:** Spark range join, geospatial filtering, test design

## Checklist bàn giao

- [ ] Deliverable và test/evidence tồn tại trong repository.
- [ ] Acceptance criteria đã được kiểm tra.
- [ ] Contract/docs đã cập nhật và không chứa secret/build artifact.
- [ ] Reviewer độc lập được khuyến nghị cho P0.

## Tài liệu liên quan

- [Kế hoạch tuần 4 mở rộng](../WEEK_4_PARALLEL_PLAN.md)
- [ML logical model](../../specs/ML_DATA_MODEL.md)
- [Roadmap HDBSCAN](../HDBSCAN_WORKSTREAM.md)
- [MLD-02](./MLD-02.md)
