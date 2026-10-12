---
task_id: "MLD-03"
status: "Done"
week: 4
block: "G - ML Dataset"
workstream: "Mainshock and candidate windows"
scope: "Core"
priority: "P0"
effort_hours: 7
assignee: "rosy179"
reviewer: "HoaiTam"
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
- [x] Nghiệm thu input Published JMA được pin qua MLD-01, audit/Mc thật có scope từ MLD-02; lưu evidence tái lập.

## Hard dependency

- [MLD-01](./MLD-01.md)
- [MLD-02](./MLD-02.md)

## Cách làm song song

### Phân công tuần 4 mở rộng

- **Owner / effort:** rosy179, 7h Core; đưa từ tuần 5 lên [kế hoạch tuần 4](../WEEK_4_PARALLEL_PLAN.md), sau MLD-01/02 cho integration thật.
- **Làm trước:** Mainshock/window resolver, bucket-join và export tests bằng Gold/candidate fixture, Mc fixture có version; phần fixture được phép, không thay evidence integration thật.
- **Làm gì / có gì:** Mainshock depth 50–200 km inclusive/magnitude >5.5, versioned pre/post/radius rule, time/bounding-box/distance prefilter, unique window grain và hard-limit reasons/counts.
- **Dùng để làm gì:** Cửa sổ candidate cho MLD-04/05, không full cross join catalog và không coi event ở nhiều windows là duplicate sai.
- **Handoff / nghiệm thu:** Pin input qua MLD-01, dùng Mc thật có scope từ MLD-02; test physical plan, PRE/POST/mainshock presence và no silent truncate. Output mới là window stage của candidate, không giả feature vector/export hoặc aftershock labels đã có; dataset giữ BUILDING tới gate feature/export.

Window resolver/range-join tests có thể dùng candidate fixture và một `Mc` giả
lập có version; integration chỉ thay input bằng output thật của `MLD-02`.

## Nghiệm thu Published JMA / Mc và window stage

[MLD-01 - Pin Gold snapshot và tạo dataset manifest](./MLD-01.md) đã Done trên main.
[MLD-02 - Audit Gold và xác định magnitude of completeness](./MLD-02.md) đã Done
theo pilot Published JMA có scope, xem [receipt và handoff](../../evidence/MLD-02.md).
MLD-03 đã Done trên branch `fix/mld-03-pinned-jma-windows` từ main
`e829fee3b690a1244dfa7d100cc955ac12c4f915`. Native CLI revalidate Published pins,
immutable manifest identity, full window/selection config và audit/Mc handoff;
QA flow chọn bounded mainshock-centred JMA sample, pin/audit lại và đọc lại
Parquet, đối soát từng window bằng Trino oracle. Nghiệm thu thật đã đạt trên
hai splits: reproduction Mc1.2/mainshocks2/candidates279 (PRE22,POST255),
extension Mc0.9/mainshock1/candidates8 (PRE1,POST6), self đúng một lần/window.
Rerun logical fingerprints/counts không đổi, bucket join và readback đều PASS;
controller verify checksum rồi release own lease. Xem [receipt metadata](../../evidence/MLD-03-window-pilot.json).
Pilot MLD-02 có 0 mainshock (`magnitude >5.5`, depth50–200km); Trino cho max
primary-JMA natural/ROI magnitude5.1. Input này kiểm tra được empty path, không
nghiệm thu windows non-empty/PRE/POST. Vì vậy nghiệm thu hiện tại dùng slice14-day
có mainshock và pin/audit lại; không hạ threshold hoặc đưa USGS vào JMA pool.

Đã sửa parser signed-padding degree từ real JMA2000 (`- 1`, `- 66`), version
`slv-03-v1.1`, JSON round-trip config và nullable Gold UDF; không bỏ reject
hoặc nới quality gate. Java targeted82/Python206 tests đạt. Dataset vẫn BUILDING,
research_ready=false: scope pilot14 ngày có selection bias, không full period,
feature/scaling/export thuộc MLD-04/05. Chi tiết và recovery nằm trong
[evidence hiện tại](../../evidence/MLD-03.md).

## Ranh giới

- Window là prefilter/baseline, không phải nhãn dư chấn.
- Không hard-code một radius/duration cho mọi magnitude mà không có config version.
- Không tự loại nested mainshock; giữ field để giải quyết downstream.

## Theo dõi

- **Trạng thái:** Done
- **Assignee:** rosy179
- **Reviewer:** HoaiTam (được phân công; chưa có review/approval)
- **Evidence / PR:** Nền tảng review [PR #61](https://github.com/HoaiTam/japan-earthquake-etl/pull/61) đã merge; bản vá hiện tại là branch `fix/mld-03-pinned-jma-windows`, chưa commit/push/mở PR. Xem [Evidence MLD-03](../../evidence/MLD-03.md), [receipt Published JMA/window pilot](../../evidence/MLD-03-window-pilot.json) và [Đặc tả kỹ thuật](../../specs/MAINSHOCK_AND_CANDIDATE_WINDOWS.md).
- **Kỹ năng phù hợp:** Spark range join, geospatial filtering, test design

## Checklist bàn giao

- [x] Deliverable và test/evidence tồn tại trong repository.
- [x] Acceptance kỹ thuật đã có test tự động.
- [x] Acceptance integration thật qua MLD-01/02 và native window readback/Trino oracle đã đạt trên pilot có scope; không dùng fixture làm evidence thật.
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
