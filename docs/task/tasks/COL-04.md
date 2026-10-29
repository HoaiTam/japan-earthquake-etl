---
task_id: "COL-04"
status: "Backlog"
week: 7
block: "H - Experiment"
workstream: "Adaptive edge-case regression"
scope: "Core"
priority: "P1"
effort_hours: 3
assignee: "ThanhTris"
reviewer: "unassigned"
dependencies: ["DAT-02"]
---

# COL-04 - Kiểm thử biên, multi-sequence và rerun adaptive

## Mục đích / phân loại

**New**, khối 3. Bổ sung regression vào implementation EXP-03 đã có, tránh
lặp test cũ hoặc sửa pilot thật để tạo tình huống mong muốn.

## Setup / đầu vào

Đọc [EXP-03](./EXP-03.md), `tests/test_adaptive_hdbscan.py`, module adaptive,
[setup chung](../../runbooks/WEEK_7_COLAB_SETUP.md) và
[kế hoạch tuần](../WEEK_7_COLAB_PLAN.md). Fixture/unit test chạy offline bằng
Python model environment, không cần Docker hoặc chờ baseline cloud của rosy179.

## Phạm vi / cách làm

1. Đối chiếu coverage cũ trước; bổ sung test còn thiếu cho ít mẫu, self-only,
   mainshock noise, toàn noise, một/nhiều cluster và empty POST.
2. Fixture overlap hai window: giữ raw grain mainshock/candidate, count unique
   sau resolution; tie theo rule hiện có phải ambiguous, không chọn theo thứ tự.
3. Kiểm nested evidence và determinism khi đảo input order; cluster label có
   thể permute nên kiểm tập membership/resolution, không chỉ integer label.
4. Kiểm rerun bằng API hiện có, same/different config, tampered artifact và
   retry identity. Không sửa writer/marker để reuse output sai.
5. Chạy notebook EXP-03 bằng pilot thật trên fresh kernel, tách rõ real-pilot
   evidence và fixture test. Fix bug phải kèm regression và cập nhật docs.

## Tiêu chí nghiệm thu

- [ ] Test mới nhỏ, deterministic, không network/credential/Docker.
- [ ] Không giảm test cũ hoặc sửa expected chỉ để PASS; báo exact test counts.
- [ ] Fixture không được gọi là pilot thật; pilot input giữ nguyên checksum.
- [ ] Rerun/overlap/noise/tie được đối soát theo contract, failure reasons rõ.
- [ ] Development output không official `_SUCCESS.json` hoặc import `ml.*`.
- [ ] Báo test matrix và giới hạn chưa cover; notebook source sạch output.

## Dependency / song song / giới hạn

[DAT-02](./DAT-02.md) đã có pilot. EXP-03 code đã merge, có thể test ngay dù
EXP-02 official còn In Progress. Cloud fit vẫn cần local gate đúng scope theo
runbook; không bỏ dependency nghiên cứu chính thức của EXP-03/EXP-04.
Nghiệm thu COL-04 không thay official reproduction run của EXP-03.

## Theo dõi

- **Trạng thái:** Backlog
- **Assignee:** ThanhTris
- **Reviewer:** unassigned
- **Evidence / PR:** Chưa triển khai; chưa có test/evidence mới tuần 7.
