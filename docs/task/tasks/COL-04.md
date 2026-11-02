---
task_id: "COL-04"
status: "Done"
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

- [x] Test mới nhỏ, deterministic, không network/credential/Docker.
- [x] Không giảm test cũ hoặc sửa expected chỉ để PASS; báo exact test counts.
- [x] Fixture không được gọi là pilot thật; pilot input giữ nguyên checksum.
- [x] Rerun/overlap/noise/tie được đối soát theo contract, failure reasons rõ.
- [x] Development output không official `_SUCCESS.json` hoặc import `ml.*`.
- [x] Báo test matrix và giới hạn chưa cover; notebook source sạch output.

## Dependency / song song / giới hạn

[DAT-02](./DAT-02.md) đã có pilot. EXP-03 code đã merge, có thể test ngay dù
EXP-02 official còn In Progress. Cloud fit vẫn cần local gate đúng scope theo
runbook; không bỏ dependency nghiên cứu chính thức của EXP-03/EXP-04.
Nghiệm thu COL-04 không thay official reproduction run của EXP-03.

## Theo dõi

- **Trạng thái:** Done
- **Assignee:** ThanhTris
- **Reviewer:** unassigned
- **Evidence / PR:** Hoàn thành bộ test regression mở rộng trong `tests/test_adaptive_hdbscan.py` (tổng 44 tests, 100% PASS offline trong 18.30s). Bổ sung assertion cluster không noise (`cluster_id >= 0`) cho `test_edge_case_mainshock_classified_as_noise_with_valid_cluster`. Chạy fresh-kernel `inspect-exp03-notebook.py --pilot-bundle staging/ml-pilot/pilot-kit/bundle` đạt `RESTART_RUN_ALL_PASSED` (7.33s), xác thực pilot thật `pilot_196b6d4081d347e1c289e331022658fbbe572dd33181ad987900895c1f1b189c` (279 candidate rows, 2 mainshocks, feature SHA `3baddf613f237d2117248ad9095230ec17e428ea47f73d499623035d3097d2e6`), tích hợp gate EXP-02 phối hợp cùng #113; notebook source sạch output. PR #116 trên branch `feat/col-04-adaptive-edge-regression`.
