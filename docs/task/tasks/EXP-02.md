---
task_id: "EXP-02"
status: "In Progress"
week: 5
block: "H - Experiment"
workstream: "Baseline algorithms"
scope: "Core"
priority: "P0"
effort_hours: 7
assignee: "rosy179"
reviewer: "unassigned"
dependencies: ["EXP-01", "DAT-02", "DAT-03", "MLD-05"]
---

# EXP-02 - Chạy Window, DBSCAN và HDBSCAN global

## Mục đích

Tạo ba baseline trên cùng reproduction dataset để đánh giá liệu clustering có
giảm background tốt hơn window rule và HDBSCAN có lợi hơn DBSCAN hay không.
Bắt đầu bằng development pilot trên Jupyter để kiểm tra kỹ thuật; pilot không
được dùng làm bằng chứng model tốt hơn trên catalog nghiên cứu.

## Phạm vi công việc

- Window baseline: POST event trong counting window được đánh dấu candidate.
- Fit DBSCAN và HDBSCAN global riêng cho từng mainshock window trên feature 4-D.
- Lấy cluster chứa chính mainshock; nếu mainshock label `-1`, ghi no productive sequence.
- Gắn PRE/MAINSHOCK/POST, cluster/noise/probability và runtime/memory theo window.

## Giai đoạn thực hiện — Jupyter trước, Colab sau

1. **Local development:** dùng DAT-02 với EXP-01, fit ba baseline trên cùng
   pilot; ghi plots/counts/noise/failure/runtime và local-smoke-report.json theo
   [workflow](../../specs/JUPYTER_PILOT_WORKFLOW.md). Chạy restart kernel → run all.
2. **Colab pilot:** chỉ sau local_smoke_status=PASSED và colab_migration_allowed=true,
   chuyển cùng code/config/package lock/pilot bytes; đối soát invariants/tolerance.
3. **Official research:** sau MLD-05 EXPORTED, dùng official result contract MLI-01;
   source dataset BUILDING/PARTIAL pilot không được nâng lifecycle cho kịp lịch.

Bước 1 có thể làm trước MLD-05, nhưng chưa đủ để EXP-02 Done hoặc research-ready.
Nếu notebook fit thành công mà sai count/roles/lineage thì local gate vẫn FAILED.
Noise/no productive sequence hợp lệ không bị coi là implementation lỗi.

## Deliverable

- Implementations/config cho `WINDOW`, `DBSCAN`, `HDBSCAN_GLOBAL`.
- Jupyter local report/plots + development pilot result manifest riêng.
- Colab pilot portability evidence sau local gate.
- Membership và sequence summary official bundle theo contract khi MLD-05 đạt.
- Unit/smoke cases: mainshock cluster hợp lệ, mainshock noise, nhiều cluster và empty POST.

## Tiêu chí hoàn thành

- [x] Local ba thuật toán cùng pilot_id/checksum; không mutate input.
- [x] Local smoke report PASSED trước Colab; restart/run-all và cases lỗi/noise/count đạt.
- [ ] Colab dùng cùng code/bundle/config, đối soát invariants/tolerance; không rewrite model (chờ môi trường Colab sau local gate).
- [ ] Official run pin EXPORTED dataset; pilot không phát _SUCCESS/RESULT_READY/CANDIDATE (MLD-05 component đã Done; chờ DAT-03 catalog nghiên cứu thật và release đủ gate).
- [x] HDBSCAN/DBSCAN chạy theo từng mainshock, không fit toàn catalog.
- [x] Chỉ cluster chứa mainshock được coi là sequence; không chọn cluster gần nhất khi mainshock noise.
- [x] Membership probability null đúng với thuật toán không cung cấp.
- [x] Thuật ngữ output là candidate, không khẳng định causal aftershock.

## Hard dependency

- [EXP-01](./EXP-01.md)
- [DAT-02](./DAT-02.md) — local pilot gate.
- [DAT-03](./DAT-03.md) — catalog thật/release cho final official acceptance;
  không chặn local development bằng DAT-02, không tạo dependency ngược.
- [MLD-05](./MLD-05.md) — final official acceptance; không chặn stage local development.

## Cách làm song song

Ba implementation có thể giao riêng cho các thành viên nếu cùng dùng harness,
schema output và feature fixture do `EXP-01` khóa.

## Ranh giới

- Không tune bằng mắt riêng từng sequence.
- Không dùng accuracy/F1 khi chưa có ground truth.
- Không đưa magnitude vào vector baseline 4-D trong task này.

## Phân công tuần 5

- **Owner / effort:** rosy179, 7h Core; [kế hoạch và handoff](../WEEK_5_PARALLEL_PLAN.md).
- **Làm gì / có gì:** Window/DBSCAN/HDBSCAN global fit per-mainshock; mainshock noise, nhiều cluster, empty POST, role/probability/runtime tests.
- **Dùng để làm gì / output:** Ba baseline dùng cùng dataset/config versions; membership/summary result bundle cho MLI-03, không tuyên bố causal.
- **Bắt đầu ngay / gate:** Algorithm fixtures làm ngay; local integration cần EXP-01/DAT-02; official acceptance cần MLD-05, Colab sau local PASSED. Mainshock noise không chọn cluster gần nhất; giữ failure/noise counts. Coverage chưa đủ thì chưa nhận nghiên cứu toàn kỳ.
- **Theo dõi:** Phân công không có nghĩa đã triển khai; chỉ đổi In Progress khi bắt đầu, Done khi đủ acceptance/evidence. Reviewer hiện unassigned, chưa có approval.

## Tuần 7 — rosy179: baseline Colab và parity

Giữ owner rosy179, In Progress. Theo [kế hoạch tuần7](../WEEK_7_COLAB_PLAN.md),
đọc [setup](../../runbooks/WEEK_7_COLAB_SETUP.md), chạy ba default baselines từ
exact pilot279/2, đối soát local–Colab bằng grain/roles/noise/membership và
tolerance công bố; output mới từng run, không sửa input/marker để pass.
COL-02 sở hữu bounded sweep; COL-03 sở hữu baseline metrics/plots, không
đưa effort các task mới vào estimate7h lịch sử của EXP-02.
Không chờ full DAT-03 để nghiệm thu pilot portability; official run vẫn cần
release thật EXPORTED. Colab pilot đạt chưa chuyển toàn EXP-02 Done.

## Theo dõi

- **Trạng thái:** In Progress
- **Assignee:** rosy179
- **Reviewer:** unassigned
- **Evidence / PR:** [docs/evidence/EXP-02.md](../../evidence/EXP-02.md)
- **Kỹ năng phù hợp:** DBSCAN, HDBSCAN, unsupervised learning

## Checklist bàn giao

- [x] Deliverable và test/evidence tồn tại trong repository.
- [ ] Acceptance criteria đã được kiểm tra (local development hoàn tất; Colab và official run chờ bước tiếp theo).
- [x] Contract/docs đã cập nhật và không chứa secret/data dump.
- [ ] Reviewer độc lập được khuyến nghị cho P0.

## Tài liệu liên quan

- [DAT-02](./DAT-02.md), [MLD-05](./MLD-05.md)
- [Jupyter pilot workflow](../../specs/JUPYTER_PILOT_WORKFLOW.md)

- [Roadmap HDBSCAN](../HDBSCAN_WORKSTREAM.md)
- [EXP-01](./EXP-01.md)

