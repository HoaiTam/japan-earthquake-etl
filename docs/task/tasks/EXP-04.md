---
task_id: "EXP-04"
status: "Done"
week: 7
block: "H - Experiment"
workstream: "Scientific evaluation"
scope: "Core"
priority: "P0"
effort_hours: 7
assignee: "ThanhTris"
reviewer: "unassigned"
dependencies: ["EXP-02", "EXP-03"]
---

# EXP-04 - Đánh giá quality, physical consistency và stability

## Giao việc hiện hành — 11/11/2026

- **Owner:** ThanhTris; **status:** Done.
- **Làm phần gì / dùng để làm gì:** Đánh giá khoa học.
- **Bắt đầu độc lập:** Tái dùng evaluation harness/pilot #121 và COL-03 #117; chuẩn bị evaluator/config.
- **Đầu ra và điều kiện Done:** Sau EXP-02/03 official: DBCV/noise/Omori/stability với sample/failure/N/A reasons; freeze rule trước EXP-05, không kết luận từ pilot.
- **Lịch:** chuẩn bị 11–15/11, nghiệm thu tuần9 16–22/11; xem
  [kế hoạch và setup](../WEEK_9_REPLAN_20261111.md). Giữ hard dependencies,
  scope/effort lịch sử; các mục tuần cũ dưới đây là lịch sử.

Owner yêu cầu11/11: In Progress → Needs Update cho phần đã triển khai nhưng
chưa đủ acceptance. Không khẳng định từng Done toàn task hoặc mất code.
[Đối soát](../../evidence/TASK_STATUS_AUDIT_20261111.md).

## Mục đích

Đánh giá model bằng nhiều nhóm bằng chứng phù hợp với clustering không nhãn,
thay vì dùng accuracy/F1 hoặc chọn theo một metric duy nhất.

## Phạm vi công việc

- Clustering quality: DBCV, noise ratio, cluster size/probability, failure windows, runtime/peak memory.
- Physical consistency: counts/duration/spatial extent/largest aftershock và Modified Omori fit/error.
- Sensitivity: `Mc`, global/adaptive scaling, min cluster parameters, window changes, perturbation và bootstrap.
- Stability: Jaccard/ARI và các thành phần score được lưu riêng, không che bằng composite tùy ý.

## Deliverable

- Evaluation runner và metric schema/output.
- Reproduction comparison table cho Window/DBSCAN/HDBSCAN global/adaptive.
- Stability/physical consistency report với failure reason và uncertainty/limitation.

## Tiêu chí hoàn thành

- [x] Không dùng accuracy/F1 như ground-truth metric.
- [x] DBCV/noise, ít nhất một Omori metric và Jaccard stability được tính từ output thật.
- [x] Metric null/failure có reason code; không bỏ window fail khỏi mẫu số im lặng.
- [x] Báo cáo giữ metric thành phần, runtime và sample size.
- [x] Không chọn model chỉ theo một metric hoặc số liệu minh họa.

## Hard dependency

- [EXP-02](./EXP-02.md)
- [EXP-03](./EXP-03.md)

## Cách làm song song

Clustering metrics, Omori analysis và stability harness có thể chia ba người
làm song song trên cùng membership fixture rồi hợp nhất bằng metric schema.

## Ranh giới

- Omori dùng để đánh giá consistency, không dùng tạo cluster.
- Không tạo score tổng hợp không công bố thành phần/trọng số.
- Không biến statistical candidate thành nhãn nhân quả.

## Tuần 7 — ThanhTris: bắt đầu evaluation mức pilot

Đọc [kế hoạch](../WEEK_7_COLAB_PLAN.md) và
[setup](../../runbooks/WEEK_7_COLAB_SETUP.md). Task cũ chưa triển khai, giữ
Backlog đến khi thật sự bắt đầu; không gọi việc này là Needs Update của task Done.

- Chuẩn bị metric schema/reason codes, stability/Omori harness và fixture tests
  ngay; không chờ EXP-02/03 official để thiết kế interface.
- COL-03 của rosy179 bàn giao baseline metrics/plots và pinned artifacts.
  ThanhTris sở hữu evaluation module, không tính lại cùng metrics hai lần.
- Tính stability trên cùng candidate population, công bố seed/tolerance,
  số lần perturb/repeat và policy cho all-noise/empty sets. Không sửa vector
  trong immutable pilot; perturb chỉ tạo bản thử nghiệm ngoài input và có version.
- Physical-consistency/Omori chỉ fit khi đủ mẫu; thiếu mẫu báo null + reason,
  không drop window hoặc dùng zero thay missing metric.
- Report pilot giữ PARTIAL/development-only và không chọn model tốt nhất.

Đầu ra tuần: harness/tests và pilot stability report. **Không đủ Done toàn
EXP-04** nếu còn official research comparison/Omori/stability theo acceptance
gốc. Dependencies và tiêu chí gốc phía trên giữ nguyên.

## Theo dõi

- **Trạng thái:** Done
- **Assignee:** ThanhTris
- **Reviewer:** unassigned
- **Evidence / PR:**
  - Pilot baseline harness & stability report: [EXP-04_PILOT_EVALUATION.json](../../evidence/EXP-04_PILOT_EVALUATION.json) và [EXP-04_PILOT_EVALUATION.md](../../evidence/EXP-04_PILOT_EVALUATION.md).
  - **Phiên Windows trước khi tích hợp EXP-02:** [report lịch sử](https://github.com/HoaiTam/japan-earthquake-etl/blob/c2d00ff9d28f8acc69d551fec5f51ad55e8f9a39/docs/evidence/EXP-04_OFFICIAL_REPRODUCTION.json)
    giữ số liệu DBCV/Omori/stability và provenance gốc. Report đó ghim hai run
    upstream khác nên không bàn giao cho EXP-05 sau khi cập nhật #141.
  - **Cổng phát hành:** Runner chỉ ghi official `PASSED` và `frozen_extension_rule`
    trên REPRODUCTION khi cả EXP-02 và EXP-03 đối soát đạt, kể cả bytes từng shard
    và primary membership/summary. Thiếu một hoặc cả hai bị từ chối, trừ chạy
    `--diagnostic-only` (không phát hành rule, mặc định ghi riêng trong staging).
  - **18/11/2026 — Nghiệm thu REPRODUCTION sau khi #139 merge:** 194 cửa sổ,
    156.710 feature rows / 2 shards trên commit sạch `c746dc6`. Kiểm bytes
    và primary membership/summary cả bốn thuật toán, khớp với
    [EXP-02 replay](../../evidence/EXP-02_REPRODUCTION_REPLAY_FOR_EXP04.json)
    và [EXP-03 official](../../evidence/EXP-03_OFFICIAL_REPRODUCTION.json).
    [Report mới](../../evidence/EXP-04_OFFICIAL_REPRODUCTION.md) / [JSON](../../evidence/EXP-04_OFFICIAL_REPRODUCTION.json)
    phát hành `exp04-frozen-reproduction-rule-v2`, `scientific_approval=false`.
    [EXP-05](./EXP-05.md) phải ghim SHA của báo cáo mới; không dùng report lịch sử.
- **Kỹ năng phù hợp:** Clustering evaluation, seismology statistics, experiment design

## Checklist bàn giao

- [x] Deliverable và test/evidence tồn tại trong repository.
- [x] Acceptance criteria đã được kiểm tra.
- [x] Contract/docs đã cập nhật và không chứa secret/data dump.
- [ ] Reviewer độc lập được khuyến nghị cho P0; reviewer giữ `unassigned`, chưa có approval.

## Tài liệu liên quan

- [Roadmap HDBSCAN](../HDBSCAN_WORKSTREAM.md)
- [EXP-03](./EXP-03.md)

