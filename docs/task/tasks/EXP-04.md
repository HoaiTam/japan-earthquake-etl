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

- **Owner:** ThanhTris; **status:** Needs Update.
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
  - **18/11/2026 — Official REPRODUCTION Evaluation v2 & Exact DBCV / Fail-Closed Cross-Verify (`ThanhTris`):** Khắc phục toàn bộ P1 review tại PR #141 trên clean commit **`4a324b886a543347bbaafa6fdf5a07505de1c529`** (`is_clean_code=true`, `scientific_approval=false`, `reviewer: unassigned`): (1) sửa `_compute_dbcv_memory_safe` matching `hdbscan.validity.validity_index` cả tie-breaking MST nội bộ trên cụm $>2000$ điểm lẫn windows $>50$ cụm; (2) bỏ hoàn toàn Silhouette fallback để lỗi DBCV luôn trả `(None, [METRIC_FIT_FAILED])`; (3) siết fail-closed `_cross_verify_exp02_exp03` kiểm tra `status=PASSED`, trusted report SHA-256, đầy đủ pins & đối soát physical bundle checksum + `primary_membership_sha256`. Kết quả **REPRODUCTION v2 (`exp-04-official-v2`, `194` windows, `156.710` rows, `repeats=5`, $\sigma=0.01$, `seed=42`)**:
    - `WINDOW`: `194/194` success (`fail=0`, `0.0%`), `135.853` POST aftershocks, noise `0.0%`, DBCV `N/A` (`METRIC_NOT_APPLICABLE: 194`), Omori `p=0.5475` / $R^2=0.9565$ (`155/194`), Jaccard `1.0000` / ARI `1.0000` (`194/194`), runtime `22.51s`.
    - `DBSCAN`: `128/194` success (`fail=66`, `34.0%`, `EXP_MAINSHOCK_CLASSIFIED_AS_NOISE: 66`), `118.487` aftershocks, noise `20.6%`, DBCV `0.0912` (`98/194`; v1 lịch sử trước khi sửa tie-breaking MST là `0.1195`), Omori `p=0.7013` / $R^2=0.9744$ (`106/194`), Jaccard `0.9743` / ARI `0.9456` (`194/194`), runtime `181.77s`.
    - `HDBSCAN_GLOBAL`: `142/194` success (`fail=52`, `26.8%`, `EXP_MAINSHOCK_CLASSIFIED_AS_NOISE: 52`), `15.118` aftershocks, noise `16.0%`, DBCV `0.3195` (`101/194`; v1 lịch sử là `0.3244`), Omori `p=0.8220` / $R^2=0.9540$ (`99/194`), Jaccard `0.8718` / ARI `0.8164` (`194/194`), runtime `407.32s`.
    - `HDBSCAN_ADAPTIVE`: `131/194` success (`fail=63`, `32.5%`, `EXP_MAINSHOCK_CLASSIFIED_AS_NOISE: 63`), `98.795` raw per-window POST aftershocks (cross-shard resolved `88.222` ở EXP-03), noise `31.9%`, DBCV `0.3056` (`32/194`; v1 lịch sử là `0.3178`), Omori `p=0.7114` / $R^2=0.9542$ (`124/194`), Jaccard `0.9342` / ARI `0.8727` (`194/194`), runtime `436.42s`.
    - Đối soát fail-closed & đọc lại physical bundles khớp 100% với [EXP-02 Official](../../evidence/EXP-02_OFFICIAL_REPRODUCTION.json) (SHA `a1472cb417b038262df8fcb8cbe0246b1356aa22a4645289aee1b90319bdefb9`) và [EXP-03 Official](../../evidence/EXP-03_OFFICIAL_REPRODUCTION.json) (SHA `e0e1bfeb5cacea1399d3a3ebd0cec167b6f8a69a6bc0f1bbc1706fe47df49697`), phát hành `frozen_extension_rule` **`exp04-frozen-reproduction-rule-v2`** (thay thế bản lịch sử `v1`) cho [EXP-05](./EXP-05.md). Xem [EXP-04_OFFICIAL_REPRODUCTION.md](../../evidence/EXP-04_OFFICIAL_REPRODUCTION.md) và [JSON](../../evidence/EXP-04_OFFICIAL_REPRODUCTION.json). Giữ `Done` (`scientific_approval=false`; reviewer giữ `unassigned`).
- **Kỹ năng phù hợp:** Clustering evaluation, seismology statistics, experiment design

## Checklist bàn giao

- [x] Deliverable và test/evidence tồn tại trong repository.
- [x] Acceptance criteria đã được kiểm tra.
- [x] Contract/docs đã cập nhật và không chứa secret/data dump.
- [ ] Reviewer độc lập được khuyến nghị cho P0; reviewer giữ `unassigned`, chưa có approval.

## Tài liệu liên quan

- [Roadmap HDBSCAN](../HDBSCAN_WORKSTREAM.md)
- [EXP-03](./EXP-03.md)

