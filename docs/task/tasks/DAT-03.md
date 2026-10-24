---
task_id: "DAT-03"
status: "In Progress"
week: 6
block: "A - Hợp đồng dữ liệu"
workstream: "Verified research data and team handoff"
scope: "Core"
priority: "P0"
effort_hours: 16
assignee: "HoaiTam"
reviewer: "unassigned"
dependencies: ["ORC-07", "MLD-05", "MLI-02", "EXP-01"]
---

# DAT-03 - Chuẩn bị catalog nghiên cứu thật và bàn giao dataset dùng chung

## Mục đích

Chạy pipeline trên catalog nghiên cứu **thật**, có source coverage, quality và
export proof; HoaiTam, ThanhTris và rosy179 nhận cùng phiên bản, đọc được trên
máy riêng không cần credential pipeline. Không lấy synthetic QA hoặc DAT-02
PARTIAL thay full research data.

## Phạm vi và từng khối công việc

1. **Dùng ngay:** theo lựa chọn owner 24/10, verify và đóng gói pilot JMA thật
   DAT-02 cho nhóm phát triển; giữ DEVELOPMENT_ONLY/PARTIAL/research_ready=false.
   Bản này không hoàn tất task hoặc mở official fit/import gate.
2. **Profile và kế hoạch tải:** kiểm archive thật đã có, counts/size/UTC months,
   runtime và resource limits. Pin source/release/SHA; tái dùng bytes đã verify.
   Từ số đo ước lượng lại effort/disk; 16h là estimate ban đầu, không SLA.
3. **Catalog thật:** backfill REPRODUCTION [2000-01-01,2018-10-01) và EXTENSION
   [2018-10-01,2024-01-01) UTC qua Bronze → Silver → Gold Published. Ghi exact
   manifest/snapshot/SHA, missing months và count reconciliation. JMA là primary
   research source; USGS overlap theo CON-01. Không dùng current-month PROVISIONAL.
4. **Giới hạn có version:** profile trước khi mở rộng bounded resolver/exporter;
   không tăng limit mù, truncate hoặc cắt window để pass. Chốt memory, batch/shard,
   UTC/JST archive boundary và window vượt split. Thay contract/code phải có
   regression/evidence và cập nhật consumer; không dùng scalar resource limit
   của QA nhỏ để cam kết chạy được toàn catalog.
   Owner chốt 24/10: **giữ split nghiêm ngặt**, loại mainshock nếu nguyên PRE/POST
   vượt period, ghi rõ từng ID/required interval/reason; không mượn data split khác.
5. **ML và bàn giao:** chạy MLI-02 từ exact Gold pin, audit/Mc/window/features/
   VALIDATED/EXPORTED, verify local loader và rerun. Release bất biến có bundle,
   code/config versions, README và expected SHA từ trusted receipt. Mỗi người
   dùng input readonly, output/experiment_run_id riêng.

## Deliverable

- [Hướng dẫn dataset dùng chung](../../runbooks/DAT-03_SHARED_DATASET.md).
- Archive profile và rollout/coverage/quality/resource reports; chỉ metadata vào Git.
- Real Gold pins, MLI-02 acceptance, immutable research bundle và external SHA.
- Công cụ đóng gói/verify offline, metadata handoff và receipts của ba người.
- [Evidence theo giai đoạn](../../evidence/DAT-03_PROGRESS.md); không viết lại
  evidence DAT-02/MLI-02 thành kết quả full catalog mới.

## Tiêu chí hoàn thành

- [x] Giai đoạn dùng trước: kit pilot thật đã verify/giải nén độc lập trên máy owner;
  giữ PARTIAL/development-only, chờ hai người nhận tự gửi receipt.
- [x] Profile đầu vào h2000/h2011/h2023 bằng shared Java parser; metadata evidence đã lưu.
  Bổ sung h2016: 18 depth token rejects; [evidence kỹ thuật](../../evidence/DAT-03_TECHNICAL_PROGRESS.md)
  và reader probe 2011 đã lưu, không coi là source/quality gate full catalog.
- [ ] Profile archive thật, ước lượng dung lượng/runtime và chốt rollout an toàn.
- [ ] Resource/boundary policies được kiểm trên dữ liệu thật; không silent sampling.
  Đã triển khai opt-in resolver `orc-06-v2` và `dat03-strict-split-v1` trong nhánh
  PR #99; regression/reader profile không thay full-period runtime acceptance.
- [ ] Source proof đủ JMA 225 tháng reproduction và 63 tháng extension; xử lý biên
  UTC/JST và source-link halo, không nhận boolean tự khai.
- [ ] Gold publication/Trino/reconciliation đạt; audit/Mc/exclusion reasons có proof.
- [ ] MLI-02 build/export thật, counts/pins/checksum/loader và rerun đạt.
- [ ] HoaiTam, ThanhTris, rosy179 verify cùng release trên máy riêng; receipt có
  người nhận, dataset/pilot ID, SHA và loader result. Không giả receipt của hai
  người khác bằng ba lần chạy trên máy owner.
- [ ] Runbook, giới hạn và metadata task/index đồng bộ, không còn blocker chưa xử lý.

## Hard dependency / làm song song

ORC-07, MLD-05, MLI-02 và EXP-01 đã Done theo scope component riêng. DAT-03 sở
hữu actual research rollout còn thiếu, không đổi các task ấy về chưa Done.
EXP-02/03 tiếp tục local development bằng pilot; official scientific run cần
release đủ gate. Không tạo dependency ngược từ DAT-03 sang EXP-02.

## An toàn và ranh giới

- Không build/pull/start/init trong test/handoff; không đổi deployment, cron,
  pause/run, lease hoặc registry production để đóng gói local.
- Không xóa dữ liệu; release khác bytes dùng identity/output mới.
- Không upload cloud/thay ACL/tạo link public. SEC-01 Done local; mở lại phần
  Colab/Drive trước migration. Người dùng tự chọn nơi chia sẻ.
- Hash kiểm byte identity theo expected pin ngoài bundle, không là chữ ký nguồn.
- Không fit/tune/cấp scientific approval: EXP-02/03/04/05 sở hữu experiment,
  MLI-04/MLQ-01 sở hữu report/ML E2E.

## Theo dõi

- **Trạng thái:** In Progress
- **Assignee:** HoaiTam
- **Reviewer:** unassigned
- **Evidence / PR:** [DAT-03 progress](../../evidence/DAT-03_PROGRESS.md). Branch
  `feat/dat-03-research-dataset-handoff` từ main `bc0ce9e` (PR #98).
  [PR #99](https://github.com/HoaiTam/japan-earthquake-etl/pull/99), commit phần
  pilot/profile `49d1784`; phần kỹ thuật tiếp theo đang sửa local, chưa push.
  Chưa có reviewer approval, full research hoặc ba máy acceptance. ThanhTris và
  rosy179 chưa chạy verifier theo xác nhận owner; tiếp tục phần kỹ thuật độc lập.

## Tài liệu liên quan

- [Source coverage](../../specs/SOURCE_COVERAGE.md), [baseline](../../specs/MVP_SCOPE_KPI_AND_DOD.md).
- [Full-month state](../../specs/FULL_MONTH_STATE_CONTRACT.md), [feature/export](../../specs/FEATURE_EXPORT.md).
- [MLI-02](../../runbooks/MLI-02_DATASET_BUILD.md), [DAT-02](./DAT-02.md).
- [SEC-01](./SEC-01.md), [EXP-02](./EXP-02.md), [EXP-03](./EXP-03.md).
