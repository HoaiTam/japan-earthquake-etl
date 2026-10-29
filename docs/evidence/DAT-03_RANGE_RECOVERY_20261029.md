# DAT-03 — Bản vá recovery range của rosy179, 29/10/2026

## Bằng chứng đầu vào và kết luận

Owner cung cấp bản tóm tắt `error.md` (không đưa private file vào Git):
range3df82813b89e456b823f77d2a890bf3b, request SHA
`7b7069c8622e05556f42a11c49748160a2c308c9080c4abcab7966c2b929244e`,
session952973b42c8d4570b653ece99c79ed78 exit1/timed_out=false/2356,013s.
Báo cáo33 checkpoints10/2018–06/2021; pending07/2021 đã Gold COMMITTED,
Verify `TRINO_QUERY_FAILED`, Trino `ABANDONED_TASK` trong thời gian máy sleep.

Chọn **publication-only recovery**, không quarantine/skip hoặc release để
resume mù. Chạy lại một query ra0 không thay acceptance toàn bundle. Chưa có
full request/volume trên máy Tâm để độc lập xác nhận33 hoặc snapshot cụ thể;
runtime recovery/remaining count phải do tool kiểm trên máy Trang.

## Thay đổi kỹ thuật

- `scripts/dat03-range-recovery.py`: prepare bằng read-only retained-state
  inspection; request/failure/code SHA, dependency pins, own lease/quiescence;
  staged helper riêng, preflight/run/finalize, host service unchanged check,
  append-only resolution và pinned resume bridge. Không thay frozen controller.
- `ResearchRangePublicationRecovery`: original JAR entries byte-identical,
  chỉ thêm một entry. Prefix stored publication/context/receipts/parent khớp,
  pending context dựng lại với original code/input/baselines. Chỉ Verify/Publish
  committed target batch, đối soát đủ sáu pins trước/sau; không tạo Spark.
- Chặn partial/unknown Publish, timeout, foreign lease, active runtime, source
  drift, checkpoint gap, quality fail và second unresolved session. Không có
  lệnh quarantine/takeover, service lifecycle, ETL replay hay automatic retry.
- Failed session giữ nguyên; recovery có execution/result/acceptance riêng.
  Chỉ own release sau result proof và host postflight. Bridge chấp nhận đúng
  một resolution đã pin, không vô hiệu acceptance cho session khác.

## Kiểm tra

- `make test-dat03-range-worker test-dat03-team-setup`: **48tests passed**
  (17range worker,20range recovery,11team setup). Có ca33-prefix/target batch,
  foreign lease/active runtime, gap/pin/failure drift, boolean/timeout, second
  failed session, overlay JAR, original controller preservation, scoped bridge,
  tampered resolution, bad final pin và host service change chặn release.
- Maven package với10test classes của range/recovery/Gold publication/reuse/
  buckets/outside-scope/Trino: **49tests passed**, không failure/error/skip;
  runner JAR build đạt. Dùng Java21, compile target17. Đây là unit/mock SQL
  fixtures, không truy vấn Trino thật của Trang.
- `make ci-docs-base`: **passed**, bao gồm docs/contracts/security/Docker
  lifecycle mocks/CI safety. Không start hoặc đổi service thật.
- Documentation check: **278files/2502links/0problems**; status check:
  **81tasks đồng bộ**; `git diff --check` đạt.
- Secret pattern scan10changed/newfiles: **0findings**; không commit file
  `error.md`, raw logs, request payload, JAR hoặc data.

Xem [runbook](../runbooks/DAT-03_RANGE_RECOVERY.md) để tái lập lệnh và quy
trình máy Trang. Không dùng fixture thành chứng nhận recovery thật; chưa full
Java/Spark integration suite hoặc actual recovery trên máy thành viên.

## Trạng thái

**DAT-03 In Progress.** Chưa chạy recovery trên máy Trang; không cộng tháng
vào global catalog hoặc ghi receipt giả. Full288/portable merge/official two-split
ML export/three-machine receipts vẫn thiếu theo [task](../task/tasks/DAT-03.md).
