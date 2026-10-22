# DOC-01 — Đồng bộ phân công và task tracking ngày 22/10/2026

Base: main `3e8625e` (PR #94); branch `docs/doc-01-sync-task-tracking`.
Owner bản docs sync: HoaiTam, reviewer unassigned; không tự ghi approval.
Đây là thay đổi metadata/tài liệu và planning checks, không có runtime acceptance mới.

## Nguồn và thay đổi

Theo bảng phân công gần nhất HoaiTam gửi, đồng bộ các điểm lệch đã báo:

| Phần | Đã sửa | Phần giữ nguyên |
|---|---|---|
| MLI-02 | Owner hiện hành rosy179 trong frontmatter/Theo dõi/plan; checklist phân biệt offline và live acceptance | 5h, Core, In Progress, dependencies; offline evidence cũ vẫn ghi HoaiTam tại mốc đó |
| DOC-01 / QA-04 / EXP-03 | Tuần 5 trong task/index và kế hoạch | Effort 4/4/6h, scope và hard gates; không chỉnh ngày/lịch sử chạy trong evidence |
| EXP-03 | Bỏ tick reviewer khi unassigned; chưa tick acceptance toàn task | Unit/contract checks đã đạt; fallback không thay fresh kernel hoặc EXP-02 dependency |
| Known issues | QA-02/03/04 Done và link đúng runtime evidence; MLD-05 component Done khác full research acceptance | QA-01/MLI-02/EXP-02/EXP-03 In Progress, SEC-01 Review |
| PR tracking | DOC-01 #89, MLI-02 #85, EXP-03 #86, QA-04 #93 thay dòng "chưa mở PR" đã cũ | Không tạo PR mới/commit/push hoặc giả reviewer approval |

## Số liệu không đếm hai lần

- Bảng giao việc: 11 task/61h, owner 17/21/23h.
- Mở đường: ORC-06/DAT-02/MLD-04/MLD-05, 4 task/25h, owner 14/0/11h.
- Toàn bộ metadata tuần 5: **15 Core task/86h**, owner **31/21/34h**.
- Backlog vẫn **76 task/386h**, Core 68/Stretch 8. Chỉ chuyển 14h từ tuần 6/7
  về tuần 5, không phát sinh task hoặc tăng effort. Lịch toàn kỳ sau tách
  pre-week: 45/45/41/93/86/4/16/44h + pre-week 12h = 386h.
- Giữ buffer cũ 4h tồn + 20h review cho ra 110h tham khảo, không phải 96h hoặc
  capacity đã xác nhận; effort task Done không phải remaining work.
- Sau bản đồng bộ DOC-01: trạng thái toàn backlog giữ **57 Done / 4 In Progress /
  1 Review / 14 Backlog**. Không nâng task nghiệm thu thiếu thành Done.

## Kiểm tra

- `make check-task-status`: PASS, đồng bộ 76 frontmatter/Theo dõi/index.
- `make check-week-5-plan`: PASS, owner/week/Core/effort, 15 task/86h và dependency gates.
- `make test-week-5-plan`: **17 tests PASS**, regression owner MLI-02, tuần DOC-01/QA-04/EXP-03,
  task Core phát sinh ngoài roster, giữ dependency MLI-02→MLD-05/EXP-03→EXP-02,
  pilot cycle, reviewer exception MLD-02/03 và newline.
- `make check-docs`: **214 Markdown files / 1.923 links / 0 problems**.
- `TMPDIR=/private/tmp make ci-docs-base`: PASS, contract/config/Compose tĩnh,
  planning/CI/security regressions, shell syntax và whitespace; không Docker runtime.
- `make check-mvp-baseline`: PASS sau khi đồng bộ roadmap và chú thích lịch sử PLN-01.
- File evidence lịch sử và JSON runtime không đổi; không sửa application code,
  .env, credential, bucket, volume, snapshots, DAG deployment hoặc model output.

## Giới hạn và task tiếp theo

- [QA-01 — Daily E2E](../task/tasks/QA-01.md): còn scheduled deployment/KPI acceptance.
- [MLI-02 — Build dataset DAG](../task/tasks/MLI-02.md): rosy179 còn real exporter/registry/DAG acceptance.
- [EXP-02 — Baselines](../task/tasks/EXP-02.md): còn Colab parity/official run.
- [EXP-03 — Adaptive/multi-sequence](../task/tasks/EXP-03.md): còn reproduction artifacts/fresh-kernel evidence và dependency EXP-02.
- [SEC-01 — Review bảo mật](../task/tasks/SEC-01.md): còn findings/coverage riêng;
  không suy docs/static PASS là security runtime đã nghiệm thu.
- Current-month/as-of-cutoff/JMA sau 2023 và full research catalog backfill
  acceptance chưa có task riêng; bản đồng bộ không tự mở rộng contract.
