# DAT-03 — Bàn giao branch giai đoạn pilot và research preparation

Branch `feat/dat-03-research-dataset-handoff`, từ main `bc0ce9e` (PR #98).
Checkout tái dùng `japan-earthquake-etl-mli-02`; deployment chính không đổi.
Task **In Progress**, assignee HoaiTam, reviewer unassigned. Không tự commit,
push hoặc mở PR. Đây là PR giai đoạn chuẩn bị, không tuyên bố DAT-03 Done.

## File gửi nhóm

ZIP local ở `staging/shared-datasets/jma-development-pilot-team.zip` trong
checkout này. [Runbook/pins/cách nhận biết thành công](./DAT-03_SHARED_DATASET.md)
và [release metadata](../evidence/DAT-03_TEAM_RELEASE.json). Gửi ZIP **kèm pins
ngoài ZIP**, không chỉ gửi đường dẫn local vì máy thành viên không truy cập được.
README/verifier/loader đã nằm trong ZIP; không cần Docker hoặc pipeline credentials.

## Git commands

Kiểm tra đúng branch và diff trước khi stage, không đưa staging/ZIP/Parquet vào Git.

```bash
cd /Users/hoaitam/Desktop/Learn/23521394/HK1-Nam-4/IE212/japan-earthquake-etl-mli-02
git status --short --branch
git diff --check
git diff
git add Makefile \
  scripts/pack-team-pilot.py scripts/verify-team-pilot.py \
  scripts/check-mvp-baseline.sh scripts/check-week-3-plan.sh scripts/check-week-5-plan.sh scripts/ci-impact.py \
  tests/test_team_pilot.py tests/test_week5_plan.py tests/ci/test_suites.py \
  spark/src/main/java/ie212/earthquake/spark/jma/JmaResearchProfileJob.java \
  spark/src/test/java/ie212/earthquake/spark/jma/JmaResearchProfileJobTest.java \
  docs/KNOWN_ISSUES.md docs/specs/MVP_SCOPE_KPI_AND_DOD.md \
  docs/task/README.md docs/task/WEEK_5_PARALLEL_PLAN.md docs/task/WORK_BLOCKS.md \
  docs/task/tasks/README.md docs/task/tasks/DAT-03.md docs/task/tasks/EXP-02.md \
  docs/task/tasks/MLD-05.md docs/task/tasks/MLI-02.md \
  docs/runbooks/DAT-03_SHARED_DATASET.md docs/runbooks/DAT-03_PILOT_TEAM_README.md \
  docs/runbooks/DAT-03_BRANCH_HANDOFF.md docs/runbooks/DOC-01_TEAM_ONBOARDING.md \
  docs/runbooks/MLD-05_FEATURE_EXPORT.md docs/runbooks/MLI-02_DATASET_BUILD.md \
  docs/evidence/DAT-03_PROGRESS.md docs/evidence/DAT-03_TEAM_RELEASE.json \
  docs/evidence/DAT-03_HOAITAM_RECEIPT.json \
  docs/evidence/DAT-03_JMA_2000_PROFILE.json docs/evidence/DAT-03_JMA_2011_PROFILE.json docs/evidence/DAT-03_JMA_2023_PROFILE.json
git diff --cached --stat
git diff --cached --check
git commit -m "feat(data): package verified team pilot and profile research archives"
git push -u origin feat/dat-03-research-dataset-handoff
```

## PR description để copy

```markdown
## Mục tiêu

Bắt đầu DAT-03: chuẩn bị catalog nghiên cứu thật và bàn giao dataset đồng nhất cho ba thành viên. Hoàn tất giai đoạn pilot dùng trước; DAT-03 vẫn In Progress.

## Thay đổi chính

- Đóng gói pilot JMA thật 279 candidate rows / 2 mainshock cùng README, requirements, loader và verifier offline; ZIP/Parquet ngoài Git.
- Verify bằng external SHA, exact file set, schema/grain/count/self/coverage. Receipt theo người nhận, không giả xác nhận ba máy.
- Đóng gói deterministic trong cùng môi trường, reuse cùng bytes, không overwrite release khác; chặn source thay đổi sau validation.
- Thêm Java profile dùng lại JMA validator/parser; đọc toàn archive, chỉ xuất metadata, không ghi lakehouse.
- Profile thật h2000/h2011/h2023; giữ coverage/research flags false, không dùng số dòng/min-max làm completeness proof.
- Thêm make targets và regression/CI impact theo phạm vi; cập nhật task/dependency/docs/accounting: 77 task/402h, lịch tuần 5 không đổi.

## Kiểm thử và evidence

- 118 pilot/model/Jupyter tests, 42 Java scoped tests và 40 CI policy tests PASS; docs/contracts/planning/secret hygiene/diff checks PASS. Xem evidence để phân biệt với full catalog/scientific acceptance.
- Kit thật giải nén sang thư mục độc lập, standalone loader đạt; rerun pack giữ nguyên SHA.
- Không build/pull/start/restart/init Docker, không đổi DAG/cron/lease; chỉ build Java local và tải public JMA 2011 để profile staging.

## Docs

- docs/task/tasks/DAT-03.md
- docs/runbooks/DAT-03_SHARED_DATASET.md và DAT-03_PILOT_TEAM_README.md
- docs/evidence/DAT-03_PROGRESS.md, profiles và release/owner receipt metadata
- Đồng bộ EXP-02, MLD-05/MLI-02, onboarding, known issues và backlog accounting.

## Giới hạn / phần còn lại

- DAT-03 còn full research backfill 225 + 63 tháng, resource/window/UTC-JST boundary policy, Gold/Trino và actual research MLI-02 export.
- Bộ gửi trước là DEVELOPMENT_ONLY/PARTIAL/research_ready=false, không official EXPORTED hoặc scientific approval.
- Còn receipt từ máy ThanhTris và rosy179; reviewer unassigned, chưa có approval.
- EXP-02/03 còn official experiment; EXP-04/05 evaluation; MLI-04/MLQ-01 report/ML E2E.
- SEC-01 chỉ Done local; chưa upload hoặc nghiệm thu Drive/Colab. Mở lại cloud acceptance trước migration.
```
