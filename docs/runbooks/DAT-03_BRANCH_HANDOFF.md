# DAT-03 — Bàn giao branch giai đoạn pilot và research preparation

Branch `feat/dat-03-research-dataset-handoff`, từ main `bc0ce9e` (PR #98).
Checkout tái dùng `japan-earthquake-etl-mli-02`; deployment chính không đổi.
Task **In Progress**, assignee HoaiTam, reviewer unassigned. Không tự commit,
push hoặc mở PR. Đây là PR giai đoạn chuẩn bị, không tuyên bố DAT-03 Done.

Đợt 1 đã được owner commit `49d1784` và push lên
[PR #99](https://github.com/HoaiTam/japan-earthquake-etl/pull/99).
**Đợt tiếp tục hiện tại dùng Git commands/PR description ở
[Đợt 2](#đợt-2--scoped-replay-và-strict-split).** Các mục tương ứng ngay dưới
đây giữ như lịch sử bàn giao đợt 1, không phải yêu cầu tạo PR mới.

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

## Đợt 2 — Scoped replay và strict-split

Vẫn branch/PR #99, không branch/PR mới, chưa tự commit/push. Pilot ZIP/pins
đợt 1 không đổi. [Evidence kỹ thuật](../evidence/DAT-03_TECHNICAL_PROGRESS.md)
và [task DAT-03](../task/tasks/DAT-03.md) liệt kê các gate chưa đạt.

### Git commands hiện tại

Review diff trước stage; các directory dưới đây chỉ là source/test ML và
orchestration liên quan, không staging/raw/ZIP/log/.env. Không dùng git add .

```bash
cd /Users/hoaitam/Desktop/Learn/23521394/HK1-Nam-4/IE212/japan-earthquake-etl-mli-02
git status --short --branch
git diff --check
git diff
git add -- Makefile scripts/ci-impact.py tests/ci/test_suites.py \
  airflow/dags/real_etl_runtime.py airflow/tests/test_real_etl_runtime.py \
  spark/src/main/java/ie212/earthquake/spark/orchestration \
  spark/src/main/java/ie212/earthquake/spark/ml \
  spark/src/main/java/ie212/earthquake/spark/jma/JmaResearchReplayProfileJob.java \
  spark/src/test/java/ie212/earthquake/spark/orchestration/FullMonthStateResolverTest.java \
  spark/src/test/java/ie212/earthquake/spark/ml \
  spark/src/test/java/ie212/earthquake/spark/jma/JmaResearchReplayProfileJobTest.java \
  docs/KNOWN_ISSUES.md docs/task/tasks/DAT-03.md \
  docs/specs/FULL_MONTH_STATE_CONTRACT.md docs/specs/FEATURE_EXPORT.md \
  docs/runbooks/DAT-03_SHARED_DATASET.md docs/runbooks/DAT-03_BRANCH_HANDOFF.md \
  docs/runbooks/MLI-02_DATASET_BUILD.md \
  docs/evidence/DAT-03_PROGRESS.md docs/evidence/DAT-03_TECHNICAL_PROGRESS.md \
  docs/evidence/DAT-03_JMA_2011_REPLAY_PROFILE.json docs/evidence/DAT-03_JMA_2016_PROFILE.json
git diff --cached --stat
git diff --cached --check
git commit -m "feat(data): add scoped replay and strict split window policy"
git push origin feat/dat-03-research-dataset-handoff
```

### PR #99 description hiện tại để copy

```markdown
## Mục tiêu

Tiếp tục DAT-03: chuẩn bị dữ liệu nghiên cứu thật và dataset dùng chung cho nhóm. Giữ task In Progress; PR này chưa chứng nhận full catalog hoặc research release.

## Thay đổi chính

- Giữ kit pilot JMA thật 279 rows/2 mainshock của đợt 1, có external SHA, verifier/loader và owner receipt; không đổi PARTIAL/development-only thành official EXPORTED.
- Thêm resolver orc-06-v2 opt-in: fresh verify mọi source pin, giữ descriptor thay tổng annual raw bytes; chỉ replay JMA affected months/halo, kiểm USGS stable-key moves và đóng old/new graphs tới fixed point.
- Bảo vệ JMA release/lineage kể cả khi record bytes không đổi; giữ giới hạn rows/pins/months, reject downgrade parent v2 về v1. Default v1 không đổi.
- Áp dụng lựa chọn owner: dat03-strict-split-v1 loại window PRE/POST vượt split, không clip/mượn data split khác; báo từng ID/required interval/reason và counts. Exclusion report SHA tham gia export identity.
- MLD-03, MLI-02 và MLD-05 dùng cùng policy; verifier chặn bỏ report rồi tự rehash. Event bị loại làm mainshock vẫn có thể là candidate của window khác.
- Thêm probe reader bằng make, đo archive thật; cập nhật CI nhóm window, contract/runbook/evidence và known issues. Không đưa raw/ZIP/log vào Git.

## Kiểm thử và số đo

- 60 test Java thuộc bảy nhóm resolver/policy/identity/export/adapter/build/probe đạt qua các lượt cuối; 12 Airflow adapter tests và 41 CI policy tests PASS. Package, docs/status/secret hygiene và diff checks PASS. Đây không phải full Java suite hoặc remote GitHub CI.
- Reader 2011: tháng 3/4 riêng đạt 56.015/57.044 revisions, có halo; gộp bị guard 100k chặn, không truncate. Đây là engineering probe, không resolver/Gold acceptance.
- Profile thêm JMA 2016: 313.487 physical = 313.469 accepted + 18 INVALID_NUMBER rejects. Đã khoanh depth 45–49, precision 1; chưa suy diễn blank nội bộ hoặc sửa/drop records để pass.
- Không đổi deployment/JAR đang chạy, DAG/cron/lease/credentials; không Docker build/pull/start/init, lake writes hoặc upload cloud trong đợt này.

## Docs

- docs/task/tasks/DAT-03.md và docs/KNOWN_ISSUES.md
- docs/specs/FULL_MONTH_STATE_CONTRACT.md, FEATURE_EXPORT.md
- docs/runbooks/DAT-03_SHARED_DATASET.md, DAT-03_BRANCH_HANDOFF.md, MLI-02_DATASET_BUILD.md
- docs/evidence/DAT-03_PROGRESS.md, DAT-03_TECHNICAL_PROGRESS.md và các profile metadata

## Giới hạn / phần còn lại

- DAT-03: xử lý depth-token encoding 2016 có căn cứ và regression/version; profile/capacity các năm còn lại; nguồn biên 2024 JST; collection/sharding có full-assignment proof; backfill 225+63 tháng, real Gold/Trino/MLI-02 export và ba máy handoff vẫn chưa đạt. Exporter vẫn cap 128 mainshock/100k feature rows, không gọi opt-in v2 là đã giải quyết mọi scale.
- ThanhTris và rosy179 chưa gửi verifier receipt; không giả xác nhận từ ba máy. Reviewer unassigned, chưa có approval.
- EXP-02/03 official experiment, EXP-04/05 evaluation và MLI-04/MLQ-01 report/E2E vẫn là gates riêng. SEC-01 Done local, cloud/Colab/Drive cần mở lại acceptance trước migration.
```
