# DAT-03 — Bàn giao branch giai đoạn pilot và research preparation

Branch `feat/dat-03-research-dataset-handoff`, từ main `bc0ce9e` (PR #98).
Checkout tái dùng `japan-earthquake-etl-mli-02`; deployment chính không đổi.
Task **In Progress**, assignee HoaiTam, reviewer unassigned. Không tự commit,
push hoặc mở PR. Đây là PR giai đoạn chuẩn bị, không tuyên bố DAT-03 Done.

Đợt 1 đã được owner commit `49d1784` và push lên
[PR #99](https://github.com/HoaiTam/japan-earthquake-etl/pull/99).
**Đợt tiếp tục hiện tại dùng Git commands/PR description ở
[Đợt 4](#đợt-4--collection-và-bounded-e2e).** HEAD hiện tại `258710d`;
chưa tự commit/push các thay đổi local. Các mục tương ứng ngay dưới
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

Mốc trước bước 1/2; dùng Đợt 3 cho bàn giao mới nhất. Các giới hạn depth/
archive boundary bên dưới giữ là lịch sử trước parser v1.2 và capture 2024.

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

## Đợt 3 — Depth parser và profile 25 archive

Tiếp tục trên branch/PR #99, HEAD `6356a44`, không branch/PR mới. Bao gồm phần
scoped replay/strict split còn local của Đợt 2 và bản vá/profile bước 1/2 mới.
DAT-03 **In Progress**, assignee HoaiTam, reviewer unassigned; kit pilot/pins
không đổi. [Kết quả](../evidence/DAT-03_RESEARCH_PROFILE_ACCEPTANCE.md) và
[lệnh profile/rollout](./DAT-03_RESEARCH_PROFILING.md) phân biệt rõ engineering
acceptance với các gate research còn thiếu.

### Git commands để cập nhật PR #99

Review cả diff đang có từ Đợt 2 trước stage. Các directory source/test dưới đây
chỉ thuộc DAT-03; không stage staging/raw/ZIP/log/.env hoặc warehouse.

```bash
cd /Users/hoaitam/Desktop/Learn/23521394/HK1-Nam-4/IE212/japan-earthquake-etl-mli-02
git status --short --branch
git diff --check
git diff
git add -- Makefile scripts/profile-research-archives.py tests/test_research_profiles.py \
  config/jma/research_boundary_archives_v1.csv \
  tests/fixtures/README.md tests/fixtures/jma/depth-blank-2016-v1.json \
  airflow/dags/real_etl_runtime.py airflow/tests/test_real_etl_runtime.py \
  spark/src/main/java/ie212/earthquake/spark/jma/JmaResearchProfileJob.java \
  spark/src/main/java/ie212/earthquake/spark/jma/JmaResearchReplayProfileJob.java \
  spark/src/main/java/ie212/earthquake/spark/silver/JmaFixedWidthParser.java \
  spark/src/main/java/ie212/earthquake/spark/orchestration \
  spark/src/main/java/ie212/earthquake/spark/ml \
  spark/src/test/java/ie212/earthquake/spark/jma \
  spark/src/test/java/ie212/earthquake/spark/silver/JmaFixedWidthParserTest.java \
  spark/src/test/java/ie212/earthquake/spark/orchestration/FullMonthStateResolverTest.java \
  spark/src/test/java/ie212/earthquake/spark/ml \
  docs/KNOWN_ISSUES.md docs/task/tasks/DAT-03.md docs/task/tasks/README.md \
  docs/specs/JMA_SILVER_PARSER.md docs/specs/JMA_ARCHIVE_INVENTORY.md docs/specs/SOURCE_COVERAGE.md \
  docs/specs/FULL_MONTH_STATE_CONTRACT.md docs/specs/FEATURE_EXPORT.md \
  docs/runbooks/DAT-03_SHARED_DATASET.md docs/runbooks/DAT-03_BRANCH_HANDOFF.md \
  docs/runbooks/DAT-03_RESEARCH_PROFILING.md \
  docs/evidence/DAT-03_PROGRESS.md docs/evidence/DAT-03_TECHNICAL_PROGRESS.md \
  docs/evidence/DAT-03_JMA_2011_REPLAY_PROFILE.json docs/evidence/DAT-03_JMA_2016_PROFILE.json \
  docs/evidence/DAT-03_JMA_2016_PROFILE_V12.json docs/evidence/DAT-03_JMA_2016_REPLAY_V12.json \
  docs/evidence/DAT-03_JMA_2024_BOUNDARY_REPLAY.json \
  docs/evidence/DAT-03_RESEARCH_ARCHIVE_PROFILES.json docs/evidence/DAT-03_RESEARCH_PROFILE_ACCEPTANCE.md
git diff --cached --stat
git diff --cached --check
git commit -m "feat(data): validate research archive profiles and replay policies"
git push origin feat/dat-03-research-dataset-handoff
```

### Mô tả PR #99 mới nhất để copy

```markdown
## Mục tiêu

Tiếp tục DAT-03: chuẩn bị catalog nghiên cứu thật và dataset đồng nhất cho nhóm. Hoàn thành bước sửa depth parser và profile/plan archive; task vẫn In Progress.

## Thay đổi chính

- Giữ pilot JMA thật 279 rows/2 mainshock, external SHA/verifier/loader; vẫn DEVELOPMENT_ONLY/PARTIAL, không thay research release.
- Scoped resolver orc-06-v2 và strict-split opt-in có version/lineage/exclusion proof; giữ default và resource guards, không clip window.
- Parser slv-03-v1.2 xử lý hẹp depth-free token BN/BZ cùng giá trị; giữ đủ 18 record năm 2016, thêm warning và fixture locator/hash/token; không đoán token mơ hồ hoặc sửa Bronze.
- Capture/profile tuần tự 25 archive JMA 2000–2024 bằng exact HTTP/SHA/release. Overlay 2024 chỉ nguồn biên UTC/halo, baseline v1 và ML periods không đổi.
- Thêm Make targets, capture/profile control plane và offline regression; summary chỉ metadata. Lập rollout từng UTC month, dừng khi closure vượt cap.

## Kết quả và kiểm thử

- 4.793.337 physical/accepted, 0 parser rejects, 18 warning rows. Năm 2016 trước sửa có 18 rejects; exact ZIP SHA giữ nguyên.
- 25 ZIP tổng 128.784.513 bytes; scan 50,938s, heap 512 MiB/process, max RSS 258,53 MiB. Không phải benchmark Spark/Gold/export hoặc ETA full ETL.
- h2024 có 186 accepted rows thuộc 2023-12 UTC, shared reader probe giữ đủ 186; chưa là source completeness proof. Reader probe 2016 có halo đạt; gộp peak 2011 vẫn bị guard 100k chặn.
- 90 Java tests (88 scoped regression + 2 inventory), 12 Python capture/evidence và 41 CI policy tests PASS; docs/contracts/status/source-example secret hygiene/diff checks và build local đạt. Không full Java suite, runtime credential audit hoặc remote CI.
- Không thay Docker/deployment/runner đang chạy, cron/DAG/lease/credentials; không lake writes, fit model hoặc upload cloud.

## Docs

- DAT-03 task/index, shared-dataset/profile/branch runbooks và known issues.
- JMA parser/inventory/source coverage, scoped replay và feature/export contracts.
- Before/after 2016, 25-archive pins/counts/measurement và evidence kế hoạch rollout.

## Giới hạn / phần còn lại

- DAT-03: nối inventory overlay/source proofs vào runtime, đo capacity Spark/lake/export; collection/sharding có full-mainshock assignment/global Mc proof; full 225+63-month Gold/Trino → MLI-02 research export và ba máy handoff vẫn chưa đạt.
- Exporter vẫn cap 128 mainshock/100k feature rows. Không suy profile 0 rejects thành research_ready/coverage/Gold Published; các cờ vẫn false.
- ThanhTris/rosy179 chưa gửi receipt; reviewer unassigned, không có approval giả.
- EXP-02/03 official experiment, EXP-04/05 scientific evaluation, MLI-04/MLQ-01 report/E2E là gates riêng. SEC-01 local-only, mở lại trước Drive/Colab migration.
```

## Đợt 4 — Collection và bounded E2E

Tiếp tục từ HEAD258710d trên cùng branch/PR #99. Bước3 implementation/test
đã có; bước4 **partial/failed**, không hoàn thành DAT-03. Owner chọn giữ lỗi
và bàn giao trước, không phục hồi run, release lease hoặc thay service/config.
Không tự commit/push/merge. Reviewer unassigned.

[Evidence exact run/pins/counts](../evidence/DAT-03_COLLECTION_BOUNDED_E2E.md),
[partial receipt](../evidence/DAT-03_BOUNDED_E2E_PARTIAL.json),
[collection/E2E runbook](./DAT-03_RESEARCH_COLLECTION_E2E.md).

### Git commands

Chỉ stage source/test/docs dưới đây, **không** staging/raw/ZIP/JAR/Parquet/log/
.env hoặc runtime private evidence. Không tạo branch/PR mới cho lần tiếp tục.

```bash
cd /Users/hoaitam/Desktop/Learn/23521394/HK1-Nam-4/IE212/japan-earthquake-etl-mli-02
git status --short --branch
git diff --check
git diff
git add -- Makefile scripts/research-e2e.py scripts/ci-impact.py \
  tests/test_research_e2e.py tests/ci/test_suites.py \
  spark/src/main/java/ie212/earthquake/spark/ml/ResearchCollectionPlan.java \
  spark/src/main/java/ie212/earthquake/spark/ml/ResearchCollectionJob.java \
  spark/src/main/java/ie212/earthquake/spark/usgs/ResearchBoundedE2EJob.java \
  spark/src/test/java/ie212/earthquake/spark/ml/ResearchCollectionPlanTest.java \
  spark/src/test/java/ie212/earthquake/spark/ml/ResearchCollectionJobTest.java \
  spark/src/test/java/ie212/earthquake/spark/usgs/ResearchBoundedE2EJobTest.java \
  docs/specs/RESEARCH_COLLECTION.md docs/specs/FEATURE_EXPORT.md \
  docs/runbooks/DAT-03_RESEARCH_COLLECTION_E2E.md \
  docs/runbooks/DAT-03_BRANCH_HANDOFF.md docs/runbooks/DAT-03_SHARED_DATASET.md \
  docs/runbooks/DAT-03_RESEARCH_PROFILING.md docs/runbooks/MLI-02_DATASET_BUILD.md \
  docs/evidence/DAT-03_COLLECTION_BOUNDED_E2E.md \
  docs/evidence/DAT-03_BOUNDED_E2E_PARTIAL.json docs/evidence/DAT-03_PROGRESS.md \
  docs/task/tasks/DAT-03.md docs/task/tasks/README.md docs/KNOWN_ISSUES.md
git diff --cached --stat
git diff --cached --check
git commit -m "feat(data): prepare research collections and bounded runtime checks"
git push origin feat/dat-03-research-dataset-handoff
```

### PR #99 description để copy

```markdown
## Mục tiêu

Tiếp tục DAT-03 bước3/4: collection preparation và bounded real-source E2E. Giữ task In Progress; bước4 chưa nghiệm thu hoàn chỉnh.

## Thay đổi chính

- Thêm collection v1: global pinned Mc/full split coverage, strict-split exclusions, deterministic whole-mainshock sharding, assignment/count/hash proof; Spark Parquet readback/rerun/tamper guards.
- Giữ parent BUILDING; output RESEARCH_COLLECTION_PREPARED, research_ready=false và official_exported=false. Không tăng cap hoặc đổi official exporter/registry/import gate.
- Thêm explicit Make/control-plane/Java runner cho bốn tháng đại diện, exact original captures/JAR/inventory, namespace riêng và whole-run lease; không build/start/reset hoặc thay deployment.
- Verify metadata/SHA chain, six snapshots, Trino actual/expected/SQL pins, rerun/retention; cập nhật CI consumer suites và docs/task/runbook/evidence.

## Kiểm thử / kết quả thực tế

- Scoped Java regression35/35 và Python metadata8/8 PASS; package và ci-docs-base đạt. Lượt sandbox bị chặn socket không tính PASS; lượt fixture loopback được phép đạt, không phục hồi runtime. Không full Java suite hoặc remote CI.
- Real2000-01 và2011-04 PASSED first/rerun, six snapshots và36 reported Trino checks/publication; SHA chain kiểm độc lập. Tháng2011-04 có57044 input revisions=57043 canonical+1 ngoài affected month/halo.
- Real2016-04 đã Silver/Gold COMMITTED (cumulative105282 events), nhưng verify lỗi HTTP EOF đúng lúc Trino restart. Chưa Published/rerun acceptance; case2023-12+h2024/USGS chưa chạy ETL.
- Giữ nguyên artifacts/own lease theo lựa chọn owner; không tự phục hồi, retry, release, restart hoặc xóa dữ liệu. Runtime không còn Java/active DagRun khi bàn giao.

## Docs và giới hạn

- Xem DAT-03_COLLECTION_BOUNDED_E2E.md, DAT-03_BOUNDED_E2E_PARTIAL.json và DAT-03_RESEARCH_COLLECTION_E2E.md để lấy exact pins, failure state, số đo và commands.
- DAT-03 còn hoàn tất bước4, rollout225+63 tháng, official collection-aware MLI-02/registry/loader/import và receipts ba máy. Prepared collection không thay EXPORTED; chưa chạy full-pin production preparation/Mc hoặc cấp scientific approval.
- Pilot giữ PARTIAL/DEVELOPMENT_ONLY. EXP-02..05 official experiments/evaluation và SEC-01 cloud/Drive/Colab chưa được chứng nhận bởi PR này. Reviewer unassigned.
```
