# CI theo phạm vi ảnh hưởng — impact-v2

## Quyết định và trạng thái

Theo yêu cầu HoaiTam ngày 2026-10-15. Branch `ci/impact-based-tests` tạo từ
main `3b23d0b` (PR #76 đã merge). Đây là cải tiến CI theo yêu cầu trực tiếp,
**chưa có task ID riêng trong backlog**; không đổi task status/assignee/effort.
Reviewer unassigned. Bản đầu impact-v1 ở commit `8fba3c2`; theo yêu cầu tiếp
theo, impact-v2 cập nhật trực tiếp nhánh PR #77, không tạo PR mới/merge hoặc
sửa GitHub Settings. Không thay logic dữ liệu hay metadata task.

Mục đích: giảm test không liên quan trên PR nhưng không bỏ regression của
consumer dùng chung. Không chọn theo tên task, branch, label hay tác giả PR.
Script [ci-impact.py](../../scripts/ci-impact.py) là nguồn policy dùng chung
trên CI/local; tests ở [tests/ci](../../tests/ci).

## Hoạt động

1. Job `impact-plan` checkout merge ref, fetch-depth=0, resolve plan bằng Python
   stdlib và xuất matrix nhóm test cho Java/Airflow. Không lấy danh sách file
   từ API có giới hạn số file. Worker tính lại plan trên cùng checkout trước chạy.
2. PR so **base SHA thực tế → merge SHA đang checkout** bằng local Git diff,
   không so HEAD của branch với main một cách cố định. Stacked PR cũng dùng
   đúng base của PR. HEAD không khớp checkout hoặc base không là ancestor thì full.
3. Union các phạm vi, không để scope sau ghi đè scope trước. Chọn leaf chỉ
   khi mapping đã được review; không đoán dependency bằng tên task.
4. Push main, workflow_dispatch, local không có PR context, diff trống/lỗi/
   quá giới hạn, rename/delete/type-change, shared/unmapped path: **full**.
5. Chia test được chọn thành [nhóm chức năng/task](./CI_TASK_SUITES.md), mỗi
   nhóm có check riêng. Ba check tổng hợp giữ tên cũ và kiểm tra kết quả
   planner + mọi worker tương ứng, kể cả khi dependency fail/skip/cancel.
6. Log JSON và Actions Summary có mode/reason/count và bảng suite/classes/modules.
   `none` là không áp dụng theo diff, không phải dấu hiệu test lỗi đã bị bỏ qua.

Git diff dùng `--no-renames -z`: rename thành delete+add và mở full, nên
không bỏ mất phía file cũ. Không truncate diff ở 300 file; trên 10,000 paths
hoặc 2 MiB metadata chuyển full. Tên path bất thường cũng chuyển full.
SHA chỉ qua environment/argv, không ghép shell hoặc in PR title/file name vào
Actions output. Output chỉ gồm enum full/selected/none và JSON matrix có ID
nhóm từ allowlist cố định; không đưa filename hoặc lệnh Maven vào matrix.

## Mapping hiện hành

| Thay đổi | Java | Airflow | Pilot Python |
|---|---|---|---|
| README, docs Markdown ngoài specs/contracts | Không chạy | Không chạy | Không chạy |
| Notebook/loader DAT-02, tests/test_pilot_*.py | Không chạy | Không chạy | Tất cả pilot tests |
| FeatureScalingConfig, SpacetimeFeatureEngine, LocalFeatureJob, PilotExportJob, WindowArtifactInput hoặc bốn test tương ứng | Bốn lớp feature/export/cross-JVM tests | Không chạy | Tất cả pilot tests |
| Ba config feature/pilot đã pin hoặc bốn script local ML/stage/inspection/pack | Nhóm feature như trên | Không chạy | Tất cả pilot tests |
| Java ML khác, gồm identity/audit/Mc/window hoặc test ML mới | Toàn bộ package ML | Tất cả Airflow tests | Tất cả pilot tests |
| Sáu leaf DAG skeleton/source wrappers hoặc airflow/tests/test_*.py | Không chạy | Tất cả Airflow tests | Không chạy |
| Silver/Gold/USGS/JMA/Bronze, root Java entrypoint, runtime/QA helpers, fixture/resource/schema, specs/contracts, build/dependency/Compose/CI/Makefile hoặc path chưa map | Full Java | Full Airflow | Full pilot |

Sáu leaf DAG: afl_01_smoke, usg_04_usgs_ingest, jma_04_year_backfill,
orc_01_etl_pipeline, orc_02_daily_sources, orc_03_backfill. Runtime helpers và
DAG mới **không** được tự động coi là leaf. Feature consumers trong exporter
và Python loader được giữ trong scope, không chỉ chạy unit test của class sửa.
Đây là explicit allowlist từ baseline trên, không phải dependency analyzer
tự suy diễn. Khi thêm consumer/model/module, cập nhật mapping và tests trong
cùng PR; chưa xác định đủ ảnh hưởng thì để full.

Docs/contracts/config/security/workflow guards vẫn luôn chạy qua
`make ci-docs-base`, kể cả docs-only. Docs job hiện vẫn cài validation tools
(gồm pyarrow theo CI requirements); tối ưu này chủ yếu tránh JDK/Maven/Spark
và Airflow test không cần thiết, không cam kết docs job tức thì.

## Giữ chất lượng và required checks

- Giữ ba tên check tổng hợp `docs-contracts`, `java-spark`, `airflow`, thêm
  check theo nhóm. Workflow không có path/branch filter cho PR.
- Worker không áp dụng được skip ở job level. Ba gate dùng `always()`:
  planner phải success; mode none chỉ nhận worker skipped; mode full/selected
  chỉ nhận worker success. Failure/cancel/skipped bất thường đều làm gate đỏ.
  Không dùng continue-on-error. Thiếu context chuyển full; planner lỗi thì đỏ.
- Mỗi nhóm Java vẫn **clean verify**: compile mọi main/test source, chạy lớp
  của nhóm trong phạm vi đã chọn, package JAR. Surefire XML phải có từng lớp, có test chạy,
  không fail/error hoặc bị skip toàn bộ. Không nhận JAR cũ/report cũ là pass.
- Inventory phân hoạch toàn bộ test classes/modules, không bỏ test mới chưa
  có mapping: đưa vào nhóm shared. Main/manual/full chạy hết các nhóm, không
  chạy full lặp lại ở mỗi nhóm. Chi tiết và giới hạn hiệu năng ở CI_TASK_SUITES.
- Các lệnh full `ci`, `ci-java`, `ci-airflow`, `ci-docs` giữ nguyên ý nghĩa.
  Local `ci-java` vẫn tạo fingerprint qua package-java; CI matrix không ghi
  fingerprint runtime, không chứng nhận Docker bằng một shard đã test.
- Main/manual luôn full; PR sửa chính workflow/planner/Makefile cũng full.
  Maintainer vẫn cần review mapping và full trước merge cho thay đổi rủi ro;
  full sau merge không thay pre-merge gate đối với code dùng chung.

Không đổi ruleset/required checks trong phiên này. Kiểm tra các tên check
thực tế sau push; không gọi local PASS là GitHub checks PASS. Tài liệu chính thức:
[workflow filters](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax),
[required checks](https://docs.github.com/en/pull-requests/how-tos/merge-and-close-pull-requests/troubleshooting-required-status-checks).

## Chạy local

```bash
python3 -m pip install -r tests/ci/requirements.txt
make test-ci-workflow
make ci-plan
CI_COMPONENT=java CI_SUITE=mld-03-windows make ci-suite
CI_COMPONENT=airflow CI_SUITE=orchestration-recovery make ci-suite
```

Mặc định không có PR context là full. Xem diff đã **commit** của branch hiện
tại với main (nếu main chưa là ancestor thì full, không đoán merge result):

```bash
CI_EVENT_NAME=pull_request CI_BASE_SHA=origin/main CI_HEAD_SHA=HEAD make ci-plan
CI_EVENT_NAME=pull_request CI_BASE_SHA=origin/main CI_HEAD_SHA=HEAD make ci-java-impact
CI_EVENT_NAME=pull_request CI_BASE_SHA=origin/main CI_HEAD_SHA=HEAD make ci-airflow-impact
CI_EVENT_NAME=pull_request CI_BASE_SHA=origin/main CI_HEAD_SHA=HEAD make ci-pilot-impact
```

Plan chỉ xét committed trees; thay đổi chưa commit không có trong diff này.
Dùng test trực tiếp/full để kiểm tra working tree. Local Java cần JDK17/21,
CI vẫn Temurin17. `clean verify` dọn target build, không dọn staging/lake/volume.
Không chạy nhiều Maven commands đồng thời trên cùng checkout.

Full bất kể PR context:

```bash
make ci
# Hoặc buộc full cho từng component:
python3 scripts/ci-impact.py run --component java --force-full
```

Không lệnh nào tự Docker build/start/restart, tải USGS/JMA hoặc trigger DAG.
Maven/Python dependencies có thể cần Internet lần chuẩn bị đầu tiên.

## Giới hạn / rollout / rollback

- Local PASS không phải GitHub-hosted run hoặc reviewer approval. Sau khi
  push bản vá, xem Checks của commit mới nhất; không dùng kết quả v1 làm v2.
  Các PR đang mở không tự nhận workflow mới: cần đưa code CI vào branch/base
  tương ứng và kích hoạt PR event mới; chỉ đổi main không chạy hồi tố mọi PR.
- Đây không phải task E2E/security acceptance: [QA-01 — Chạy E2E daily đa nguồn](../task/tasks/QA-01.md)
  vẫn cần daily thật; [SEC-01 — Review secret và bề mặt truy cập](../task/tasks/SEC-01.md)
  vẫn có runtime/ML handoff gates riêng. Không đổi status vì CI xanh.
- GitHub required checks/rulesets chưa có task riêng để cấu hình, cần maintainer
  xác nhận. Không rename check hoặc bỏ required check để xử lý lỗi selection.
- Rollback: revert thay đổi CI/planner/tests/Make/docs trong PR, giữ ba tên check
  và các lệnh full; không cần thay dữ liệu/runtime hoặc xóa bucket/volume.

## Evidence local impact-v1 — 2026-10-15 (lịch sử, trước chia suite)

- `make test-ci-workflow`: 25 tests PASS, gồm mutation guardrails, fixture Git
  merge-ref/base tiến lên độc lập, stacked PR, delete/rename, diff 306 paths,
  fallback thiếu history và fail propagation/report validation.
- `make ci-docs`: PASS; guards tĩnh/config/Compose, policy/workflow và 16 pilot
  Python tests. Không có Docker build/start hoặc gọi nguồn thật.
- Selected Java runner trên controlled feature scope: 4 test classes,
  **16 tests PASS**, clean verify/package và kiểm đủ từng Surefire report.
- `make ci-java-impact` không có PR context: fallback full,
  **363 tests PASS**, 0 failure/error/skip, package/fingerprint PASS.
- `make ci-airflow-impact`: fallback full, **216 tests PASS**;
  `make ci-pilot-impact`: fallback full, **16 tests PASS**.
- Docs-only plan: Java/Airflow/pilot đều none, runner không gọi subprocess.
  Diff PR #76 thật từ `884c422` đến `3b23d0b` chọn full vì có Makefile/shared
  helper, không giả coi đó là notebook-only.
- actionlint **1.7.12** PASS; binary macOS ARM64 đối chiếu SHA256 từ release
  chính thức trước chạy. `git diff --check` PASS.

Java acceptance ở host JDK **21.0.11**, Spark **3.5.9**, Python **3.13.3**;
chưa phải Ubuntu/Temurin17 GitHub-hosted run. Reviewer unassigned,
chưa commit/push hoặc chạy Actions tại thời điểm nghiệm thu impact-v1.
Evidence impact-v2 ở [CI_TASK_SUITES.md](./CI_TASK_SUITES.md).
