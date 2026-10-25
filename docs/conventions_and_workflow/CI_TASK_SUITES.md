# CI theo nhóm chức năng/task

## Mục tiêu

Cập nhật trực tiếp PR #77, branch `ci/impact-based-tests`, từ head `8fba3c2`,
theo yêu cầu HoaiTam ngày 2026-10-15. Reviewer unassigned; chưa có task ID
riêng cho cải tiến CI, không đổi trạng thái/owner các task sản phẩm.

Không đặt mục tiêu số check hoặc tạo một workflow cho từng task backlog.
Task có logic độc lập có nhóm test riêng; task cùng contract dùng chung nhóm.
Mã task trong tên nhóm dùng để tra cứu, không dùng title/label PR để quyết định
chạy gì. Nguồn chọn test vẫn là [impact policy](./CI_IMPACT_TESTING.md).

## Các nhóm và mục đích

| Check/suite ID | Task/khối liên quan | Kiểm tra gì / dùng để làm gì |
|---|---|---|
| `docs-contracts` | Contracts, CFG, SEC, workflow | Guard luôn chạy: contract/config/secret hygiene/Compose tĩnh/CI policy; đồng thời gate kết quả pilot |
| `Java / foundation-integration` | SPK, Bronze shared, Silver integration | Smoke Spark, resource pilot, Bronze reuse, tích hợp Bronze→Silver; test Java mới ngoài mapping vào đây để không mất coverage |
| `Java / usgs-bronze` | USG | Request/HTTP/planner, write/readback Bronze, runner và fixture acceptance |
| `Java / jma-bronze` | JMA | Inventory, archive/download/validation, retry, Bronze và year ingest |
| `Java / silver` | SLV | Parse USGS/JMA, lineage/key, quality, dedup/entity, Parquet và publication |
| `Java / gold` | GLD | Canonical transform, Iceberg commit, publication và Trino client |
| `Java / orc-06-full-month` | ORC-06 | Coverage registry, baseline+delta, month/link closure, immutable state và Gold handoff |
| `Java / orc-07-real-adapters` | ORC-07 | Exact context/receipt chain, lease, full-month proof và actual local Iceberg retry; MinIO/Trino nghiệm thu riêng |
| `Java / mld-01-02-contract-audit` | MLD-01/02 và shared ML | Dataset/result contract, Gold audit, Mc/histogram; ML test mới chưa map cũng vào shared ML |
| `Java / mld-03-windows` | MLD-03 | Mainshock runtime/window engine/integration/regression/model resolver |
| `Java / mld-04-features` | MLD-04, Java boundary DAT-02 | Scaling, feature engine, pilot exporter và cross-JVM Parquet |
| `Java / mld-05-export` | MLD-05 | Receipt-bound coverage, feature quality, pinned Iceberg retry/retention, multi-part export và corruption guards |
| `Java / mli-02-build` | MLI-02, DAT-03 | Real dataset build adapter và full-catalog research build gate (`DatasetBuildJobTest`, `ResearchDatasetBuildJobTest`); fixture/pure tests không chứng nhận official export thật |
| `Control / source-ingestion` | AFL, USG-04, JMA-04/05 | Smoke/source DAG contracts, ingest/year-backfill runtime và JMA live-QA adapter bằng mock |
| `Control / orchestration-recovery` | ORC | ETL, source schedule/readiness, backfill, recovery, observability; Python control test chưa map vào đây |
| `Control / silver-gold-qa` | SLV/GLD QA | Mock adapter tích hợp Silver, Gold storage/publication |
| `Control / ml-qa` | MLD-01/02/03/05 | Dataset manifest, Gold input audit, window và feature export QA control-plane |
| `DAT-02 / pilot-handoff` | DAT-02 | Python loader, checksum/schema/grain/coverage và pack bundle bằng synthetic fixture |

Không tuyên bố các check mock này là E2E live, đã ingest thật hoặc đã fit model.
Các task không có implementation/test chưa được tạo check giả để hiển thị xanh.

## Luồng chạy và gate

1. `impact-plan` resolve diff base→merge ref và xuất mode/matrix ID cố định.
2. Matrix chỉ materialize nhóm có test được chọn; `none` skip worker trước setup.
   Docs-only có các worker skipped, không chạy Maven/Spark/Airflow/pilot.
3. Java/Airflow workers cùng dùng `make ci-suite`, tính lại plan và kiểm tra suite
   thuộc inventory hiện tại. Suite sai/không áp dụng/không có test thì **fail**.
4. Tên `java-spark`/`airflow` giữ làm gate tổng hợp, không chạy lại toàn bộ test.
   `docs-contracts` luôn chạy guards sau pilot rồi gate pilot. Ba gate đều dùng
   `always()` để không bị dependency failure biến thành skipped success.
5. Gate chỉ pass khi planner success và:
   - mode none → worker skipped;
   - mode full/selected → worker success.
   Worker failure/cancelled/skipped khi phải chạy, mode/output thiếu hoặc
   planner failure đều fail. DAT-02 đỏ chặn `docs-contracts`, không lọt required gate.

Matrix `fail-fast: false` giữ nhóm khác chạy tiếp khi một nhóm fail, giúp người
review thấy đủ lỗi. Java tối đa 3, control-plane tối đa 4 worker đồng thời,
mỗi worker trên runner/checkout riêng. Artifact XML Java tên theo suite, giữ
7 ngày, không upload JAR/data/secret. Planner lỗi không được coi là docs-only.
Mode none xuất placeholder matrix an toàn; job-level condition ngăn chạy placeholder.

Giữ required-check names không đồng nghĩa đã cấu hình branch protection.
Chỉ ba gate tổng hợp nên là required checks ổn định; không yêu cầu một matrix
check chỉ tồn tại ở một số PR. Không sửa GitHub Settings hoặc tự merge.

## Không mất coverage khi chia nhóm

Inventory Java quét đệ quy `spark/src/test/java`, theo các tên mặc định Surefire
`Test*`, `*Test`, `*Tests`, `*TestCase`; mỗi class đúng một nhóm. Inventory
Python quét `airflow/tests/test_*.py`; mỗi module đúng một nhóm. Nếu thêm nested
Python test package, planner fail yêu cầu cập nhật discovery/mapping trước,
không âm thầm bỏ sót. Test grouping
đối chiếu toàn bộ inventory, không dùng danh sách test cố định để bỏ test mới.

Mỗi Java suite chạy `clean verify`, compile mọi main/test source, chỉ test các
class thuộc nhóm và package runner. Mọi class được chọn phải có XML, có test
chạy và không fail/error/all-skipped. Không dùng report cũ. Python reject
module rỗng/import lỗi và trả lỗi nếu suite fail. Không chạy Maven đồng thời
trên cùng local checkout; `.NOTPARALLEL` trong Make không khóa hai terminal.

Chia check **không** tự chứng minh được độc lập dependency: USGS/JMA/Silver/Gold
hoặc helper/schema/build/fixture dùng chung vẫn fallback full như v1. Một sửa
MLD-03 chạy cả ba nhóm ML + control-plane + pilot; sửa feature leaf chỉ chạy
MLD-04 + MLD-05 consumer + DAT-02; sửa docs thông thường chỉ guards. Chỉ thu hẹp thêm khi có
mapping consumer và regression chứng minh; chưa rõ giữ full.

Thêm module/test: cập nhật mapping nếu cần tên nhóm riêng; nếu chưa map,
fallback nhóm shared vẫn chạy. Kiểm tra `make ci-plan` và inventory regression
trước merge. Không tự đánh dấu task Done chỉ vì check cùng mã task xanh.

## Chạy local

Cài prerequisites theo [CI guide](./CONTINUOUS_INTEGRATION.md). Dùng đúng ID
ở bảng trên, không nhập cả tiền tố `Java /` hoặc `Control /`:

```bash
make test-ci-workflow
make ci-plan
CI_COMPONENT=java CI_SUITE=mld-03-windows make ci-suite
CI_COMPONENT=java CI_SUITE=silver make ci-suite
CI_COMPONENT=airflow CI_SUITE=orchestration-recovery make ci-suite
make test-pilot PILOT_PYTHON=python3
```

Không PR context: `ci-suite` chạy toàn bộ test **của nhóm được chỉ định**, không
phải toàn project. Có PR context: suite phải nằm trong phạm vi plan, nếu không
fail thay vì green với 0 test. Buộc kiểm tra nhóm ngoài PR scope:

```bash
python3 scripts/ci-impact.py run-suite --component java --suite gold --force-full
```

`make ci`/`ci-java`/`ci-airflow`/`ci-docs` vẫn là full entrypoint cũ. Full local
Java vẫn package/fingerprint; mỗi shard CI không ghi fingerprint runtime.
Không Docker build/start/restart hoặc ghi lakehouse. Runner dependencies có
thể tải mạng lần đầu; HTTP/MinIO/Trino tests dùng mock/temporary storage.

## Đánh đổi và giới hạn

- Nhiều check giúp định vị/rerun lỗi, không đồng nghĩa tăng số test. Java shard
  lặp compile/package và setup dependency trên runner riêng; tổng runner-minutes
  có thể tăng. Maven cache/max-parallel giúp kiểm soát, không cam kết luôn nhanh hơn.
- Không bật `push` mọi feature branch để tránh nhân đôi check với PR. Main/manual
  vẫn full inventory; PR sửa chính CI/Makefile cũng full.
- Chưa có task riêng cho rulesets/required checks; maintainer cấu hình theo guide.
- [QA-01 — Chạy E2E daily đa nguồn](../task/tasks/QA-01.md) vẫn cần interval
  thật Bronze→Silver→Gold→Trino; mock CI không hoàn tất task này.
- [SEC-01 — Review secret và bề mặt truy cập](../task/tasks/SEC-01.md) vẫn có
  runtime/ML handoff gates riêng; số check không thay evidence các gate đó.
- Reviewer unassigned. Local PASS không phải GitHub-hosted Ubuntu/Temurin17 PASS;
  kiểm tra Checks của commit bản vá sau push trước merge. Không tự merge.

Tham khảo chính thức:
[job dependencies và always](https://docs.github.com/en/actions/how-tos/write-workflows/choose-what-workflows-do/use-jobs),
[matrix và fail-fast](https://docs.github.com/en/actions/how-tos/write-workflows/choose-what-workflows-do/run-job-variations).

## Evidence impact-v2 (mốc trước ORC-06)

ORC-06 bổ sung nhóm Java thứ 9 `orc-06-full-month`; policy path của package mới
vẫn conservative full regression. Các số 8 suites/363 tests ở evidence dưới
là mốc PR #77, không phải kết quả chạy sau khi thêm ORC-06.

Kiểm tra local ngày 2026-10-15, Python 3.13.3, JDK 21.0.11/Spark 3.5.9:

| Suite | Test classes/modules | Tests PASS |
|---|---|---|
| foundation-integration | 4 classes | 23 |
| usgs-bronze | 6 classes | 31 |
| jma-bronze | 7 classes | 65 |
| silver | 14 classes | 135 |
| gold | 4 classes | 17 |
| mld-01-02-contract-audit | 8 classes | 43 |
| mld-03-windows | 5 classes | 33 |
| mld-04-features | 4 classes | 16 |
| source-ingestion | 6 modules | 42 |
| orchestration-recovery | 10 modules | 142 |
| silver-gold-qa | 3 modules | 16 |
| ml-qa | 3 modules | 16 |
| pilot-handoff | test_pilot_*.py | 16 |

- 8 Java suites chạy thật bằng `make ci-suite`: tổng **363 tests**, 0
  failures/errors/skips, clean verify/package và XML từng class đều đạt.
- 4 Airflow suites chạy thật bằng `make ci-suite`: tổng **216 tests**,
  khớp inventory/discovery full, không trùng module. Pilot **16 tests** đạt.
- `make test-ci-workflow`: **37 tests PASS**, gồm inventory không mất/trùng
  test, selected/full/none matrices, nested Python package fail-closed,
  invalid/empty selector, module rỗng/import/suite failure, 125 tổ hợp
  planner/mode/worker result và mutation guardrails của graph/gate/workflow.
- `make ci-docs-base`: PASS; contract/config/Compose/security/lifecycle static
  regressions và shell syntax; actionlint 1.7.12 PASS; whitespace check PASS.
- Source/history scoped security audit: 0 findings, 3 pending coverage items,
  exit 2; không gọi đó là full SEC-01 acceptance hoặc chuyển task Done.
- Không Docker build/start/restart, gọi nguồn thật hoặc ghi dữ liệu lakehouse.

Đây là evidence local của impact-v2, không phải kết quả Temurin17/Ubuntu trên
GitHub. Checks của commit mới sau push là gate nghiệm thu hosted riêng;
không dùng ba checks xanh cũ ở `8fba3c2` làm bằng chứng cho bản vá.
