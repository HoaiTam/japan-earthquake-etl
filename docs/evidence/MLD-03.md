# Evidence MLD-03 - Bản vá review PR #61

## Nghiệm thu runtime hiện tại — 2026-10-12

- Branch `fix/mld-03-pinned-jma-windows`, từ main mới nhất
  `e829fee3b690a1244dfa7d100cc955ac12c4f915` (PR#66 đã merge).
- Assignee **rosy179**; reviewer **HoaiTam** (được phân công; chưa review/approval).
- Bổ sung native exact-pin/audit-handoff gate, sampling mainshock JMA có physical
  lineage, isolated QA flow và Trino oracle; **Done** trên scoped live pilot.
- Phần bên dưới là evidence lịch sử PR#61, không phải trạng thái/ownership của
  bản vá hiện tại. Không nâng synthetic fixture thành Published JMA evidence.

### Kết quả live đã đạt

- [Receipt metadata](./MLD-03-window-pilot.json) là projection của native report
  86936 bytes, SHA256 `9246338b3d754ff70835a5d092efe12fbb5ce479ebe197bdb6197579e56f6353`.
  Không commit raw dumps/Parquet; projection bỏ verbose publication SQL, giữ
  manifests/audit/window config, pins/counts/oracle/checksum/fingerprints. Snapshot
  IDs64-bit giữ nguyên bằng Python/Java JSON, không round-trip qua JS Number.
- Own QA run `mld03-qa-3786f56677984ae7a5430efd32f5de77`; window resume child
  `resume-77a4623bd3f04745b19726bad9a6992c`. Status
  `VERIFIED_PUBLISHED_JMA_WINDOWS`, research_ready=false, resumed_from_published_qa=true.
- Exact physical table `iceberg.gold_qa_3786f56677984ae7a5430efd32f5de77.event_current`,
  snapshot `1323730420318869265`, count11034; PASSED publication/six pins fresh
  reverified. Source Gold artifact `713d1d6a9b8072f753431639e3b38a17ce0322628e287be004d64b28e6ee44a3`;
  window builder artifact `06554f81c58eda74fc6419edaf31d934061432434d7f3ab992f844c8c6193c95`.
  Immutable source manifest không bị sửa để giả builder version mới.

| Split | Mc thật | Input | Eligible | Excluded | Mainshocks | PRE | POST | Total candidates |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| REPRODUCTION | 1.2 | 11034 | 1783 | 9251 | 2 | 22 | 255 | 279 |
| EXTENSION | 0.9 | 11034 | 4150 | 6884 | 1 | 1 | 6 | 8 |

- Dataset IDs: reproduction `ds_98d4837dcb142bad1fb0ba2e77ad7524193056e57ca1b9a68269cc4b814022f5`,
  extension `ds_af9c0f7204fd96dfd331f99edb9d5b52f41c659a565228769d59f33fdaedcb6a`.
  Cả hai vẫn BUILDING: đây là **window stage**, không feature/Colab bundle.
- Mỗi split chạy CLI hai lần: fingerprints/counts/Mc hash giữ nguyên; self đúng
  một lần/window, PRE/POST non-empty, zero flagged/rejected windows; distributed
  readback invariants, actual Parquet checksums, bucket physical plan không
  Cartesian/BNLJ và independent Trino **per-window** counts/roles đều PASS.
  Trino integer-microsecond ±1µs gate cũng PASS. Controller xác nhận receipt rồi
  release **own** lease; đã đọc lại owner và xác nhận lease trống.
- Java runtime17.0.19, Spark3.5.9, Iceberg1.10.1, Trino483; opt-in QA local[1],
  driver768m, memory2GiB. Reallocate riêng QA container lên2 CPU khi test riêng
  pause; không đổi resource/service nền, không benchmark full-period.
- Scope **first-mainshock-centred14-day slices** trong JMA2000/JMA2023,
  USGS16 control. Có selection bias/missing full-duration edges; Mc chỉ đúng
  scope pilot. Không tuyên bố train dataset/full40-year completeness/aftershock
  labels. QA-03/QA-05 và MLD-04/MLD-05/MLI-02 là phần tiếp theo theo spec.
- Lệnh live thực tế: fresh `docker compose --env-file .env -f compose.yaml --profile gold run --rm --no-deps --name ie212-mld03-window-qa mainshock-window-qa`,
  sau sửa window `make resume-ml-window QA_RUN_ID=mld03-qa-3786f56677984ae7a5430efd32f5de77`.
  Fresh run đã Published/audit nhưng lỗi UDF; chỉ resume cuối mới là nghiệm thu
  window thành công. Không ghi "make smoke-ml-window full fresh PASS".

### Kiểm tra cuối trên bản vá đã nghiệm thu

- **82 Java targeted tests PASS**, 0 failures/errors/skips, BUILD SUCCESS
  2026-10-12T07:08:32Z, elapsed1m20s: resolver6, engine6, fixture integration1,
  window regression9, runtime11, sampler4, JMA parser31, JMA integration6,
  Silver integration8. Chạy Maven offline `-pl spark -am -Dtest=WindowModelResolverTest,MainshockWindowEngineTest,MainshockWindowIntegrationTest,MainshockWindowRegressionTest,MainshockRuntimeTest,JmaMainshockPilotSamplerTest,JmaFixedWidthParserTest,JmaParserIntegrationTest,SilverMultiSourceIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test`
  trong builder Java17.0.20.1/Maven3.9.16, network none/loopback, CPU2/memory2GiB.
  Lượt cuối chỉ test, không package/ghi đè runner JAR đã nghiệm thu.
- Python **206 PASS**, bao gồm controller window7 tests; `make ci-docs`,
  task-status, Compose gold config và diff checks PASS; final docs được kiểm tra
  lại khi bàn giao. Full-suite Java327 ở dưới là lượt trước
  các regression cuối, không gọi là final full-suite.

### Kiểm tra bản vá hiện tại

- Java parser/Silver/runtime targeted `verify`: **57 tests PASS**, 0 failures/errors/skips;
  gồm `MainshockRuntimeTest` (8), `JmaMainshockPilotSamplerTest` (4),
  `JmaFixedWidthParserTest` (31), `JmaParserIntegrationTest` (6),
  `SilverMultiSourceIntegrationTest` (8). BUILD SUCCESS ngày2026-10-12T03:01:49Z.
  Builder Java17.0.20.1/Maven3.9.16, Spark3.5.9, offline cache, loopback;
  package runner và Iceberg runtime1.10.1 thành công, không skip tests.
- `python3 -m unittest discover -s airflow/tests -p 'test_*.py'`:
  **205 tests PASS**, Python3.13; gồm sáu tests controller window QA.
- `PATH=<PyYAML-venv>/bin:$PATH make ci-docs`: PASS (repo/example only);
  gồm task-status/contract/Compose/secret hygiene và CI workflow mutation tests.
- Full Maven `clean verify` trong lượt đầu của bản vá: **327 tests PASS**;
  đây là kết quả trước các bổ sung oracle microsecond, diagnostic và signed-degree
  regression cuối, không gọi là full-suite cuối cùng. Phần mới có targeted57
  và44 tests (round-trip config) ở đây; các kết quả lịch sử307/193 ở dưới thuộc PR#61.

### Chẩn đoán live trước nghiệm thu

- Run `mld03-qa-3786f56677984ae7a5430efd32f5de77` đã có Published Gold và
  reproduction audit đạt như lượt trước; runtime window phát hiện NPE do
  Catalyst đánh giá dimension UDF với `magnitude=null` trên row không eligible.
  Fixture cũ non-null không bắt được. Bản vá làm cả5 UDF null-safe, không đổi
  eligible counts/Mc/thresholds; thêm Spark regression schema nullable + derived
  eligibility, missing magnitude/latitude và non-finite/out-of-range fields.
  `verify` **54 tests PASS** ngày2026-10-12T06:50:43Z (31 parser,4 sampler,
  10 runtime,9 window regression); sau đồng bộ exact oracle bounds/bbox/tolerance,
  `MainshockRuntimeTest` **11 PASS**, BUILD SUCCESS2026-10-12T06:53:29Z.
  Python **206 tests PASS**, controller window QA7 tests.
- Lượt broader79 tests đã kết thúc exit137, không hoàn tất và **không tính PASS**;
  container không còn, không suy ra OOM hoặc nguyên nhân từ exitcode đơn lẻ.
  Các targeted runs thành công phía trên là evidence riêng, không thay full-suite.
- Giữ nguyên Published Gold/Silver và own retained lease của run3786…;
  bổ sung opt-in `make resume-ml-window QA_RUN_ID=...` chỉ reverify source pins
  và tiếp tục window stage. Source code_version cũ giữ nguyên; builder artifact
  mới được pin bằng window_code_version và dataset identity mới, không sửa
  manifest cũ. Resume sau đó đã đạt theo kết quả hiện tại ở đầu tài liệu;
  không coi fresh run lỗi UDF hoặc lượt test exit137 là PASS.
- Run `mld03-qa-bc23073bbd474152a637769d7a1cd406` sau sửa JMA parser đã đọc
  đủ11034 observations, publish Gold QA `PASSED` (natural/ROI serving11002).
  Reproduction audit đọc exact snapshot: input11034, eligible1783, excluded9251,
  `candidate_build_allowed=true`. Window gate phát hiện bug so sánh config
  Jackson IntNode/LongNode cho hard-limit10000 qua JSON; chưa có window success.
  Sửa so sánh canonical JSON text, giữ nguyên parameters/immutable manifest;
  thêm round-trip regression cho cả hai splits và reject changed thresholds.
  Targeted `verify` mới nhất **44 tests PASS** (31 parser,4 sampler,9 runtime),
  BUILD SUCCESS ngày2026-10-12T03:16:32Z. Gold QA cũ giữ nguyên, own lease được
  release sau xác minh process đã dừng/PASSED/audit completed; không xóa data.
- Run `mld03-qa-573fda92ea344115934f7d72962779ad` dùng đúng ba Bronze pins
  USGS16, JMA2000 và JMA2023. Sample tổng11034 rows, valid11024/reject10:
  `INVALID_LATITUDE=9`, `INVALID_LONGITUDE=1`; cả bốn reconciliation checks PASS.
  Gate chặn **trước** Silver publication/Gold schema writes, không có success
  receipt. Bounded metadata nằm tại scoped `silver-blocked-report.json`.
- Đã đối chiếu SHA manifest, ZIP và từng raw record hash trước đọc byte:
  `h2000;line=3595` degree latitude `- 1`, `line=4449` longitude `- 66`.
  Đây là signed degree padding trong I3/I4, không phải minutes ngoài range.
  Sửa riêng coordinate degree parser thành `slv-03-v1.1`, giữ native slices
  cho source key/hash/locator, giữ ngoài ROI trong Silver và toàn bộ gate.
  [Format JMA](https://www.data.jma.go.jp/eqev/data/bulletin/data/format/hypfmt_e.html)
  xác định columns22–24 I3 và33–36 I4; bằng chứng padding là byte Bronze thật.
- Run đầu dừng vì provenance URL null bị đọc thành empty string: đổi accessor
  để giữ null như `JmaParseContext` cho phép, không bịa source URL.
- Recovery chỉ release lease chính run lỗi sau xác nhận container JVM đã dừng,
  scoped Gold/ML schema không tồn tại và metadata cho thấy blocked-before-publication.
  Không xóa Bronze, warehouse, QA schemas, staged artifacts hay volumes.

## Evidence lịch sử PR#61

- Task: MLD-03 - Chọn mainshock và tạo candidate windows.
- Assignee bản vá: **HoaiTam**; tác giả implementation ban đầu: rosy179.
- Reviewer: **unassigned** (chưa có review độc lập).
- Branch: `feat/mld-03-mainshock-candidate-windows`.
- PR: [#61](https://github.com/HoaiTam/japan-earthquake-etl/pull/61), sửa trực tiếp, không tạo PR/branch mới.
- Trạng thái: **Needs Update**; acceptance kỹ thuật có regression tests, chưa nghiệm thu Published JMA/Mc thật.
- Main được tích hợp: `b29478bd6190338a8d60818177b12370650a5375`, giữ Gold writer/publication, dataset manifest và MLD-02 mới.
- Ngày kiểm tra: **2026-10-11**, Asia/Ho_Chi_Minh.

## 1. Đối chiếu 5 điểm review

| Điểm review | Bản vá và bằng chứng |
|---|---|
| Non-equi join vẫn BroadcastNestedLoopJoin | Window expand các bucket UTC-day / floor(latitude) / floor(longitude); candidate thuộc một bucket; equi-join ba keys rồi time/bbox/distance residual. Test executed physical plan không có CartesianProduct/BroadcastNestedLoopJoin, có hash/sort-merge join; chạy cả broadcast mặc định và tắt broadcast với input repartition. WindowModelConfig Serializable để UDF hoạt động qua shuffle. |
| Invariant collect toàn candidates và lặp M×C trên driver | GroupBy grain, aggregate self/count, full-outer đối soát window và distributed join/filter kiểm tra role/time/vector. Chỉ scalar count/boolean trả về driver; không collect candidate rows. Engine chỉ collect metadata theo window cho report. |
| Parquet lỗi nhưng catch(Throwable) fallback JSON báo success | Bỏ fallback; propagates storage/config exceptions; readback cả hai Parquet counts rồi mới ghi success reports. Invalidate hai report cũ trước computation. Regression tạo missing output committer, xác nhận job throw và không còn success report/JSON fallback; reject hard-limit không export. |
| Cast timestamp xuống giây làm mất precision | Dùng unix_micros/timestamp_micros cho bounds và delta; PRE -0.8s và POST +0.2s giữ đúng dấu. Test exact boundaries inclusive, ngoài biên một microsecond, qua ngày và spatial cell. Tie non-self cùng timestamp giữ POST/delta=0, không bịa epsilon/aftershock label. |
| Done/evidence dùng synthetic input gây hiểu nhầm | Task/index đồng bộ Needs Update, assignee HoaiTam. Integration hiện là audit→window **fixture**, lấy mcApplied được chấp nhận từ MLD-02, không dùng central Mc chưa reliable. Chưa có Published JMA audit/Mc thật; không gọi đây là runtime E2E hoặc nghiệm thu dataset thực. |

## 2. Kiểm thử và cách chạy lại

Target `make test-ml-window` gồm **21 tests**:

- WindowModelResolverTest: 6 tests công thức/config/bounding-box theo mô hình phẳng cục bộ. Không dùng test này để tuyên bố kiểm chứng khoảng cách cầu/haversine.
- MainshockWindowEngineTest: 6 tests grain/self/PRE/POST, overlapping windows, physical plan, resource flag không truncate, nested mainshock và selection boundaries.
- MainshockWindowIntegrationTest: 1 **fixture** audit→accepted Mc→window→Parquet readback/report/manifest enrichment.
- MainshockWindowRegressionTest: 8 tests precision/corrupt vector, simultaneous non-self, exact microsecond/day/spatial boundaries, shuffle join/order independence, invalid count/duplicate/missing self, Parquet/validation failure invalidation, rejected export và empty/invalid Mc.

Chạy tại repo root:

```bash
make test-ml-window
make ci-docs
make ci-airflow
make ci-java
git diff --check
```

Môi trường kiểm chứng Java: image builder local, Linux ARM64, Java 17.0.20.1,
Maven 3.9.16, Spark 3.5.9, Spark loopback; chạy trực tiếp recipe Maven vì image
builder không có GNU Make. Dependency Iceberg và build plugin mới trên main
được tải từ Maven Central vào cache tạm, không thay version/bỏ tests. Unit/fixture tests không
gọi USGS/JMA API hoặc thay đổi bucket/warehouse.

Kết quả cục bộ đã kiểm chứng:

- Targeted MLD-03: **21 tests PASS**, 0 failures/errors/skips.
- Full Maven `clean verify`: **307 tests PASS**, 0 failures/errors/skips,
  **BUILD SUCCESS**, đóng gói runner JAR thành công; thời gian 6m06s.
  Full suite chạy sau thay đổi cuối cùng, bao gồm regression invalidate report
  khi validation lỗi trước export.
- `make ci-docs`: PASS, gồm contract/config/Compose static checks và 12 Java
  build-input regression tests; không có local .env, chỉ kiểm tra repo/example.
- `make ci-airflow`: **193 tests PASS**, Python 3.13.
- `make test-ci-workflow`: **4 tests PASS**, gồm 16 mutation subcases.
- actionlint 1.7.12, `make check-task-status`, `git diff --check` và staged
  diff check: PASS. Không còn conflict markers/unmerged files.

Đây là local evidence, không phải kết quả GitHub Actions. Sau push, xem
checks trên merge ref mới nhất của PR #61; không suy ra checks PR khác đã xanh.
Run GitHub đầu của bản vá đã pass docs/contract/guardrails và Airflow nhưng
fail whitespace gate do blank line cuối file task từ PR cũ. Đã bỏ blank line
và kiểm tra thêm toàn diff `git diff --check origin/main HEAD`, không chỉ diff
working tree/index. Không tắt hoặc nới lỏng CI gate để xử lý lỗi này.
Các lỗi lần đầu (cache thiếu Iceberg, config chưa Serializable ở nhánh shuffle)
đã được xử lý và giữ regression test để tái lập; không coi lượt lỗi là PASS.

## 3. Giới hạn tại thời điểm review PR#61 (lịch sử)

Trạng thái Needs Update/candidate chưa chạy trong phần này là lịch sử trước
nghiệm thu2026-10-12. Kết quả hiện tại là Done theo receipt ở đầu tài liệu;
các giới hạn full historical/feature/export bên dưới vẫn áp dụng.

- [MLD-02 - Audit Gold và xác định magnitude of completeness](../task/tasks/MLD-02.md):
  đã Done theo pilot Published JMA ngày 2026-10-11, xem
  [receipt/handoff MLD-02](./MLD-02.md). Các nhận định thiếu upstream trong
  bảng review ở trên là lịch sử trước nghiệm thu này. MLD-03 vẫn Needs Update;
  chưa chạy candidate live. Dùng output này với dataset_id/snapshot đã pin qua
  [MLD-01 - Pin Gold snapshot và tạo dataset manifest](../task/tasks/MLD-01.md),
  lưu config/hash/Mc, reconcile counts và artifact readback thật rồi nghiệm thu.
  Không dùng Mc=2 fixture cho runtime hoặc chuyển task Done vì CI xanh.
  Trino readback pilot MLD-02: 0 mainshock magnitude>5.5/depth50–200km, max
  primary-JMA natural/ROI magnitude5.1. Cần sample có mainshock, pin/audit lại
  để test window non-empty/PRE/POST; không hạ baseline threshold.
- [QA-05 - Profile dữ liệu lịch sử và tài nguyên local](../task/tasks/QA-05.md):
  chưa benchmark full historical, phân bố bucket/skew và chi phí metadata report
  theo số windows; equi-join không phải bằng chứng hiệu năng toàn bộ 40 năm.
- [MLD-04 - Tạo và scale feature không-thời gian 4-D](../task/tasks/MLD-04.md):
  feature scaling/vector chưa thuộc bản vá này.
- [MLD-05 - Validate feature snapshot và export Parquet bundle](../task/tasks/MLD-05.md):
  chưa có verified Colab bundle/checksum hoặc lifecycle dataset VALIDATED.
- Hai local Parquet directories không có atomic multi-table commit; khi export
  lỗi có thể còn partial directory, nhưng không được dùng nó làm readiness.
  Completed window audit report chỉ là gate local, không phải Published Iceberg.
- Candidate window là tiền lọc hồi cứu, không phải nhãn dư chấn hoặc dự báo động đất.

## 4. Tài liệu liên quan

- [Task MLD-03](../task/tasks/MLD-03.md)
- [Mainshock/window spec](../specs/MAINSHOCK_AND_CANDIDATE_WINDOWS.md)
- [ML logical model](../specs/ML_DATA_MODEL.md)
- [CI và stacked PR](../conventions_and_workflow/CONTINUOUS_INTEGRATION.md)
