# MLD-02 — Published JMA audit/Mc pilot

Cập nhật: 2026-10-11. **Done theo phạm vi pilot của task**.
Branch `fix/mld-02-published-jma-audit`, từ `origin/main`
`f653a04773620faf01abfcacfbc52c457b4290a7` (đã có GLD-04/MLD-01).
Assignee **rosy179**; reviewer **HoaiTam** (được phân công; chưa có review/approval).
Implementation ban đầu: [PR #60](https://github.com/HoaiTam/japan-earthquake-etl/pull/60).
Bản vá hiện tại chưa commit/push/mở PR.

## Làm gì và bằng cách nào

- Giữ audit rules, split boundaries, reasons và no-fake-Mc guard; không xóa Gold.
- Spark tổng hợp `groupBy(bin).count()`; driver nhận tối đa 10.000 bins/group
  ALL/SHALLOW/INTERMEDIATE/DEEP. Vượt giới hạn fail closed, không collect raw
  magnitudes. Sửa cumulative count ở threshold không có bin, reject cấu hình
  nhỏ hơn độ chính xác; Markdown ghi đúng applied/fixed Mc, N/A thay NaN.
- Controller/service/Make target QA đọc lại exact Silver marker/manifest/file
  checksums, tạo Gold QA mới với empty baselines, commit six snapshots và Trino
  verify PASSED trước Published. Không thay production gate COMPLETE_MONTHS.
- Pin hai persisted manifests BUILDING; gọi production CLI `GoldInputAuditJob`
  hai lần/split. Trino độc lập kiểm tra input/eligible; kiểm tra handoff Mc,
  canonical config/hash/counts. Shared lease chỉ release sau tất cả gates.

## Evidence live — không phải fixture

[Receipt metadata đầy đủ](./MLD-02-published-jma-runtime.json) dùng
[SilverReady SLV-09 thật](./SLV-09-live-readback.json): 16 USGS và 256 physical
records đầu mỗi archive JMA 2000/2023, tổng 528 observations. Runner đọc lại
persisted bundle; không dùng rows sinh bằng code.

- Run: `mld02-qa-13275d43f6514a0db6f2b4e71309be3c`.
- Gold: `iceberg.gold_qa_13275d43f6514a0db6f2b4e71309be3c`.
- ML: `iceberg.ml_qa_13275d43f6514a0db6f2b4e71309be3c`.
- Silver SHA: `2fdf83893ede773c811d5b83117c42b05920e680d4a078ecaf036a3f00340b21`.
- Publication: `pub_788fb67d18673a9af6cb6ab23fa0392c94544261f69fdceaa28a89d4e812c063`.
- Physical event snapshot: **`8798380333486384901`** (integer 64-bit nguyên vẹn;
  không round-trip bằng JavaScript Number).
- Cutoff `2026-10-11T15:33:46.539Z`, sau publication; không chứng minh historical
  source state as-of 2018/2023. Audit kết thúc `2026-10-11T15:34:29.039702922Z`.
- Runtime Java 17.0.19 / Spark 3.5.9 / Iceberg 1.10.1; Trino image 483.
- Binary: `artifact_65605a3bfbe9ea6f322dbefd603c6f1c3ef9dac552bcf7f014a6c27cfe8e639e`.
  Đây là hash runner chưa commit, không gán main SHA thành code chứa bản vá.
  Production phải pin commit/tag thật sau commit.

| Split | Input | Pre-Mc sample | Mc central | Sensitivity | Eligible | Excluded |
|---|---:|---:|---:|---|---:|---:|
| REPRODUCTION | 528 | 176 | 0.6 | [0.4, 0.8] | 140 | 388 |
| EXTENSION | 528 | 247 | 0.9 | [0.7, 1.1] | 125 | 403 |

Method MAXIMUM_CURVATURE, version `maximum-curvature-v1`, bin0.1,
sensitivity±0.2, guard>=50, region `overall-bounded-pilot-v1`.
Mỗi estimator chỉ dùng events qua rules1–8 trong period tương ứng; extension
không tune reproduction. Global Mc là ngưỡng áp dụng; depth chỉ diagnostic:
reproduction shallow152/Mc0.6, intermediate23/deep1 không đủ sample;
extension shallow232/Mc0.9, intermediate15/deep0 không đủ sample.

| Primary reason | Reproduction | Extension |
|---|---:|---:|
| ML_MISSING_MAGNITUDE | 15 | 15 |
| ML_NON_NATURAL_EVENT | 2 | 2 |
| ML_SOURCE_NOT_COMPARABLE | 16 | 16 |
| ML_OUTSIDE_TIME_RANGE | 319 | 248 |
| ML_BELOW_COMPLETENESS | 36 | 122 |
| Tổng | 388 | 403 |

`528=140+388=125+403`; tổng primary reasons bằng excluded.
Trino independently xác nhận 528 input và 140/125 eligible trên exact snapshot.
Commit/publication/dataset IDs/stable audit counts/Mc/hash không đổi khi rerun
(audit clock được phép thay đổi). Own lease đã release. Không thay serving alias
hoặc Gold sản phẩm. Iceberg manifest/handoff vẫn BUILDING, research_ready=false.

Dataset IDs và Mc config hashes:

- Reproduction: `ds_b0d94968d9471049fcb41895eafd0a09c26d37463c87af91b870f9fee39a4690`,
  hash `f5d0a12f8a7bc8458375f0d8876005f2548ba0bce78f380005fa95192f4168ff`.
- Extension: `ds_3f1b48d922dc012bbf015d12ee4e2810951bdf11459ce1fa279df3d0730791cd`,
  hash `d7dba4026ba9faa6e73bdbd33cc4f15878446776a69cd06d1f6ef6d76b448278`.

## Kiểm thử thực tế

- Offline Docker builder Java17.0.20.1 / Maven3.9.16:
  `mvn --offline --batch-mode --no-transfer-progress clean verify`:
  **317 Java tests đạt**, 0 failures/errors/skipped.
- Rebuild sau chỉnh Markdown/cutoff:
  `mvn --offline --batch-mode --no-transfer-progress -pl spark -am -Dtest=MagnitudeCompletenessEstimatorTest,MagnitudeHistogramSparkTest,GoldAuditAcceptanceTest,GoldInputAuditEngineTest,GoldInputAuditIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false verify`:
  **25 tests đạt**. Regression một triệu events với driver result limit1MiB;
  excessive bins fail closed, không truncate.
- `python3 -m unittest discover -s airflow/tests`: **199 tests đạt**.
  Sáu controller tests: success/own-release, busy owner, timeout/nonzero,
  bad receipt, unverified reference, tampered pins/hash/nonfinite Mc.
- `make ci-docs` (venv có PyYAML), Compose profile gold config và
  `git diff --check`: đạt.
- `make verify-samples`: MinIO đọc USGS16/JMA2023 structural257020; đây là
  input check, không lấy count257020 làm rows đã audit.
- Live dùng runner đã build và image có sẵn:
  `docker compose --env-file .env -f compose.yaml --profile gold run --rm --no-deps --name ie212-mld02-audit-qa gold-input-audit-qa`:
  **VERIFIED_BOUNDED_JMA_PILOT**. Không claim đã chạy lại toàn Docker build qua
  `make smoke-ml-audit`; Make target là entry point để team lặp lại flow.
- Receipt đã validate lại sau hardening handoff/counts/Mc/hash; Trino readback
  exact64-bit snapshot vẫn trả528, shared lease trống.
  Sáu controller tests cũng đạt trên Spark image Python3.10.12 với UID runtime;
  không submit JVM hoặc gọi storage trong các unit tests này.

Lần live đầu phát hiện image Python3.10.12 không có `hashlib.file_digest`;
đã sửa hash streaming và chạy lại thành công, không acquire lease ở preflight
lỗi này. Test offline đầu thiếu loopback hostname; đã chạy lại với
`--hostname localhost` / `SPARK_LOCAL_IP=127.0.0.1`, rồi toàn suite đạt.

## Handoff và giới hạn

Scoped staging `/opt/pipeline/staging/gold-input-audit-qa/<run_id>/` chứa hai
thư mục reproduction/extension với audit JSON/Markdown và audited manifests.
Receipt Git chỉ là metadata, không làm portable warehouse. Môi trường khác
phải còn exact Silver objects rồi chạy `make smoke-ml-audit` để tạo QA pins của
mình; không dùng snapshot IDs này cho catalog khác.

- [MLD-03 — Chọn mainshock và tạo candidate windows](../task/tasks/MLD-03.md):
  nhận exact audited manifests/Mc/config/hash/counts; còn candidate live acceptance,
  vẫn Needs Update. MLD-02 Done không tự nghiệm thu MLD-03.
  Trino readback trên cùng physical snapshot: 0 mainshock magnitude>5.5,
  depth50–200km; max primary-JMA natural/ROI magnitude5.1. Pilot hiện chỉ hỗ trợ
  empty path, chưa nghiệm thu windows non-empty/PRE/POST. Cần sample có mainshock,
  pin/audit lại; không hạ baseline threshold hoặc dùng USGS thay JMA.
- [MLD-04 — Tạo và scale feature không-thời gian 4-D](../task/tasks/MLD-04.md),
  [MLD-05 — Validate feature snapshot và export Parquet bundle](../task/tasks/MLD-05.md):
  chưa feature/export; BUILDING không phải VALIDATED/EXPORTED hoặc Colab input.
- [MLI-02 — Tạo Airflow DAG build ML dataset](../task/tasks/MLI-02.md):
  QA runner chưa phải scheduled ML DAG.
- [QA-01 — Chạy E2E daily đa nguồn](../task/tasks/QA-01.md),
  [QA-03 — Kiểm thử JMA historical backfill](../task/tasks/QA-03.md):
  chưa production full-month/full-history hoặc E2E acceptance.
- [QA-05 — Profile dữ liệu lịch sử và tài nguyên local](../task/tasks/QA-05.md):
  histogram regression không thay full-history performance benchmark.
- MAXC0.6/0.9 chỉ là first256 pilot có selection bias; guardN>=50 không chứng minh
  completeness khoa học. Không dùng để chốt Mc toàn period hoặc kết luận
  network-change từ sample thưa; không gọi sequence là causal.
- HoaiTam mới được phân công reviewer; chưa có review độc lập/approval.

Evidence cũ PR#60 chỉ có15 synthetic tests (140 rows sinh bằng code), không phải
DAT-01/SLV-09 hay Published JMA. Receipt thật đóng gate pilot còn thiếu, không
nâng fixture thành dữ liệu thật.
