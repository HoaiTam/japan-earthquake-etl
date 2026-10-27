# DAT-03 — Bàn giao bản vá phục hồi catalog

## Cập nhật bàn giao tối27/10

Owner đã yêu cầu commit/push nhánh `perf/dat-03-verification-benchmark`, gồm
recovery/CAS prerequisite bên dưới và benchmark mới. Các lệnh/branch trong
phần lịch sử không phải nhánh cần push hiện tại. V2 đã bị Docker shutdown ở
verify ordinal134, thiếu terminal receipt; không còn worker, giữ own lease.
Accepted vẫn113/288. Profile primary single-pass mới chỉ opt-in component,
chưa wiring vào recovery; A/B thật16,47s so88,70s,0vi phạm cả hai cách.
Xem [benchmark](DAT-03_VERIFICATION_BENCHMARK_20261027.md).
Không dùng mô tả lịch sử "đang chạy" bên dưới làm trạng thái hiện tại.

Branch `fix/dat-03-catalog-publication-recovery`, base main `fe80830`.
Không tự commit/push/merge. Giữ nguyên các thay đổi storage/CAS có sẵn trước
phiên27/10; chúng là prerequisite của host recovery trên descriptor layout,
không bỏ các file đó khi đóng gói PR. Không đưa staging, JAR, ZIP, Parquet,
private logs hoặc credential vào Git.

Trạng thái runtime và actual pins nằm tại
[evidence107](./DAT-03_CATALOG_RECOVERY_20261027.md) và
[bản vá polling/V2](./DAT-03_CATALOG_VERIFY_RECOVERY_200906.md), không suy acceptance
từ unit tests. DAT-03 vẫn In Progress cho full catalog/export/ba máy.

## Lệnh Git cho owner

Chạy từ repository checkout chứa branch trên. Xem diff trước khi stage vì
working tree có cả CAS prerequisite và bản vá recovery mới.

```bash
cd /Users/hoaitam/Desktop/Learn/23521394/HK1-Nam-4/IE212/japan-earthquake-etl-mli-02
git status --short --branch
git diff --check
git diff --stat
git add Makefile \
  scripts/ci-impact.py \
  scripts/research-catalog.py \
  scripts/research-catalog-progress.py \
  scripts/research-catalog-recovery.py \
  scripts/research-observation.py \
  scripts/research-ml-export.py \
  tests/test_research_catalog.py \
  tests/test_research_catalog_recovery.py \
  tests/test_research_observation.py \
  tests/test_research_ml_export.py \
  tests/ci/test_suites.py \
  spark/src/main/java/ie212/earthquake/spark/usgs/ResearchCatalogRecoveryJob.java \
  spark/src/main/java/ie212/earthquake/spark/usgs/ResearchCatalogJob.java \
  spark/src/main/java/ie212/earthquake/spark/usgs/ResearchCatalogCompatibility.java \
  spark/src/main/java/ie212/earthquake/spark/gold/TrinoSqlClient.java \
  spark/src/test/java/ie212/earthquake/spark/usgs/ResearchCatalogRecoveryJobTest.java \
  spark/src/test/java/ie212/earthquake/spark/gold/TrinoSqlClientTest.java \
  docs/specs/RESEARCH_HOST_OBSERVATIONS.md \
  docs/specs/RESEARCH_CATALOG_ROLLOUT.md \
  docs/runbooks/DAT-03_CATALOG_PUBLICATION_RECOVERY.md \
  docs/runbooks/DAT-03_BATCH_EXECUTION.md \
  docs/runbooks/DAT-03_RESEARCH_ML_EXPORT.md \
  docs/evidence/DAT-03_CATALOG_RECOVERY_20261027.md \
  docs/evidence/DAT-03_CATALOG_VERIFY_RECOVERY_200906.md \
  docs/evidence/DAT-03_CATALOG_CHECKPOINT_107.json \
  docs/evidence/DAT-03_CATALOG_RECOVERY_HANDOFF.md \
  docs/evidence/DAT-03_INCREMENTAL_OBSERVATIONS_20261026.json \
  docs/evidence/DAT-03_INCREMENTAL_OBSERVATIONS_20261026.md \
  docs/evidence/DAT-03_ZIP_SPACE_TRIAL_20261026.json \
  docs/evidence/DAT-03_OVERNIGHT_20261026.md \
  docs/evidence/DAT-03_PROGRESS.md \
  docs/task/tasks/DAT-03.md \
  docs/task/tasks/README.md
git diff --cached --check
git diff --cached --stat
git commit -m "fix(dat-03): recover catalog verification with bounded polling"
git push -u origin fix/dat-03-catalog-publication-recovery
```

## Mô tả PR để copy

```markdown
## Mục tiêu
Phục hồi DAT-03 khi batch đã commit Gold nhưng lỗi ở verify/publish, không chạy lại ETL hoặc sửa bằng chứng lỗi.

## Thay đổi
- Thêm publication-only recovery, exact failed-session pins, 13-query preflight và SHA/receipt/snapshot gates.
- V1 giữ nguyên runner; V2 chỉ thay Trino client theo profile SELECT research polling tối đa300s, mặc định/DDL/DML120s và HTTP90s, không resubmit POST. Parser/resolver/publisher/ML/data pins không đổi.
- Thêm read-only slow-query probe và resolution riêng cho primary failure + failed recovery liên quan; không sửa execution cũ hoặc bỏ qua failed sessions.
- Chỉ release own lease sau runtime validation và host service checks; normal resume yêu cầu resolution liên kết acceptance mới, không bỏ qua failed session.
- Bao gồm CAS/manifest host-storage prerequisite đang có local để không copy lại toàn bộ lịch sử sau mỗi phiên.
- Bổ sung regression tests và mapping nhóm CI phù hợp.

## Docs và kiểm thử
- Có runbook phục hồi, evidence27/10, task/index/progress đồng bộ.
- Python catalog/progress/recovery/storage/ML-export: 78 tests PASS.
- Java client/recovery/compatibility/publication/bucket/reuse: 36 tests PASS; không Spark fixture song song runtime.
- Local Maven package, task status, docs links, whitespace và configuration/secret checks PASS.
- Lịch sử runtime107:13/13preflight, verify140/publication148checks và186SQL thật đạt; six pins/own release/host SHA report khớp. Normal continuation sau đó accepted113/288 đến2009-05.
- Batch06–11/2009 verify và recovery V1 failed/ABANDONED_QUERY, giữ nguyên artifacts/lease. V2 preflight13metadata+actual primary_provenance:2 probe58,637s/0vi phạm đạt; full verify/publish đang chạy, chưa accepted119.
- Host capture từng bị allowlist chặn sau runtime acceptance; đã lưu exact own dead lock và recapture chỉ đọc, không replay ETL/publication.

## Giới hạn
DAT-03 vẫn In Progress: còn full288-month coverage, official ML export/loader/rerun và receipts ba máy. SEC-01 còn cloud recipient/identity acceptance; không thay ACL hoặc mở scientific gates.
```
