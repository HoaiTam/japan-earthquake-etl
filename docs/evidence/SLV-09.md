# SLV-09 — Evidence bản vá verified Bronze / immutable Silver

## Baseline và đính chính

Branch `fix/slv-09-verified-bronze-handoff` bắt đầu đúng head PR #58
`775afe87f54864934d7ae8388845a3c5743a16cc` bằng `gh pr checkout 58`.
Assignee HoaiTam; reviewer unassigned.

Evidence cũ bị thay thế: in-memory S3 URI không phải MinIO live; hai JMA records
tạo bằng tay không phải archive thật; compile release 17 trên JDK 23 không chứng
minh runtime Java 17. Counts 18/17 thuộc fixture offline, không phải live receipt.

## Kiểm thử

- 8 Python control-plane tests pass: lease, busy/contender, same request/rerun,
  timeout giữ lease, release sau confirmed idle, receipt/runtime sai, bounded
  600s submit profile một core và sanitized failure metadata.
- Toàn bộ 253 Java tests pass, Maven `verify` BUILD SUCCESS trên JDK 21.0.11
  (compile release 17): 11 bundle, 8 integration, 11 legacy writer, 5 live-job
  scope, 3 MinIO stat, 1 cross-process (sáu JVM độc lập) và các suites
  parser/dedup/link/Gold/ML hiện có; bản cuối hoàn tất 2026-10-10 15:26 +07.
  Command thực tế: `JAVA_HOME=<Azul JDK21> ./mvnw --offline --batch-mode
  --no-transfer-progress -pl spark -am verify`.
- Coverage: empty datasets, context conflict, partial failure/resume, final
  corruption, manifest/marker validation, regression stale SUCCESS trên S3
  prefix, accepted/ambiguous/revision/reject và persisted offline Gold handoff.
- Regression mới xác nhận ingest ID Bronze khác run xử lý Silver: whitelist
  từ verified manifest giữ lineage gốc; manifest chưa resolve hoặc ingest ID
  bị đổi vẫn chặn. Không sửa gate, sample hay payload để làm test pass.
- `make test-airflow`: 181 tests pass; `make test-contracts` và
  `make check-task-status check-data-model-contract check-compose check-config`
  pass. Python control-plane SLV-09 được chạy riêng với 8 tests ở trên.
- Live smoke cuối **pass cả hai submit**, JSON đi kèm đã thay bằng receipt thật.
  Không gọi host JDK21 là runtime Java17; runtime thật là Java 17.0.19/Spark3.5.9.

## Nghiệm thu live cuối — Done

Command thực tế dùng mode bind JAR mới read-only, không restart foundation:
`make smoke-silver-integration SILVER_RUNNER_JAR="$(pwd)/spark/target/japan-earthquake-etl-runner.jar"`.
Runner JAR SHA `4c5e958e2114cbacd783d6e99efbbc2683339d324150450d90e60989328723a0`.
Run `slv09-live-59a5e3725b14453e89a127eb2c8a70d2`, standalone
`spark://spark-master:7077`; ứng dụng `app-20261010082720-0006` và
`app-20261010082832-0007` đều Gold handoff verified, Gold Published = false.

- 16 USGS + 256 JMA 2000 + 256 JMA 2023 = **528** parsed/valid/current.
- Rejected/duplicate/superseded/candidate/accepted/ambiguous = **0**.
- Observation **528**, reject **0**, link **0**, membership **528**;
  cả bốn dataset có exact manifest/SHA/count và final Parquet readback.
- Gold current/canonical/bridge = **528/528/528**, gồm 16 USGS-only và 512 JMA-only.
  Empty link không phải coverage accepted match; accepted/ambiguous có fixture riêng.
- Attempt 1 `idempotent_reuse=false`; attempt 2 `true`; `rerun_unchanged=true`.
  Bundle SHA `2fdf83893ede773c811d5b83117c42b05920e680d4a078ecaf036a3f00340b21`,
  identity `198f0881ca9a3c2405be76945da68ad1888b9ae66eb27f5c699a795899f586f9`
  và mọi dataset/count/SHA không đổi giữa hai JVM độc lập.
- Driver cgroup peak **484462592 bytes** (~462 MiB), OOM/oom_kill **0**;
  không dùng số này để bảo đảm sizing toàn catalog/Gold commit.
- Whole-run lease released. Fresh read-only SDK sau smoke xác nhận bundle SHA,
  marker, serialization policy; source owner không tồn tại và Spark app list rỗng.
- Full final Docker image rebuild chưa chạy lại sau lần build đầu bắt source
  snapshot trước sửa test; không dùng lần build lỗi đó làm evidence pass.
  Bản cuối đã `mvn verify`/package và chạy thực bằng JAR trên runtime Java17.

Exact metadata, hai receipts/run context/input pins/cgroup nằm trong
[SLV-09-live-readback.json](./SLV-09-live-readback.json). Report runtime giữ tại
`/opt/pipeline/staging/backfill/qa/slv09-live-59a5e3725b14453e89a127eb2c8a70d2/integration_report.json`.
Chỉ metadata nhỏ được commit; raw/Parquet/JAR/private logs không nằm trong Git.

## Lịch sử lỗi và regression đã sửa

Smoke đầu bị chặn trước Silver writes: 528/528 `CONTRACT_MISMATCH` vì overload
pre-parsed cũ buộc `ingest_run_id == processing run_id`. Parsed/raw SHA đúng,
source lease được nhả sau khi xác nhận cluster idle. Bản vá xác minh lineage
theo từng Bronze manifest thay vì làm mất ingest ID; selection vẫn là 256 dòng
đầu/archive. Đây là lỗi integration context, không phải 528 raw records hỏng.

Run tiếp theo `slv09-live-68acf1dbcb37450c9b0f65cefbaece75` ghi/verify bundle
Silver nhưng chưa hoàn tất Gold handoff trong profile 300s; stdout receipt rỗng,
stderr cuối có các count jobs hoàn tất và không có Java exception. Không được
dùng run đó làm evidence handoff thành công. Đã xác nhận app list rỗng, worker
0 cores/0 memory used và lease null trước khi chạy lại. Profile cuối dùng 600s,
tắt AQE/whole-stage codegen như offline integration (không bỏ quality/validation
hoặc distributed actions). Scoped QA artifacts được giữ, không xóa data/volume.

Run `slv09-live-8abdaa023a9547ce8626c814259e87c8` có attempt 1 đạt thật:
528 parsed/valid/current/canonical/memberships (16 USGS + 512 JMA), rejects/
duplicate/superseded/links đều 0; bốn dataset readback và Gold handoff đạt trên
Java 17.0.19 / Spark 3.5.9. Attempt 2 bị `IMMUTABLE_BUNDLE_CONFLICT`, nên cả
run vẫn **không** đạt acceptance rerun. Lease được nhả và app list rỗng.
Test JVM độc lập có perturb identity-hash allocation tái lập checksum khác ở
Parquet có dữ liệu; sort footer encoding/metadata lists sửa được regression
(23 writer/bundle/cross-process tests pass). Không sửa bundle QA cũ hoặc nới
checksum gate; smoke cuối dùng run mới với serialization policy đã pin.

## Lặp lại

1. `make test-silver-integration` — offline suite, không gọi MinIO.
2. Foundation healthy và exact pins tồn tại: `make smoke-silver-integration`.
   Build image mới, submit hai lần với cùng request/run/context; không restart
   foundation, gọi source API hay commit Gold.
3. Đọc report path in stdout. Phải có cả bốn exact dataset manifests/SHA/count,
   marker bundle được verify, Java 17, standalone master/application IDs.
4. `rerun_unchanged=true`, attempt 1 không reuse, attempt 2 reuse; bundle SHA,
   identity, reconciliation và Gold counts phải giống nhau.

Input pins ở [slv_09_bronze_inputs.json](../../airflow/dags/fixtures/slv_09_bronze_inputs.json).
USGS nguyên raw DAT-01 16-event. JMA 2000 tái lập JMA-05 và 2023 mở rộng:
256 dòng đầu/archive sau full raw SHA/ZIP CRC/count verification. Full archive
count không phải count được publish; không tạo JMA giả hoặc chọn theo match.

## Cơ chế và giới hạn

[SILVER_INTEGRATION.md](../specs/SILVER_INTEGRATION.md) giải thích bundle,
marker-last, gate, fingerprint/reservation, lease và scoped staging.
Không overwrite/xóa month partitions/bundle khác; raw Bronze chỉ đọc.

- [GLD-03 — Ghi Gold Iceberg và commit snapshot](../task/tasks/GLD-03.md):
  chưa commit Gold; smoke chỉ transformation từ persisted Silver đã verify.
- [GLD-04 — Tạo Trino views và verification SQL](../task/tasks/GLD-04.md):
  chưa verify snapshot/Published qua Trino.
- [QA-01 — Chạy E2E daily đa nguồn](../task/tasks/QA-01.md):
  còn ghép adapter orchestration và nghiệm thu daily thật toàn flow.
- Mẫu đầu năm không đại diện full-year/research period và không bảo đảm có
  accepted pair. Accepted/ambiguous/revised coverage dùng fixture riêng.
