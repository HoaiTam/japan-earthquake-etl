# GLD-03 — Evidence Gold Iceberg commit

## Baseline và kết quả

Branch `feat/gld-03-iceberg-snapshot-writer` bắt đầu từ main `5373cb8`.
GLD-01 và SLV-09 đã Done trên baseline đó. Assignee Trang, reviewer unassigned.

[Receipt thật](./GLD-03-runtime.json) ghi Java 17.0.19 / Spark 3.5.9,
namespace QA riêng trên REST Catalog/MinIO, sáu snapshots và runner SHA.
16 events USGS → 16 canonical events → 16 bridge rows; dimensions date/region/
magnitude/depth có 3/1/7/5 rows. Cùng operation rerun trả cùng receipt/snapshot;
readback mỗi bảng kiểm tra count, logical keys và SHA của toàn bộ row content.
`status=COMMITTED`, `published=false`, chưa có publication PASSED.

Input là public USGS capture đã pin SHA từ fixture real_samples, không phải
response gọi API mới hay DAT-01 gốc. Production Bronze writer và MinIO adapter
ghi raw/manifest; Silver publisher ghi bốn datasets và fresh verify; GoldInputReader
đọc đúng bytes manifest/Parquet trước GLD-01 và writer. Không mock MinIO/Catalog.

## Test/check đã chạy

- `./mvnw --batch-mode --no-transfer-progress -pl spark -am verify`: 257 tests,
  0 failures/errors/skipped, BUILD SUCCESS trên JDK 17.0.12 Windows.
- Bản cuối: cùng Maven `verify` với `-Dtest=GoldIcebergWriterTest
  -Dsurefire.failIfNoSpecifiedTests=false`: 4 tests pass, package/shade thành công.
  Gồm snapshot thật, rerun, revision, empty initial/month, ngoài scope, time travel,
  partial commit recovery, stale baseline, payload conflict, null quality và lost lease.
- `python3 -m unittest discover -s airflow/tests -p test_gold_storage_qa.py`:
  4 tests pass trong image Linux QA, chạy bằng UID 50000; busy lease/timeout/
  interruption/nonzero JVM được kiểm tra.
- `docker compose --env-file .env.gold-qa run --rm --no-deps spark-gold-qa`:
  pass trên JAR cuối, receipt liên kết bên trên. Env QA bị ignore, không commit;
  chỉ public metadata được lưu. Private stdout/stderr nằm trong run staging.
- Trino đọc snapshot lần smoke trước với count 16; `information_schema.columns`
  trả UTC `timestamp(6) with time zone`, JST `timestamp(6)`. GLD-04 vẫn cần gate đầy đủ.
- `check-task-status`, `check-java-build-inputs`, `check-data-model-contract` pass.
  `check-config.sh --require-local` với env QA cũng pass.
  Changed-file scan đối chiếu tám credential values QA: không có leak.
  `git diff --check` pass.

## Môi trường và cách lặp lại

Build bằng Maven trước, init staging bằng foundation Airflow, start MinIO/Catalog/
Trino rồi `make smoke-gold-storage` hoặc command Compose đã ghi trong receipt.
QA dùng `local[1]`, heap 512 MiB, resource limit 1 CPU/2 GiB, UID Airflow và NSS
wrapper có sẵn; không mở thêm host port, không chạy scheduler/DAG khác.

Lần này registry Quay không tải được mc tag đã pin. QA dùng override local riêng:
mc được build từ official source tag `RELEASE.2025-08-13T08-35-41Z`, commit
`7394ce0dd2a80935aded936b09fa12cbb3cb8096`, Go 1.24.8, Alpine 3.22.1;
`minio-init` dùng image local đó. Không thay mc pin trong Compose sản phẩm.
MinIO được build theo Dockerfile/commit hiện có. Các volume được giữ nguyên.
LF attribute của `compose/minio/init.sh` sửa lỗi CRLF khi Windows bind vào Linux.

## Giới hạn / Rủi ro / Bước tiếp theo

- [GLD-04 — Tạo Trino views và verification SQL](../task/tasks/GLD-04.md):
  cần Trino blockers và publication PASSED cho bundle này. Sáu commit không atomic
  cùng nhau; consumer chỉ nhận exact bundle hoàn chỉnh, không coi current là Published.
- [MLD-01 — Pin Gold snapshot và tạo dataset manifest](../task/tasks/MLD-01.md):
  pin snapshot sau GLD-04, chưa build candidate/feature trong task này.
- [QA-01 — Chạy E2E daily đa nguồn](../task/tasks/QA-01.md): cần full-month
  current-state resolver và adapter điều phối Gold production. Bounded namespace QA
  không chứng minh JMA, toàn tháng nguồn hoặc toàn lịch sử; không dùng 16 rows để
  overwrite tháng sản phẩm. [Commit contract](../specs/GOLD_ICEBERG_COMMIT.md) yêu cầu
  caller bàn giao COMPLETE_MONTHS và đủ baseline pins.
- Chưa có reviewer độc lập; không ghi approval giả. Không claim benchmark standalone.
