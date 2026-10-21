# QA-02 — Runtime acceptance

Trạng thái: **PASS — 8/8 cases**, hoàn tất ngày 21/10/2026 lúc
11:06 (UTC+07), tức `2026-10-21T04:06:52Z`.

- Branch: `test/qa-02-runtime-acceptance`, từ main `257cc20` sau PR #90.
- Assignee giữ `ThanhTris`; reviewer `unassigned`, không có approval được tự điền.
- Input tổng hợp; MinIO, REST Catalog, Iceberg và Trino thật.
- [Capsule JSON](./QA-02-runtime.json) giữ checksum, operation IDs, sáu bảng/
  snapshot pins của mỗi publication, reconciliation và kết quả từng case.
- Snapshot ID trong capsule là chuỗi thập phân để không mất độ chính xác 64-bit.

## Lượt nghiệm thu cuối

- Run: `qa02-runtime-1868715b4bb34f5a84b568c3cf64c1b6`.
- Namespace: `iceberg.qa02_ecd3eb5e306d413abde624109ed4323a`.
- Root: `/opt/pipeline/staging/qa-02/qa02-runtime-1868715b4bb34f5a84b568c3cf64c1b6/`.
- Report SHA-256: `fd7d8deb2c2bf0b62b96dd89f629f2e98514c4a105ff32e7eb200657ddb8a72d`.
- Summary: 8 passed, 0 failed, 0 pending; Java exit 0.
- Sáu operation Published, mỗi operation đủ sáu receipt và 36 Trino checks.
- Validator hậu kiểm chạy lại read-only đạt; checksum/receipt/artifact,
  snapshot bundle và publication identity đều khớp.
- Lease đã `RELEASED` đúng owner; global lease trống khi capture evidence.
- Wall time 267,40 giây; child peak RSS 1348,25 MiB trên Linux.
  Profile local[1], driver heap 768 MiB, container limit 2 GiB; không phải phép đo
  throughput toàn catalog hoặc peak memory tổng của cả stack.

| Case | Kết quả quan sát |
|---|---|
| Rerun cùng input | PASS; chạy lại đủ sáu phase, giữ nguyên sáu snapshot IDs và canonical count 3. |
| Duplicate cùng nguồn | PASS; một current row cho source key; canonical vẫn 3. |
| USGS late revision | PASS; updated mới thắng; revision cũ còn trong history. |
| JMA release replacement | PASS; release-v2 thay release-v1; bytes archive cũ còn đọc/audit được. |
| Overlap USGS/JMA | PASS; 3 canonical events, 4 bridge rows, không double count. |
| Event đổi tháng | PASS; September current cũ 0, October current mới 1; affected months gồm cả 09 và 10/2023. |
| JMA rút record | PASS; observation JMA tháng 09 bị rút; snapshot cũ còn đọc được và counts balanced. |
| Retention ngoài scope | PASS; toàn bộ nội dung partition có dữ liệu tháng 08/2023 không đổi; snapshot cũ còn đọc được. |

Fixture withdrawal thay record JMA tháng 09 bằng một record mới tháng 10 để
archive vẫn hợp lệ, không rỗng. Event tháng 09 vẫn còn observation USGS,
nên cuối lượt có **4 canonical events và 4 bridge rows**, không phải duplicate.
Silver reconciliation chỉ đếm affected months; Gold readback đếm toàn bảng,
gồm event tháng 08 giữ ngoài scope. Vì vậy Silver canonical cuối là 3 nhưng
Gold toàn bảng là 4; các phép đối soát vẫn balanced.

## Đối soát Trino độc lập

Sau khi runner PASS, chạy read-only trên đúng namespace:

```sql
SELECT count(*) AS publications,
       count(DISTINCT publication_id) AS distinct_publications,
       count_if(verify_status='PASSED') AS passed
FROM iceberg.qa02_ecd3eb5e306d413abde624109ed4323a.publication_status;

SELECT count(*) AS serving_events
FROM iceberg.qa02_ecd3eb5e306d413abde624109ed4323a.earthquake_event_current;
```

Kết quả lần lượt: `6 / 6 / 6` và `4`.

## Kiểm thử và build thực tế

- `make test-qa-02`: 21 tests đạt; PREPARED template vẫn không được coi là runtime PASS.
- `make test-qa-acceptance`: 23 Python guard tests và 3 Java safety tests đạt.
- `TMPDIR=/private/tmp make ci-airflow`: 359 tests đạt, bao gồm các tests QA-02.
- Java 17 trong container Maven cache, `clean verify` với 10 lớp liên quan:
  **77 tests, 0 failures, 0 errors, 0 skipped**; compile và package JAR đạt.
  Các lớp: PipelineQaSafetyTest, UsgsBronzeWriterTest,
  UsgsBronzeFixtureAcceptanceTest, JmaBronzeWriterTest, JmaBronzeQaVerifierTest,
  JmaArchiveValidatorTest, SourceDedupTransformerTest, SilverLineageAndKeyTest,
  SilverQualityValidatorTest, GoldPublicationTest.
- `make smoke-qa-acceptance PIPELINE_QA_TASK=QA-02`: PASS 8/8;
  preflight xác nhận không build/pull/start/init.
- `make ci-docs-base`: docs, contracts, task status, configuration, Compose,
  Docker lifecycle và CI workflow checks đạt.
- Source security audit: không có finding trong file thay đổi. Audit toàn repo
  vẫn có 7 findings cũ ngoài phạm vi này; không coi đây là nghiệm thu SEC-01.

JAR SHA-256:
`d5158109b7f9c9878f38acc982b1a5a177ed035f969763589a075f904ed62114`.
Source/build fingerprint:
`e9aed615f99ec3af15ac01837459afdd552dc9d2d8a6cdb2a4b4cb3e2f67cb45`.
Không thay label image hoặc bỏ qua preflight để dùng artifact cũ.

**Không công bố full Java regression PASS.** Hai lượt `make docker-build`
bị Docker Hub metadata timeout; lượt verify đầy đủ thay thế có
FeatureParquetCrossProcessTest vượt 45 giây và đã dừng khi chạy các module
ngoài scope. Nhóm 77 tests và build JAR ở trên là lượt scoped riêng đã đạt,
không phải đổi report thất bại thành thành công. CI GitHub của branch này
chưa được chạy vì chưa push.

## Gián đoạn và bảo toàn evidence

Run trước `qa02-runtime-126962377b0048bb8fd66741902e0102` bị Docker dừng,
mới có hai operation Published và Silver của operation tiếp theo.
Không có final acceptance và không được tính là PASS. Container cũ đã kết thúc,
Trino khởi động lại không có query cũ; hai publication cũ được query lại đạt.
Người vận hành lưu `operator-abort.json` và `operator-lease-release.json`
trong root cũ, release đúng owner rồi tạo namespace mới cho lượt cuối.
Mọi Bronze, partial commit, snapshot, publication và log cũ được giữ nguyên;
không xóa/reset volume hoặc sửa operation dở để làm xanh.

## Giới hạn / handoff

- Source fixtures tổng hợp, không phải coverage archive/API public.
- [QA-03 — Kiểm thử JMA historical backfill](../task/tasks/QA-03.md)
  còn cần nghiệm thu archive public, resume/changed release và coverage.
- [QA-04 — Test failure và recovery theo tầng](../task/tasks/QA-04.md)
  còn cần nghiệm thu các fault ETL/ML và recovery. Bản này không chứa
  implementation đang dở của hai task đó.
- [SEC-01 — Review secret và bề mặt truy cập](../task/tasks/SEC-01.md) còn theo dõi findings/
  coverage riêng; không đổi trạng thái task đó trong QA-02.
- Reviewer độc lập vẫn được khuyến nghị; chưa ghi nhận approval.

Lệnh tái tạo, chuẩn bị riêng và cách đọc artifacts:
[QA-02 runtime runbook](../runbooks/PIPELINE_QA_ACCEPTANCE.md).
