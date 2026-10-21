# QA-03 — Historical backfill runtime acceptance

Trạng thái: **PASS — 6/6 cases**, hoàn tất `2026-10-21T04:45:25Z`
(11:45 ngày 21/10/2026, UTC+07).

Branch `test/qa-03-runtime-acceptance` từ main `2ad8655` sau PR #91.
Assignee giữ ThanhTris; reviewer unassigned, chưa có approval độc lập.

## Input và phạm vi

Bốn archive public nguyên bytes h199701.zip, h199710.zip, h2000.zip, h2023.zip
và USGS GeoJSON full-month 09/2023; file/hash được pin ở bước chuẩn bị riêng.
Lượt chạy không tải lại nguồn và không đưa raw dumps vào Git.

Các tháng đại diện: 1997-09 (LEGACY), 1997-10 (UNIFIED), 2000-01
(reproduction), 2023-09 (extension/overlap). Baseline retention 2000-02 có
dữ liệu dương, thuộc release slot khác tháng 1997 bị sửa.
1997-09 nằm trước mốc train 10/1997, chỉ dùng kiểm tra parser/backfill.

Changed release là fault tổng hợp: đổi magnitude đúng một native record tháng
09/1997 rồi rezip, giữ raw public release-v1 để audit. release-v1/release-v2 là
nhãn thứ tự QA, không phải tên correction chính thức của JMA.

## Nghiệm thu cuối

Run: `qa03-runtime-2bf249ede8a84947afb22c48beb77e07`.
Namespace: `iceberg.qa03_4e00e47b3c7d4bb38c74385c4be609a0`.
Root: `/opt/pipeline/staging/qa-03/qa03-runtime-2bf249ede8a84947afb22c48beb77e07/`.
Report SHA-256:
`c20758ce8bae508667fddd46b2bdc78cb64f3c712b37d68bb762ade4027ebca9`.
Java exit 0; six-case summary 6 passed / 0 failed / 0 pending.
Final acceptance đã ghi sau validation và release đúng owner, global lease
trống khi export. Hậu kiểm read-only toàn report đạt; mỗi publication có đủ
sáu phase receipts, six-table pins, balanced Silver và 36 Trino checks.

[Capsule JSON](./QA-03-runtime.json) giữ exact snapshot IDs dưới dạng chuỗi
64-bit, checksum, operation/publication identities, reconciliation và readback.
Raw captures, full receipts và private logs giữ ở staging/MinIO, không đưa vào Git.
controller_sha256_at_export là checksum lúc hậu kiểm/export, không phải claim
attestation source của process đã nạp trước đó; thay đổi guard nonzero/evidence
I/O được xác nhận bằng regression, không thay đổi đường chạy thành công.

| Case | Kết quả quan sát |
|---|---|
| Resume | IOException có kiểm soát sau durable Silver; Gold chưa đổi. Retry cùng context tái sử dụng ba receipt đầu và Published thành công. |
| Archive reuse | Production bronze phase trả đúng receipt cũ; source download trong execution bằng 0. |
| Changed release | release-v2 đổi đúng native magnitude; affected month 1997-09, snapshot mới; snapshot cũ của đủ sáu bảng đọc được. |
| Same release/different bytes | JMA_RELEASE_IDENTITY_OR_ORDER_CONFLICT, downstream bị chặn và sáu baselines không đổi. |
| Retention | 2000-02 có 5.068 physical rows; toàn nội dung partition hash trước/sau cùng a45508462a2920b10250a4e1dacd330f965eceadbfd0cad8c6a9f5e92a6d92aa. |
| Resource guard | Request 25 tháng bị BOUNDED_EXACT_MONTHS_REQUIRED; replay operation cuối trả nguyên sáu snapshots/publication. |

## Trino readback và đối soát counts

Query độc lập trên publication_status trả **6 publications / 6 distinct / 6 PASSED**.
Đọc physical event_current bằng **FOR VERSION AS OF 5148454550118777185**
trả 41.625 rows; bridge snapshot 4885261737998908264 có 41.734 rows.
Stable serving view earthquake_event_current có **41.358 rows**.

| Tháng UTC | Physical current | Serving natural + study area |
|---|---:|---:|
| 1997-09 | 3.378 | 3.280 |
| 1997-10 | 5.371 | 5.316 |
| 2000-01 | 5.418 | 5.351 |
| 2000-02 | 5.068 | 5.030 |
| 2023-09 | 22.390 | 22.381 |
| **Tổng** | **41.625** | **41.358** |

Chênh 267 không phải mất dữ liệu khi ingest: serving contract lọc
is_natural_earthquake AND is_in_study_area. Physical giữ cả 262 non-natural
và 258 outside ROI; hai nhóm có thể chồng nhau, không cộng thẳng hai count.
Silver reconciliation chỉ đếm affected months, còn Gold counts là toàn bảng
tích lũy; capsule giữ cả hai để không so nhầm scope.

## Capacity note

- Wall time đo qua monotonic: **425,81 giây**, khoảng 7 phút 6 giây cho cả suite.
- Child peak RSS trên Linux: **1.361,99 MiB**; không phải peak cả stack.
- Profile: local[1], driver heap 768 MiB, container limit 2 GiB.
- Năm capture public tổng **11.509.460 bytes**; cached từ bước chuẩn bị riêng,
  không tải lại trong execution. Không coi số này là dung lượng warehouse.
- Mỗi request ghi một closed UTC month; đọc ZIP/year được pin để resolve
  full-month state/halo. Không tải hoặc reprocess toàn catalog 40 năm.
- Đây là phép đo bounded suite gồm verify/replay, không là benchmark throughput
  tuyến tính hay bằng chứng full-history đủ RAM. Resource failure là guard
  cardinality, không ép OOM hoặc thay memory limit để làm xanh.

## Kiểm thử và artifact đã xác nhận

- Matrix/external validator: `make test-qa-03`, 14 tests đạt; PREPARED vẫn
  có sáu PENDING_RUNTIME, không được đổi thành PASS bằng sửa template.
- Fixed-runner guard: 43 Python tests, gồm capture pin/symlink, six publications,
  scope, resume receipts, collision, retention, lease, unknown outcome và
  tương thích Java nanoseconds trên Python 3.8. Nonzero JVM giữ lease ngay cả
  khi ghi execution evidence thất bại; regression này được thêm lúc rà soát.
- `TMPDIR=/private/tmp make ci-airflow`: 379 tests đạt sau regression cuối.
- Java 17 offline `clean verify` với 10 lớp liên quan: 78 tests đạt, không skip;
  compile và package JAR thành công. Thêm 3 lớp parser/release/inventory:
  38 tests đạt. Tổng 116 tests scoped; **không công bố full Java regression PASS**.
- `make ci-docs-base`: docs/contracts/task sync/config/Compose/lifecycle/CI checks đạt.
- Image MinIO/client/Gold QA đã build thật bằng Dockerfiles hiện có và fingerprint
  mới; JAR xuất từ scoped Java17 verify. Không thay nhãn giả hoặc bypass guards.
- Source audit: 7 findings cũ ngoài file thay đổi, 3 pending coverage;
  không coi đây là nghiệm thu SEC-01.

JAR SHA-256:
`78f57b1d920dfcecd6a4ac67c2d8ec4b2b31a2bed5f6118f50b69ecba7f34b7d`.
Source/build fingerprint:
`38b59297c8dabe23c6f21d52a3acbfe341e424c121e0d33faa09af7906d3ae03`.

## Các lượt chưa được tính PASS

- `qa03-runtime-775984ef23a242519a115bc4e53e5bcf`: Java exit 1,
  HttpTimeoutException, một publication và hai Gold commits. Trước đó operator
  đã xác minh không còn QA container/query Trino, lưu operator-quiescence.json
  và operator-lease-release.json. Namespace, Bronze, snapshots, partial commits
  và failed logs giữ nguyên; không replay request này hoặc dùng làm acceptance.
- `qa03-runtime-6475f9da05844e0aacf9818be31a2d2b`: Java exit 0, sáu case và
  sáu publication hoàn tất; Python 3.8 từ chối completed_at_utc có 9 chữ số
  thập phân. Lease được release đúng owner sau Java kết thúc, không có
  acceptance.json. Sau sửa parser metadata, hậu kiểm read-only toàn report đạt;
  không ghi acceptance bù hoặc sửa timestamp/bytes report cũ. Chạy namespace
  mới để kiểm tra lệnh smoke từ đầu và ghi capacity trước validation.
- Một lần gọi ban đầu bị chặn trước lease/Java vì đường dẫn inventory host
  không đúng trong container; đã dùng explicit read-only mount path và thêm
  regression test. Không có remote ETL write từ lần gọi đó.

## Giới hạn và handoff

- Đây là kiểm thử storage thật với vài tháng đại diện, không chứng minh đủ
  coverage 40 năm hoặc dataset nghiên cứu sẵn sàng. Chưa có task rollout toàn kỳ.
- [QA-05 — Profile dữ liệu lịch sử và tài nguyên local](../task/tasks/QA-05.md)
  còn cần profiling mở rộng; không suy tuyến tính throughput từ mẫu này.
- [QA-04 — Test failure và recovery theo tầng](../task/tasks/QA-04.md)
  còn cần fault ETL/ML riêng. IOException sau Silver không phải crash JVM;
  resource guard 25 tháng không phải phép thử OOM.
- [SEC-01 — Review secret và bề mặt truy cập](../task/tasks/SEC-01.md)
  tiếp tục theo dõi findings/coverage riêng; không đổi trạng thái trong task này.
- Chưa commit/push, chưa có GitHub CI của branch hoặc approval reviewer.

Cách chuẩn bị, chạy và đọc evidence:
[QA-03 runbook](../runbooks/QA-03_JMA_HISTORICAL_BACKFILL.md).
