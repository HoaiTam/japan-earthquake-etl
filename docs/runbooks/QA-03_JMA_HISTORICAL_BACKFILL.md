# QA-03 — JMA historical backfill

## Mục đích và phạm vi

Kiểm tra JMA thật qua Bronze immutable → full-month Silver → sáu bảng Gold
Iceberg → Trino Published trước khi mở rộng lịch sử. ORC-07, ORC-03 và JMA-05
đã Done. Python chỉ tải nguyên bytes, kiểm tra metadata/checksum và quản lý
lease; toàn bộ đọc ZIP/native record và ETL nằm trong Java/Spark.
Fixed runner không trigger Airflow backfill production hoặc sửa Gold đang phục vụ.

| Tháng UTC | Archive | Ý nghĩa |
|---|---|---|
| 1997-09 | h199701.zip + h199710.zip | LEGACY và halo ±16 giây qua ranh giới JST/UTC |
| 1997-10 | h199710.zip | UNIFIED sau mốc đổi format |
| 2000-01 | h2000.zip | Mẫu reproduction |
| 2023-09 | h2023.zip + USGS full-month | Mẫu extension, overlap hai nguồn |
| 2000-02 | h2000.zip | Baseline retention có dữ liệu dương |

Retention thuộc release slot khác 1997, tránh giả định sai rằng đổi release chỉ
tác động một tháng trong cùng archive. Chỉ các tháng đăng ký trong namespace QA
được resolve; không đọc/ghi lại toàn catalog 40 năm.

## Chuẩn bị riêng

Từ repo root, với .env hợp lệ và Docker Desktop đang chạy:

```bash
make prepare-qa-history
make docker-build
make up-ml WAIT_TIMEOUT=600
```

Preparer tải nguyên bản bốn ZIP và USGS GeoJSON từ inventory/ORC-06, TLS bật,
32 MiB/file, 60 giây/lượt và tối đa hai retry. Có file thì reuse; pin toàn bộ
bytes tại staging/qa-history-captures/capture-pins.json. Sai pin thì dừng, không
âm thầm refresh. Captures và .partial ignored, không đưa vào Git.
ZIP nguyên bytes đi vào Bronze; Java đọc native member cho Silver, không ghi đè raw.
release-v1/release-v2 là nhãn thứ tự của phép thử QA, không khẳng định đó là
tên release chính thức do JMA phát hành.

Build/start là bước tường minh riêng. Nếu image/JAR đúng fingerprint và init đã
hoàn tất cùng cấu hình, có thể chỉ bật MinIO/Catalog/Trino theo
[runbook chung](./PIPELINE_QA_ACCEPTANCE.md). Không bypass freshness guards.

## Test và nghiệm thu

```bash
make test-qa-03
make test-qa-acceptance
make smoke-qa-acceptance PIPELINE_QA_TASK=QA-03
```

Hai test chạy offline. Smoke chỉ preflight rồi chạy one-off client
--no-deps --pull never; không build/pull/start/init hoặc clear DagRun.
Mỗi lượt có namespace <catalog>.qa03_<32 hex> mới, exact-owner global source
lease; Java local[1], heap 768 MiB, container 2 GiB.

Thành công: terminal trả task=QA-03, status=PASS, summary 6 passed / 0 failed /
0 pending, lease_release.status=RELEASED. Phải có acceptance.json sau validation
và release; chỉ runtime-report.json hoặc publication rời chưa đủ.

| Case | Kiểm tra thực tế |
|---|---|
| resume_after_failure | Throw/catch IOException sau durable Silver, trước Gold; baselines không đổi. Retry cùng context/operation, ba receipt đầu giữ nguyên, kết thúc Published. Không tuyên bố restart JVM. |
| reuse_verified_archive | Replay production bronze phase trả cùng receipt/checksum; Java job không có source HTTP downloader. |
| changed_release | Đổi magnitude đúng một native record 09/1997 thành release-v2; affected month chỉ 1997-09, snapshot mới, snapshot cũ đọc được. Fault tổng hợp trên bản copy, không phải correction thật do JMA công bố. |
| same_release_different_bytes | Cùng release-v2 nhưng bytes khác bị JMA_RELEASE_IDENTITY_OR_ORDER_CONFLICT; downstream chặn và sáu pins không đổi. |
| retention_outside_scope | Tháng 02/2000 có dữ liệu dương, hash toàn nội dung partition trước/sau bằng nhau, snapshot cũ đọc được. Trino kiểm tra outside current/bridge mỗi publication. |
| resource_guard | Request 25 tháng vượt giới hạn 24, bị chặn trước transform/commit; replay operation cuối giữ nguyên pins/publication. Không phải thử OOM hoặc chứng minh đủ RAM cho 40 năm. |

Mỗi publication có đủ sáu phase receipts, balanced Silver, exact six-table
bundle, commit SHA/publication ID và Trino count/identity/unique/quality/outside
checks. Validator bind matrix/inventory/capture pins và case evidence với
publication tương ứng; boolean PASS đơn lẻ không đủ.

## Evidence, capacity và contract

Event bắt đầu in root /opt/pipeline/staging/qa-03/qa03-runtime-<uuid>/:

- request.json, matrix.json: identity/checksum pin.
- resume-stop.json: controlled exception/context/Silver receipts trước retry.
- <operation>/: sáu phase artifacts và publication acceptance.
- runtime-report.json: **qa03-storage-acceptance-v1**, sáu case, input pins,
  original/synthetic distinction và sáu publication.
- execution.json, lease-release.json, acceptance.json: exit, ownership,
  duration và child peak RSS đo trên Linux.
- Raw stdout/stderr mode 0600, không commit private logs hoặc raw catalog.

Helper qa_03_jma_historical_backfill.py và make smoke-qa-03 cũ vẫn là
**external runner** với contract qa-03-report-v3 và request/evidence layout riêng.
Không hạ validator cũ để nhận storage report; hai contract không dùng lẫn.
make test-qa-03 tạo PREPARED v3 với capacity null/PENDING_RUNTIME, không phải
fixed-runner PASS. Dùng smoke-qa-acceptance PIPELINE_QA_TASK=QA-03 cho runner này.

Capacity chỉ áp dụng các closed-month batch đại diện. Không nhân tuyến tính
ra 40 năm hoặc gọi child RSS là peak cả stack. Research coverage toàn kỳ chưa
hoàn tất và **chưa có task rollout toàn kỳ**.
[QA-05 — Profile dữ liệu lịch sử và tài nguyên local](../task/tasks/QA-05.md)
còn cần profiling mở rộng, không thay task rollout/coverage nghiên cứu.

## Kế hoạch mở rộng sau nghiệm thu

QA này chỉ có bốn archive được capture và năm tháng UTC đăng ký, không chứng
nhận bất kỳ năm nghiên cứu nào đã có đủ 12 tháng. Toàn kỳ 1984–2023 vẫn cần
rollout/coverage riêng; không dùng namespace hoặc release label QA làm official input.

1. Sau profiling QA-05, chọn một year/segment/release bằng ORC-03 preview,
   pin exact manifests/checksums; không dùng wildcard hoặc tải cả 40 năm một lần.
2. Chia từng closed UTC month, resolve cả support archive/halo JST. Thiếu archive
   ngoài inventory/cutoff phải dừng và ghi gap, không giả COMPLETE_MONTHS.
3. Mỗi batch chạy cùng real adapters, chỉ tiến checkpoint sau six-table commit
   và independent Trino Published; giữ context/receipts để retry có phạm vi.
4. Đối soát counts, rejected/superseded, outside scope và source coverage sau
   từng batch; chỉ công bố dataset nghiên cứu khi coverage/audit thực sự đủ.

Implementation rollout toàn kỳ và giải quyết các gap chưa có task riêng;
QA-05 là profiling Stretch, không tự bao gồm phần rollout đó.

## Khi thất bại

Timeout/nonzero Java giữ lease vì có thể còn query remote. Giữ failed evidence,
partial commits, Bronze và snapshots. Không reset volume hoặc stale takeover.
Theo [ORC-05 — Recovery/concurrency](../task/tasks/ORC-05.md), operator xác minh
đúng container/JVM/query đã quiescent, reconcile và ghi abort evidence trước khi
release đúng owner. Smoke mới tạo namespace mới; không replay cả suite bằng
request cũ trên namespace đã ghi dở.
[QA-04 — Test failure và recovery theo tầng](../task/tasks/QA-04.md) còn cần
nghiệm thu fault ETL/ML riêng; QA-03 không thay QA-04.
