# DAT-03 — Capacity fingerprint và physical source mapping, 15/11/2026

> Mốc tiếp theo cùng ngày: [portable importer phần 1](DAT-03_PORTABLE_IMPORT_20261115.md).
> Các mục chưa có importer dưới đây là tình trạng tại lúc chốt capacity/provenance;
> không thay evidence cũ bằng kết quả actual chưa nghiệm thu.

## Kết luận và phạm vi đã đạt

**DAT-03 vẫn In Progress**, assignee HoaiTam, reviewer unassigned. Nhánh
`feat/dat-03-unified-catalog` tạo từ main `f09ac66`, kế thừa code/evidence đã
push PR #131 qua `0b39ad4`. Owner yêu cầu commit/push phần capacity/provenance
vào cùng PR #131; không mở PR mới hoặc merge main.

Đã vượt hai gate component còn chặn ở [mốc 14/11](DAT-03_PORTABLE_READBACK_20261114.md):

| Actual gate | Workload / nguồn | Kết quả |
| --- | --- | --- |
| Fingerprint `event_current` | 6.012.210 rows, required ≥4.488.650 | PASS; 267,868s wall; hai fingerprint khớp |
| Fingerprint `event_source_bridge` | 6.012.210 rows, required ≥4.489.823 | PASS; 166,617s wall; hai fingerprint khớp |
| Physical Bronze readback + mapping | 37 physical inputs, 74 originals từ hai worker | PASS; 130.062.575 raw bytes, SHA/size/structure/request/window/release |

Đây là synthetic capacity stress và original-to-physical mapping, **không phải**
Gold publication, full transformation/join/write capacity, official ML export
hoặc full transitive Iceberg graph restoration. Không gộp các gate thành Done.
125 tháng Silver readback đã đạt trước, không chạy lại hoặc cộng thêm tháng.

## Chẩn đoán startup Spark và cách khắc phục

Log v2 lỗi cho thấy executor heartbeat gọi re-register khi BlockManager ID còn
null, trong lúc fetch JAR qua driver. Source Spark3.5.9 cho thấy heartbeater bắt
đầu trước `updateDependencies`; đây là cơ chế phù hợp với timeline quan sát,
không khẳng định đã loại trừ mọi nguyên nhân ngẫu nhiên của Spark.

Profile mới dùng `local:/` cho runner và đúng hai JAR đã pin, có sẵn trong cùng
container local[1]. Không tăng heartbeat/network timeout, memory hoặc deadline.
Hai attempt mới không còn lỗi startup và đã hoàn tất fingerprint thật.

Nguồn kỹ thuật: [Spark3.5.9 dependency management](https://spark.apache.org/docs/3.5.9/submitting-applications.html#advanced-dependency-management),
[Executor3.5.9](https://github.com/apache/spark/blob/v3.5.9/core/src/main/scala/org/apache/spark/executor/Executor.scala).

- Heap768MiB, direct256MiB, local[1], shuffle4; deadline360s/attempt giữ nguyên.
- Giữ session-only row reader cho encoding Trino `DELTA_LENGTH_BYTE_ARRAY`.
- Dùng chính fingerprint của GoldIcebergWriter, không thay bằng count hoặc hash đơn giản.
- Event fingerprint: `b197e51ebae1e59fa4c8613946a6f15bab231e78090dbe197ed73ec67f3ad474`.
- Bridge fingerprint: `95e97fb077df4a72e26cdeaa55e3d88607f03090d69debfdadcc334319ac2a51`.
- Giữ attempt lỗi v1/v2 cũ và runner pins; không overwrite hoặc retry mù.

## Physical provenance — không khôi phục source keys giả

Owner source pins lấy từ exact CAS observation, không đọc descriptor như full
physical tree hoặc dùng mtime. Request SHA giữ
`ad306633ac438b33276cfa3cf5e797482cc78e139410db3385863bbd3c9501fb`,
observation manifest SHA
`5a77f710655d0b748b79bae63fca65657002ef2130301fb1718c6d90c427f860`,
Bronze pins SHA
`f671d6d8cd951b3ef51f905da3a55c17d80a00b726087544702fe3935c8b9182`.

Java gọi shared BronzeReuseVerifier đọc lại **25 JMA +12 USGS**: raw SHA/size,
ZIP/GeoJSON structure, declared counts, BronzeReady flags và exact source interval.
Sau đó ghép74 manifests gốc đã pin của Trang/Trí vào37 nguồn vật lý duy nhất.
Receipt giữ original/physical URI, manifest IDs, manifest SHA và raw SHA/size.
Mỗi worker phải phủ đủ37 logical identities; duplicate/ambiguous/partial bị chặn.

Logical identity không chỉ dựa SHA: bao gồm source kind/system, release,
request method/URL/path/query, interval, media/encoding/count và format provenance.
USGS không bắt buộc object provenance theo writer hiện có. JMA so format/member/
datum/inventory/URLs; citation_text chứa retrieval time được giữ trong original
bytes nhưng không dùng làm logical archive identity. Không sửa citation nguồn.

Attempt đầu mapper đã fail `SOURCE_IDENTITY_FIELD_MISSING` do yêu cầu provenance
USGS quá chặt. Giữ log/receipt lỗi; sửa verifier theo writer contract và regression
rồi dùng attempt/output mới. V2 đạt mapping, V3 thêm original/physical manifest IDs
và đạt lại. Không sửa input để vượt check.

Gói frontier114 objects nằm trong private/tmp cũ không còn. Đã fresh list đúng
Drive file ID `1QsZHjXXsGmWvBDFHiicMdejmAN2Lehb-`, tải101.446 bytes về staging;
SHA vẫn `0dd1c504f27489709c46748b313e9f764ecfc25a58ef50a2699cbcf0626e8453`.
Giữ plan cũ, tạo follow-up plan mới chỉ đổi local archive path; request/archive
pins không đổi. Không yêu cầu thành viên upload lại, không thay ACL hoặc ghi Drive.

## Artifacts và runtime

[Machine-readable pins](DAT-03_CAPACITY_PROVENANCE_20261115.json) ghi SHA của
launch/execution/result/postflight. Các artifacts/private logs/runner chỉ ở
ignored `staging/dat03-integration-20261114/`, không vào Git.

- Event: `capacity-v3-event-20261115/`.
- Bridge: `capacity-v3-bridge-20261115/`.
- Source mapping cuối: `bronze-physical-readback-v3-20261115/`.
- Cả ba exit0, không timeout; postflight services được guard giữ nguyên,
  không Java/DagRun/Trino query active, source lease trống.
- Docker Desktop mở lại5 containers cũ; không start thêm Spark/API/DAG processor,
  build/pull/recreate, thay deployed runner/cron/credential/IAM/ACL hoặc sửa nguồn.
- Không có write job, Gold namespace mới chưa được tạo trong phiên này.

## Tests và bước còn lại thuộc DAT-03

- 11 Python controller/preparation regression; 8 Java provenance regression.
- 12 Java portable Silver +7 capacity validation regression; Maven package PASS.
- Actual component probes là các kết quả trên; unit tests không thay runtime.

**DAT-03 — Chuẩn bị catalog nghiên cứu thật và bàn giao dataset dùng chung** còn:

1. Versioned import adapter: baseline163 tháng + worker125 tháng, xử lý lineage
   mapping thật, global/boundary dedup/revision/link/reconciliation và own lease.
   Chưa có implementation ghi dữ liệu từ portable packages trong code hiện hành.
2. Commit/publish six tables trên namespace được duyệt `iceberg.dat03_unified_20261114`,
   Trino readbacks/rerun và coverage receipt mới. Không tạo `_SUCCESS`/identity
   hoặc FullMonthReady registry giả cho các bundle worker chưa restore.
3. Adapter official export: FeatureExportCoverage.resolve vẫn yêu cầu receipt-bound
   FullMonth state + SilverBundlePublisher.verify. ResearchDatasetBuildJob hiện
   yêu cầu288 checkpoint của single catalog. Cần hỗ trợ import contract có version
   và test mới; không fake288 legacy checkpoints hoặc bỏ quality gate.
4. Hai official EXPORTED collections, all-shard loader/rerun, immutable team kit
   và ba receipts độc lập. Không giả receipt của ThanhTris/rosy179 hoặc scientific approval.

Tham chiếu [task DAT-03](../task/tasks/DAT-03.md),
[runbook component](../runbooks/DAT-03_PORTABLE_READBACK.md).
