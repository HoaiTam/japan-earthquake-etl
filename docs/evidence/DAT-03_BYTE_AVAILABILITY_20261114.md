# DAT-03 — Kết quả kiểm tra tiếp ngày 14/11/2026

## Kết luận

DAT-03 vẫn **In Progress**. Đã kiểm kê đủ original content bytes được tham
chiếu của cả hai worker và các nguồn frozen dùng chung; chưa có portable Gold
import, live Trino readback hoặc official ML export mới trong phiên này.

Đây là audit offline/handoff, không phải chạy lại ETL. Không thay lease,
snapshot, Docker, source pins, ACL hoặc dữ liệu MinIO/Drive. Phiên audit không ghi Git; tài liệu/pins được bổ sung PR #131 theo yêu cầu sau audit.

## Những phần đã kiểm tra được

| Kiểm tra | ThanhTris | rosy179 |
|---|---:|---:|
| First-hop metadata theo roots đã pin | 75/75 đã kiểm trước | 83/83 chạy lại đạt |
| Silver dataset manifests / parent Gold commits | 76 / 18 đã kiểm trước | 92 / 22 chạy lại đạt |
| Payload references trong data/proofs ZIP cũ | 100/119 | 121/144 |
| Payload references sau bổ sung bytes tái dùng | **119/119** | **144/144** |
| Raw source references có cùng bytes trong frozen local inputs | **37/37** | **37/37** |
| Phase publication receipts giữ trong data ZIP đã pin | 19 groups | 23 groups |
| Recorded checks trong mỗi phase publication receipt | 148, actual khớp expected | 148, actual khớp expected |

Các recorded checks là **evidence cũ đã đối chiếu offline**, không phải 148 SQL
queries vừa được chạy lại. Trí có18 original publication JSON trong metadata
ZIP; cả18 cùng nội dung JSON với phase publish receipts trong data ZIP độc lập
đã pin. Trang có22parent commits đã pin khớp Gold/Verify/Publish phase reports;
chưa có original publication objects theo S3 URI trong gói metadata của Trang.

## File 595 bytes không còn là blocker về content availability

Cả19references còn thiếu của Trí và23references của Trang trỏ tới cùng content:

- SHA-256: `d07bbf8dbdcf824135f51d69e79418a4f5f35f37ed87f9ad7eac5396cab668c7`.
- Size:595bytes.
- Đã tìm được ba bản byte-identical trong retained QA outputs của HoaiTam.
- Giữ một bản nguyên bytes, không regenerate/repartition Parquet; đóng trong
  gói audit nhỏ `staging/dat03-audit-20261114/byte-identical-reuse.zip` (chỉ có trên máy owner, chưa upload) để công cụ inventory kiểm lại.
- Đây là tái dùng **content có cùng SHA/size**, không dùng dữ liệu QA để thay
  catalog nghiên cứu, không chứng minh worker S3 keys đang tồn tại, không tự
  upload alias hoặc restore key vào lakehouse.

Các flags `s3_keys_verified`, `parquet_semantics_verified`,
`portable_import_ready`, `official_export_ready` vẫn false. Việc đủ bytes
không thay kiểm schema/rows/lineage trên Java/Spark khi portable import được triển khai.

## Frontier còn thiếu — đã bao gồm current head cuối cùng

Frontier cũ chỉ theo **parent commits**, chưa bao gồm Gold commit của batch cuối
mỗi worker. Không được coi22/18parent commits là đủ23/19current Gold commits.

### rosy179

[Request cho agent Trang](../../config/research/dat03_lineage_rosy179_followup_20261114.json):

- 22 original identity files của các parent commits.
- 1 original commit và1identity của current head cuối: tổng24pinned objects.
- 23 original publication URI chưa có bytes trong metadata package; đối chiếu
  với phase publish receipts đã giữ, không tự tạo checksum rồi coi là source trust.
- 138snapshot references trên23Gold phase reports; cần actual Iceberg metadata/
  manifests/data inventory mới có thể nói đến portable graph completeness.
- Head run: `orc07-22b5e7647a3df9ed83370139e11c48048845ee8f`.
- Head phase: `months/2023-10/gold.json` (batch cuối phủ đến12/2023).
- Expected raw head commit SHA:
  `07ad99e8c6ef7a54d09c03806bb23866d463869826a31087d4d7e1b3c00d1137`.

### ThanhTris

[Request cho agent Trí](../../config/research/dat03_lineage_ThanhTris_followup_20261114.json):

- 1 original commit +1identity của current head cuối và original publication
  tương ứng. Không yêu cầu gửi lại18identities/publications đã nhận.
- 114snapshot references trên19Gold phase reports; vẫn cần portable graph
  inventory/verification, không copy snapshot IDs sang máy khác để thay acceptance.
- Head run: `orc07-5679c9c378edacb4e5f804613c8475351ce326b4`.
- Head phase: `2018-09/gold.json`.
- Expected raw head commit SHA:
  `cfbc547badf4807c2530d2960a0cb3988d21a82913f299113787e4dc61da5e72`.

### Cách đáp ứng request

1. Chỉ GET/read original objects từ object storage hoặc retained original export
   theo exact URI; không replay ETL, không đổi source pins, lease, publication flags.
2. Kiểm đúng digest trước khi pack. `identity_sha256` là canonical **payload**
   digest theo serialization contract, không phải SHA của toàn identity file.
3. Phase `gold.json` đã reserialize JSON nên **không** dùng bytes đó để giả
   original `commit.json`. Pin head commit lấy từ bundle SHA trong phase publish
   receipt đã gắn với independently retained data ZIP.
4. Giữ original paths và bytes trong gói nhỏ; gửi external SHA, Drive file ID và
   readback proof vào thư mục nhóm đã được duyệt. Không đổi ACL hoặc gửi credential.
5. Giữ snapshot IDs dưới dạng exact int64; không chuyển qua float/JavaScript
   Number. Request có canonical snapshot-reference digest và regression test
   để phát hiện làm tròn IDs lớn hơn2^53. Digest này bảo vệ reference metadata,
   không thay portable graph/readback acceptance.
6. Nếu original object không còn, báo exact URI/lỗi; không regenerate hoặc nới guard.
7. Không gửi lại các data ZIP lớn chỉ để đáp ứng metadata requests này.
8. Đây là follow-up request, không phải request114frontier hoặc83first-hop.
   Không truyền trực tiếp vào pack-frontier/extract-minio-provenance vốn khóa
   loại/count khác, rồi nới guard để chạy. Dùng S3 client đã cấu hình để lấy
   exact objects và giữ mapping; mọi công cụ metadata mới cần test/schema phù hợp.

## Các gate chưa hoàn tất

- **DAT-03:** original identities/current-head metadata, complete Iceberg graph
  và publication proof closure; content inventory không phải S3 key acceptance.
- **DAT-03:** portable Java/Spark adapter (dùng baseline ORC-07 đã Done) và actual unified Gold boundary,
  source-link/dedup/reconciliation, six snapshots, Trino/rerun acceptance.
- **DAT-03:** hai official REPRODUCTION/EXTENSION exports bằng MLI-02/MLD-05 hiện có,
  lifecycle VALIDATED/EXPORTED, all-shard loader và rerun thật từ unified Gold pin.
- **DAT-03:** immutable team kit và verifier receipts của ba máy thật.

Không chuyển DAT-03 Done trong phiên audit này.

## Báo cáo máy đọc được

[Summary có version và pins](DAT-03_BYTE_AVAILABILITY_20261114.json) giữ count,
hash báo cáo thật, input pins, follow-up requests và các flags acceptance false.

[Payload request Trang](../../config/research/dat03_lineage_rosy179_payload.json)
được trích từ dataset manifests đã matched với trusted roots. Công cụ
`inventory-dat03-payloads.py` nhận được request này; pin file nằm trong summary.

Original audit reports giữ trong ignored `staging/dat03-audit-20261114/`:
`rosy-combined-audit.json`, `rosy-payload-inventory.json`,
`rosy-payload-with-reuse.json`, `tri-payload-inventory.json`,
`tri-payload-with-reuse.json`, `shared-source-byte-inventory.json`,
`publication-phase-crosscheck.json`. Không commit raw report hoặc ZIP dữ liệu.
Các báo cáo này là evidence local; summary/pins trong Git không có nghĩa hai
máy worker đã tải file595bytes hoặc verifier receipts đã được thu đủ.

## Pins và phạm vi

- Roots Trang:
  `f4fdc2115fc97da409231823b54a6eb7cecffe0557cb9f893e905b9cbdb39a4c`.
- Data ZIP Trang:
  `18bc436a25969a2d019b5db04bb1a2d20aa11a0c3039e2fd159da7f876b4a788`.
- Proof ZIP Trang:
  `7050d8201ce79bf8b3760830d187edbb13fd369364cafc7c4e43f94ca15df35a`.
- Data ZIP Trí:
  `c072e14f4aa2964af3f9a641e0973bdff6b4dbdc6084d820df79a18707d3b231`.
- Proof ZIP Trí:
  `b88c65f5a14a7f2ebe4c1c5483d5c6575bae28c73e4ac6cdc7b7bf83fe614b68`.
- Frozen source request:
  `ad306633ac438b33276cfa3cf5e797482cc78e139410db3385863bbd3c9501fb`.
- Byte reuse ZIP:
  `31017fe5c98ea24f4d1c70ddd0d9b9dc7cae12aea784d7c43d11b2c9f44c9c9f`.

Phiên audit ban đầu:45tests metadata lineage và17tests byte inventory đều đạt
(62tests). Cập nhật PR #131 thêm5tests cho committed requests, current heads,
exact int64 snapshot pins và closed acceptance flags; payload suite hiện22tests.
Chỉ đọc/hash streams trong ZIP; không giải nén dataset. Phiên audit tải
một ZIP data Trang292024778bytes (~278,5MiB); các source ZIP/warehouse/evidence
cũ được giữ nguyên. File tải tạm vẫn giữ để kiểm lại, chưa cleanup hoặc upload.

## Kiểm tra trước commit cập nhật PR #131

- `make test-dat03-payload-inventory`:22tests đạt.
- `make test-dat03-lineage`:45tests đạt.
- `make test-dat03-provenance`:13+9tests đạt.
- `make test-docs`:10+7tests đạt.
- `python3 -B -m unittest tests.ci.test_impact tests.ci.test_suites -q`:38tests đạt.
- Tổng144offline tests đạt; không cần Docker, nguồn thật hoặc credential.
- `make check-task-status`:81task/index/tracking đồng bộ; DAT-03 In Progress.
- `python3 -B scripts/check-docs.py`:link/file/heading checks offline.
- `ENV_FILE=/dev/null sh scripts/check-config.sh`:repository/example secret
  hygiene đạt; không chứng nhận runtime credential hoặc cloud sharing.
- Lệnh inventory Trang trong runbook đã chạy lại với exact ZIP SHA:exit0,
  144/144content refs, official/portable flags false. Report SHA vẫn
  `e81048b1613c80c41aa48917e93e1eab3e11235bfa9bbab5466dab66d143c5db`.
- `git diff --check` và staged artifact/secret review trước push.
