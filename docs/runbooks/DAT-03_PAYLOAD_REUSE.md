# DAT-03 — Inventory tái dùng payload

## Hiện hành14/11 — đủ content bytes trên coordinator

[Actual evidence14/11](../evidence/DAT-03_BYTE_AVAILABILITY_20261114.md) và
[summary/pins](../evidence/DAT-03_BYTE_AVAILABILITY_20261114.json) thay phần
blockers13/11:119/119refs Trí,144/144Trang và37/37raw-source refs mỗi người
có bytes khớp SHA/size. Một file595bytes retained local giải quyết42references
cùng content; **không** chứng minh42original S3keys hoặc cho phép restore alias.
Không lấy QA output để thay research rows; chỉ bytes đã hash bằng đúng pin được
tái dùng. File nhỏ chưa upload/chia sẻ cho máy worker, không tự nhận team handoff.

Trang114frontier metadata đã supplied/matched. Không yêu cầu gửi lại138journals,
114metadata hay data ZIP lớn. Phần còn lại đọc theo
[request Trang24pinned objects/23publication URIs](../../config/research/dat03_lineage_rosy179_followup_20261114.json)
và [request Trí2pinned objects/1publication URI](../../config/research/dat03_lineage_ThanhTris_followup_20261114.json):
original identities và **current head cuối** trước đây chưa nằm trong parent
frontier. Snapshot references138/114 không phải portable graph đã đủ files.

Các follow-up requests này không cùng schema/count với pack-frontier114 hoặc
extract-minio-provenance75/83. Chỉ GET exact original bytes bằng S3 client đã
cấu hình, không truyền secret trên command line, không nới guard hoặc regenerate
metadata. Identity pin là canonical payload SHA, không full-file SHA. Giữ original
commit/publication bytes; phase `gold.json` đã reserialize không thay original
`commit.json`. Xem evidence14/11 cho source/archive pins và cách đối chiếu.

### Chạy lại inventory Trang trên máy coordinator

[Request144refs](../../config/research/dat03_lineage_rosy179_payload.json) có
SHA `c2421b6e4ea6df2f30299fecf134d010661835e0af8a80cfb42951e5a375728f`.
Các input dưới đây nằm trong ignored `staging/dat03-audit-20261114/` trên máy
owner; máy khác chỉ chạy sau khi tải đúng ZIP/nhận file reuse và kiểm external
pins từ summary. Gói reuse chỉ là bản sao nguyên595bytes cùng README, không
chạy parser hoặc tạo Parquet mới.

```bash
make test-dat03-payload-inventory
python3 -B scripts/inventory-dat03-payloads.py --request config/research/dat03_lineage_rosy179_payload.json --request-sha256 c2421b6e4ea6df2f30299fecf134d010661835e0af8a80cfb42951e5a375728f --archive staging/dat03-audit-20261114/rosy179-data.zip --archive-sha256 18bc436a25969a2d019b5db04bb1a2d20aa11a0c3039e2fd159da7f876b4a788 --archive staging/dat03-audit-20261114/rosy179-proofs.zip --archive-sha256 7050d8201ce79bf8b3760830d187edbb13fd369364cafc7c4e43f94ca15df35a --archive staging/dat03-audit-20261114/byte-identical-reuse.zip --archive-sha256 31017fe5c98ea24f4d1c70ddd0d9b9dc7cae12aea784d7c43d11b2c9f44c9c9f --output staging/dat03-audit-20261114/rosy-payload-repeat.json
```

Expected exit0:144/144requested content references available, nhưng mọi official/
portable/S3-key flags vẫn false. Output immutable: nếu đã có report cùng tên,
chọn tên mới; không ghi đè evidence. Code không tự tải, upload hoặc cleanup.

## Mục đích và ranh giới

Sau [metadata frontier](DAT-03_LINEAGE_FRONTIER.md), đối chiếu **original Parquet
bytes** trong data/proof ZIP với URI/SHA/size từ dataset manifests đã pin.
[Công cụ](../../scripts/inventory-dat03-payloads.py) chỉ hash streams, không
giải nén, parse rows, ETL, SQL hoặc ghi S3. Python là audit/handoff; portable
Gold/ML import vẫn thuộc Java/Spark.

ZIP SHA đến từ checkpoint độc lập, không lấy manifest trong chính ZIP làm trust.
Request SHA cũng pin bên ngoài. Member cùng SHA **và** size ở tên khác là bytes
có thể tái dùng, không chứng minh original S3 key tồn tại hoặc cho phép tự ghi
alias. Không thay original bytes bằng Parquet repartition/regenerate dù rows giống nhau.

## Chạy lại inventory thật của Trí

Inputs nằm trong ignored `staging/dat03-payload-20261113/`. Máy mới tải đúng
data/proof ZIP từ worker folder được duyệt, đối chiếu [pins/evidence](../evidence/DAT-03_PAYLOAD_REUSE_20261113.md).
Đầu phiên host còn khoảng19GiB; hai ZIP khoảng220MiB, không extract. Giữ physical
reserve4GiB; không tự xóa warehouse/source pins/evidence để vượt gate.

```bash
make test-dat03-payload-inventory
python3 -B scripts/inventory-dat03-payloads.py --request config/research/dat03_lineage_ThanhTris_payload.json --request-sha256 0643f60280fa381f610b69c1f1e0c17fcc899521818363d32392b4883bdd1e25 --archive staging/dat03-payload-20261113/ThanhTris-dat03-62months-data.zip --archive-sha256 c072e14f4aa2964af3f9a641e0973bdff6b4dbdc6084d820df79a18707d3b231 --archive staging/dat03-payload-20261113/ThanhTris-dat03-62months-proofs.zip --archive-sha256 b88c65f5a14a7f2ebe4c1c5483d5c6575bae28c73e4ac6cdc7b7bf83fe614b68 --output staging/dat03-payload-20261113/tri-payload-repeat.json
```

Expected exit2:100/119references byte-identical available,19references chưa có
trong hai ZIP,1unique content/595bytes. Exit2 là partial inventory, không ETL
failure. Chạy lại chọn output mới, không overwrite checkpoint. Validation
failure không tạo report; giữ inputs/chẩn đoán, không retry mù.
Exit0 chỉ nói đủ requested bytes, không là portable import hoặc DAT-03 Done.

Guard:8archives,1GiB/archive,20.000entries/archive,256MiB/member,4GiB declared
decoded total,1MiB chunks; chỉ hash Parquet candidates cùng size cần tìm.
ZIP hash dùng cùng descriptor trước member scan. Chặn unsafe paths, symlink
file/parent/member, encryption/special entries, duplicate member/URI, conflicting
size, exact physical key khác SHA/size và input đổi khi scan. Member không là
candidate chưa được semantic/CRC audit; toàn raw ZIP vẫn khớp external pin.

## Delta13/11 của ThanhTris — lịch sử

14/11 đã có bản byte-identical tại coordinator; giữ delta request làm provenance
của lần inventory trước. Không còn yêu cầu Trí transfer lại595bytes, trừ khi cần
xác minh original key/source-side existence riêng, và không yêu cầu gửi lại100refs.

[Delta request](../../config/research/dat03_lineage_ThanhTris_payload_delta.json)
giữ **19 original reject_record/part-00000.parquet references cùng1content**:

- SHA `d07bbf8dbdcf824135f51d69e79418a4f5f35f37ed87f9ad7eac5396cab668c7`,595bytes.
- Tổng19references11.305bytes; content độc nhất chỉ595bytes.
- Dùng S3 client đã cấu hình GET đúng một URI trong request, giữ original bytes,
  kiểm SHA/size, gửi kèm original URI. Không gửi credential/private logs.
- Một member khớp đủ cho **content inventory** của19refs; giữ mapping/pins,
  không tự chứng nhận19S3keys hoặc upload alias vào lakehouse.
- Đóng gói bất biến với safe path, checksum và original-URI mapping; sau khi
  owner duyệt upload, gửi Drive link/ID/external SHA để coordinator kiểm lại.
- Không ETL replay, không regenerate empty Parquet, không gửi lại100payloads
  đã matched hoặc thay/xóa gói62tháng cũ. Nếu original object không còn, báo
  exact URI/lỗi, không tạo bytes thay thế để pass.

Không suy payload counts của Trang từ inventory Trí.

## Gate chưa hoàn tất

- **DAT-03 — Chuẩn bị catalog nghiên cứu thật và bàn giao dataset dùng chung:**
  original identities/current-head commit/publication theo follow-up requests
  hiện hành phía trên, không còn chờ lại114metadata của Trang.
- **DAT-03:** Trí18original publications khớp JSON với independently pinned
  phase receipts; recorded checks đọc offline không thay live Trino acceptance
  hoặc complete Iceberg graph.37/37raw-source contents đã matched mỗi worker,
  nhưng original worker S3keys/BronzeReady chưa được revalidate.
- **DAT-03:** còn portable Java/Spark adapter, actual catalog/boundary/
  reconciliation/six pins/Trino/rerun, hai official ML exports và receipts3máy.

Inventory không start/build Docker, đổi lease/cron/ACL, upload/cleanup hoặc
publish official artifacts. Chỉ source/tests/docs/pins nhỏ vào Git, không ZIP/Parquet.
