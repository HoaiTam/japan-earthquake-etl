# DAT-03 — Inventory tái dùng payload

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

## Bổ sung tối thiểu cho ThanhTris

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
  rosy179 còn bytes gốc92dataset manifests+22parent commits theo
  [request](../../config/research/dat03_lineage_rosy179_metadata.json).
  Nhắc rõ khi review PR #127:138phase journals/spec không thay114objects;
  cần gói đúng và Drive link/ID/SHA.
- **DAT-03:** publication Trí giữ `PUBLICATION_NOT_VERIFIED`; matched
  backreferences/108snapshot pointers không thay trusted publish receipt hoặc
  complete Iceberg graph.37raw references còn cần so frozen shared sources.
- **DAT-03:** còn portable Java/Spark adapter, actual catalog/boundary/
  reconciliation/six pins/Trino/rerun, hai official ML exports và receipts3máy.

Inventory không start/build Docker, đổi lease/cron/ACL, upload/cleanup hoặc
publish official artifacts. Chỉ source/tests/docs/pins nhỏ vào Git, không ZIP/Parquet.
