# DAT-03 — Kiểm lineage hiện có trước portable integration

> Cập nhật14/11: [byte/metadata audit mới](../evidence/DAT-03_BYTE_AVAILABILITY_20261114.md)
> đã nhận đủ114metadata của Trang,119/119payload refs Trí/144/144Trang và37/37
> raw-source contents mỗi worker trên coordinator. Các requests12/11 bên dưới
> giữ làm lịch sử; dùng [follow-up Trang](../../config/research/dat03_lineage_rosy179_followup_20261114.json)
> và [follow-up Trí](../../config/research/dat03_lineage_ThanhTris_followup_20261114.json)
> cho identities/current-head và publication còn thiếu. Không chuyển Done.

## Mục tiêu và phạm vi

Đọc ZIP metadata offline, xác minh original bytes theo **trusted checkpoint
requirements SHA bên ngoài ZIP**, lần theo các schema đã nhận diện và tạo
frontier còn thiếu. Đây không phải ETL, portable import, Trino readback hoặc
nghiệm thu full transitive closure. Không thực thi code trong ZIP.

`scripts/audit-dat03-lineage.py` chỉ đọc local archives; không có client mạng,
credential, MinIO writes, lease hay Docker. Không giải nén ra cây thư mục.
Guard: tối đa8archives,8MiB/archive,512entries/archive,2MiB/entry và64MiB tổng
decoded. Đây là công cụ metadata, **không dùng để ingest payload lớn**.
Chặn traversal/symlink/encryption/duplicate member/JSON keys, khác bytes cho
cùng object, conflicting pins, SHA/byte-size sai và overwrite report.

Không dùng checksum do package tự khai để chứng nhận trusted provenance.
`metadata/` là transport wrapper được bỏ sau kiểm path safety; expected SHA
vẫn lấy từ checkpoint. Package-level manifest/README không phải bucket objects.
Parent `commit_key=gold-commits/...` trong registry tương đối SilverObjectStore,
nên physical key là **`silver/gold-commits/...`**, không bucket-root alias.

## Cách kiểm lại trên máy HoaiTam

Input nhỏ đã tải nằm trong ignored `staging/dat03-lineage-20261112/`. Trên máy
mới, tải đúng objects từ folder Drive nhóm hiện có, giữ riêng các phiên và
đối chiếu pins trong [evidence](../evidence/DAT-03_LINEAGE_FRONTIER_20261112.md).
Các lệnh sau là offline và không tự tải, build/start Docker hoặc chạy ETL:

```bash
make test-dat03-lineage
python3 scripts/audit-dat03-lineage.py --roots staging/dat03-lineage-20261112/tri-required.json --roots-sha256 33f9773fee0c1d87972cc8020459cf1b230cfa8687b04b513e40183e92f10aa6 --archive staging/dat03-lineage-20261112/tri-first-hop.zip --archive staging/dat03-lineage-20261112/tri-lineage.zip --output staging/dat03-lineage-20261112/tri-repeat-audit.json
python3 scripts/audit-dat03-lineage.py --roots staging/dat03-lineage-20261112/rosy-required.json --roots-sha256 f4fdc2115fc97da409231823b54a6eb7cecffe0557cb9f893e905b9cbdb39a4c --archive staging/dat03-lineage-20261112/rosy-first-hop.zip --output staging/dat03-lineage-20261112/rosy-repeat-audit.json
```

Exit2 + report hợp lệ là **frontier còn thiếu**, không phải full acceptance.
Lỗi validation trả nonzero và không ghi report mới; giữ nguyên inputs/checkpoint.
Report có thể được tạo lại vào tên output mới, không ghi đè evidence cũ.
`MATCHED` chỉ chứng nhận object bytes khớp pin. Snapshot IDs không phải snapshot
files; cả portable_import_ready/official_export_ready/full_transitive_closure_verified
luôn false trong component này, kể cả frontier được cung cấp đủ.

## Giao Trí12/11 — yêu cầu lịch sử

1. Đọc [metadata request](../../config/research/dat03_lineage_ThanhTris_metadata.json).
   **18 identity.json** có expected SHA từ18commit đã verify. Không cần gửi lại
   75 first-hop,76 dataset manifests hay18parent commits đã matched.
2. Request còn18publication URI **chưa có expected SHA**,108snapshot references.
   Thu original publication bytes và proof journal/acceptance liên kết theo
   existing pinned evidence; tự tính checksum mới không chứng minh source trust.
   Không copy snapshot IDs sang máy nhận để giả portable acceptance. Muốn
   transport snapshots phải bổ sung metadata/manifests/data files theo actual
   table location và snapshot graph; chưa có đủ URI/SHA để gọi danh sách này complete.
3. [119 payload references](../../config/research/dat03_lineage_ThanhTris_payload.json)
   có URI/SHA/byte_size tổng522659149bytes (~498,4MiB). **Đây là inventory, chưa
   phải yêu cầu upload lại toàn bộ.** So với data ZIP/shared bytes đã có theo
   exact SHA trước; chỉ transfer phần thiếu thực sự, theo batch có checkpoint
   sau khi chốt dung lượng/quyền. Giữ đủ source paths và proof mapping.
4. Agent trên máy worker dùng S3 client đã cấu hình để GET từng exact key,
   kiểm SHA/size; không regenerate manifest, không ETL replay, không đổi lease.
   Công cụ extract-minio-provenance hiện khóa count first-hop75/83, **không**
   truyền request18objects vào đó rồi nới guard để làm xanh.

## Giao Trang12/11 — metadata114 đã matched14/11

1. Đọc [metadata request](../../config/research/dat03_lineage_rosy179_metadata.json):
   **92 dataset manifests +22 parent commit JSON**,114 exact objects có pin.
2. Parent physical prefix đã sửa `silver/gold-commits/`. Không báo mất object
   chỉ vì bucket-root alias không tồn tại. Không cần gửi lại83first-hop.
3. GET original bytes, verify SHA từng object, pack với paths an toàn và
   manifest/readme/checksums; metadata ZIP nhỏ upload vào worker folder nhóm
   khi được duyệt, không thay ACL. Gửi link/ID/external SHA cho coordinator.
4. Sau metadata readback, coordinator mới mở inventory payload/identity/
   publication/snapshot tầng tiếp theo. Chưa được suy số lượng tầng đó bằng
   cách lấy counts của Trí áp sang Trang.

## Checkpoint và phần còn lại

**Tiếp tục13/11 sau #128:** [payload reuse inventory](DAT-03_PAYLOAD_REUSE.md)
đã đối chiếu actual data/proof ZIP Trí:100/119references byte-identical,
19references còn lại cùng1content595bytes. Không yêu cầu upload lại498MiB;
[delta request](../../config/research/dat03_lineage_ThanhTris_payload_delta.json)
giữ exact URI/SHA. Không thay full closure hoặc Gold acceptance.

`NOT_SUPPLIED_IN_PACKAGES` nghĩa là chưa có trong ZIP của lần kiểm này,
**không chứng minh mất trên MinIO hoặc không có trong các data ZIP khác**.
37raw source references của mỗi worker có thể tái dùng shared sources theo
bytes/SHA; không yêu cầu download lại archives trước khi inventory đối chiếu.

DAT-03 tiếp theo vẫn cần versioned portable adapter, capacity/boundary/source-link
proof, actual unified Gold/six snapshots/Trino/rerun, hai official ML exports
và receipts3máy. Python ở đây chỉ audit metadata/handoff; xử lý dữ liệu/import
Gold/ML dataset vẫn Java/Spark. Không tự upload report, cleanup bản duy nhất,
commit/push/merge, start/rebuild runtime hoặc đổi source pins.
