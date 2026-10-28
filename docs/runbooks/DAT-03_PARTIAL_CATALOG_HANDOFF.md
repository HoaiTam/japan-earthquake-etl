# DAT-03 — Bản Parquet 101 tháng trên Drive

**28/10:** đã có thêm24 + 9tháng, tổng partial coverage134tháng đến2011-02.
[Link tải, trusted pins và verify](../evidence/DAT-03_DRIVE_RANGES_20261028.md).
Không official ML export; các số101/còn187 bên dưới là lịch sử26/10.

## Kết quả đã bàn giao26/10/2026

- [ZIP 229.180.092 bytes (~218,6 MiB)](https://drive.google.com/file/d/1wLnASLdSnnis91wGApcRnfFCh2ASQQUd/view).
- [Folder chứa README, verifier, requirements và release pins](https://drive.google.com/drive/folders/1Sd8obJp-4_QF-FGtMwZL4DUeS828etVB).
- [Evidence kiểm chứng và cleanup](../evidence/DAT-03_PREFIX_101_DRIVE_20261026.json).

155 Parquet giữ nguyên bytes từ Java/Spark readback:1.064.869 source_observation,
1.064.869 canonical_membership và0 source_link. Có101 record/checkpoint/context
và accepted catalog proof, phủ [2000-01-01,2008-06-01) UTC. Đã kiểm SHA/member,
counts từng tháng, PK uniqueness và membership join trên fresh download.
source_link rỗng phù hợp giai đoạn JMA này; USGS overlap2023 nằm ngoài101tháng.

Đây là **PARTIAL_PROCESSED_CATALOG / EXPLORATORY_ONLY**, chưa có feature/window/
full-split Mc hoặc official EXPORTED. Không chứa sáu bảng Gold và không là
backup MinIO/Iceberg. Các snapshot IDs trong proof là acceptance pins, không
fresh Trino SQL trong phiên này; Docker daemon không kết nối được và không
được agent bật/restart. Không có ETL hoặc lease/cron/runtime mutation.
Operator đóng gói dùng current local CAS tooling và helper ignored; không
phát hành helper phụ thuộc storage code chưa commit như public pack CLI.
Verifier được gửi là standalone, không cần code checkout/CAS của máy owner.

## Pins bên ngoài ZIP

| Artifact | SHA256 |
|---|---|
| ZIP | `8e54381cb02f5d7b3f33f68382516651b0b6eeca26e6c84ecb65cac962743619` |
| manifest.json trong ZIP | `fc6a4ccac560902db81a2e785316c4578577f63fa1eb2d64914300d36104840b` |
| verify_catalog.py | `b7b2a682e428bef4fee8228f430e62ff08d399626ca2132a8f9e0e89afbe9bb9` |

Owner gửi pins qua kênh nhóm tin cậy. Đối chiếu ZIP và verifier trước chạy
code tải về; không chỉ tin checksums nằm cùng bộ download. Nội dung ZIP không
có .env, OAuth config, private logs, JAR hay source captures.

## Tải và kiểm tra

Tải đủ6file trong folder bằng browser hoặc remote đã setup. Từ một thư mục
mới, không overlay dữ liệu cũ:

```bash
rclone copy dat03_team:data/dat03/shared-inputs/partial-catalog-101/8e54381cb02f ./dat03-prefix-101 --immutable --checksum
cd ./dat03-prefix-101
shasum -a 256 jma-processed-catalog-101-months.zip verify_catalog.py
```

Dừng nếu hash không khớp pins ở trên. Python3.11–3.13 và pyarrow21.0.0;
dùng venv riêng ngoài input. Sau khi cài requirements trong venv của mình:

```bash
python3 -B verify_catalog.py jma-processed-catalog-101-months.zip --zip-sha256 8e54381cb02f5d7b3f33f68382516651b0b6eeca26e6c84ecb65cac962743619
```

Expected PARTIAL_PROCESSED_CATALOG_VERIFIED, completed_months=101,
unique_observation_ids=1064869, unique_canonical_ids=1064869,
membership_coverage_verified=true. research_ready=false/official_exported=false.
Sau verify mới giải nén vào thư mục mới. README trong kit hướng dẫn đọc đúng
Parquet paths từ manifest, không wildcard trộn ba schema. Verifier dùng
temporary SQLite ngoài input; không Docker/pipeline credential hoặc ETL.

Trên Colab tải ZIP từ Drive về runtime local rồi verify/giải nén; không dùng
Drive làm live Iceberg warehouse. Kit hỗ trợ cùng bytes để phát triển notebook,
không tự cho phép official fit/import; giữ Jupyter-first gate của EXP-02/03.
ThanhTris/rosy179 phải tự chạy verifier rồi gửi stdout+pin, không tạo hộ receipt.

## Đã dọn local và phần giữ lại

Sau remote download/semantic/ACL checks đã xóa41exact files: hai ZIP đóng gói
local,6file fresh readback của release mới,29source readback và4pilot readback
cũ đã kiểm lại với Drive. Tổng816.432.440bytes (~778,6MiB). Trong số đó,
128.875.920bytes là bản readback có từ trước task; phần còn lại chủ yếu là
bản ZIP/readback mới tạo trong phiên, không gọi toàn bộ là net saving so với
đầu task. Host free trước/sau cleanup6.528.217.088→7.343.804.416bytes; delta
815.587.328bytes là số đo quan sát, có thể chịu OS/APFS activity.

Không còn ZIP release101 trên local. Metadata/cleanup plan giữ để tải lại;
original source captures, frozen inputs, CAS blobs, checkpoint và warehouse
vẫn nguyên. Reporter trước/sau cleanup khớp101/288, không mất accepted progress.
Không dùng ZIP này làm lý do xóa referenced CAS blobs hoặc Iceberg files:
ZIP không chứa đủ warehouse/runtime để phục hồi hệ thống đó.

**DAT-03 — Chuẩn bị catalog nghiên cứu thật và bàn giao dataset dùng chung**
còn187tháng, full official ML export và ba máy receipts. **SEC-01 — Review
secret và bề mặt truy cập** còn recipient identity/download acceptance và
Colab-session audit khi thực sự dùng; task giữ Needs Update. Không có approval
khoa học hoặc reviewer approval được suy ra từ ZIP/readback thành công.
