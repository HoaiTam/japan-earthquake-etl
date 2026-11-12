# DAT-03 — Bàn giao Metadata Frontier của ThanhTris, 12/11/2026

Branch `feat/dat-03-lineage-readiness` từ `main c0494a9`. Tác giả: **ThanhTris**.
Đáp ứng yêu cầu metadata frontier từ Hoài Tâm trong [dat03_lineage_ThanhTris_metadata.json](../../config/research/dat03_lineage_ThanhTris_metadata.json), [runbook](../runbooks/DAT-03_LINEAGE_FRONTIER.md) và [evidence](DAT-03_LINEAGE_FRONTIER_20261112.md).
Task DAT-03 vẫn giữ **In Progress**, không tự chuyển Done; không khẳng định full transitive closure, unified Gold hay official ML export.

## 1. Gói dữ liệu bàn giao trên Google Drive

Thư mục đích của worker: `1bcRDZ5feOeos_U8HcsfOQZ2ELPoOGuDg` (Google Drive ThanhTris).

| Tên tệp | Drive File ID | Kích thước | SHA-256 | Số mục |
|---|---|---:|---|---:|
| `ThanhTris-dat03-frontier-metadata-20261112.zip` | `1LP0vzLiQkqENk3gVPTPL04mMKW_8f8GJ` | 149.712 bytes | `cb6cee8e1fd3315f35548bcd21f40f40d5392a33b533bdbce20207e6e2342415` | 38 |
| `ThanhTris-dat03-frontier-metadata-20261112.zip.sha256` | `1ljSpTMtCkGlhLvNw-Mi5JAKn76nEb6MW` | 108 bytes | `21b58514138e4a9ba148da3da006b528b18dc8df0a73b984bf72da89f925ea76` | 1 |
| `DAT-03_LINEAGE_ThanhTris_METADATA_20261112.md` | `16hz_kxqUcf0625nlz79i_BC01AQEG16F` | 15.2 KB | `0d8b4bf46f7bb8a7066f1e292a806932a394ec68ea1e70c67aa419f8dd022b72` | 1 |

> **Readback test:** Đã dùng Google Drive API/CLI tải ngược về môi trường local và kiểm tra băm SHA-256 khớp 100% bitwise.

## 2. Đối soát 18 tệp `identity.json` (18/18 MATCHED)

Tất cả các tệp `identity.json` được trích xuất từ MinIO local (`s3://japan-earthquake/silver/gold-commits/<run_id>/identity.json`).

| STT | Gold Run ID | Kích thước | Pinned `identity_sha256` | File Raw SHA-256 | Trạng thái |
|---:|---|---:|---|---|:---:|
| 1 | `orc07-082db36f5ca34c6f7be6f210bee1197f10240880` | 7,623 B | `f816d88d5fa45035214d001a501638d63c8756f81472ccca31fa5d289166297c` | `cdb8f0c0bca20efba7d21a17bebf8daa1ee1579292566f65460bea984b41f020` | MATCHED |
| 2 | `orc07-0953d5d2d5ea1f9b4150985340367fa47a4abfd4` | 7,613 B | `216d76cb663cb958014edbcb93263068fe4571cac29233ff706315fdba491069` | `5411049c7428a9815e7a81b340f07a7e32b839b4255b63f61959e66ff86108bd` | MATCHED |
| 3 | `orc07-17326468816eae5c920b0317d3ad748207c00e1e` | 7,497 B | `1e2f3a5a1baf30c91d5535e0f70ae9fbcb41b8a3c453ae7d5544a54560138c10` | `e6d34c690f3e603f926e378937f2af98e268902c1c700fdad007831d48892562` | MATCHED |
| 4 | `orc07-1ca9bb57617c5647777f3fe8502366c32019362b` | 7,614 B | `8f7da879b91f10b9be6959a5b74b912aaa59bb388afac623aedebd63b4b89676` | `73e29a5f3e251ec4c65f8bf23d6f371f1d2aa496687d23b6d8edfc749b711db0` | MATCHED |
| 5 | `orc07-2cee619a8c2e42f34431ad4783d0fce30c38f9c6` | 7,615 B | `203729ece02d0de08f4a08480872646aebc0967f9dad7c5092f069f195c07045` | `88a67e027a26034f5ccc471cd0da91b080515fba202d794f9228097e1764ab0c` | MATCHED |
| 6 | `orc07-45708f6ec5641fb8efb50aa28e0edd6699dba1c3` | 7,625 B | `5e62e3ca7664d292d131d328318bb7720cfe715d84a5e615cfc54a1410b960ca` | `cd1b8a4c229c3affc2b7b6963612b083e1afb0bf1f9374f2b3e852c57ef0a90a` | MATCHED |
| 7 | `orc07-4c388509c64df68a3ff50a0af3491d808810d557` | 7,666 B | `449e10a54a9e586d8168482ae526412620f6fa68511ddc4d403c9aff8f460b6b` | `1e7e2359e14df1506558ff703c37c7749ea20c7c3e4365779e9482ef858af7eb` | MATCHED |
| 8 | `orc07-4e50aaad50668cd69b238e2b68ff467e2ba32da4` | 7,666 B | `54a6646ce56461268e4debb16a03932699520982f1209cd7e2311cd571b1633c` | `8c1a571662493d0f1a0da91a663ea91a8a167ef7e9fe942f11ecb14b484cf36e` | MATCHED |
| 9 | `orc07-98eee411ac549e4b18e06bf939d6039e8664d99c` | 7,625 B | `adf3c1529cbca930c34f86cf1cf71a791ccb48f878bb4c820d36a10436489f3c` | `982634c064a200ccd014d29b5682da6adee5b50f9a753213153443b8defadc95` | MATCHED |
| 10 | `orc07-9cfd261527b05f5851778addeebf2d09d5799fb0` | 7,624 B | `092149e7ed84b052a185a578d3b59ffe0cb5e0cf58048ed858f755de6ff468fe` | `648fe4ebe275503abf325c3d6d3a637c1bbd839bf6df45f22b3b6a553b8d9f08` | MATCHED |
| 11 | `orc07-ab3c35d5fe38dda32bfedbfd1340d8872cae120b` | 7,613 B | `350532211b81599c8921928d273ba56f25a27cd06183e58e77d3354e4b05a4eb` | `2611fc09d46401651e6e03af73bbbb311e7fdc7313967a7791bba662686dc907` | MATCHED |
| 12 | `orc07-c3c985db61b11a4bbd3f72ba2db88ca1d7975595` | 7,646 B | `2edb512714921e9dc00c62e4580062d3f936fa3661cf824c951b497d50838833` | `dd3f380a6e2f0796b0e4232c26400ab528b5a4b804b3ffee6a785469db718d1b` | MATCHED |
| 13 | `orc07-c9fa6b114dfcfa7af3653cd54f35a64c5e84d1a2` | 7,605 B | `b2d932f92ee5165ca47ba930d78a2da2846e18cb218e437d3e75d477494bea3d` | `d0295c52f7be098ccba8f77d4821c56d92558752080e1f725fb1a4fb6453038c` | MATCHED |
| 14 | `orc07-d954963fdd926abdc5e7807631f2e24f8112ff70` | 7,656 B | `421ce52b3fd76405d45e07a35eca8d7bac1c94f5faf88b82f1f4b597debbb9c3` | `f7201a053099947e9bf30093e7898b22c1c4e58f3ded3a53ebf12beb4d31e2a7` | MATCHED |
| 15 | `orc07-ddb6a71d10f03598aa6f8138858e522165486410` | 7,656 B | `d13e3a63022f28ac51e02ffe270c96f3032498cddf1c270bc7ab9b901c377dc2` | `75603b355534a9575df1de6c0e40b2d91bdf39ec2e9565339cac5f9cddc403d0` | MATCHED |
| 16 | `orc07-e4e879c8df576f57961ae02989a982618213e653` | 7,623 B | `2640733d4db484db34b8376e917e169bf57021fc32b22c05d8b3d139133e268d` | `f3b7ff9a359ed9b38023091dfd3568d2c7bcf8e7da2426c03247244f23d5926f` | MATCHED |
| 17 | `orc07-ec9d8788a27175295ff6a47e9b94f33a1170b6ca` | 7,615 B | `a0b46907d565f47554c111c9d20109f329eef1832309916a504f18e16845b801` | `e960f012486f93b4217b76b38c4c1cebfc65cec2ddfa3e2857cb38f600569b6a` | MATCHED |
| 18 | `orc07-fb052729d91a1922e1328e3322594b4926b724d1` | 7,625 B | `40fda41d7988bb58abff7dec36353d8eb738bad0c234d265a1cabd32b907ab5f` | `333cd8b374dcf15d544d563fc1930428694dbdfa3016c3d6e5cdec51374781f9` | MATCHED |

### Giải trình kỹ thuật về phân biệt `raw_file_sha256` và `identity_payload_sha256`
Trong quá trình đối soát, mã SHA-256 tính trên raw bytes của tệp `identity.json` khác với mã `identity_sha256` được pin trong `commit.json` và yêu cầu checkpoint.
Nguyên nhân đến từ thiết kế tại mã nguồn Spark Java `GoldIcebergWriter.java` (dòng 79–81):
```java
String identitySha = SilverBundlePublisher.sha(JSON.writeValueAsBytes(identity));
identity.put("identity_sha256", identitySha);
byte[] claimBytes = JSON.writeValueAsBytes(identity);
```
- `identity_payload_sha256` là chữ ký hàm băm chuẩn hóa của đối tượng JSON **trước khi** thuộc tính `"identity_sha256"` được chèn vào payload.
- Khi lưu trữ xuống MinIO, tệp vật lý bao gồm cả thuộc tính `"identity_sha256": "<hash>"`, làm cho raw byte hash của file bị thay đổi.
- Verifier hiện tại đã thực hiện tính toán lại chính xác payload hash sau khi bóc tách trường khai báo theo đúng serialization contract của Java; đạt kết quả đối soát: **18/18 tệp MATCHED**.

## 3. Đối soát 18 tệp `publication.json` và Proof Journal (18/18 PASSED)

Các tệp `publication.json` đã được trích xuất từ `s3://japan-earthquake/silver/gold-verification/<run_id>/publication.json`.
Trước đây, các tệp này nằm trong danh sách `unpinned_publication_uris` chưa có SHA. Bảng dưới đây cung cấp đầy đủ mã băm SHA-256 của tệp gốc để Hoài Tâm và nhóm đưa vào pinned requirements:

| STT | Gold Run ID | Publication ID | Bản ghi snapshot | File Size | Raw File SHA-256 | Verify Status | Published |
|---:|---|---|---:|---:|---|:---:|:---:|
| 1 | `orc07-082db36f5ca34c6f7be6f210bee1197f10240880` | `pub_1b5e9df724ca761937651f47fa08ec811c3de092f4a59dae3565fb6eb37ff8e4` | 1,098,320 | 112,666 B | `2a37240cc736c57ea75ab4eeaa5ac71d63cd33fc70d8d060618c7df344d1cc45` | PASSED | True |
| 2 | `orc07-0953d5d2d5ea1f9b4150985340367fa47a4abfd4` | `pub_387e10ffa892eec44c1539cccd0cf30c996dd818d78d01e3a9235c948dfdde53` | 514,848 | 111,210 B | `6d4cd3de6d804a62f11915a55e19dec35c264bb0aa2562715b2d163a38a21def` | PASSED | True |
| 3 | `orc07-17326468816eae5c920b0317d3ad748207c00e1e` | `pub_925102c838dd05495876215d2ec4e8c88f09f862a5c845f02100f9473a93df9e` | 13,026 | 73,836 B | `87b3c00583f9d014c6a68e3afe5c5b978edf9913c7e468051199a04a5daefb42` | PASSED | True |
| 4 | `orc07-1ca9bb57617c5647777f3fe8502366c32019362b` | `pub_19ae7aa53ae62692c794efc4297d9bc7d68e28f9346a340601d3e85d3e6aa49e` | 843,782 | 111,324 B | `5047b503dfca65a7e86e72455e3579fda4f8b29f40c3ed612414e770a64e26cc` | PASSED | True |
| 5 | `orc07-2cee619a8c2e42f34431ad4783d0fce30c38f9c6` | `pub_c8845dc69faf75577858a5c645390d6c906c3343df4d7877516a4d187df21e69` | 461,606 | 111,248 B | `cbebe044b266dc78db04b12891f4beeb3bff6e08478fd85d98dc357ed25fb3da` | PASSED | True |
| 6 | `orc07-45708f6ec5641fb8efb50aa28e0edd6699dba1c3` | `pub_1a7835aaa52e1bff1c68da54c587545f4481befa1c17eda5bf6a1a4619605aef` | 1,031,147 | 112,654 B | `7f8bc93bd7113fa39e893e7d45f280628413c9595e0ea28c7fe28818b2dd4ad9` | PASSED | True |
| 7 | `orc07-4c388509c64df68a3ff50a0af3491d808810d557` | `pub_36ef843c8b89ce879ff95c640383f719a483bebdd9a2921dd8d9cab3a07ac3b8` | 350,835 | 117,854 B | `a2ec4a127d5f0e92047b6f955268f1809c2f67fe2e37c81ec3ca4c93e483f2fe` | PASSED | True |
| 8 | `orc07-4e50aaad50668cd69b238e2b68ff467e2ba32da4` | `pub_8bf133fb61cf1fd895d19a325b86821f591ef3c8ae42aa50b63e2f653d18cb1a` | 226,908 | 117,854 B | `6f34396275b1b43beb8a060da64b8a30c14d600d91e7aeb77230cf44c1413d2a` | PASSED | True |
| 9 | `orc07-98eee411ac549e4b18e06bf939d6039e8664d99c` | `pub_2e401385a2d946e6b092bbe80c18b4683d25122ee0f4d024df9e2ababdbde4b6` | 707,553 | 112,643 B | `c95f031f7611867d4d010c6967aa09f35a733733fd29898b3c9112a3eb26fc67` | PASSED | True |
| 10 | `orc07-9cfd261527b05f5851778addeebf2d09d5799fb0` | `pub_42d20ef31fbe1a8c34a33a04927a80b97b3fd7832edcd15a3e4d25ae5283eadc` | 964,285 | 112,624 B | `4796c5208d5d4e2ff326ed31dd49f32a38232f4e50e508622ce495273be8fa66` | PASSED | True |
| 11 | `orc07-ab3c35d5fe38dda32bfedbfd1340d8872cae120b` | `pub_a935ae254bc95a1d21ba40f80981f7e317b7615c89e89b8c98827fa920b7c052` | 571,769 | 111,323 B | `de324dfe8b2d6e4b8a4ad7ab0b138381e075ff4ba6de96bad03a59bf4d6d0616` | PASSED | True |
| 12 | `orc07-c3c985db61b11a4bbd3f72ba2db88ca1d7975595` | `pub_2a19c4a890329eeb20e9c058d88dd2bc814efa8ac2ab79eacc2e960c28c3c3aa` | 280,217 | 115,254 B | `27b9e0b9e8ac5fb9ea5154babf5adb40cc94341ba690ded968887512dbabbe70` | PASSED | True |
| 13 | `orc07-c9fa6b114dfcfa7af3653cd54f35a64c5e84d1a2` | `pub_b74ec97f5090999d3055ecc09ddf5c17b75e747a60baf17bee6527c50f861d10` | 393,644 | 110,057 B | `b89002f8e2998402fe5b82456b6ac81c5e209116482532fe6e129bb9a51ea679` | PASSED | True |
| 14 | `orc07-d954963fdd926abdc5e7807631f2e24f8112ff70` | `pub_d3209fc6d0d895d58b2bd3d5d8f6a805ebc9ec86bcc6bcf672dd5fe9b8b0baa7` | 152,511 | 116,554 B | `17abfc83ccf5e2145f33fdda5771983796db1ea371d35712e91e38487bed5e90` | PASSED | True |
| 15 | `orc07-ddb6a71d10f03598aa6f8138858e522165486410` | `pub_8eee20062279b94d5edf9c557f272a596f992292bff2d2b3d3f7709f7a4c0bf6` | 78,122 | 116,543 B | `f1dfd493c965b157f21d38d2477305c965e12909170781a40c0b1ac861298e96` | PASSED | True |
| 16 | `orc07-e4e879c8df576f57961ae02989a982618213e653` | `pub_b89b5a768f9f5125e0e82099f21defc0d39c2ff0f749b94b4d64ab646b673384` | 782,116 | 112,549 B | `b2508eb8e333c876db6c68afcc420087068d62c96341e6674470bd6db1d285e5` | PASSED | True |
| 17 | `orc07-ec9d8788a27175295ff6a47e9b94f33a1170b6ca` | `pub_6617051a154cced0910b6fab5582e7b975b2f0d1116d91185ab34123d134177f` | 634,669 | 111,357 B | `da777abce9ca343fc4d03b66fa945e4c59510dce105383c724f667031bcbbb29` | PASSED | True |
| 18 | `orc07-fb052729d91a1922e1328e3322594b4926b724d1` | `pub_4492b74bf2f98cbe81abfd57186d46efc326a975284db5121b01a01f16662461` | 907,155 | 112,555 B | `756f060a1d8ba60ddcbacfaac1afb2f93c658f2874318139045bc537b4a6f3ae` | PASSED | True |

### Liên kết mật mã học (Cryptographic Linkage):
- **`bundle_sha256`**: Đã xác thực liên kết mật mã học: 18/18 tệp liên kết hợp lệ.
- **`verify_status`**: 18/18 tệp đạt `PASSED`.
- **Tổng số bản ghi tích lũy qua các snapshot/run:** **10,012,513 bản ghi** (Lưu ý: đây là tổng số bản ghi tích lũy qua 18 snapshot/partition tháng của từng run, không phải số lượng canonical events duy nhất trên toàn catalog tích hợp).

## 4. Báo cáo về 108 Snapshot References của Iceberg

- Trong 18 tệp `publication.json`, mỗi tệp chứa `snapshot_bundle` trỏ đến 6 bảng Iceberg cốt lõi:
  1. `catalog_gold.event_current`
  2. `catalog_gold.event_source_bridge`
  3. `catalog_gold.dim_date`
  4. `catalog_gold.dim_region`
  5. `catalog_gold.dim_magnitude_band`
  6. `catalog_gold.dim_depth_band`
- Tổng cộng: **18 runs × 6 tables = 108 snapshots**.
- **Lưu ý nguyên tắc:** Theo đúng [runbook](../runbooks/DAT-03_LINEAGE_FRONTIER.md), snapshot IDs là metadata pointers cục bộ của catalog, không phải portable files độc lập. Các snapshots này chỉ có thể được vận chuyển hoặc import khi có kèm theo toàn bộ Iceberg metadata, manifest-list, manifest files và data Parquet files tương ứng trong pipeline tích hợp.

## 5. Báo cáo về 119 Parquet Payload References

- Danh mục yêu cầu: `119` tệp Parquet (522,659,149 bytes ~498,4 MiB).
- **Trạng thái:** **INVENTORY REGISTERED** (chưa chuyển giao trong gói metadata này; chờ differential inventory đối chiếu với shared packages).
- **Kế hoạch:** Theo chỉ đạo tại mục 3 runbook ("đây là inventory, chưa phải yêu cầu upload lại toàn bộ"), các payload này được lưu trữ sẵn sàng tại local MinIO. Khi Hoài Tâm hoàn tất đối soát SHA với các data packages đã chia sẻ trước đây và xác định phần thiếu thực sự (differential delta), ThanhTris sẽ thực hiện đóng gói và tải lên theo các lô có checkpoint.

## 6. Kiểm thử và Kiểm định Hệ thống

1. **Công cụ kiểm định `scripts/audit-dat03-lineage.py`:**
   - Cập nhật hàm `verify_gold_identity()`: phân biệt `raw_file_sha256` và `identity_payload_sha256`, kiểm tra chặt chẽ serialization contract của Jackson và tính lại SHA payload.
   - Chạy kiểm định 3 archive kết hợp (`tri-first-hop`, `tri-lineage`, `ThanhTris-dat03-frontier-metadata-20261112.zip`):
     * `roots_matched`: **75/75** (100%).
     * `silver_dataset_manifest`: **76/76** MATCHED (100%).
     * `gold_commit`: **18/18** MATCHED (100%).
     * `gold_identity`: **18/18** MATCHED (tính lại payload hash và đối soát pin).
2. **Unit tests:** Bộ unit tests tự động offline bao phủ cả positive test và negative tests (tampered namespace, tampered scope, tampered count, duplicate JSON keys).

## 7. Giới hạn, Rủi ro và Bước tiếp theo

- **Giới hạn:** Gói bàn giao này chỉ bao gồm phần metadata frontier cấp thiết theo yêu cầu của Hoài Tâm. Gói này chưa bao gồm 119 payloads Parquet lớn (chờ differential inventory) và chưa tạo ra unified Gold catalog.
- **Trạng thái:** DAT-03 vẫn giữ trạng thái **In Progress** theo đúng chỉ đạo trong runbook.
- **Bước tiếp theo:**
  1. Hoài Tâm chạy đối soát offline `audit-dat03-lineage.py` trên gói `ThanhTris-dat03-frontier-metadata-20261112.zip` từ Drive.
  2. Cập nhật mã băm của 18 tệp `publication.json` vào trusted requirements.
  3. Hoài Tâm đối soát 119 payload references với các archive data cũ để xác định delta cần tải bổ sung.
