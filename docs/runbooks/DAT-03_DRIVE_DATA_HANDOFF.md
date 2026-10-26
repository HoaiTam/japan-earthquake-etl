# DAT-03 — Dữ liệu đã có trên Drive nhóm, 26/10/2026

## Bổ sung: Parquet đã xử lý101tháng

[ZIP101tháng](https://drive.google.com/file/d/1wLnASLdSnnis91wGApcRnfFCh2ASQQUd/view)
đã upload và fresh download/semantic verifier đạt. Có155Parquet,
1.064.869observations/memberships mỗi bảng, phủ2000-01–2008-05;
giữ EXPLORATORY_ONLY/research_ready=false, không chứa sáu bảng Gold/ML features
hoặc backup warehouse. [Pins/cách dùng/cleanup](./DAT-03_PARTIAL_CATALOG_HANDOFF.md).
Đã dọn readback-source/readback-pilot tạm của phiên đầu cùng ZIP/readback mới;
nguồn gốc/frozen inputs/CAS/warehouse vẫn giữ. Bảng dưới là hai release đầu.

## Tải bộ nào?

| Bộ | Đường dẫn | Mục đích / giới hạn |
|---|---|---|
| Pilot JMA | [ZIP 55.408 bytes](https://drive.google.com/file/d/12eLvQJzEkBuT0WYaxHYvb9KZlk5MK_vb/view) · [folder và README](https://drive.google.com/drive/folders/1b7m-DE6f3OcM4Qm8wQf46U0T8O2whLQb) | 279 candidate/features, 2 mainshock; DEVELOPMENT_ONLY/PARTIAL, không phải bộ nghiên cứu chính thức |
| Nguồn JMA | [Folder 25 ZIP, 128.784.513 bytes](https://drive.google.com/drive/folders/1YWUGWzuC8K1VKEzZj-6sk6OUV2vASAP6) | Giữ bytes nguồn 2000–2024; 2000–2023 dùng chuẩn bị nghiên cứu, 2024 chỉ boundary/halo cuối2023 |

Owner đã duyệt upload cả hai bộ. Transfer dùng allowlist và immutable copy;
đã đối chiếu remote bằng content download, tải xuống thư mục độc lập rồi chạy
verifier nguồn và semantic verifier pilot. [Evidence](../evidence/DAT-03_DRIVE_UPLOAD_20261026.json).
Không public link, đổi ACL, xóa local hay upload toàn bộ staging/warehouse.

Đây **không phải** output catalog101tháng, BronzeReady, Gold Published hoặc
official ML EXPORTED. Các cờ research_ready/coverage_verified/Bronze/Gold của
bộ nguồn giữ false; pilot cũng research_ready=false. Upload không thay đổi
tiến độ [DAT-03 — Dataset nghiên cứu dùng chung](../task/tasks/DAT-03.md).

## Pins gửi riêng qua kênh nhóm tin cậy

| Artifact | SHA256 |
|---|---|
| Source manifest.json | `79a44764037f6357be1f8a9c95183958f7a1879b6146415c338cf5b27a0d701b` |
| Source verify_sources.py | `aa0507323f916f59b4de7e4902de8e0bf0e5cda9498a660f8abc0f7f1664c203` |
| Pilot ZIP | `f58291b72873e1b26791fef97818fdcf5e2b028a5dfa528885a666d8cdc1b870` |
| Pilot handoff.json trong ZIP | `9121d6839f9ef8e5646a06cd0bf870fcde50b65da00c03e7b268760237cddc9a` |

Nhận pins trực tiếp từ owner/repository đã tin cậy; không chỉ tin checksum
nằm cùng file tải xuống. Hash kiểm bytes theo pin, không là chữ ký JMA hoặc
scientific approval. Nguồn gốc, thời gian capture và từng SHA có trong manifest.

## Máy thành viên: kiểm bộ nguồn, không chạy ETL

Python3.11+ standard library; không cần Docker/Java/.env. Tải **toàn bộ**
folder release vào thư mục mới, giữ cấu trúc sources/hYYYY/hYYYY.zip. Không đặt
output/venv/receipt trong input vì verifier yêu cầu exact file set.

Nếu đã setup remote theo [Drive setup](./DAT-03_GROUP_DRIVE.md), từ thư mục
làm việc mới có thể tải chỉ đúng release (không sync/move/delete):

```bash
rclone copy dat03_team:data/dat03/shared-inputs/jma-sources-2000-2024/79a44764037f ./jma-sources-2000-2024 --immutable --checksum
shasum -a 256 ./jma-sources-2000-2024/manifest.json ./jma-sources-2000-2024/verify_sources.py
```

**Dừng nếu hai SHA không khớp pins ở trên, trước khi thực thi code tải về.**
Windows dùng Get-FileHash -Algorithm SHA256. Sau khi khớp:

```bash
python3 -B ./jma-sources-2000-2024/verify_sources.py ./jma-sources-2000-2024 --manifest-sha256 79a44764037f6357be1f8a9c95183958f7a1879b6146415c338cf5b27a0d701b
```

Thành công: PINNED_JMA_SOURCES_VERIFIED, archives=25,
compressed_archive_bytes=128784513 và các readiness flags=false. Kiểm này
đọc SHA/ZIP member/CRC, không parse thành Silver, chạy Spark hoặc chứng nhận
đủ coverage. Người nhận gửi stdout cùng tên người nhận và pin về nhóm.
Không sửa manifest hoặc tự tạo BronzeReady để vượt gate.

## Máy thành viên: kiểm pilot trước khi dùng Jupyter

Tải ZIP cùng README/checksums/release.json từ folder pilot vào thư mục mới,
hoặc dùng remote đã setup:

```bash
rclone copy dat03_team:data/dat03/shared-inputs/development-pilot/f58291b72873 ./jma-pilot-download --immutable --checksum
shasum -a 256 ./jma-pilot-download/jma-development-pilot-team.zip
```

**ZIP SHA phải khớp pin trước khi giải nén/thực thi code.** Giải nén vào một
thư mục mới khác, đọc [hướng dẫn verifier pilot](./DAT-03_SHARED_DATASET.md#người-nhận-làm-gì).
Chạy verify_dataset.py với external handoff pin ở trên và đúng recipient
ThanhTris hoặc rosy179; lưu receipt ngoài kit. Expected:
TEAM_PILOT_VERIFIED, feature_count=279, mainshock_count=2, PARTIAL,
DEVELOPMENT_ONLY, research_ready=false.

HoaiTam đã kiểm fresh download trên máy chuẩn bị. Hai thành viên phải chạy
thật trên máy riêng rồi gửi receipt; agent không tạo hộ. Nếu thiếu quyền,
báo owner cấp đúng tài khoản; không mở anyone-with-link hoặc copy OAuth token.

## Tình trạng bảo mật và bước tiếp theo

Exact35objects của hai release (2folder+33files) đều có3user permissions
(owner1/writer2), không anyone/domain;33files cho phép download với account
đang audit. Đây là scoped permission check, chưa map email với roster hoặc
chứng minh hai máy khác đã tải được. Không audit mọi descendant của Drive.

- **SEC-01 — Review secret và bề mặt truy cập:** Needs Update cho recipient/
  identity acceptance; local23/10 vẫn là lịch sử đã đạt. Không mở Colab.
- **DAT-03 — Dataset nghiên cứu dùng chung:** In Progress; còn full288tháng,
  official collection export và receipts nghiên cứu ba máy. Pilot/source
  không thay các gate này.
- Range worker/portable batch export/import/merge chưa có acceptance; không
  dùng bộ nguồn trên Drive như warehouse sống hoặc giao chạy187tháng ngay.
- Giữ nguyên originals và readback local; chưa dọn bất kỳ data nào ở phiên
  upload này. Handoff thành công không tự cấp quyền xóa dữ liệu ETL.
