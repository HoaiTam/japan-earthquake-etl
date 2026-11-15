# DAT-03 — Nhận hai collection nghiên cứu chính thức, 16/11/2026

Đây là **official feature export**, không phải pilot 279 dòng hoặc ZIP catalog
partial. Cả hai split đã EXPORTED, rerun bất biến và all-shard loader đạt;
13/13 Trino readbacks độc lập khớp counts, grain, Mc, registry và Gold pins.
[Evidence và các SHA](../evidence/DAT-03_OFFICIAL_COLLECTIONS_20261115.md).

## Bộ dữ liệu nào?

| Collection | Khoảng nghiên cứu UTC | Tháng | Mainshock | Feature memberships | Shard | Global Mc |
|---|---|---:|---:|---:|---:|---:|
| REPRODUCTION | 2000-01-01 đến trước 2018-10-01 | 225 | 194 | 156.710 | 2 | 0,5 |
| EXTENSION | 2018-10-01 đến trước 2024-01-01 | 63 | 45 | 233.664 | 4 | 0,4 |

Tổng 390.374 dòng là **candidate memberships của 239 mainshock**, không phải
390.374 động đất khác nhau. Một event có thể thuộc nhiều cửa sổ. Gold nền có
4.488.650 canonical events, pin `7580315846938041321` từ publication
`pub_832ce36ac875f546528718c3545442a77a0097f7573379f30ed92f2a43c481f4`.
Giữ nguyên window, Mc và vector/scales; không clip window, sampling hoặc trộn
hai split. Các mainshock thiếu coverage bị loại nguyên window, có 6/3 reason
records trong manifests. Profile 90k chỉ thay giới hạn tài nguyên đã đo/duyệt.

## Release và external pins

- ZIP: `jma-research-team-20261115-v4.zip`, **37.524.315 bytes**, 27 files.
- ZIP SHA256: `687170587e9db1d5d767722bf2fd4e1e2eecc1c3141ebfcdb10269a8b83b4c21`.
- `handoff.json` SHA256: `6cf0af7c2a3d8e8ec4c993f84ed6d86cdc417324102133dd3068eb2ebc40d297`.
- Bản local owner: `staging/dat03-windows-fix-20261115/jma-research-team-20261115-v4.zip`.
- [Tải ZIP v4 trên Drive nhóm](https://drive.google.com/file/d/1lSzNuYXt-mcTdci9nHNyoo3jdDVvRogj/view),
  [folder release bất biến](https://drive.google.com/drive/folders/1LJJBpc7FjNQga_Ur-Kx3Z8AOPmJg-Req).
- V4 đã đóng gói/rerun và fresh-process verifier PASS trên macOS. [Native Windows/Linux fixture CI](https://github.com/HoaiTam/japan-earthquake-etl/actions/runs/43741683342) PASS trên commit `ed655bd`.
- Upload immutable + exact4files/size/remote SHA + fresh Drive download/restore/verifier
  PASS trên máy HoaiTam. Không đổi ACL hoặc overwrite release v2.
- Không dùng ZIPv2 để nghiệm thu Windows: ThanhTris đã báo lỗi POSIX directory index và PyArrow timezone database. Giữ ZIPv2 làm lịch sử, không sửa kit cũ bằng tay.

Giữ hai SHA ở ngoài ZIP, nhận từ HoaiTam/revision repo được tin cậy. V4 sửa metadata directory names bằng PurePosixPath và UTC epoch integer cast,
không cần tải C++ timezone database vào Downloads/tzdata cho frozen UTC writer.
Khi cài requirements, Windows tự cài **Python tzdata==2025.2** cho zoneinfo khi
Arrow chuyển timestamp sang Python datetime. Phải cài lại requirements của v4;
không tái dùng venv thiếu dependency từ kit cũ. Candidate v3 chưa phát hành vì
native CI đã phát hiện thiếu Python zoneinfo data. Byte-compare20/20 file
collection với v2 giữ nguyên dataset IDs/manifest/checksum/feature bytes.
[Evidence Windows fix](../evidence/DAT-03_WINDOWS_HANDOFF_20261116.md). Kit không có source ZIP, warehouse,
private logs, `.env`, OAuth JSON/token hoặc venv.

## Máy ThanhTris và rosy179: không cần Docker hoặc Java

Yêu cầu Python 3.11–3.13, khoảng 1 GiB dung lượng trống cho ZIP/giải nén/venv.
Đây là mức để **nhận/verify**, không là capacity chứng nhận cho model fit.

1. Tải đúng ZIP vào thư mục mới. Kiểm SHA **trước** giải nén hoặc chạy code.
2. Giải nén thành `research-kit`; giữ nguyên file set. Không đặt venv, receipt,
   notebook hoặc output vào trong kit vì verifier sẽ reject file/directory thừa.
3. Đứng ở **thư mục cha của `research-kit`**, tạo venv ngoài kit và chạy verifier.

### macOS/Linux

```bash
shasum -a 256 jma-research-team-20261115-v4.zip
```

Chỉ tiếp tục khi khớp ZIP SHA ở trên:

```bash
unzip jma-research-team-20261115-v4.zip -d research-kit
python3 -m venv .venv-research
.venv-research/bin/python -m pip install -r research-kit/requirements.txt
.venv-research/bin/python -B research-kit/verify_dataset.py --expected-handoff-sha256 6cf0af7c2a3d8e8ec4c993f84ed6d86cdc417324102133dd3068eb2ebc40d297 --recipient ThanhTris --receipt ThanhTris-research-receipt.json
```

### Windows PowerShell

```powershell
Get-FileHash .\jma-research-team-20261115-v4.zip -Algorithm SHA256
```

Chỉ tiếp tục khi khớp ZIP SHA ở trên:

```powershell
Expand-Archive .\jma-research-team-20261115-v4.zip -DestinationPath .\research-kit
py -3.11 -m venv .venv-research
.\.venv-research\Scripts\python.exe -m pip install -r research-kit\requirements.txt
.\.venv-research\Scripts\python.exe -B research-kit\verify_dataset.py --expected-handoff-sha256 6cf0af7c2a3d8e8ec4c993f84ed6d86cdc417324102133dd3068eb2ebc40d297 --recipient rosy179 --receipt rosy179-research-receipt.json
```

Đổi recipient và tên receipt cho đúng **người thực sự chạy trên máy riêng**.
Không dùng `../receipt.json`: path phải canonical và ở ngoài kit. Không copy
receipt HoaiTam, chạy ba tên trên cùng máy hoặc sửa fields/hash để gọi là đạt.

## Nhận biết thành công và gửi lại gì?

Expected: `TEAM_RESEARCH_VERIFIED`, `all_shards_verified=true`; REPRODUCTION
156710/194/2 và EXTENSION 233664/45/4 (features/mainshocks/shards), đúng handoff
SHA ở trên. Verifier đọc cả sáu shard và kiểm source/code/file SHA, lineage,
schema, counts, Mc, feature math và whole-window assignments. Reader UTC/INT96
có version, chỉ tạo view trong RAM, không sửa bytes Parquet.

Gửi đúng file `ThanhTris-research-receipt.json` hoặc `rosy179-research-receipt.json`
cho HoaiTam qua folder nhóm/kênh đã thống nhất; không gửi `.env`, token hoặc
OAuth JSON. Nếu fail, giữ ZIP/receipt/checkpoint và gửi mã lỗi đã che thông tin
nhạy cảm; không sửa expected SHA, tắt verifier hoặc regenerate input để pass.

## Giới hạn và task còn lại

- **[DAT-03 — Chuẩn bị catalog nghiên cứu thật và bàn giao dataset dùng chung](../task/tasks/DAT-03.md):**
  giữ In Progress đến khi đủ receipt thật của ba máy. HoaiTam đã có receipt local
  và fresh Drive restore; vẫn chỉ tính **một máy**, còn ThanhTris và rosy179.
- **[SEC-01 — Review secret và bề mặt truy cập](../task/tasks/SEC-01.md):**
  kiểm quyền/identity của người nhận trên Drive/Colab còn riêng; scoped folders
  hiện chỉ có user permissions nhưng chưa map roster. Không đổi ACL hoặc gọi
  SEC-01 Done chỉ vì upload/verify ZIP.
- **[EXP-02 — Chạy Window, DBSCAN và HDBSCAN global](../task/tasks/EXP-02.md)** và
  **[EXP-03 — Xây HDBSCAN adaptive và giải quyết multi-sequence](../task/tasks/EXP-03.md):**
  official experiment/evaluation và capacity model fit là bước riêng. Số đo
  materialize 90k không chứng nhận RAM/thời gian fit DBSCAN/HDBSCAN 90k points.
- Scientific approval/research-ready không tự bật. Không ghi nhãn ML vào Gold
  hoặc dùng kết quả như dự đoán động đất; kit này là input nghiên cứu hồi cứu.
