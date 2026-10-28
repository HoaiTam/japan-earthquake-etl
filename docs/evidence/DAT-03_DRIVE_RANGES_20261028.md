# DAT-03 — Bàn giao thêm 33 tháng lên Drive, 28/10/2026

Owner duyệt đóng gói 24 + 9 tháng đã accepted, upload, fresh download verify,
cleanup bản tạm. Không chạy thêm ETL, commit/push hoặc tạo automation.

| UTC tháng inclusive | Rows mỗi bảng observation/membership | ZIP bytes | Folder |
|---|---:|---:|---|
| 2008-06–2010-05 | 278509 | 59850079 | [24 tháng](https://drive.google.com/drive/folders/1wybhRVRZjWW28vtf8Z80r0K8en2b6stP) |
| 2010-06–2011-02 | 102540 | 22014159 | [9 tháng](https://drive.google.com/drive/folders/1jSCWac54SLj6uASWQyMkqI7km7NAEgFR) |

## Trusted pins

- ZIP 24 tháng: `cef8e3522dcc8ab651a65372aa260574794dd559ed7cbaa6d5ecea76a077cdf5`.
- Manifest 24 tháng: `4ef5084d7efd4f77b622882994dd3ae771654a3f0d336a37a7a96a8f6a0d3310`.
- ZIP 9 tháng: `d3abc28ec4fa13b420b937b507af5756a8bfdf9f500e2c4b49b08f90d7d99660`.
- Manifest 9 tháng: `c459d01ba4dad8c40607affe50569d9143069d1a59587eb35822a718b2329303`.
- Standalone verifier cả hai: `b7b2a682e428bef4fee8228f430e62ff08d399626ca2132a8f9e0e89afbe9bb9`.

Remote dưới `dat03_team:data/dat03/shared-inputs/`:
`partial-200806-201005/cef8e3522dcc` và `partial-201006-201102/d3abc28ec4fa`.
Mỗi folder có ZIP, README, requirements, verifier, release.json, checksums.
Python 3.11–3.13, pyarrow21.0.0; đối chiếu trusted ZIP/verifier SHA trước chạy:

```bash
python3 -B verify_catalog.py jma-processed-200806-201005.zip --zip-sha256 cef8e3522dcc8ab651a65372aa260574794dd559ed7cbaa6d5ecea76a077cdf5
python3 -B verify_catalog.py jma-processed-201006-201102.zip --zip-sha256 d3abc28ec4fa13b420b937b507af5756a8bfdf9f500e2c4b49b08f90d7d99660
```

## Verification và giới hạn

Unchanged Spark Parquet, whole accepted batches; không filter/rewrite events.
Parent host observation `runtime-observation-83e684d123f040ab8bf68350578568a6`,
manifest SHA `7ee6dd6f8000464e2ba609bd841e66ce04ddc75f6ed429a3b06f135c4d2d1263`:
actual reporter accepted134/288 đến2011-02,1.445.918canonical/bridge, own lease
RELEASED. Hai gói thêm381.049rows mỗi bảng; không overlap release101 cũ.

Local semantic + secret scan đạt; 12 verifier regression tests PASS.
Immutable upload, rclone check --download, fresh independent local download,
exact file/SHA, all ZIP member hashes, per-month counts, unique observation/
canonical IDs và membership join đều PASS. source_link=0. Owner kiểm trên
máy chuẩn bị; không giả receipt máy ThanhTris/rosy179.

Đầu phiên phát hiện anyone/writer kế thừa từ data/dat03. Owner duyệt gỡ public
tại cha; thao tác DELETE thành công, lần kiểm ngay sau còn propagation delay,
readback sau xác nhận data/dat03, workers/HoaiTam, shared-inputs chỉ ba user
(owner + hai writer). Không thay quyền user. Hai release folders và 12 files
đều ba user, không anyone/domain, download được bởi account owner. Không audit
mọi file cũ hoặc tự chứng nhận identity của hai người nhận.

Cleanup sau remote/semantic checks:16exact temporary files,245.618.383bytes
logical (~234,2MiB). Physical free quan sát14.993.346.560→15.241.854.976bytes;
delta248.508.416bytes chịu OS/APFS activity, không gọi là net saving từ đầu
task vì đây chủ yếu copies mới tạo. Có thể tải lại từ Drive. Giữ source,
frozen inputs, CAS, checkpoints, warehouse và failure evidence; không prune.

**DAT-03 vẫn In Progress:** Drive có partial processed coverage134tháng,
46,528% catalog; chưa full288/official ML export/receipts ba máy.
EXPLORATORY_ONLY/research_ready=false/official_exported=false; không phải
Gold table export hoặc warehouse backup. **SEC-01** còn recipient identity/
download acceptance và Colab audit khi thực dùng. Không tự cấp scientific approval.
