# DAT-03 — Bổ sung 47 Tệp Metadata Nguyên bản Follow-up của rosy179 (14/11/2026)

Tác giả: **rosy179** (@rosy179).
Đáp ứng yêu cầu bổ sung metadata nguyên bản từ Hoài Tâm (@HoaiTam) trong PR #131 và cấu hình [dat03_lineage_rosy179_followup_20261114.json](../../config/research/dat03_lineage_rosy179_followup_20261114.json).

Đây là bằng chứng lịch sử bàn giao metadata cho Cổng 1 ngày 14/11. DAT-03
hiện **Done** theo [nghiệm thu cuối](DAT-03_FINAL_ACCEPTANCE_20261116.md); PR #134
chỉ hoàn thiện helper và regression, không thay dữ liệu hay receipt đã chốt.

---

## 1. Gói dữ liệu bàn giao trên Google Drive

Thư mục đích của worker: `data/dat03/workers/rosy179/` (Folder worker trên Google Drive nhóm).

| Tên tệp | Kích thước | SHA-256 | Mô tả |
| :--- | :--- | :--- | :--- |
| `rosy179-dat03-lineage-followup.zip` | 190.379 bytes | `517f8625ef7de46d50e81c79f22b1ad878a5820174104ee5c3daa27ce5cadc7c` | ZIP chứa 47 file metadata nguyên bản (23 identities, 1 head commit, 23 publications) và `manifest.json` |
| `rosy179-dat03-lineage-followup.zip.sha256` | 101 bytes | — | Sidecar chứa SHA-256 của ZIP ở dòng trên, không phải SHA của bản thân sidecar |

* **Trạng thái lưu trữ:** Đã đóng gói từ MinIO local, cập nhật checksum tại `staging/dat03-range-handoff-rosy179/checksums.sha256` và `staging/dat03-range-handoff-rosy179/handoff-summary.json`.

---

## 2. Đối soát 24 tệp Metadata Pinned (24/24 MATCHED)

- **Head Run ID:** `orc07-22b5e7647a3df9ed83370139e11c48048845ee8f` (tháng 2023-10).
- **Phân loại:** 23 `gold_identity` (22 parent + 1 head) và 1 `gold_commit` (head).

### A. Head Run: commit.json & identity.json
1. **commit.json:**
   - URI: `s3://japan-earthquake/silver/gold-commits/orc07-22b5e7647a3df9ed83370139e11c48048845ee8f/commit.json`
   - Kích thước: 8.232 bytes
   - Raw-file SHA-256: `07ad99e8c6ef7a54d09c03806bb23866d463869826a31087d4d7e1b3c00d1137`
   - Expected SHA-256: `07ad99e8c6ef7a54d09c03806bb23866d463869826a31087d4d7e1b3c00d1137`
   - Kết quả: **MATCHED 100%**.

2. **identity.json:**
   - URI: `s3://japan-earthquake/silver/gold-commits/orc07-22b5e7647a3df9ed83370139e11c48048845ee8f/identity.json`
   - Kích thước: 7.625 bytes
   - Raw-file SHA-256: `bfbc0351ce81084b6c376137ce74a625aeb4578b7b253ba878a8be65cf570220`
   - Identity Payload Digest (Java contract): `335021da12aeb01581b8e16f86e2fdf878ed998a473289e83b3ed1cebfa84faa`
   - Internal Declared SHA: `335021da12aeb01581b8e16f86e2fdf878ed998a473289e83b3ed1cebfa84faa`
   - Expected Payload SHA: `335021da12aeb01581b8e16f86e2fdf878ed998a473289e83b3ed1cebfa84faa`
   - Kết quả: **MATCHED 100%**.

### B. 22 Parent identity.json (22/22 MATCHED)
Tất cả 22 tệp identity cha được trích xuất từ MinIO local, tách bỏ trường `identity_sha256` và tính toán lại payload digest theo chuẩn `GoldIcebergWriter.java`:

| STT | Run ID | Size (B) | Recomputed Payload SHA-256 | Expected Pin SHA-256 | Trạng thái |
|:---:|:---|---:|:---|:---|:---:|
| 1 | `orc07-0f87aad8b4ca1841cde6fe131eb17844ff848b7a` | 7.625 | `c107af920ad7d17ad081f734fecaa5665d05da73942ccc07576bb79a7e3ef944` | `c107af920ad7d17ad081f734fecaa5665d05da73942ccc07576bb79a7e3ef944` | MATCHED |
| 2 | `orc07-1afeb8c3aa73a9f0e2ee03a5f028596edc454ea5` | 7.635 | `c1df05800eadab24407e9163ec8e9c31a40e6fd460f67459569d64241686512d` | `c1df05800eadab24407e9163ec8e9c31a40e6fd460f67459569d64241686512d` | MATCHED |
| 3 | `orc07-2e17ab4d8060235aabf7412a0ebb109bbdaa97f6` | 7.625 | `60af017627a1207751809d7d27b583256919aefd43bef563628a3018cf84b736` | `60af017627a1207751809d7d27b583256919aefd43bef563628a3018cf84b736` | MATCHED |
| 4 | `orc07-3963e6a5e322185df68fb64b09807ea934efb059` | 7.624 | `a80bf02ce84943025819dd65d92077b9348077204fca3e13a940fe4d60de032d` | `a80bf02ce84943025819dd65d92077b9348077204fca3e13a940fe4d60de032d` | MATCHED |
| 5 | `orc07-5a56c90a6a45e7cc83a1b412b1314e43ae8e5df5` | 7.614 | `773262565b87ec780a41ba008e154d91b7f77bcad40d2e74e6b020c9de0b6c64` | `773262565b87ec780a41ba008e154d91b7f77bcad40d2e74e6b020c9de0b6c64` | MATCHED |
| 6 | `orc07-5f6634681f7964e2edbf1b4a47888f02587bc6d8` | 7.497 | `21e945c96faeb0489e5b054266838046bf228ee12842d566702cd5e1976bd4e4` | `21e945c96faeb0489e5b054266838046bf228ee12842d566702cd5e1976bd4e4` | MATCHED |
| 7 | `orc07-729094b2006decd4cc2929726b675b4548299dac` | 7.625 | `ac7ec5531761001fc31061c19e14d56ac083ab3926fbf3fafd7efd15b9280006` | `ac7ec5531761001fc31061c19e14d56ac083ab3926fbf3fafd7efd15b9280006` | MATCHED |
| 8 | `orc07-77c7f572ba474bfd67e407048501690ea6a0c4ec` | 7.635 | `59c35371e3d4b44b2cd1960e3e70dad9e6637e279cb534c6a900029f7da52fdc` | `59c35371e3d4b44b2cd1960e3e70dad9e6637e279cb534c6a900029f7da52fdc` | MATCHED |
| 9 | `orc07-77f499538541bddd8e01c5a9fd050fb8370796ab` | 7.625 | `484683d7cd49333c9171cc6ac9be9c4388e3f2b07cfc5a779d10242f45be12d8` | `484683d7cd49333c9171cc6ac9be9c4388e3f2b07cfc5a779d10242f45be12d8` | MATCHED |
| 10 | `orc07-7bb7f5dd6c0343856dec22c77b7b0c9381583976` | 7.625 | `aca351e6d855b4556a20a46ede393e6e5dc5c36b0b81780bc6702bc1c5696d72` | `aca351e6d855b4556a20a46ede393e6e5dc5c36b0b81780bc6702bc1c5696d72` | MATCHED |
| 11 | `orc07-88ca2c31b5642a360ab704e22842dde969223902` | 7.625 | `69174751bf9a7ce523968b1380c712b4376858ebeab0ab0a19d79896334ac5ec` | `69174751bf9a7ce523968b1380c712b4376858ebeab0ab0a19d79896334ac5ec` | MATCHED |
| 12 | `orc07-91128054466629e71699527e14717be2fb068b11` | 7.625 | `7b3481d4d42b0fb4b1a3eab5644bf39c67f008442dc046c909733f97643412ef` | `7b3481d4d42b0fb4b1a3eab5644bf39c67f008442dc046c909733f97643412ef` | MATCHED |
| 13 | `orc07-9205e79ce793864082fe05235fe7716bb19d2726` | 7.625 | `be43b4684ea63b80a4bd322e078bb7771d35713a074fa83b87e30b17955830eb` | `be43b4684ea63b80a4bd322e078bb7771d35713a074fa83b87e30b17955830eb` | MATCHED |
| 14 | `orc07-923b4a3c11914704618ea793479520a1f7949f2a` | 7.625 | `bb3ac214deb3dbf871b4cce13c4a18b11394e33a9e442a502a18a2362238bdd3` | `bb3ac214deb3dbf871b4cce13c4a18b11394e33a9e442a502a18a2362238bdd3` | MATCHED |
| 15 | `orc07-999c65a30355df0ebedb27c47b866eac6e34a5d4` | 7.615 | `7d55273dde64b29595542dc2e36e7e45b01eba7c0830ae4c9c1baadc7e09143e` | `7d55273dde64b29595542dc2e36e7e45b01eba7c0830ae4c9c1baadc7e09143e` | MATCHED |
| 16 | `orc07-d015477d95e71434be37f54ad3dbaba08d06b2a4` | 7.625 | `544a1518580dd36380737550a7bb83a0509c737eb1922c8a006e1b628c7f0013` | `544a1518580dd36380737550a7bb83a0509c737eb1922c8a006e1b628c7f0013` | MATCHED |
| 17 | `orc07-d2b7e004aaa0294b6605046ad91b6a50a1ae30a9` | 7.624 | `5826c93fdb0b167556a85949eb6b706a023d4ca164a250b16148c383102e2c4b` | `5826c93fdb0b167556a85949eb6b706a023d4ca164a250b16148c383102e2c4b` | MATCHED |
| 18 | `orc07-dbf9d836daa31a55127ad1325cfecc79fa533e1b` | 7.625 | `4e36d96f263f70fc7fc8e03d01c31e91ab31611202046f0bc54064c113e3cc8e` | `4e36d96f263f70fc7fc8e03d01c31e91ab31611202046f0bc54064c113e3cc8e` | MATCHED |
| 19 | `orc07-de8370dfe09b068c4fa13df76756e44232eecb16` | 7.615 | `607337413a0f5d31b819a8f206cd5e7a92a2ec3088a22d1daa24f616c0e5bda5` | `607337413a0f5d31b819a8f206cd5e7a92a2ec3088a22d1daa24f616c0e5bda5` | MATCHED |
| 20 | `orc07-ecf3820c1d74aec97bbbf32c195ef3ebc4fe076c` | 7.625 | `95372dacc017ffcdc8a5ea25e58f52a15a307672bdcb355075be914186224018` | `95372dacc017ffcdc8a5ea25e58f52a15a307672bdcb355075be914186224018` | MATCHED |
| 21 | `orc07-ed8f8eb8179a5331b93b57c25afcfbdf64a9fd6a` | 7.625 | `ea6124d9a7dbbaa272e1df12e3c91386886682c929c47903ec58c3493e2a44fd` | `ea6124d9a7dbbaa272e1df12e3c91386886682c929c47903ec58c3493e2a44fd` | MATCHED |
| 22 | `orc07-fe87d2edce1aa09024dd03d7c88ecfdefca1605c` | 7.615 | `4bdf3ac30237cfcdad6a4d3600f580163698b0528c9f2aab95472a523e25ab8a` | `4bdf3ac30237cfcdad6a4d3600f580163698b0528c9f2aab95472a523e25ab8a` | MATCHED |

---

## 3. Kiểm 23 tệp `publication.json` và phạm vi xác minh

Toàn bộ 23 tệp `publication.json` được trích xuất từ `s3://japan-earthquake/silver/gold-verification/<run_id>/publication.json`:
- Cả 23 object ghi `verify_status: "PASSED"` và `published: true`. Helper kiểm
  exact paths, bytes/manifest SHA và các liên kết identity/commit có pin; kết quả
  cho publication là `PACKAGE_SELF_CHECK_ONLY`, không phải chứng nhận nguồn độc lập.
- Đối soát publication với các phase JSON đã pin và liên kết đủ sáu snapshot
  thuộc [verifier độc lập](../../scripts/verify-dat03-followup.py) và
  [nghiệm thu coordinator](DAT-03_FINAL_ACCEPTANCE_20261116.md). Kết quả của helper
  không thay thế kiểm tra đó hoặc xác minh SQL live.

---

## 4. Cấu trúc tệp bên trong ZIP bàn giao

File `rosy179-dat03-lineage-followup.zip` giữ nguyên cấu trúc đường dẫn tương đối S3:
```text
rosy179-dat03-lineage-followup.zip
├── manifest.json
├── silver/gold-commits/
│   ├── orc07-0f87aad8b4ca1841cde6fe131eb17844ff848b7a/identity.json
│   ├── ... (21 parent runs)
│   ├── orc07-fe87d2edce1aa09024dd03d7c88ecfdefca1605c/identity.json
│   └── orc07-22b5e7647a3df9ed83370139e11c48048845ee8f/
│       ├── commit.json
│       └── identity.json
└── silver/gold-verification/
    ├── orc07-0f87aad8b4ca1841cde6fe131eb17844ff848b7a/publication.json
    ├── ... (21 parent runs)
    ├── orc07-fe87d2edce1aa09024dd03d7c88ecfdefca1605c/publication.json
    └── orc07-22b5e7647a3df9ed83370139e11c48048845ee8f/publication.json
```

---

## 5. Kiểm thử Tự động và Công cụ Đóng gói

[Helper](../../scripts/pack-dat03-rosy179-followup.py) kiểm đúng 48 member duy nhất
(manifest + 47 object), giới hạn size trước khi đọc payload, kiểm checksum và từ
chối symlink/member không hợp lệ. Manifest cũ `LEGACY_MINIMAL` vẫn được đọc và
phải khớp toàn bộ dữ liệu/tham chiếu. CLI mặc định dùng source local và không
fallback sang mạng; `--s3` là opt-in và cần một bộ credential đầy đủ.

```bash
make test-dat03-rosy179-followup
python3 -B scripts/pack-dat03-rosy179-followup.py --verify
```

Lệnh đầu chạy 33 fixture tests offline (33 đạt trên macOS, không skip) và được
`ci-docs-base` gọi. Lệnh thứ hai chỉ đọc ZIP có sẵn tại đường dẫn staging mặc
định; có thể chỉ bản sao có sẵn khác bằng `--output-zip`. Bản sao readback đã
được kiểm trong PR #134: SHA ZIP không đổi, 24 pinned + 23 publication đạt trong
phạm vi helper. Không tải mới, ghi đè hoặc upload lại.

Khi pack, ZIP và `.sha256` trùng bytes được reuse, khác bytes mặc định bị từ
chối. `--overwrite` là opt-in trên cặp hợp lệ; test chứng minh khôi phục ZIP
cũ khi lỗi ghi sidecar. Hai đường dẫn không phải một giao dịch filesystem;
nếu bị ngắt đột ngột hoặc lock còn giữ, giữ nguyên file và đối chiếu cặp trước
khi chạy lại. Với bản release đã ghim, dùng tên mới nếu cần đóng gói lại.

---

## 6. Kết luận và Bước tiếp theo của DAT-03

- **Bàn giao phía rosy179:** 83 first-hop provenance, 138 supplemental phase
  journals, 114 frontier metadata và 47 follow-up objects. Helper #134 chỉ
  đóng gói và tự kiểm tập 47 object cuối, không chứng nhận toàn bộ lineage.
- **Tiến độ DAT-03:** [Nghiệm thu cuối](DAT-03_FINAL_ACCEPTANCE_20261116.md) đã
  đóng task. Không yêu cầu chạy lại ETL, export hay tạo lại receipt cho PR này.
