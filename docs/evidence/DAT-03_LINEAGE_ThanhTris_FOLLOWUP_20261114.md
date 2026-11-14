# DAT-03 — Bổ sung 3 file Metadata Nguyên bản Head Run của ThanhTris (14/11/2026)

Tác giả: **ThanhTris** (@ThanhTris).
Đáp ứng yêu cầu bổ sung metadata nguyên bản từ Hoài Tâm (@HoaiTam) trong PR #131 và cấu hình [dat03_lineage_ThanhTris_followup_20261114.json](../../config/research/dat03_lineage_ThanhTris_followup_20261114.json).

Task DAT-03 vẫn giữ trạng thái **In Progress**; việc bàn giao này nhằm cung cấp metadata phục vụ đối soát, chưa đồng nghĩa DAT-03 Done.

---

## 1. Gói dữ liệu bàn giao trên Google Drive

Thư mục đích của worker: `data/dat03/workers/ThanhTris/` (Folder ID: `1bcRDZ5feOeos_U8HcsfOQZ2ELPoOGuDg`).

| Tên tệp | Drive File ID | Kích thước | SHA-256 | Mô tả |
| :--- | :--- | :--- | :--- | :--- |
| `ThanhTris-dat03-lineage-followup.zip` | `1kKsPu9cMf_dFP-G73s1oeF6Fta-vTkSp` | 10,540 bytes | `0118968a5cfbecc27945c4fcdd970a467078280861855b57984e921684d353ed` | ZIP chứa 3 file metadata nguyên bản và manifest.json |
| `ThanhTris-dat03-lineage-followup.zip.sha256` | `1sbUyXXTQxUuvcLSJL-MfMNn2SmPwREgV` | 103 bytes | `53fe1db529c8789d660233633d45ef420b925b682337a6b2518e268677c7f1fe` | Checksum SHA-256 của file ZIP |

* **Link tải ZIP:** [ThanhTris-dat03-lineage-followup.zip](https://drive.google.com/file/d/1kKsPu9cMf_dFP-G73s1oeF6Fta-vTkSp/view?usp=drivesdk)
* **Link checksum:** [ThanhTris-dat03-lineage-followup.zip.sha256](https://drive.google.com/file/d/1sbUyXXTQxUuvcLSJL-MfMNn2SmPwREgV/view?usp=drivesdk)

---

## 2. Đối soát 3 tệp metadata nguyên bản từ MinIO

* **Current head run ID:** `orc07-5679c9c378edacb4e5f804613c8475351ce326b4`
* **Nguồn trích xuất:** MinIO local `s3://japan-earthquake/` (không chạy lại ETL, không sửa đổi hay serialize lại JSON).

### A. commit.json
* **URI:** `s3://japan-earthquake/silver/gold-commits/orc07-5679c9c378edacb4e5f804613c8475351ce326b4/commit.json`
* **Kích thước:** 8,233 bytes
* **Raw-file SHA-256:** `cfbc547badf4807c2530d2960a0cb3988d21a82913f299113787e4dc61da5e72`
* **Expected SHA-256:** `cfbc547badf4807c2530d2960a0cb3988d21a82913f299113787e4dc61da5e72`
* **Kết quả:** **VERIFIED MATCH** (khớp 100% với expected SHA đã pin trong PR #131).

### B. identity.json
* **URI:** `s3://japan-earthquake/silver/gold-commits/orc07-5679c9c378edacb4e5f804613c8475351ce326b4/identity.json`
* **Kích thước:** 7,605 bytes
* **Raw-file SHA-256:** `787812712a3f163f0b164f3e1107e58b3d34433f53bdcd27ea6eb6a352568a5a`
* **Identity payload digest (Java contract):** `140c5ba2919d1c519faaecc25ddaaae6d7e94b1c7f564cf63613e3824572b3fd`
* **Expected payload digest:** `140c5ba2919d1c519faaecc25ddaaae6d7e94b1c7f564cf63613e3824572b3fd`
* **Kết quả:** **VERIFIED MATCH** (Payload digest khớp 100% với pinned digest; raw-file SHA được cung cấp riêng biệt).

### C. publication.json
* **URI:** `s3://japan-earthquake/silver/gold-verification/orc07-5679c9c378edacb4e5f804613c8475351ce326b4/publication.json`
* **Kích thước:** 109,860 bytes
* **Raw-file SHA-256:** `00789cbdafffca95abd0a57c8137c101a1b142e16803b14ad24c9c7d2ed67b4d`
* **Trạng thái xác minh:**
  * `verify_status`: `PASSED`
  * `published`: `true`
  * `publication_id`: `pub_cfbc547badf4807c2530d2960a0cb3988d21a82913f299113787e4dc61da5e72`
  * `bundle_sha256`: `cfbc547badf4807c2530d2960a0cb3988d21a82913f299113787e4dc61da5e72`
  * `identity_sha256`: `140c5ba2919d1c519faaecc25ddaaae6d7e94b1c7f564cf63613e3824572b3fd`
  * Bảng snapshots ghi nhận trong publication:
    * `event_current`: Snapshot ID `384623706548840615` (1,122,941 rows)
    * `event_source_bridge`: Snapshot ID `408236325069611033` (1,122,941 rows)
    * `dim_date`: Snapshot ID `4337183067691343816` (1,888 rows)
    * `dim_region`: Snapshot ID `1183779034061335840` (1 row)
    * `dim_magnitude_band`: Snapshot ID `6198756060092499354` (7 rows)
    * `dim_depth_band`: Snapshot ID `2653556059144864682` (5 rows)
  * Khớp toàn bộ 6 snapshot references được pin trong `dat03_lineage_ThanhTris_followup_20261114.json`.

---

## 3. Cấu trúc tệp bên trong ZIP

Tệp ZIP giữ nguyên đường dẫn tương đối theo chuẩn S3:
```text
ThanhTris-dat03-lineage-followup.zip
├── manifest.json
├── silver/gold-commits/orc07-5679c9c378edacb4e5f804613c8475351ce326b4/
│   ├── commit.json
│   └── identity.json
└── silver/gold-verification/orc07-5679c9c378edacb4e5f804613c8475351ce326b4/
    └── publication.json
```
