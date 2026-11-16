# DAT-03 — Báo cáo Đối soát Hợp nhất 288 Tháng và Provenance First-hop (05/11/2026)

## 1. Tóm tắt Hiện trạng Toàn kỳ 288 Tháng

Theo kiểm tra offline ngày 02/11/2026 và cập nhật đối soát từ Hoài Tâm, toàn bộ chuỗi dữ liệu 24 năm (**288 tháng**, từ `2000-01` đến `2023-12` UTC) đã được xử lý và kiểm tra qua 4 gói phân đoạn độc lập. Không ghi nhận tháng nào bị thiếu (gap) hoặc bị chồng lấn (overlap).

Tuy nhiên, đây là kết quả của các gói riêng lẻ trên từng máy worker/Drive, chưa phải catalog Gold hợp nhất được nghiệm thu chính thức trên lakehouse.

| Phân đoạn / Thành viên | Khoảng thời gian (UTC) | Số tháng | Trạng thái gói dữ liệu | Trạng thái Provenance First-hop |
|---|---|---:|---|---|
| Catalog nền | 2000-01–2008-05 | 101 | Đã kiểm tra gói | Lưu trữ trên coordinator ban đầu (1.064.869 canonical events) |
| HoaiTam | 2008-06–2013-07 | 62 | Đã kiểm tra các gói | Lưu trữ Drive theo các batch (24m + 9m + còn lại) |
| ThanhTris | 2013-08–2018-09 | 62 | Đã kiểm tra gói | **75/75 objects khớp 100% SHA-256** |
| rosy179 | 2018-10–2023-12 | 63 | Đã kiểm tra gói | **Đã hoàn thành 83/83 first-hop (PASS), 138 phase journals bổ sung (PACKAGED), và 114/114 objects frontier gốc (VERIFIED 100% SHA-256 trên Drive ngày 14/11; Cổng 1 tiếp tục mở chờ follow-up PR #131)** |
| **Toàn kỳ hợp nhất** | **2000-01–2023-12** | **288** | **Đủ 288/288 tháng** | **75 first-hop + 114 journals (Trí); 83 first-hop + 138 journals + 114 frontier bytes (Trang); Cổng 1 tiếp tục MỞ chờ follow-up metadata (PR #131) và đối soát Trí** |

---

## 2. Kết quả Đối soát Provenance First-hop của ThanhTris

ThanhTris đã trích xuất từ MinIO local, đóng gói ZIP kèm SHA-256 và tải lên Google Drive tại thư mục worker được phân công (`ThanhTris-dat03-provenance-metadata.zip`).

Hoài Tâm đã tải về và đối chiếu độc lập trên máy host:
- **Tỷ lệ khớp:** **75/75 objects (100%)** khớp tuyệt đối với checksums trong danh sách yêu cầu `ThanhTris-required-provenance.json`.
- **Cấu trúc phân loại 75 objects:**
  - **37 Bronze manifests** nguyên bản của từng đợt ingest JMA/USGS (`s3://japan-earthquake/bronze/jma/year=*/.../manifest.json`).
  - **19 Silver bundle manifests** (`s3://japan-earthquake/silver/jma/year=*/.../manifest.json`).
  - **19 registry JSON** xác nhận snapshot metadata.
- **Kết luận:** Hoàn thành toàn bộ first-hop provenance phần việc của ThanhTris; không yêu cầu gửi lại 75 file này.

---

## 2.1. Kết quả Đối soát Provenance First-hop của rosy179

rosy179 đã trích xuất từ MinIO local, đối chiếu và đóng gói thành công file ZIP provenance kèm mã băm SHA-256 (`rosy179-dat03-provenance-metadata.zip`).

- **Tỷ lệ khớp:** **83/83 objects (100%)** khớp tuyệt đối với các URIs và checksums được ghi nhận trong metadata 63 tháng.
- **Cấu trúc phân loại 83 objects:**
  - **37 Bronze manifests** (25 JMA archives h2000–h2024 và 12 captures USGS năm 2023).
  - **23 Silver bundle manifests** (`s3://japan-earthquake/silver/bundles/*/manifest.json`).
  - **23 registry JSON** (`s3://japan-earthquake/silver/full-month/*/registry.json`).
- **Thông số gói:**
  - Dung lượng: `253.284 bytes`.
  - SHA-256: `f818ba4697d3915376420ad66cd040c542853333a31bc2b4e4a5d0b4115d859e`.
  - Đã cập nhật `staging/dat03-range-handoff-rosy179/checksums.sha256` và `handoff-summary.json`.
- **Kết luận:** Hoàn thành toàn bộ việc trích xuất, xác thực và đóng gói 83/83 objects first-hop provenance cho phân đoạn 63 tháng của rosy179.

---

## 2.2. Kết quả Đóng gói Phase Journals Bổ sung (Tier 2: Supplemental Phase Evidence) của rosy179

rosy179 đã trích xuất, xác thực đối chiếu và đóng gói thành công gói phase journals bổ sung (`rosy179-dat03-lineage-metadata.zip`) gồm **138 objects** metadata tương ứng 23 runs x 6 journals giai đoạn `2018-10..2023-12`:

> [!NOTE]
> Gói 138 phase journals này là **bằng chứng giai đoạn thực thi bổ sung (Tier 2: Supplemental Phase Evidence)** ghi lại nhật ký tiến trình (`readiness`, `bronze`, `silver`, `gold`, `verify`, `publish`), KHÔNG PHẢI là 114 objects frontier để đóng transitive closure (xem mục 2.3).

- **Tỷ lệ khớp:** **138/138 objects (100%)** khớp tuyệt đối với các `artifact_sha256` trong receipts của 63 tháng.
- **Cấu trúc phân loại 138 objects (23 runs x 6 phase journals):**
  - **23 readiness journals** (`silver/real-etl/<run_id>/readiness.json`).
  - **23 bronze journals** (`silver/real-etl/<run_id>/bronze.json`).
  - **23 silver journals** (`silver/real-etl/<run_id>/silver.json`).
  - **23 gold commit receipts** (`silver/real-etl/<run_id>/gold.json`).
  - **23 verify journals** (`silver/real-etl/<run_id>/verify.json`).
  - **23 publish journals** (`silver/real-etl/<run_id>/publish.json`).
- **Thông số gói:**
  - Dung lượng: `401.335 bytes`.
  - SHA-256: `45c1f02ff80454aca2a6bad5587e6b0bfabc4f8cb7fa08a8043a8c6bb7b6de01`.
  - Manifest SHA-256: `4444c7f0c0041511ade0b122f2faaaefa427c6b4a1ee690eb05830236d807694`.
  - File đặc tả đối soát trusted: `staging/dat03-range-handoff-rosy179/rosy179-required-lineage.json`.
- **Bảo mật & layout:** Công cụ `scripts/pack-worker-lineage.py` đã được cập nhật chốt chặn symlink (từ chối symlink file, parent folder, và symlink ZIP entries mode `0o120000`), chống thoát thư mục gốc (`resolve().is_relative_to()`), và áp dụng kiểm tra nghiêm ngặt layout `silver/real-etl/<operation_id>/<phase>.json` kèm kiểm tra nhất quán trường manifest và chống trùng lặp logic.

---

## 2.3. Đặc tả Kỹ thuật Transitive Closure Frontier (Tier 3: 114 Objects Frontier) của rosy179

Theo phân tích chu trình đóng transitive closure của coordinator Hoài Tâm, frontier thực tế cần thu thập để đóng hoàn toàn Cổng 1 của phân đoạn `rosy179` gồm đúng **114 objects**:

- **92 Silver dataset manifests:** 23 runs x 4 datasets:
  - `source_observation` (`silver/bundles/<run_id>/source_observation/manifest.json`)
  - `reject_record` (`silver/bundles/<run_id>/reject_record/manifest.json`)
  - `source_link` (`silver/bundles/<run_id>/source_link/manifest.json`)
  - `canonical_membership` (`silver/bundles/<run_id>/canonical_membership/manifest.json`)
- **22 parent Gold commit JSONs:** 22 commit JSON của run cha tại prefix vật lý `silver/gold-commits/<run_id>/commit.json` (trong số 23 runs thì run đầu tiên `orc07-5f6634681f7964e2edbf1b4a47888f02587bc6d8` là root run không có parent commit).
- **Tổng số objects frontier:** **92 + 22 = 114 objects**.

### Chi tiết file đặc tả `rosy179-frontier-114-spec.json`:
- **Đường dẫn:** `staging/dat03-range-handoff-rosy179/rosy179-frontier-114-spec.json`
- **Dung lượng:** `81.654 bytes`
- **SHA-256:** `69ba9cb8e259ee5375c01ac3f5e1833aa1e0e5dc75bd40adf252197e1356c7b4`
- **Trạng thái đặc tả:** **CHECKED & MATCHED**.
- **Nguyên tắc kỹ thuật:** File spec này cung cấp ánh xạ chính xác giữa URI S3 và SHA-256 kỳ vọng để trích xuất bytes gốc trực tiếp từ MinIO/object storage mà không cần chạy lại pipeline ETL.

---

## 2.4. Kết quả Nghiệm thu Bytes Gốc 114 Objects Frontier của rosy179 trên Google Drive (14/11/2026)

Ngày 14/11/2026, coordinator Hoài Tâm đã tải và đối soát độc lập gói dữ liệu bytes gốc 114 objects frontier do máy `rosy179` đóng gói và tải lên Google Drive:

- **Thông số gói dữ liệu:**
  - **Tên file:** `rosy179-dat03-lineage-frontier.zip`
  - **Google Drive ID:** `1QsZHjXXsGmWvBDFHiicMdejmAN2Lehb-`
  - **Dung lượng:** `101.446 bytes`
  - **Mã băm SHA-256:** `0dd1c504f27489709c46748b313e9f764ecfc25a58ef50a2699cbcf0626e8453`
  - **Sidecar Checksum:** `rosy179-dat03-lineage-frontier.zip.sha256` (khớp tuyệt đối).
- **Kết quả đối soát độc lập:**
  - **Tỷ lệ khớp:** **114/114 objects (100%)** khớp tuyệt đối với danh sách URI và SHA-256 được quy định trong `config/research/dat03_lineage_rosy179_metadata.json`.
  - **Phân loại 114 objects gốc:**
    - **92 Silver dataset manifests:** 23 runs x 4 datasets (`source_observation`, `reject_record`, `source_link`, `canonical_membership` dưới `silver/bundles/<run_id>/<dataset>/manifest.json`).
    - **22 parent Gold commit JSONs:** `silver/gold-commits/<run_id>/commit.json` của 22 run cha.
  - **Tính toàn vẹn ZIP:** Hoài Tâm xác nhận không có missing hoặc extra metadata, không có duplicate ZIP members, không có unsafe paths hay symlink.
- **Phân định phạm vi nghiệm thu 114 objects frontier của rosy179:**
  - Hoài Tâm đã xác nhận nghiệm thu 114/114 objects bytes gốc (92 Silver dataset manifests + 22 parent Gold commit JSONs) đạt 100% SHA-256.
  - **Giới hạn phạm vi nghiêm ngặt:** Việc nghiệm thu 114 objects bytes gốc này **KHÔNG đồng nghĩa với việc đóng toàn bộ lineage hay đóng Cổng 1**.
  - **Cổng 1 tiếp tục MỞ (`CONG_1_TRANSITIVE_CLOSURE_OPEN`)**: Tiếp tục xử lý danh sách yêu cầu metadata bổ sung theo file `config/research/dat03_lineage_rosy179_followup_20261114.json` (PR #131) gồm 24 pinned metadata objects (22 parent gold identity objects + commit và identity của đúng current head `orc07-22b5e7647a3df9ed83370139e11c48048845ee8f`) cùng 23 publication URIs gốc.
  - **Nguyên tắc kỹ thuật:** Phân biệt rõ ràng canonical payload digest với raw-file SHA-256; không tự ý tổng hợp commit/identity từ phase JSONs (`gold.json` / `publish.json`); snapshot references trên Iceberg cần được kiểm tra inventory thực tế trước khi coi là đồ thị hoàn tất.

---

## 3. Các Điểm nghẽn Còn lại Trước khi Hoàn tất DAT-03

1. **Cổng 1 — Transitive closure frontier & follow-up lineage:** Tier 1 (83 objects first-hop) và 114 objects frontier bytes gốc của rosy179 đã được tải lên Google Drive và Hoài Tâm đối soát độc lập đạt 100% SHA-256; Tier 2 (138 objects supplemental phase journals) đã đóng gói. Cổng 1 vẫn đang MỞ (OPEN), chưa đóng transitive closure: tiếp tục xử lý danh sách đối soát bổ sung `config/research/dat03_lineage_rosy179_followup_20261114.json` (PR #131) gồm 24 pinned metadata objects và 23 publication URIs gốc, cùng phần đối soát tương ứng của ThanhTris trước khi có thể đóng Cổng 1.
2. **Release nghiên cứu cuối:** Thư mục `accepted/` trên Drive chưa có release cuối cùng. Hai receipt hiện có trong thư mục `receipts/` là receipt chạy dải tháng cá nhân của Trí, chưa phải xác nhận của 3 thành viên nhận cùng một dataset nghiên cứu chính thức.
3. **Catalog Gold hợp nhất:** Chưa thực hiện merge 4 gói vào Iceberg catalog chung để kiểm tra liên kết ranh giới và chạy Trino verification queries.
4. **Official ML Export:** Chưa chạy build job chính thức cho 2 split `REPRODUCTION` và `EXTENSION` từ snapshot Gold chung.

---

## 4. Bốn Cổng Kỹ thuật để DAT-03 Đạt Trạng thái Done

Để đưa task [DAT-03](../task/tasks/DAT-03.md) từ `In Progress` / `Needs Update` sang `Done`, quy trình 4 cổng phải được nghiệm thu tuần tự:

```text
[Cổng 1] Nhận provenance, đối soát frontier 114 objects & follow-up metadata -> Đóng transitive closure
   │
   ▼
[Cổng 2] Tích hợp 4 gói vào catalog Iceberg Gold -> Khử trùng lặp ranh giới -> Verify Trino
   │
   ▼
[Cổng 3] Chạy ResearchDatasetBuildJob -> Xuất REPRODUCTION (225m) & EXTENSION (63m)
   │
   ▼
[Cổng 4] Đóng gói PAIRED_COLLECTION_EXPORT vào accepted/ -> 3 thành viên ký đủ 3 receipts
   │
   ▼
DAT-03 CHÍNH THỨC HOÀN TẤT (DONE)
```

1. **Cổng 1 — Đối soát Provenance & Transitive Closure:** First-hop provenance cho cả hai worker (ThanhTris: 75/75 objects, rosy179: 83/83 objects) đã được đưa lên Google Drive và đối chiếu độc lập khớp 100% SHA-256. Đã có supplemental phase journals (ThanhTris: 114, rosy179: 138). Phân đoạn rosy179 đã nghiệm thu 114 objects frontier bytes gốc (92 Silver manifests + 22 parent Gold commits). Cổng 1 hiện đang MỞ để chờ tiếp tục đối soát follow-up metadata theo `config/research/dat03_lineage_rosy179_followup_20261114.json` (PR #131: 24 metadata objects + 23 publication URIs) và hoàn tất phần đối soát tương ứng của ThanhTris trước khi đóng Cổng 1.
2. **Cổng 2 — Tích hợp Catalog & Khử trùng Ranh giới:** Tích hợp 4 gói Parquet vào catalog Gold, kiểm tra tại 3 điểm ranh giới (`2008-05/06`, `2013-07/08`, `2018-09/10`) đảm bảo không trùng lặp `canonical_event_id`, chạy Trino query audit count.
3. **Cổng 3 — Xuất bản Official ML Feature Export:** Kích hoạt `ResearchDatasetBuildJob` cho hai tập:
   - `REPRODUCTION`: [2000-01-01, 2018-10-01) UTC (225 tháng, Mc cutoff theo contract).
   - `EXTENSION`: [2018-10-01, 2024-01-01) UTC (63 tháng, Mc cutoff đồng bộ).
   - Xác minh window, feature scaling, format shards Parquet và checksums.
4. **Cổng 4 — Đóng gói Release & Nghiệm thu Ba bên:**
   - Dùng `scripts/pack-research-team.py` tạo bộ release chung đặt vào `accepted/`.
   - Mỗi thành viên chạy độc lập `scripts/verify-research-team.py` trên môi trường riêng của mình để sinh 3 file receipt (`HoaiTam`, `ThanhTris`, `rosy179`).

---

## 5. Trạng thái Audit Ranh giới Catalog (Boundary Gate Audit)

- Công cụ kiểm tra ranh giới [`scripts/verify-catalog-boundaries.py`](../../scripts/verify-catalog-boundaries.py) được quản lý và hoàn thiện trong PR #122 (`feat/dat-03-integration-completion`) với cơ chế fail-closed chặt chẽ và kiểm thử độc lập qua unit tests [`tests/test_catalog_boundaries.py`](../../tests/test_catalog_boundaries.py).
- **Hiện trạng kiểm tra:** Trạng thái cổng ranh giới là `NOT_CHECKED` / `PENDING_CATALOG_MERGE` tại [DAT-03_BOUNDARY_INTEGRATION_AUDIT.json](DAT-03_BOUNDARY_INTEGRATION_AUDIT.json).
- PR #123 (`feat/dat-03-rosy179-provenance`) tập trung và giới hạn phạm vi vào bộ công cụ đóng gói / trích xuất / xác thực provenance first-hop (`scripts/pack-worker-provenance.py`, `scripts/extract-minio-provenance.py`), cùng bằng chứng kiểm tra 83 objects của rosy179.
- **Nguyên tắc kỹ thuật:** Đối soát 288 tháng ở trên là đối soát phân bổ tháng trên các gói độc lập riêng lẻ. Việc khử trùng lặp và xác minh ranh giới thực tế giữa các bản ghi `canonical_event_id` chỉ được công nhận `PASS` khi cả 4 gói được hợp nhất vào catalog Gold Iceberg và đối soát qua Trino theo Cổng 2. Không coi việc kiểm tra lịch tháng hoặc giả định không input là chứng nhận chống trùng thành công.
