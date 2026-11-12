# DAT-03 — Báo cáo Đối soát Hợp nhất 288 Tháng và Provenance First-hop (05/11/2026)

## 1. Tóm tắt Hiện trạng Toàn kỳ 288 Tháng

Theo kiểm tra offline ngày 02/11/2026 và cập nhật đối soát từ Hoài Tâm, toàn bộ chuỗi dữ liệu 24 năm (**288 tháng**, từ `2000-01` đến `2023-12` UTC) đã được xử lý và kiểm tra qua 4 gói phân đoạn độc lập. Không ghi nhận tháng nào bị thiếu (gap) hoặc bị chồng lấn (overlap).

Tuy nhiên, đây là kết quả của các gói riêng lẻ trên từng máy worker/Drive, chưa phải catalog Gold hợp nhất được nghiệm thu chính thức trên lakehouse.

| Phân đoạn / Thành viên | Khoảng thời gian (UTC) | Số tháng | Trạng thái gói dữ liệu | Trạng thái Provenance First-hop |
|---|---|---:|---|---|
| Catalog nền | 2000-01–2008-05 | 101 | Đã kiểm tra gói | Lưu trữ trên coordinator ban đầu (1.064.869 canonical events) |
| HoaiTam | 2008-06–2013-07 | 62 | Đã kiểm tra các gói | Lưu trữ Drive theo các batch (24m + 9m + còn lại) |
| ThanhTris | 2013-08–2018-09 | 62 | Đã kiểm tra gói | **75/75 objects khớp 100% SHA-256** |
| rosy179 | 2018-10–2023-12 | 63 | Đã kiểm tra gói | **Đã trích xuất & đóng gói 83/83 objects first-hop và 138/138 objects transitive lineage metadata khớp 100% SHA-256** |
| **Toàn kỳ hợp nhất** | **2000-01–2023-12** | **288** | **Đủ 288/288 tháng** | **Đã đủ 75/75 first-hop + 114 lineage (Trí) và 83/83 first-hop + 138 lineage (Trang)** |

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

## 2.2. Kết quả Đóng gói Lineage Bổ sung (Transitive Lineage) của rosy179

rosy179 đã trích xuất, xác thực đối chiếu và đóng gói thành công gói lineage bổ sung tầng tiếp theo (`rosy179-dat03-lineage-metadata.zip`) gồm **138 objects** metadata tương ứng 23 runs x 6 journals giai đoạn `2018-10..2023-12`:

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
  - Đã xuất file đặc tả đối soát trusted: `staging/dat03-range-handoff-rosy179/rosy179-required-lineage.json`.
- **Kết luận:** Hoàn thành toàn bộ việc trích xuất và đóng gói 138 objects transitive lineage metadata của rosy179, sẵn sàng cho Hoài Tâm đối soát độc lập Cổng 1.

---

## 3. Các Điểm nghẽn Còn lại Trước khi Hoàn tất DAT-03

1. **Provenance & Lineage worker rosy179:** Đã đưa gói `rosy179-dat03-provenance-metadata.zip` (83 objects first-hop) lên Google Drive, xác nhận khớp 100% SHA-256. Gói bổ sung `rosy179-dat03-lineage-metadata.zip` (138 objects transitive lineage metadata) đã được đóng gói thành công kèm file đối soát `rosy179-required-lineage.json` (SHA-256 `45c1f02ff80454aca2a6bad5587e6b0bfabc4f8cb7fa08a8043a8c6bb7b6de01`). Cổng 1 đang chờ Hoài Tâm hoàn tất đối soát độc lập gói lineage này cùng gói của ThanhTris.
2. **Release nghiên cứu cuối:** Thư mục `accepted/` trên Drive chưa có release cuối cùng. Hai receipt hiện có trong thư mục `receipts/` là receipt chạy dải tháng cá nhân của Trí, chưa phải xác nhận của 3 thành viên nhận cùng một dataset nghiên cứu chính thức.
3. **Catalog Gold hợp nhất:** Chưa thực hiện merge 4 gói vào Iceberg catalog chung để kiểm tra liên kết ranh giới và chạy Trino verification queries.
4. **Official ML Export:** Chưa chạy build job chính thức cho 2 split `REPRODUCTION` và `EXTENSION` từ snapshot Gold chung.

---

## 4. Bốn Cổng Kỹ thuật để DAT-03 Đạt Trạng thái Done

Để đưa task [DAT-03](../task/tasks/DAT-03.md) từ `In Progress` sang `Done`, quy trình 4 cổng phải được nghiệm thu tuần tự:

```text
[Cổng 1] Nhận 83 provenance của Trang -> Đối soát transitive lineage toàn kỳ 288 tháng
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

1. **Cổng 1 — Hoàn tất đối soát Provenance:** First-hop provenance cho cả hai worker (ThanhTris: 75/75 objects, rosy179: 83/83 objects) đã được đưa lên Google Drive và đối chiếu độc lập khớp 100% SHA-256. Cổng 1 hiện đang chờ kiểm tra transitive closure và lineage sâu hơn của cả hai phần đến các frozen archives trước khi hoàn tất Cổng 1.
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
