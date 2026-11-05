# DAT-03 — Báo cáo Đối soát Hợp nhất 288 Tháng và Provenance First-hop (05/11/2026)

## 1. Tóm tắt Hiện trạng Toàn kỳ 288 Tháng

Theo kiểm tra offline ngày 02/11/2026 và cập nhật đối soát từ Hoài Tâm, toàn bộ chuỗi dữ liệu 24 năm (**288 tháng**, từ `2000-01` đến `2023-12` UTC) đã được xử lý và kiểm tra qua 4 gói phân đoạn độc lập. Không ghi nhận tháng nào bị thiếu (gap) hoặc bị chồng lấn (overlap).

Tuy nhiên, đây là kết quả của các gói riêng lẻ trên từng máy worker/Drive, chưa phải catalog Gold hợp nhất được nghiệm thu chính thức trên lakehouse.

| Phân đoạn / Thành viên | Khoảng thời gian (UTC) | Số tháng | Trạng thái gói dữ liệu | Trạng thái Provenance First-hop |
|---|---|---:|---|---|
| Catalog nền | 2000-01–2008-05 | 101 | Đã kiểm tra gói | Lưu trữ trên coordinator ban đầu (1.064.869 canonical events) |
| HoaiTam | 2008-06–2013-07 | 62 | Đã kiểm tra các gói | Lưu trữ Drive theo các batch (24m + 9m + còn lại) |
| ThanhTris | 2013-08–2018-09 | 62 | Đã kiểm tra gói | **75/75 objects khớp 100% SHA-256** |
| rosy179 | 2018-10–2023-12 | 63 | Đã kiểm tra gói | Chờ 83 objects trong thư mục worker của Trang |
| **Toàn kỳ hợp nhất** | **2000-01–2023-12** | **288** | **Đủ 288/288 tháng** | **Chờ hoàn tất đối soát cả hai phần** |

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

## 3. Các Điểm nghẽn Còn lại Trước khi Hoàn tất DAT-03

1. **Provenance worker rosy179:** Thư mục worker của Trang trên Drive hiện chưa có gói provenance tương ứng (yêu cầu 83 objects first-hop).
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

1. **Cổng 1 — Hoàn tất đối soát Provenance:** Nhận và đối soát 83 object của rosy179; kiểm tra transitive closure đến các nguồn frozen archives.
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

- Công cụ kiểm tra ranh giới [`scripts/verify-catalog-boundaries.py`](../../scripts/verify-catalog-boundaries.py) đã được xây dựng với cơ chế fail-closed chặt chẽ và kiểm thử độc lập qua unit tests [`tests/test_catalog_boundaries.py`](../../tests/test_catalog_boundaries.py).
- **Hiện trạng kiểm tra:** Trạng thái cổng ranh giới là `NOT_CHECKED` / `PENDING_CATALOG_MERGE` tại [DAT-03_BOUNDARY_INTEGRATION_AUDIT.json](DAT-03_BOUNDARY_INTEGRATION_AUDIT.json).
- **Nguyên tắc kỹ thuật:** Đối soát 288 tháng ở trên là đối soát phân bổ tháng trên các gói độc lập riêng lẻ. Việc khử trùng lặp và xác minh ranh giới thực tế giữa các bản ghi `canonical_event_id` chỉ được công nhận `PASS` khi cả 4 gói được hợp nhất vào catalog Gold Iceberg và đối soát qua Trino theo Cổng 2. Không coi việc kiểm tra lịch tháng hoặc giả định không input là chứng nhận chống trùng thành công.
