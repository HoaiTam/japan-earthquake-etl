# Late-Revision Test Fixtures (SLV-06)

Tài liệu và fixture kiểm thử cho quy trình source-local deduplication và xử lý late revision (SLV-06).

## Mục đích
Kiểm chứng hành vi của `SourceDedupTransformer` trong các kịch bản:
1. **USGS Late Revision Chain:**
   - Bản ghi ban đầu v1 (`mag = 4.0`, `status = automatic`, `updated = t1`).
   - Bản ghi cập nhật sau v2 (`mag = 4.2`, `status = reviewed`, `updated = t2 > t1`).
   - Bản ghi tinh chỉnh v3 (`mag = 4.3`, `status = reviewed`, `updated = t3 > t2`).
   - Gửi bản ghi theo thứ tự đảo ngược (v3 -> v1 -> v2) hoặc out-of-order arrival: bản mới nhất (v3) vẫn luôn được chọn làm CURRENT.
2. **JMA Multi-Release Chain:**
   - Cùng hypocenter observation trong release v1 (`mag = 4.0`) và release v2 (`mag = 4.2`).
   - Release v2 thay thế v1 làm CURRENT. Bản cũ v1 được gắn cờ `is_current_source_revision = false` và lưu vào history observations.
3. **Exact Duplicates:**
   - Bản ghi lặp lại nguyên bản (cùng key, cùng hash, cùng revision) được phân loại thành `DUPLICATE_SOURCE_RECORD`.
4. **Cross-Source Isolation:**
   - Quan sát từ USGS và JMA cho cùng một trận động đất độc lập hoàn toàn, không deduplicate lẫn nhau.
