# DEMO-02 — Mẫu biên bản diễn tập

Sao chép mẫu này vào `staging/demo-02/rehearsal-record.md` trên máy demo.
Dùng [runbook](./DEMO-02_REHEARSAL.md) để chuẩn bị, chạy và kiểm các cổng.
Ghi `NOT_RUN` cho phần chưa thực hiện. Chỉ ghi `PASS` khi có kết quả quan sát
và bằng chứng tương ứng. Giữ `PREPARED_PENDING_OFFICIAL_GATES` khi tập với
report pilot hoặc evidence replay.

## Phiên diễn tập

| Mục | Kết quả thực tế |
|---|---|
| Ngày giờ, múi giờ và tổng thời lượng | Chưa ghi |
| Commit demo và config version | Chưa ghi |
| Manifest, SHA-256 và phạm vi | Chưa ghi |
| Preflight JSON, SHA-256, `overall_status` | Chưa ghi |
| Người điều phối và người xác nhận | Chưa ghi |

## Walkthrough

| Phần | Người trình bày | Thời lượng | Kết quả | Bằng chứng |
|---|---|---|---|---|
| ETL và Gold publication | Chưa ghi | Chưa ghi | NOT_RUN | Chưa ghi |
| Dataset, thí nghiệm và giới hạn | Chưa ghi | Chưa ghi | NOT_RUN | Chưa ghi |
| Receipt, Trino và static report | Chưa ghi | Chưa ghi | NOT_RUN | Chưa ghi |
| Chuyển sang phương án dự phòng | Chưa ghi | Chưa ghi | NOT_RUN | Chưa ghi |

## Bằng chứng Core

| Cổng | Pin hoặc kết quả cần ghi | Kết quả thực tế |
|---|---|---|
| ETL và recovery | QA-01 run ID, Gold publication và snapshot IDs, evidence QA-02/QA-04 | NOT_RUN |
| ML | Dataset ID, algorithm/run IDs, membership/summary/experiment snapshots, SHA-256 receipt | NOT_RUN |
| Serving và E2E | Trino query IDs, SHA-256 report MLI-04 và receipt MLQ-01, phạm vi dataset | NOT_RUN |
| Bảo mật và tài liệu | Evidence SEC-01/DOC-01 đúng phạm vi, kết quả kiểm màn hình và file đã mở | NOT_RUN |
| Demo | Evidence chính thức DEMO-01, ba phần trình bày và một lần chuyển fallback | NOT_RUN |

## Lỗi và quyết định

| Lỗi quan sát được | Tác động Core hoặc Stretch | Người xử lý | Kết quả kiểm lại |
|---|---|---|---|
| Chưa ghi | Chưa ghi | Chưa ghi | NOT_RUN |

- Tag đề xuất và commit release candidate — Chưa ghi.
- Xác nhận phát hành của HoaiTam — Chưa ghi.
- Biên bản chỉ lưu checksum, đường dẫn evidence an toàn và kết quả. File
  credential, private log và payload gốc được giữ ngoài tài liệu chia sẻ.
