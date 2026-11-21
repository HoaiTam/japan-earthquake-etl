# BI-05 — Nghiệm thu refresh và đối soát Power BI

Ngày 21/11/2026, chạy trên bản sao [PBIX ở main `5d61efd`](../../powerbi/japan_earthquake_model.pbix) có đủ trang BI-03 và BI-04. Power BI Desktop dùng DSN `TrinoJapanEarthquake` qua proxy ODBC của repo trên loopback `8081 → 8083` tới Trino 483. Thao tác đổi pin và chạy DAX chỉ thực hiện trên bản sao QA.

## Refresh với publication đã ghim

Trino xác nhận đúng một dòng `PASSED` cho `pub-202302-001`, bundle checksum QA `bndl20230201sha256test01`, serving view `iceberg.gold.v_bndl20230201sha256test01_earthquake_event_current`, freshness `2023-01-03T09:51:59.188Z` và 16 sự kiện.

| Ca | Power BI Desktop | Evidence |
|---|---|---|
| Pin hợp lệ | Refresh toàn model thành công, thẻ lần refresh chuyển từ giá trị lưu sẵn sang `2026-11-21 09:58:58 UTC`. | [Baseline vừa refresh](bi-05/refresh-valid.jpg) |
| Pin rỗng | Close & Apply từ chối load với lỗi `PinnedPublicationId parameter is mandatory for snapshot consistency`. | [Lỗi pin rỗng](bi-05/refresh-empty-pin.jpg) |
| ID không có trong catalog | Close & Apply từ chối `qa-bi05-absent-control` vì không có dòng `PASSED`. SQL preflight trả 0 dòng khớp và 0 dòng đủ điều kiện. | [Lỗi ID vắng](bi-05/refresh-absent-pin.jpg) |
| Khôi phục pin | Close & Apply tải lại model, thẻ refresh hiển thị `2026-11-21 10:07:31 UTC`, cùng publication, freshness và 16 sự kiện. | [Refresh phục hồi](bi-05/refresh-restored.jpg) |

Cả hai ca từ chối giữ dữ liệu đã import từ lần baseline và để lại cảnh báo thay đổi chưa áp dụng. Chỉ tính hai lần có timestamp mới là refresh thành công. Catalog QA có 0 bản ghi chưa `PASSED`. Ca ID tồn tại với `PENDING` hoặc `FAILED` được kiểm bằng [fixture SQLite](../../tests/test_bi05_preflight.py). [Hồi quy BI-01](../../tests/test_bi01_connectivity.py) kiểm commit-before-publish riêng.

## KPI trên model sau refresh

Chạy [DAX](../../powerbi/bi05_reconciliation.dax) trên measures đã lưu trong model vừa phục hồi và chạy [SQL độc lập](../../powerbi/kpi_reconciliation.sql) với cùng filter theo [BI-03](BI-03_OVERVIEW.md#tái-kiểm-tra). SQL query IDs dưới đây cùng trả về `pub-202302-001`.

| Filter JST | Tổng | Avg M | Max M | Avg sâu (km) | M ≥ 5 | USGS / JMA / cả hai | Trino query ID |
|---|---:|---:|---:|---:|---:|---:|---|
| Toàn publication | 16 | 4.4875000 | 5.3000000 | 92.3628125 | 3 | 16 / 0 / 0 | `20261121_100731_00251_y2brh` |
| Tháng 01/2023, UNKNOWN, USGS_ONLY, M4_TO_LT5, SHALLOW | 9 | 4.3555556 | 4.5000000 | 35.0347778 | 0 | 9 / 0 / 0 | `20261121_100743_00255_y2brh` |
| Kỳ rỗng tháng 02/2023 | 0 | BLANK / NULL | BLANK / NULL | BLANK / NULL | 0 | 0 / 0 / 0 | `20261121_100751_00256_y2brh` |

Counts khớp tuyệt đối, chênh lệch số thực nhỏ hơn `0.000001`. DAX đo freshness thấp hơn Trino `1.333 ms` ở cả ba ca, nằm trong độ phân giải Date/Time của model. [KPI trên Desktop](bi-05/dax-kpi-parity.jpg), [nguồn, publication và freshness trên Desktop](bi-05/dax-publication-freshness.jpg).

Lặp lại theo [runbook BI-05](../runbooks/BI-05_REFRESH_RECONCILIATION.md).
