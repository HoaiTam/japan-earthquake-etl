# MLD-02 — Audit/Mc evidence và giới hạn nghiệm thu

Ngày cập nhật: 2026-10-11. Branch: `feat/mld-02-input-audit-and-completeness`.
Assignee rosy179; reviewer unassigned, chưa có review độc lập.
PR: [#60](https://github.com/HoaiTam/japan-earthquake-etl/pull/60).

## Thay đổi đã kiểm chứng

- Main đã đồng bộ đến `1339b85` (GLD-03 đã merge), conflict Makefile/index đã xử lý.
- Nhánh có dependency GLD-04 và MLD-01 để compile exact manifest reader.
- Khi estimator unreliable và không có fixed config, `mc_applied`/manifest `mc_value`
  giữ null; pre-completeness events bị exclude `ML_MC_UNRELIABLE`. Không dùng mode
  của sample quá nhỏ hoặc fallback Mc=0 để cho candidate chạy.
- Canonical ID null/rỗng bị reject; report/config JSON không chứa NaN/Infinity.
- Fixed threshold được ghi rõ `FIXED_CONFIG`, applied Mc và sensitivity khớp config.
  Fixture fixed Mc không phải evidence ngưỡng bảo thủ trên catalog thật.
- CLI `GoldInputAuditJob` nhận exact dataset ID, đọc manifest Iceberg qua Trino,
  đối chiếu PASSED publication và đọc physical Gold snapshot bằng snapshot-id.
  Shared lease bắt buộc; cutoff trước publication bị reject vì chưa chứng minh
  historical source state. Output là audit/report và manifest handoff BUILDING,
  chưa tự update lifecycle hoặc publish candidate.

## Tests đã chạy

Java 17.0.12, Spark 3.5.9, Maven Wrapper:
`./mvnw --batch-mode --no-transfer-progress -pl spark -am -Dtest=MagnitudeCompletenessEstimatorTest,GoldInputAuditEngineTest,GoldInputAuditIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false verify`

Kết quả: **15 tests đạt**, 0 failures/errors/skipped, BUILD SUCCESS.

| Suite | Tests | Phạm vi |
|---|---:|---|
| MagnitudeCompletenessEstimatorTest | 6 | histogram, MAXC, sensitivity, sample guard, canonical hash |
| GoldInputAuditEngineTest | 8 | exclusion/counts, fixed method, unreliable Mc, empty input, pin conflict |
| GoldInputAuditIntegrationTest | 1 | Spark engine/report với 140 rows sinh bằng code |

Test integration trên là **synthetic fixture**: 70 JMA-like năm 2000, 60 JMA-like
năm 2023 và 10 USGS-like. Nó không đọc Bronze DAT-01, Silver SLV-09, Published
Gold hoặc persisted MLD-01 manifest. Mô tả trước đây gọi đây là sample thật
DAT-01/SLV-09 và xem task Done là chưa đủ căn cứ; trạng thái hiện tại Needs Update.

## Giới hạn / bước tiếp theo

- **MLD-01 — Pin Gold snapshot và tạo dataset manifest**: implementation và
  runtime evidence có tại [PR #65](https://github.com/HoaiTam/japan-earthquake-etl/pull/65),
  [task MLD-01](../task/tasks/MLD-01.md); chưa merge vào main tại lần cập nhật này.
- **GLD-04 — Tạo Trino views và verification SQL**: [PR #63](https://github.com/HoaiTam/japan-earthquake-etl/pull/63)
  đã đổi base main, còn kiểm tra/merge trước khi merge #60.
- **MLD-02 — Audit Gold và xác định magnitude of completeness**: còn live CLI
  integration với persisted manifest và primary-JMA/UNIFIED Published pilot,
  đối soát snapshot counts và chốt Mc/sensitivity có evidence. Gold local hiện
  chỉ có 16 USGS events; không đủ comparable JMA cho Mc.
- **SLV-09 — Kiểm thử tích hợp Silver đa nguồn**: ba exact Bronze manifest URIs
  trong `airflow/dags/fixtures/slv_09_bronze_inputs.json` đã HEAD/read existence
  bằng Java adapter trên MinIO local: cả ba không tồn tại. Cần seed/chạy lại
  đúng handoff trên warehouse này hoặc cung cấp receipt của môi trường có data.
- **MLD-03 — Chọn mainshock và tạo candidate windows**: chưa có audit thật để
  nghiệm thu downstream; không được chạy candidate với Mc null/unreliable.
- Estimator đang collect magnitudes về driver; cần resource guard hoặc histogram
  phân tán trước catalog toàn phần. Pilot nhỏ không chứng minh production scale.
- PR #60 đang chờ dependencies/data gate; CI fixture xanh không thay live evidence.
