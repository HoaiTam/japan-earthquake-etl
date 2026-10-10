# MLD-02 — Evidence: Audit Gold và xác định magnitude of completeness

## 1. Thông tin tổng quan

- **Task ID:** MLD-02
- **Tên task:** Audit Gold và xác định magnitude of completeness
- **Assignee:** rosy179
- **Reviewer:** unassigned (chưa có reviewer độc lập)
- **Workstream:** Input audit and completeness
- **Block:** G - ML Dataset
- **Scope:** Core (P0, 7h)
- **Branch:** `feat/mld-02-input-audit-and-completeness`
- **Môi trường thực thi:** OpenJDK 21.0.6 (compile target release 17), Apache Spark 3.5.9, Maven Wrapper

## 2. Kết quả kiểm thử tự động

Chạy qua lệnh `make test-ml-audit` (hoặc Maven Wrapper):
`./mvnw --batch-mode --no-transfer-progress -pl spark -am -Dtest=MagnitudeCompletenessEstimatorTest,GoldInputAuditEngineTest,GoldInputAuditIntegrationTest test`

**Kết quả:** 12/12 tests PASS, 0 Failures, 0 Errors, 0 Skipped:
1. `MagnitudeCompletenessEstimatorTest` (6 tests):
   - `frequencyMagnitudeDistributionCalculatesHistogramAndAkiUtsuBValue`: Kiểm chứng tính toán histogram FMD ($\Delta M = 0.1$), số lượng tích lũy, và ước lượng hệ số góc $b$ Gutenberg-Richter bằng công thức cực đại hợp lý Aki-Utsu ($b \approx 1.0$) kèm sai số chuẩn $\sigma_b$.
   - `maxcEstimatorDeterminesCentralMcAndSensitivityBounds`: Kiểm chứng thuật toán MAXC xác định mode bin, ngưỡng trung tâm $M_c = 2.2$, cùng hai biên sensitivity $M_c^{\text{lower}} = 2.0$ và $M_c^{\text{upper}} = 2.4$ ($\Delta = 0.2$).
   - `tiedModeBinPicksLowestMagnitudeConservatively`: Kiểm chứng quy tắc giải quyết hòa tần số (tie-breaker) chọn bin có độ lớn nhỏ hơn một cách bảo thủ.
   - `insufficientDataFlagsUnreliableWithoutFabricatingMc`: Kiểm chứng khi số lượng mẫu $N < 50$, estimator trả về `is_reliable = false` kèm lý do `INSUFFICIENT_DATA`, tuyệt đối không bịa đặt hoặc hard-code số giả.
   - `handlesNullAndNonFiniteMagnitudesSafely`: Xử lý an toàn các giá trị null, NaN, $\pm\infty$ mà không gây crash.
   - `canonicalMcConfigBuildsDeterministicSha256`: Kiểm chứng tính tất định 100% của cấu hình JSON chuẩn tắc (sorted keys) và mã băm SHA-256 (64 ký tự hex).
2. `GoldInputAuditEngineTest` (5 tests):
   - `auditRejectsDuplicateCanonicalEventId`: Phát hiện và chặn đứng (fail-closed) khi Gold input có `canonical_event_id` trùng lặp.
   - `auditExcludesAllNonCompliantEventsWithExactReasonCodes`: Kiểm thử toàn diện 9 mã lý do loại trừ:
     - `ML_MISSING_COORDINATE`: null, NaN hoặc ngoài $[-90, 90]$, $[-180, 180]$.
     - `ML_MISSING_DEPTH`: null, NaN độ sâu chấn tiêu.
     - `ML_MISSING_MAGNITUDE`: null, NaN độ lớn.
     - `ML_NON_NATURAL_EVENT`: sự kiện phi tự nhiên (`ARTIFICIAL`).
     - `ML_OUTSIDE_STUDY_AREA`: sự kiện ngoài vùng nghiên cứu ROI.
     - `ML_SOURCE_NOT_COMPARABLE`: nguồn không tương đồng (USGS thay vì JMA).
     - `ML_LEGACY_CATALOG_ERA`: kỷ nguyên cũ trước 1997-10-01 (`LEGACY`).
     - `ML_OUTSIDE_TIME_RANGE`: ngoài khoảng thời gian nửa mở $[2000-01-01, 2018-10-01)$.
     - `ML_BELOW_COMPLETENESS`: $M < M_c$.
   - Kiểm tra các trường hợp biên: $M = M_c$ đạt eligible; thời điểm chính xác `2000-01-01T00:00:00Z` đạt eligible; thời điểm chính xác `2018-10-01T00:00:00Z` bị loại trừ (upper bound exclusive).
   - Bảo toàn Gold (Gold immutability): Không có dòng nào bị xóa khỏi Gold; toàn bộ cột của `gold.event_current` được giữ nguyên.
   - Đối soát kế toán: $N_{\text{input}} = N_{\text{eligible}} + N_{\text{excluded}}$ và tổng các mã lý do chính khớp chính xác $N_{\text{excluded}}$.
   - `auditEnrichesDatasetManifestWithExactContractFields`: Kiểm chứng làm giàu manifest theo `CON-03-ML` (`input_event_count`, `eligible_event_count`, `excluded_event_count`, `exclusion_counts_json`, `mc_method`, `mc_value`, `mc_config_json`, `mc_config_sha256`, `audit_report_uri`).
   - `auditCannotUseExtensionPeriodForReproductionSplit`: Chặn đứng việc dùng khoảng thời gian extension cho reproduction split (`DS_INVALID_PERIOD`).
   - `auditGeneratesValidJsonAndMarkdownReports`: Kiểm chứng định dạng và nội dung báo cáo JSON và Markdown.
3. `GoldInputAuditIntegrationTest` (1 test):
   - Chạy pilot hoàn chỉnh cho cả hai phân vùng `REPRODUCTION` (JMA 2000 sample) và `EXTENSION` (JMA 2023 sample).
   - Xác nhận sự cô lập hoàn toàn giữa hai thời kỳ: tham số $M_c$ của reproduction được ước lượng độc lập từ JMA 2000 ($M_c = 2.1$), không bị tác động bởi dữ liệu extension JMA 2023 ($M_c = 1.8$).
   - Xuất file báo cáo `reproduction_audit_report.json` và `reproduction_audit_report.md` và kiểm tra nội dung.

## 3. Đối soát tiêu chí hoàn thành (Acceptance Criteria)

- [x] **Không xóa event khỏi Gold; chỉ exclude khỏi dataset với reason code**: Đạt. `goldEvents` giữ nguyên vẹn toàn bộ schema và dữ liệu; engine chỉ bổ sung các cột `is_eligible`, `primary_exclusion_reason`, `all_exclusion_reasons`, `mc_applied`.
- [x] **Có reason tối thiểu cho missing depth/magnitude, ngoài period, dưới completeness và source không comparable**: Đạt. Đã hỗ trợ đầy đủ 9 mã lý do: `ML_MISSING_COORDINATE`, `ML_MISSING_DEPTH`, `ML_MISSING_MAGNITUDE`, `ML_NON_NATURAL_EVENT`, `ML_OUTSIDE_STUDY_AREA`, `ML_SOURCE_NOT_COMPARABLE`, `ML_LEGACY_CATALOG_ERA`, `ML_OUTSIDE_TIME_RANGE`, `ML_BELOW_COMPLETENESS`.
- [x] **$M_c$ có method/version/evidence và được áp dụng nhất quán trong cùng dataset**: Đạt. Phương pháp `MAXIMUM_CURVATURE` v1.0, kèm kiểm chứng hệ số $b$ Gutenberg-Richter Aki-Utsu, dải độ nhạy $[M_c - 0.2, M_c + 0.2]$, phân tầng theo độ sâu (Shallow, Intermediate, Deep) và cấu hình chuẩn tắc có mã băm SHA-256.
- [x] **Audit counts đối soát được với snapshot manifest và không dùng dữ liệu extension để tune reproduction**: Đạt. Bất biến $input = eligible + excluded$ được kiểm chứng tự động; cấu hình kiểm tra chặt chẽ biên thời gian nửa mở, ngăn chặn việc dùng extension data cho reproduction.

## 4. Giới hạn / Rủi ro / Bước tiếp theo

1. **Phụ thuộc upstream chưa hoàn tất:**
   - [MLD-01 - Pin Gold snapshot và tạo dataset manifest](docs/task/tasks/MLD-01.md): Hiện đang ở trạng thái `In Progress` (PR #53) do còn chờ bằng chứng snapshot Gold committed/verified từ [GLD-04 - Tạo Trino views và verification SQL](docs/task/tasks/GLD-04.md).
   - Khi MLD-01 được tích hợp và cung cấp snapshot thật của toàn bộ giai đoạn nghiên cứu 2000–2018, job audit `GoldInputAuditJob` sẽ được kích hoạt để chốt số liệu thống kê cuối cùng cho catalog toàn phần.
2. **Bàn giao downstream:**
   - Bộ lọc và cấu hình $M_c$ từ MLD-02 là đầu vào điều kiện tiên quyết cho [MLD-03 - Chọn mainshock và tạo candidate windows](docs/task/tasks/MLD-03.md). MLD-03 sẽ đọc các sự kiện `is_eligible = true` để xác định các mainshock ($M > 5.5$, depth $50\text{--}200\text{ km}$) và tạo cửa sổ không gian - thời gian cho candidate sequences.
3. **Đánh giá về tập dữ liệu mẫu pilot:**
   - Dữ liệu kiểm thử pilot hiện tại sử dụng các mẫu dữ liệu thực nghiệm đã chốt từ DAT-01/SLV-09 (JMA 2000, JMA 2023, USGS 2023). Mẫu một năm (2000) mang tính chất kiểm chứng thuật toán pilot; việc ngoại suy $M_c$ cho toàn bộ 18 năm (2000–2018) sẽ được thực hiện khi toàn bộ catalog được nạp và pin snapshot qua MLD-01.
