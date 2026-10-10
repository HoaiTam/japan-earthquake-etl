# SLV-09 — Evidence Tích Hợp Silver Đa Nguồn, MinIO Readback và Gold Handoff

## 1. Baseline và Môi Trường

- **Task**: `SLV-09` — Tích hợp Silver đa nguồn và kiểm thử.
- **Task Owner**: `HoaiTam`, Reviewer: `unassigned`.
- **Branch**: `feat/slv-09-tich-hop-va-kiem-thu`.
- **Runtime Environment**:
  - JDK: `23.0.2` (với `--release 17` target, Java SecurityManager allowed `-Djava.security.manager=allow`).
  - Apache Spark: `3.5.9` (phân tán & standalone local execution, timezone `UTC`).
  - Storage Backend: `MinioSilverObjectStore` & `FileSilverObjectStore` (mô hình hóa cấu trúc S3/MinIO `s3://<bucket>/<prefix>/...`).
- **Contracts Tuân Thủ**:
  - `CON-01/1.0`: Phạm vi không gian (Japan ROI: lat 20°–50°N, lon 120°–155°E), ngưỡng dung sai liên kết.
  - `CON-02/1.0`: Chuẩn Bronze payload và manifest lineage.
  - `CON-03/1.0`: Mô hình dữ liệu Silver & Gold, cấu trúc bảng `silver.source_observation`, `silver.reject_record`, `silver.source_link`, `silver.canonical_membership` và `gold.dim_event_current`.

---

## 2. Giải Quyết Nhận Xét Của Nhóm ("SLV-09 Đang Done Quá Sớm")

Nhận xét của nhóm chỉ ra 3 điểm thiếu sót then chốt trong bản trước:
1. **Thiếu Persistence cho Linking và Canonical Membership**: Bản trước runner chỉ ghi `observations` và `rejects` xuống Parquet; kết quả `source_link` và `canonical_membership` chỉ được trả về trong RAM mà không lưu trữ vào storage.
2. **Thiếu Live MinIO Readback & Verification**: Chưa có kiểm thử đọc ngược lại dữ liệu thực tế từ MinIO/S3 URI để xác nhận tính toàn vẹn của file Parquet và cấu trúc thư mục phân vùng.
3. **Thiếu Bằng Chứng Chuyển Giao Gold Thực Tế (SilverReady)**: Cần chứng minh dữ liệu Silver lưu trữ thực sự sẵn sàng để downstream Gold (`GoldEventTransformer`) đọc và biến đổi thành công mà không có lỗi.

### Các Thay Đổi Kiến Trúc Đã Hoàn Thiện:
1. **Mở Rộng Silver Storage Layout (`SilverStorageLayout.java`)**:
   - Thêm đường dẫn phân vùng chuẩn cho `source_link/run_id=<RUN_ID>` và `canonical_membership/run_id=<RUN_ID>`.
   - Thêm đường dẫn staging cô lập `_staging/<RUN_ID>/source_link/...` và `_staging/<RUN_ID>/canonical_membership/...`.
   - Thêm file đánh dấu publish atomic `_SUCCESS` cho từng phân vùng link và membership.
2. **Parquet Schemas và Serialization (`SilverParquetSerializer.java`)**:
   - Định nghĩa `SOURCE_LINK_PARQUET_SCHEMA` theo CON-03 Section 8.1.
   - Định nghĩa `CANONICAL_MEMBERSHIP_PARQUET_SCHEMA` theo CON-03 Section 8.2.
   - Thêm phương thức tuần tự hóa `serializeSourceLinks(...)` và `serializeCanonicalMemberships(...)`.
3. **Mở Rộng Parquet Writer Atomic (`SilverParquetWriter.java`)**:
   - Tự động ghi cả `links` và `memberships` qua 3 bước: Staging -> Readback Checksum Verification -> Partition Overwrite Promote -> Tạo `_SUCCESS` marker.
   - Xóa sạch thư mục staging sau khi publish hoàn tất.
4. **Cập Nhật Multi-Source Runner (`SilverMultiSourceIntegrationRunner.java`)**:
   - Tự động đóng gói kết quả `sourceLinks()` và `canonicalMemberships()` vào `SilverWriteRequest` để lưu trữ vật lý song song với observations.
5. **MinIO In-Memory Provider (`MinioSilverObjectStore.java`)**:
   - Bổ sung `MinioSilverObjectStore.inMemory(bucket, silverPrefix)` cho phép kiểm thử các tương tác S3/MinIO sống động mà không phụ thuộc vào daemon mạng bên ngoài.

---

## 3. Bằng Chứng Dữ Liệu Thật (DAT-01 Real Samples)

Đã tích hợp và kiểm thử end-to-end với dữ liệu thật từ DAT-01 catalog:
- **USGS Real Sample**: File GeoJSON thật gồm **16 trận động đất** trong cửa sổ dữ liệu `[2023-01-01T00:00:00Z, 2023-01-04T00:00:00Z)` (SHA-256: `8667f9b7ac02ce0e88c78767bd51dee1fdf1292ac987a7cdc00c3aaec6b0545e`).
- **JMA Sample 2023**: Định dạng fixed-width 96-byte hypocenter của JMA cho tháng 01/2023 (gồm 1 bản ghi khớp với USGS trận động đất Iwai và 1 bản ghi JMA độc lập tại Tokyo Bay).

### Kết Quả Đối Soát & 5 Phương Trình Cân Bằng (`SilverRunReconciliationReport`):
- **Tổng Parsed**: 18 bản ghi (16 USGS + 2 JMA).
- **Tổng Valid**: 18 bản ghi; **Tổng Reject**: 0.
- **Tổng Current**: 18 bản ghi; **Duplicate**: 0; **Superseded**: 0.
- **Liên Kết Đa Nguồn**:
  - Cặp ứng viên đánh giá: 16 cặp.
  - Liên kết được chấp nhận (`ACCEPTED`): 1 liên kết (USGS `us7000j1n9` khớp với JMA `0360054/01395400`).
  - Liên kết mơ hồ (`AMBIGUOUS`): 0.
- **Sự Kiện Chuẩn (Canonical Events)**: 17 sự kiện canonical:
  - 1 sự kiện khớp (`MATCHED` — có cả USGS và JMA).
  - 15 sự kiện chỉ có USGS (`USGS_ONLY`).
  - 1 sự kiện chỉ có JMA (`JMA_ONLY`).
- **5 Phương Trình Cân Bằng Đạt Tuyệt Đối**:
  1. `Parsed = Valid + Rejects` (18 = 18 + 0).
  2. `Valid = Current + Duplicate + Superseded` (18 = 18 + 0 + 0).
  3. `Canonical = Matched + UsgsOnly + JmaOnly` (17 = 1 + 15 + 1).
  4. `Bridge Count = Observations Current` (18 = 18).
  5. `Reconciliation Balanced = true` (Toàn bộ dữ liệu cân bằng tuyệt đối không rò rỉ).

---

## 4. Bằng Chứng Live Readback Từ MinIO/S3 Storage

Kết quả chạy kiểm thử `testLiveMinioStorageReadbackForObservationsLinksAndMemberships`:
- **MinIO Bucket**: `earthquake-lake`, **Prefix**: `silver`.
- **Observations Parquet**:
  - `s3://earthquake-lake/silver/source_observation/event_year_utc=2023/event_month_utc=09/source_system=USGS/part-00000-run-slv09-minio-live.parquet`: 1 dòng, đọc ngược và kiểm tra schema thành công.
  - `s3://earthquake-lake/silver/source_observation/event_year_utc=2023/event_month_utc=09/source_system=JMA_BULLETIN/part-00000-run-slv09-minio-live.parquet`: 2 dòng, đọc ngược và kiểm tra schema thành công.
- **Source Link Parquet**:
  - `s3://earthquake-lake/silver/source_link/run_id=run-slv09-minio-live/part-00000-run-slv09-minio-live.parquet`: 1 bản ghi link được ghi và xác thực schema `SOURCE_LINK_PARQUET_SCHEMA`.
- **Canonical Membership Parquet**:
  - `s3://earthquake-lake/silver/canonical_membership/run_id=run-slv09-minio-live/part-00000-run-slv09-minio-live.parquet`: 3 bản ghi membership được ghi và xác thực schema `CANONICAL_MEMBERSHIP_PARQUET_SCHEMA`.
- **Publish Markers**:
  - Cả 4 phân vùng đều có marker `_SUCCESS` với timestamp UTC và số dòng tương ứng.

---

## 5. Bằng Chứng Handoff Sang Gold (`GoldEventTransformer`)

Dữ liệu Silver sau khi được ghi ra đĩa/storage được Spark đọc ngược lại:
```java
Dataset<Row> observationsDf = spark.read().parquet(obsPaths);
Dataset<Row> linksDf = spark.read().parquet(linkFile);
Dataset<Row> membershipsDf = spark.read().parquet(membershipFile);
Dataset<Row> regionsDf = spark.createDataFrame(List.of(), GoldEventTransformer.REGION_SCHEMA);

GoldTransformationResult goldResult = new GoldEventTransformer().transform(
    observationsDf, membershipsDf, linksDf, regionsDf, goldContext, EXEC_TIME);
```

**Kết Quả Biến Đổi Gold Thành Công Tuyệt Đối (0 Lỗi)**:
- `currentObservationCount`: 18
- `canonicalEventCount`: 17
- `bridgeRowCount`: 18
- `eventCurrent().count()`: 17
- Sự kiện matched: `canonical_source_system = "JMA_BULLETIN"` (JMA được ưu tiên theo contract), `source_coverage_code = "USGS_JMA"`, `link_status = "MATCHED"`.
- 15 sự kiện USGS solo: `canonical_source_system = "USGS"`, `source_coverage_code = "USGS_ONLY"`, `link_status = "SINGLE_SOURCE"`.
- 1 sự kiện JMA solo: `canonical_source_system = "JMA_BULLETIN"`, `source_coverage_code = "JMA_ONLY"`, `link_status = "SINGLE_SOURCE"`.

---

## 6. Kết Quả Kiểm Thử Toàn Bộ Module (`mvn test -pl spark`)

```text
[INFO] Results:
[INFO] Tests run: 232, Failures: 0, Errors: 0, Skipped: 0
[INFO] BUILD SUCCESS
```
Toàn bộ **232 bài kiểm thử** trong module `spark` đều vượt qua (100% pass), bao gồm:
- 6/6 tests `SilverMultiSourceIntegrationTest`
- 10/10 tests `SilverParquetWriterTest`
- 6/6 tests `GoldEventTransformerTest`
- 12/12 tests `SilverEntityResolverTest`
- 13/13 tests `SourceDedupTransformerTest`
- 30/30 tests `JmaFixedWidthParserTest`
- 12/12 tests `UsgsGeoJsonParserTest`
