---
contract_id: "DAT-01"
contract_version: "1.0"
status: "Verified"
verified_on: "2026-10-04"
catalog_id: "dat01-real-samples-v1"
---

# Shared real-sample catalog cho USGS và JMA

Tài liệu này khóa hai input thật nhỏ dùng cho integration tuần 3. Metadata máy
đọc được nằm tại
[`tests/fixtures/real-samples/catalog.json`](../../tests/fixtures/real-samples/catalog.json).
Raw payload không nằm trong Git: USGS đã ở Bronze MinIO, còn JMA nằm trong
namespace `_staging` và chưa phải `BronzeReady`.

## 1. Mục đích và ranh giới

Catalog giúp các nhóm dùng cùng source identity, checksum và expected count mà
không tải lại nguồn trong mỗi unit test. Nó không phải ground truth khoa học,
benchmark hiệu năng hoặc full historical dataset.

| Loại input | Dùng cho | Có gọi mạng trong test | Có raw trong Git |
|---|---|---:|---:|
| Synthetic fixture `CON-04` | Unit/component test hằng ngày | Không | Có, kích thước nhỏ và do project tạo |
| Real sample `DAT-01` | Integration có chủ đích cuối tuần | Không, đọc object đã stage | Không |
| Source live | Smoke/re-ingest do operator chủ động chạy | Có | Không |

Consumer không được thay real sample bằng network call ngầm. Nếu MinIO local
không có object, unit test tiếp tục dùng `synthetic_fixture_hint`; integration
test phải báo thiếu sample thay vì tải một payload mới dưới cùng identity.

## 2. Hai sample đã khóa

| Sample | Trạng thái | Khoảng dữ liệu | Raw identity | Size | Count |
|---|---|---|---|---:|---:|
| USGS fixed window | `BRONZE_READY` | `[2023-01-01T00:00:00Z, 2023-01-04T00:00:00Z)` | Manifest và raw object của run `usg06-live-20261004T121205Z-db580aece6dc` | 11,771 byte | 16 event |
| JMA `h2023.zip` | `STAGED_SOURCE` | Năm 2023 theo JST | ZIP release `jma-lm-20251210T014153Z-sha256-e5ced2bf7275` | 6,977,812 byte | 257,020 record |

### 2.1. USGS `BRONZE_READY`

```text
s3://japan-earthquake/bronze/usgs/ingest_date=2023-01-01/
  run_id=usg06-live-20261004T121205Z-db580aece6dc/attempt=01/
  ├── response.geojson
  └── manifest.json
```

Manifest được tạo bởi `USG-06`. Readback ngày `2026-10-04` xác nhận
`BronzeReady`, raw length `11771`, SHA-256
`8667f9b7ac02ce0e88c78767bd51dee1fdf1292ac987a7cdc00c3aaec6b0545e`
và `record_count_estimate=16`.

Consumer Bronze/Silver phải bắt đầu từ `manifest_uri`, không scan prefix hoặc
đọc `response.geojson` bằng wildcard. Manifest là commit point của sample này.

### 2.2. JMA `STAGED_SOURCE`

Archive được chọn là `h2023.zip` vì nằm trong catalog era `UNIFIED`, gần phần
extension của nghiên cứu và đủ đại diện để kiểm tra ZIP/fixed-width. Source URL,
`Last-Modified` và size khớp inventory `JMA-01`.

```text
s3://japan-earthquake/bronze/_staging/jma/ingest_date=2026-10-04/
  run_id=dat01-jma-2023-e5ced2bf7275/attempt=01/archive.zip
```

Archive có đúng một member `h2023`, gồm 257,020 record; mọi record dài 96 byte
không tính line ending. SHA-256 của đúng ZIP nguồn là
`e5ced2bf7275825ba75405b071bb54e9d4c2a5eb55aa6bc9b8d670de1f58b98f`.
Object đã được đọc lại từ MinIO và khớp size/checksum.

`STAGED_SOURCE` có nghĩa:

- chưa có JMA `manifest.json`;
- không được resolver Silver coi là input hợp lệ;
- không được đổi nhãn thành `BRONZE_READY` bằng cách sửa catalog;
- `JMA-03` phải tự validate, ghi final immutable key, readback và upload manifest
  sau cùng theo Bronze contract.

## 3. Cách consumer sử dụng

| Consumer | Input được phép | Hành vi |
|---|---|---|
| `JMA-02` | JMA inventory và catalog metadata | So checksum/release; không cần tải lại sample để unit test |
| `JMA-03` | `staged_object_uri` JMA | Validate/write thành JMA Bronze; output final phải có manifest mới |
| `SLV-01/02` | `manifest_uri` USGS | Resolve chính xác raw object rồi đối soát SHA/count |
| `SLV-03` | Synthetic JMA fixture hằng ngày; JMA real sample khi integration | Stream unzip và parse 96 byte, không sửa staged ZIP |
| `SLV-04/05/08` | Observation/output của parser | Đối soát input/parsed/valid/rejected/output theo sample ID |

`synthetic_fixture_hint` chỉ là fixture cùng loại format, không phải bản sao và
không có cùng event/count/checksum với real sample.

## 4. Kiểm tra lặp lại

Kiểm tra catalog hoàn toàn offline:

```bash
./scripts/check-real-sample-catalog.sh
```

Đọc lại hai object từ MinIO đang chạy:

```bash
DAT01_AIRFLOW_CONTAINER=japan-earthquake-etl-airflow-api-server-1 \
  ./scripts/verify-real-samples.sh
```

Hoặc dùng Compose cùng file cấu hình local:

```bash
ENV_FILE=/absolute/path/to/local.env ./scripts/verify-real-samples.sh
```

Verifier chỉ in sample ID, state, byte/count và checksum result; không in raw
payload hoặc credential. Nó kiểm tra USGS manifest/raw và mở JMA ZIP trực tiếp
từ MinIO. Tên container là runtime selector, không được ghi vào catalog.

## 5. Quy tắc thay đổi sample

1. Không sửa object dưới cùng sample identity.
2. Payload hoặc checksum đổi phải tạo `sample_id` và catalog version mới.
3. USGS run mới không tự thay sample fixed hiện tại.
4. JMA source cùng năm đổi SHA-256 phải tạo `catalog_release` mới.
5. Giữ catalog cũ khi cần tái lập integration evidence đã công bố.
6. Không đưa presigned URL, credential, absolute path cá nhân hoặc raw payload
   vào catalog/PR.

## 6. Nguồn contract

- [USGS live Bronze runbook](./USGS_LIVE_BRONZE_RUNBOOK.md)
- [JMA archive inventory](./JMA_ARCHIVE_INVENTORY.md)
- [Bronze storage contract](./BRONZE_STORAGE_CONTRACT.md)
- [Shared synthetic fixtures](../../tests/fixtures/README.md)
- [DAT-01 task](../task/tasks/DAT-01.md)
