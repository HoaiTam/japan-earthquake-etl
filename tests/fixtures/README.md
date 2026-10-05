# Shared source fixtures

Bộ fixture `CON-04` cung cấp input nhỏ, xác định và chạy hoàn toàn offline cho
USGS Bronze, JMA Bronze, parser Silver, quality, dedup/revision, source linking
và Gold. Payload dưới `usgs/` và `jma/` đều là **synthetic**, không sao chép
event thật và không chứa credential hay endpoint riêng tư. Thư mục
`real-samples/` chỉ giữ catalog metadata `DAT-01`; raw sample thật nằm trong
MinIO/staging và không được commit.

Expected output của từng case nằm trong hai dạng:

- [`cases.json`](./cases.json): contract máy đọc được cho test harness.
- [`TEST_MATRIX.md`](./TEST_MATRIX.md): bảng dành cho người phát triển/reviewer.

## 1. Cấu trúc

```text
tests/fixtures/
├── cases.json
├── TEST_MATRIX.md
├── SHA256SUMS
├── real-samples/
│   └── catalog.json
├── usgs/
│   ├── success.geojson
│   ├── empty.geojson
│   ├── invalid-fields.geojson
│   ├── invalid-json.geojson
│   ├── duplicate.geojson
│   ├── revision-v1.geojson
│   ├── revision-v2.geojson
│   ├── timezone-boundary.geojson
│   ├── ambiguous.geojson
│   └── checksum-mismatch.sha256
└── jma/
    ├── fixed-width/*.hyp
    ├── archives/*.zip
    └── checksum-mismatch.sha256
```

`SHA256SUMS` chứa checksum đúng để phát hiện fixture bị thay đổi ngoài ý muốn.
Hai file `checksum-mismatch.sha256` cố ý chứa digest toàn số `0`; test Bronze
phải phát hiện mismatch và không tạo `BronzeReady`.

## 2. Chạy kiểm tra

Từ repository root:

```bash
./scripts/check-shared-fixtures.sh
```

Checker cần `jq`, `unzip`, `rg` và một trong `sha256sum`/`shasum`. Build script
cần thêm `zip`; không có bước nào gọi mạng.

Script kiểm tra:

- schema tối thiểu của `cases.json` và coverage case bắt buộc cho cả hai nguồn;
- GeoJSON hợp lệ, cùng trường hợp malformed JSON có chủ đích;
- JMA record hợp lệ dài đúng 96 byte, invalid fixture dài 95 byte;
- ZIP mở được, chỉ chứa `hypo.dat` và bytes khớp fixed-width source;
- checksum chuẩn không drift và checksum mismatch thực sự sai;
- không có chuỗi giống credential trong fixture.

Catalog metadata thật được kiểm tra riêng bằng:

```bash
./scripts/check-real-sample-catalog.sh
```

Không dùng `real-samples/catalog.json` thay cho fixture unit test: URI trong
catalog là handoff cho integration có MinIO, không phải yêu cầu gọi network
hoặc tải raw về source tree.

Sau khi chủ động sửa dữ liệu nguồn trong bộ fixture, tái tạo JMA files, ZIP và
checksum bằng:

```bash
./scripts/build-shared-fixtures.sh
./scripts/check-shared-fixtures.sh
```

Không chạy build script như một phần của unit test. Unit test chỉ đọc fixture đã
commit; build script dành cho maintainer khi thay đổi contract fixture.

## 3. Quy ước USGS

- Mọi payload hợp lệ có `type=FeatureCollection`, `metadata.count` bằng số
  `features` và dùng URL reserved `example.invalid`.
- `properties.time`/`updated` là epoch milliseconds UTC.
- `geometry.coordinates` theo đúng thứ tự longitude, latitude, depth.
- `revision-v1` và `revision-v2` dùng cùng `id`; v2 có `updated` mới hơn.
- `timezone-boundary` có một event trước `window_end_utc` đúng 1 ms và một
  event đúng tại biên exclusive để test half-open interval.
- `invalid-fields` vẫn là JSON/GeoJSON có cấu trúc hợp lệ để Bronze chấp nhận;
  Silver mới reject từng record theo reason code.
- `invalid-json` lỗi ở tầng cấu trúc Bronze và không được có manifest
  `BronzeReady`.

## 4. Quy ước JMA

Mỗi dòng hợp lệ là 96 byte ASCII **không tính LF**. ZIP chỉ chứa một entry tên
`hypo.dat`. Các cột cần cho parser contract:

| Cột | Nội dung |
|---|---|
| 01 | Agency code |
| 02–17 | Origin time JST: year đến second |
| 22–28 | Latitude degree/minute |
| 33–40 | Longitude degree/minute |
| 45–49 | Depth |
| 53–58 | Magnitude 1/2 và type |
| 61 | Event subsidiary information |
| 62 | Maximum intensity |
| 66–92 | Region number/name |
| 96 | Hypocenter determination flag |

Giá trị thập phân dùng implied decimal theo format JMA, ví dụ second `5678`
là `56.78`, latitude minute `4020` là `40.20`, depth `01000` là `10.00` km
và magnitude `52` là `5.2`.

`timezone-boundary.hyp` chứa ba case: cuối năm 2023 JST, ngay trước và đúng tại
mốc `1997-10-01 00:00:00 JST` để kiểm tra UTC conversion cùng
`LEGACY`/`UNIFIED`. `success.hyp` có một natural earthquake và một artificial
event; cả hai là observation hợp lệ nhưng chỉ natural earthquake vào KPI mặc
định.

Format được đối chiếu với tài liệu chính thức:

- [JMA hypocenter file format](https://www.data.jma.go.jp/eqev/data/bulletin/data/format/fmthyp_e.html)
- [JMA hypocenter record format](https://www.data.jma.go.jp/eqev/data/bulletin/data/format/hypfmt_e.html)

## 5. Cách dùng theo module

| Consumer | Cách dùng |
|---|---|
| USGS/JMA Bronze | Đọc raw file/ZIP, inject checksum sidecar khi test verify |
| Silver parser | Đọc GeoJSON hoặc fixed-width trực tiếp, assert theo `cases.json` |
| Quality | Đối chiếu counts và `reason_codes`; không coi duplicate/revision là parse reject |
| Source linking | Chạy `FX-LINK-01`; candidate mơ hồ phải giữ riêng |
| Gold/BI | Dùng expected canonical count, natural count và boundary assertions |

Test không được sửa trực tiếp file dùng chung. Nếu cần test corruption, copy
fixture vào temporary directory trước rồi thay đổi bản copy.

## 6. Contract áp dụng

- [CON-01 source coverage](../../docs/specs/SOURCE_COVERAGE.md)
- [CON-02 Bronze storage](../../docs/specs/BRONZE_STORAGE_CONTRACT.md)
- [CON-03 Silver/Gold model](../../docs/specs/SILVER_GOLD_DATA_MODEL.md)
- [CON-04 task tracking](../../docs/task/tasks/CON-04.md)
