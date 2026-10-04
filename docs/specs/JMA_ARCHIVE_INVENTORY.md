---
contract_id: "JMA-01"
contract_version: "1.0"
status: "Verified"
verified_on: "2026-10-04"
inventory_version: "1.0"
baseline_start_year: 1984
baseline_end_year: 2023
calendar_years: 40
archive_entries: 41
native_timezone: "Asia/Tokyo"
record_length_bytes: 96
geodetic_datum: "Japanese Geodetic Datum 2000"
---

# JMA hypocenter archive inventory và source-format contract

Tài liệu này khóa danh sách đầu vào JMA cho `JMA-02`, `JMA-03`, `DAT-01` và
`SLV-03`. Inventory mô tả archive cần tải; HTML index chỉ dùng để phát hiện
link/update và không được scrape thành event rows.

## 1. Phạm vi đã khóa

| Thuộc tính | Quyết định |
|---|---|
| Source system | `JMA_BULLETIN` |
| Dataset | The Seismological Bulletin of Japan — Hypocenters |
| Baseline | 40 năm hoàn chỉnh `1984–2023`, tính theo JST |
| Native interval | `[1984-01-01T00:00:00+09:00, 2024-01-01T00:00:00+09:00)` |
| Index chính thức | `https://www.data.jma.go.jp/eqev/data/bulletin/hypo.html` |
| Archive | ZIP, một member fixed-width không có phần mở rộng |
| Record | Một hypocenter trên một record dài đúng 96 byte, không tính line ending |
| Native time | JST, `Asia/Tokyo`, UTC+09:00, không daylight saving |
| Datum | Japanese Geodetic Datum 2000 (`JGD2000`) |
| Revision check | Ít nhất hằng tuần; xác nhận cuối bằng SHA-256 sau download |

Baseline là dải cố định, không phải rolling window. Khi JMA xuất bản năm mới,
việc append cần một thay đổi inventory được review; không tự bỏ năm 1984 để
duy trì con số “40 năm”.

## 2. Inventory máy đọc được

File chuẩn là
[`config/jma/hypocenter_archives_v1.csv`](../../config/jma/hypocenter_archives_v1.csv).
Nó có 41 dòng archive cho 40 năm vì năm 1997 được JMA chia thành hai file:

| Khoảng JST | Archive | Era |
|---|---|---|
| `1984-01-01` đến trước `1997-01-01` | `hYYYY.zip` | `LEGACY` |
| `1997-01-01` đến trước `1997-10-01` | `h199701.zip` | `LEGACY` |
| `1997-10-01` đến trước `1998-01-01` | `h199710.zip` | `UNIFIED` |
| `1998-01-01` đến trước `2024-01-01` | `hYYYY.zip` | `UNIFIED` |

Mốc `1997-10-01` phản ánh thay đổi quy trình JMA khi dữ liệu của các tổ chức
hợp tác bắt đầu được phân tích cùng dữ liệu JMA. Không ghép hai file 1997 trước
khi đã lưu từng ZIP nguyên bản và lineage riêng.

### 2.1. Ý nghĩa các cột

| Cột | Ý nghĩa |
|---|---|
| `inventory_version` | Version contract; thay đổi dòng/semantics phải tăng version |
| `year`, `segment` | Partition logic; chỉ 1997 có `jan-sep` và `oct-dec` |
| `native_start_jst`, `native_end_jst` | Cửa sổ half-open của archive theo JST |
| `archive_name` | Tên ZIP chính thức trên JMA |
| `member_name` | Member duy nhất dự kiến trong ZIP, bằng tên archive bỏ `.zip` |
| `source_url` | URL trực tiếp; downloader không tự nối tên ngoài inventory |
| `observed_release_hint` | Hint từ `Last-Modified`, không phải định danh cuối cùng |
| `observed_last_modified_utc` | Header quan sát tại lần verify inventory |
| `observed_content_length_bytes` | `Content-Length` quan sát; dùng phát hiện drift sớm |
| `media_type` | Phải là `application/zip` |
| `record_format` | Contract `jma-hypocenter-96-byte-v1` |
| `catalog_era` | `LEGACY` hoặc `UNIFIED` để audit thay đổi catalog |

Các giá trị `observed_*` là snapshot ngày `2026-10-04`, không phải cam kết JMA
sẽ giữ file bất biến.

## 3. URL và nguyên tắc tải

Downloader phải đọc đúng `source_url` từ inventory. Pattern hiện tại là:

```text
https://www.data.jma.go.jp/eqev/data/bulletin/data/hypo/hYYYY.zip
```

Ngoại lệ duy nhất trong baseline là:

```text
1997-01-01..1997-10-01  -> h199701.zip
1997-10-01..1998-01-01  -> h199710.zip
```

Yêu cầu khi tải:

1. Đọc index/update/errata để phát hiện thông báo nguồn.
2. Gửi request đến URL chính xác trong inventory và lưu HTTP status,
   `Content-Type`, `Content-Length`, `Last-Modified`, `ETag` nếu sau này có,
   `retrieved_at_utc` và final URL sau redirect.
3. Stream ZIP vào staging; không log hoặc đưa bytes archive vào metadata.
4. Tính SHA-256 trên đúng bytes đã nhận, mở ZIP và kiểm tra member dự kiến.
5. Chỉ JMA Bronze writer mới publish archive cùng manifest `BronzeReady`.

HTML index không phải catalog dữ liệu sự kiện. Không parse text trên trang
`hypo.html` thành observation và không tải toàn bộ 40 năm vào một process.

## 4. Catalog release và phát hiện file sửa

JMA giữ URL theo năm nhưng có thể thay bytes tại cùng URL. Vì vậy `year` hoặc
`archive_name` không đủ làm version.

### 4.1. Hai tầng phát hiện thay đổi

| Tầng | Metadata | Quyết định |
|---|---|---|
| Preflight | URL, HTTP status, `Last-Modified`, `Content-Length`, `ETag` nếu có | Bất kỳ khác biệt nào là `revision_candidate` và phải download lại |
| Definitive | SHA-256 của ZIP sau download | SHA khác là release mới; SHA giống là idempotent reuse |

Tại lần verify `2026-10-04`, JMA trả `Last-Modified` và `Content-Length` nhưng
không trả `ETag` cho các archive đại diện. Downloader vẫn phải lưu `ETag` nullable
để tương thích nếu server bổ sung sau này.

Header không được dùng thay SHA-256: cache/proxy có thể giữ header cũ hoặc thay
header dù bytes không đổi. Ngược lại, định kỳ vẫn phải revalidate/download có
phạm vi để phát hiện trường hợp bytes đổi nhưng header không đổi.

### 4.2. Quy tắc `catalog_release`

Sau khi có bytes, tạo slug xác định:

```text
jma-lm-<last-modified-UTC-basic>-sha256-<12-hex-first>
```

Ví dụ hình thức:

```text
jma-lm-20251210T014153Z-sha256-a1b2c3d4e5f6
```

Nếu `Last-Modified` thiếu, thay phần `lm-...` bằng
`retrieved-<retrieved-at-UTC-basic>`. SHA-256 đầy đủ vẫn phải nằm trong manifest;
12 ký tự đầu chỉ dùng làm slug dễ đọc.

| Trường hợp | Hành vi |
|---|---|
| URL/header/SHA đều giống release đã verify | Reuse manifest/object đã verify |
| Header đổi nhưng SHA giống | Cập nhật observation metadata; không tạo raw duplicate |
| SHA đổi tại cùng URL | Tạo `catalog_release` mới, giữ release cũ và reprocess đúng year/segment |
| ZIP/member/record structure lỗi | Quarantine; không publish `BronzeReady` |
| Index thêm archive mới | Review rồi tăng inventory version trước khi ingest Core |

Update history ngày `2025-12-15` cho thấy JMA có thể bổ sung hoặc sửa dữ liệu
cũ, gồm archive 2022 và 2023. Vì vậy check hằng tuần phải đọc cả
`update.html` và `errata.html`, không chỉ nhìn danh sách năm.

## 5. Source-format contract

### 5.1. Container và record boundary

- Archive là ZIP và dự kiến có đúng một regular-file member.
- Member name khớp cột `member_name`, không có absolute path, `..` hoặc nested
  path.
- Mỗi dòng là một hypocenter record dài đúng 96 byte; `LF`/`CRLF` không thuộc
  96 byte.
- Validate theo byte trước khi decode text. Tài liệu JMA không chốt charset ở
  trang format, vì vậy parser không được mặc định toàn file là UTF-8.
- Blank trong fixed-width là giá trị thiếu theo format, không tự đổi thành `0`.

### 5.2. Các cột quan trọng

| Vị trí byte (1-based) | Nội dung |
|---|---|
| `01` | Record/agency: `J`, `U` hoặc `I` |
| `02–17` | Origin time từ year đến second, diễn giải trong JST |
| `18–21` | Sai số origin time |
| `22–32` | Latitude degree/minute và sai số |
| `33–44` | Longitude degree/minute và sai số |
| `45–52` | Depth và sai số |
| `53–58` | Hai magnitude cùng loại magnitude |
| `59` | Travel-time table code |
| `60` | Hypocenter evaluation code |
| `61` | Hypocenter auxiliary information/event category |
| `62` | Maximum seismic intensity |
| `63–64` | Damage và tsunami scale |
| `65–68` | Major/minor region numbers |
| `69–92` | Epicentral region name |
| `93–95` | Số station dùng xác định hypocenter |
| `96` | Hypocenter determination/quality flag |

Byte 01 mô tả cơ quan xác định hypocenter, không phải nguồn phân phối file:

- `J`: JMA.
- `U`: USGS.
- `I`: tổ chức quốc tế khác như ISC/IASPEI.

`source_system=JMA_BULLETIN` phải được giữ cho toàn archive, đồng thời parser
giữ riêng `agency_code`; không được gán mọi record là JMA-origin chỉ vì ZIP do
JMA công bố.

Byte 96 có các giá trị `K`, `S`, `k`, `s`, `A`, `a`, `N`, `F`. Parser phải giữ
raw flag; quality rules thuộc Silver và không được loại record tại Bronze.

## 6. Timezone, interval và datum

JMA công bố origin time bằng JST. Parser phải:

1. Parse native timestamp bằng zone cố định `Asia/Tokyo`.
2. Giữ `event_time_jst` hoặc native components cho audit.
3. Dẫn xuất `event_time_utc` bằng phép chuyển timezone rõ ràng.
4. Không dùng timezone của host/Airflow để diễn giải record.

Ví dụ biên:

```text
2023-12-31T23:59:00+09:00 -> 2023-12-31T14:59:00Z
```

Toạ độ dựa trên `JGD2000`. Bronze giữ nguyên datum trong manifest. Nếu Silver
chuyển hoặc coi gần tương đương một datum khác, quyết định đó phải được ghi rõ
trong parser contract; JMA-01 không âm thầm relabel thành WGS84.

## 7. Citation và metadata bắt buộc

JMA áp dụng Public Data License 1.0 cho nội dung website trừ khi có thông báo
quyền riêng. Khi dùng dữ liệu phải ghi nguồn; khi chỉnh sửa/biến đổi phải nói
rõ nội dung đã được xử lý và không trình bày như sản phẩm chính thức của JMA.

Citation tối thiểu cho raw archive:

```text
Source: Japan Meteorological Agency (JMA), The Seismological Bulletin of
Japan — Hypocenters, <source_url>, retrieved <retrieved_at_utc>,
catalog release <catalog_release>.
```

Citation cho Silver/Gold/report:

```text
Derived from Japan Meteorological Agency (JMA), The Seismological Bulletin
of Japan — Hypocenters, <source_url>; processed by the project.
```

Manifest/catalog phải giữ tối thiểu:

- `source_system`, dataset title, archive year/segment và inventory version.
- Index URL, archive URL, retrieval timestamp và final URL.
- `Last-Modified`, `ETag` nullable, content length, SHA-256 và catalog release.
- Native interval/timezone, datum, record format và citation text/version.
- Link đến JMA terms, update history và errata page.

Không sao chép logo JMA hoặc gán endorsement. Người dùng downstream vẫn phải
xem xét quyền của bên thứ ba nếu record/nội dung cụ thể có thông báo riêng.

## 8. Evidence đã kiểm tra

Ngày `2026-10-04`:

- 41/41 URL trong baseline trả HTTP `200`, `Content-Type: application/zip`,
  `Content-Length` dương và `Last-Modified` parse được.
- Bốn archive đại diện được tải vào thư mục tạm ngoài repository rồi xoá sau
  kiểm tra; không commit raw payload.

| Archive | Member | Record count quan sát | Record bytes | Agency quan sát |
|---|---|---:|---:|---|
| `h1984.zip` | `h1984` | 5,192 | 96 | `J` |
| `h199701.zip` | `h199701` | 39,951 | 96 | `J`, `U` |
| `h199710.zip` | `h199710` | 16,284 | 96 | `J`, `U` |
| `h2023.zip` | `h2023` | 257,020 | 96 | `J`, `U` |

Kiểm tra offline inventory/docs:

```bash
./scripts/check-jma-inventory.sh
```

Đối soát live bằng HEAD request, không tải 40 năm:

```bash
./scripts/check-jma-inventory.sh --live
```

Live check báo lỗi khi header drift. Đó là tín hiệu tạo
`revision_candidate`; JMA-02 vẫn phải download và so SHA-256 để kết luận release
mới.

## 9. Handoff

| Consumer | Sử dụng |
|---|---|
| `JMA-02` | Đọc CSV, HEAD/GET từng URL, tính SHA-256 và phát hiện release mới |
| `JMA-03` | Lưu ZIP nguyên bản cùng catalog release/manifest bất biến |
| `DAT-01` | Chọn đúng một archive đại diện, mặc định `h2023.zip`, để stage sample |
| `SLV-03` | Parse member 96-byte theo JST/JGD2000, giữ agency và quality flag |
| `ORC-02/03` | Kiểm tra catalog có phạm vi và chỉ kích hoạt year/segment đổi |

## 10. Nguồn chính thức

- [JMA Hypocenters index](https://www.data.jma.go.jp/eqev/data/bulletin/hypo.html)
- [JMA hypocenter file format](https://www.data.jma.go.jp/eqev/data/bulletin/data/format/fmthyp_e.html)
- [JMA hypocenter record format](https://www.data.jma.go.jp/eqev/data/bulletin/data/format/hypfmt_e.html)
- [JMA bulletin notes: JST và JGD2000](https://www.data.jma.go.jp/eqev/data/bulletin/readme_e.html)
- [JMA update history](https://www.data.jma.go.jp/eqev/data/bulletin/update_e.html)
- [JMA correction information](https://www.data.jma.go.jp/eqev/data/bulletin/errata.html)
- [JMA website terms of use](https://www.jma.go.jp/jma/en/copyright.html)
