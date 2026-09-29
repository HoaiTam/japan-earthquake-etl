---
contract_id: "CON-01"
contract_version: "1.0"
status: "Review"
verified_on: "2026-09-29"
study_area_id: "japan-regional-v1"
roi_min_latitude: 20.0
roi_max_latitude: 50.0
roi_min_longitude: 120.0
roi_max_longitude: 155.0
jma_baseline_start_year: 1984
jma_baseline_end_year: 2023
usgs_seed_start_utc: "2023-01-01T00:00:00Z"
usgs_revision_window_days: 3
---

# Source coverage contract: USGS và JMA

Tài liệu này là nguồn quyết định chính thức về vai trò, phạm vi thời gian, vùng
nghiên cứu, múi giờ, độ trễ và phần giao nhau giữa USGS và JMA. Các task ingest,
Silver, Gold, orchestration và BI không được tự thay đổi các giá trị này.

## 1. Quyết định tóm tắt

| Nội dung | Quyết định |
|---|---|
| Vùng nghiên cứu | Envelope `20.0°N–50.0°N`, `120.0°E–155.0°E` |
| USGS | Nguồn cập nhật hằng ngày, seed từ `2023-01-01T00:00:00Z`, không đặt ngày kết thúc |
| JMA | Baseline lịch sử cố định 40 năm `1984–2023`, tính theo năm JST |
| Múi giờ lưu chuẩn | UTC; đồng thời dẫn xuất JST bằng `Asia/Tokyo` |
| Múi giờ điều phối | `Asia/Ho_Chi_Minh`; không dùng để diễn giải event time |
| USGS target window | Ngày UTC hoàn chỉnh liền trước, dạng `[start, end)` |
| USGS revision window | Ba ngày UTC hoàn chỉnh gần nhất, có thể cấu hình nhưng không được âm thầm đổi default |
| JMA refresh | Kiểm tra index/update metadata ít nhất hằng tuần; ingest khi file mới hoặc checksum thay đổi |
| Cross-source overlap | Năm 2023 để kiểm thử source linking và bảo đảm chuyển tiếp không có gap |
| Canonical count | Một sự kiện vật lý chỉ được đếm một lần; observation của cả hai nguồn vẫn được giữ |

## 2. Vùng nghiên cứu

### 2.1. Envelope thống nhất

Một observation thuộc vùng nghiên cứu khi tâm chấn thỏa đồng thời:

```text
20.0 <= latitude  <= 50.0
120.0 <= longitude <= 155.0
```

Envelope rộng hơn lãnh thổ đất liền để giữ các sự kiện ngoài khơi, rãnh Nhật
Bản, quần đảo Ryukyu và vùng lân cận có ý nghĩa phân tích. Đây là ranh giới kỹ
thuật của dataset, không phải tuyên bố về biên giới hành chính hoặc vùng chủ
quyền.

### 2.2. Cách áp dụng

- USGS query dùng rectangle tương ứng. `USG-01` phải kiểm tra hành vi tại biên
  của API và có thể nới request rất nhỏ nếu cần; Silver vẫn post-filter theo
  envelope chính thức ở trên.
- JMA archive được lưu nguyên bản ở Bronze. Sau khi parse, Silver mới đánh dấu
  observation trong/ngoài envelope.
- Không dùng polygon đất liền để loại sự kiện. Event không match địa giới vẫn
  tồn tại với phân loại `Offshore` hoặc `Unknown` ở downstream.
- Không đặt tọa độ giả cho record thiếu hoặc sai tọa độ.

## 3. Hợp đồng nguồn USGS

| Thuộc tính | Giá trị |
|---|---|
| Source system | `USGS` |
| Endpoint family | FDSN Event Web Service `/fdsnws/event/1/query` |
| Vai trò | Operational/daily source và nguồn hiện hành khi JMA chưa phát hành archive tương ứng |
| Định dạng Bronze | GeoJSON response nguyên bản và request/response metadata |
| Thời gian nguồn | ISO 8601; luôn gửi timezone `Z` rõ ràng |
| Temporal coverage Core | `[2023-01-01T00:00:00Z, +∞)` |
| Tần suất | Một daily run sau khi ngày UTC trước đó kết thúc |
| Filter sự kiện | `eventtype=earthquake`; không đặt `minmagnitude` ở baseline |
| Vùng request | Rectangle của `japan-regional-v1` |

USGS mô tả `starttime` là “on or after” và `endtime` là “on or before”. Trong
project, run context luôn dùng cửa sổ half-open `[window_start_utc,
window_end_utc)`. `USG-01` phải xử lý khác biệt ở biên bằng tham số request
phù hợp và/hoặc post-filter; event đúng tại `window_end_utc` thuộc run kế tiếp.

Daily run lúc `07:15 Asia/Ho_Chi_Minh` tương đương `00:15 UTC`. Run xử lý ngày
UTC hoàn chỉnh liền trước và request ba ngày UTC hoàn chỉnh gần nhất để nhận
revision muộn. Ví dụ run ngày `2026-09-16` query `[2026-09-13T00:00:00Z,
2026-09-16T00:00:00Z)` nhưng target window vẫn là ngày `2026-09-15`.

USGS có thể cập nhật preferred origin, magnitude hoặc status. Bronze không ghi
đè response cũ; Silver giữ bản có `updated` mới nhất cho cùng USGS `id` theo
tie-break xác định tại `SLV-06`.

## 4. Hợp đồng nguồn JMA

| Thuộc tính | Giá trị |
|---|---|
| Source system | `JMA_BULLETIN` |
| Nguồn | The Seismological Bulletin of Japan — Hypocenters |
| Vai trò | Historical baseline và nguồn địa phương ưu tiên khi observation JMA đạt quality gate |
| Baseline Core | 40 năm hoàn chỉnh `1984–2023` |
| Native interval | `[1984-01-01T00:00:00+09:00, 2024-01-01T00:00:00+09:00)` |
| Native timezone | JST, `Asia/Tokyo`, UTC+09:00 và không có daylight saving |
| Định dạng Bronze | ZIP theo năm và manifest/version metadata; không scrape HTML thành event rows |
| Record | Hypocenter fixed-width, 96 byte/record |
| Datum | Japanese Geodetic Datum 2000 theo ghi chú của JMA |
| Tần suất | Initial backfill theo năm; sau đó kiểm tra file mới/thay đổi ít nhất hằng tuần |

Trang JMA hiện công bố file theo năm đến 2023 và có file riêng cho 1984, vì vậy
`1984–2023` là baseline 40 năm có thể tái lập. Đây là range cố định, không phải
cửa sổ rolling: khi có năm mới, nhóm có thể append sau review nhưng không được
tự bỏ năm đầu để vẫn giữ đúng “40 năm”.

JMA có thể sửa archive cũ. Update history ghi nhận dữ liệu từ tháng 4 đến tháng
12 năm 2023 được mở rộng vào ngày 15/12/2025. Vì vậy URL/năm không đủ làm
version; downloader phải lưu ETag/Last-Modified nếu có, size, SHA-256,
retrieval time và `catalog_release`.

JMA đổi quy trình phân tích từ `1997-10-01`. Downstream phải phân biệt hai era:

- `legacy`: `1984-01-01` đến `1997-09-30` theo JST.
- `unified`: từ `1997-10-01` theo JST.

Không được kết luận xu hướng dài hạn là thay đổi địa chấn thực chỉ từ chênh lệch
count giữa hai era nếu chưa đánh giá tác động của thay đổi catalog/phương pháp.

Ký tự agency ở cột đầu của JMA record cho biết cơ quan xác định (`J`, `U`, `I`,
...). `source_system=JMA_BULLETIN` mô tả nơi phân phối file, không được dùng để
khẳng định mọi record đều do JMA xác định.

## 5. Múi giờ và ranh giới ngày

| Khái niệm | Quy tắc |
|---|---|
| USGS event/request time | UTC với hậu tố `Z` |
| JMA origin time | Parse trong `Asia/Tokyo`, sau đó dẫn xuất UTC |
| Silver event partition | Dựa trên `event_time_utc`, không dựa trên ingest date/JST date |
| Hiển thị cho người dùng Nhật Bản | `event_time_jst`, có nhãn JST |
| Airflow schedule | `Asia/Ho_Chi_Minh`; resolve data interval sang UTC trước khi gọi source |

JMA baseline đổi sang UTC thành
`[1983-12-31T15:00:00Z, 2023-12-31T15:00:00Z)`. Không được parse chuỗi JMA như
UTC hoặc dùng timezone của host.

## 6. Phạm vi event và quy tắc bảo toàn

- Bronze giữ nguyên payload/archive nhận được, kể cả record sau này không đủ
  điều kiện đưa vào KPI.
- USGS request dùng `eventtype=earthquake` và không đặt magnitude floor để
  tránh bias ngầm theo độ lớn.
- JMA parser giữ mọi hypocenter record. Silver gắn event type/agency/quality;
  record artificial, eruption hoặc không xác định không được âm thầm trộn vào
  canonical earthquake count.
- Record thiếu magnitude vẫn được giữ nếu các field bắt buộc khác hợp lệ.
  Magnitude null không được đổi thành `0`.
- Các dataset JMA khác như arrival time, CMT, intensity và tsunami không thuộc
  deliverable `CON-01`; chúng chỉ được bổ sung bằng task/contract riêng.

## 7. Overlap và source priority

### 7.1. Hai loại overlap

1. **USGS revision overlap:** daily request gồm ba ngày UTC hoàn chỉnh gần nhất
   để bắt revision của cùng USGS event.
2. **Cross-source overlap:** USGS bắt đầu từ `2023-01-01T00:00:00Z`, còn JMA
   baseline bao phủ hết năm 2023 theo JST. Phần giao chuẩn hóa là
   `[2023-01-01T00:00:00Z, 2023-12-31T15:00:00Z)`.

Năm 2023 là tập kiểm chứng bắt buộc cho `SLV-07`; nó không được `UNION ALL` rồi
đếm trực tiếp.

### 7.2. Bảng quyết định

| Trường hợp | Quyết định canonical | Dữ liệu phải giữ |
|---|---|---|
| Chỉ có USGS | Tạo một canonical event từ USGS observation | USGS observation và revision lineage |
| Chỉ có JMA | Tạo một canonical event từ JMA observation hợp lệ | JMA observation, agency/quality và catalog release |
| Hai observation match 1–1 với confidence đạt ngưỡng | Tạo một canonical event, không cộng hai lần | Cả hai observation và source-link evidence |
| Nhiều candidate hoặc confidence không đạt | Không auto-merge; giữ event riêng và gắn `ambiguous` | Tất cả observation/candidate score |
| JMA record hợp lệ, agency `J`, latest release và quality gate đạt | JMA là primary observation cho thuộc tính hypocenter lịch sử | USGS fields/source URL vẫn giữ ở observation |
| JMA record không đạt quality gate hoặc chỉ là record agency khác | USGS là primary nếu USGS hợp lệ | JMA record vẫn giữ để audit |
| USGS có revision mới cùng `id` | Thay primary USGS observation bằng bản `updated` mới nhất | Các Bronze version và supersession lineage |
| JMA archive cùng năm đổi checksum | Tạo release mới và re-evaluate năm bị ảnh hưởng | Cả hai archive version và match report trước/sau |

Ngưỡng thời gian/khoảng cách/depth/magnitude dùng để tạo candidate thuộc
`SLV-07`. `CON-01` chỉ chốt hành vi: không match bằng tọa độ/thời gian chính xác
tuyệt đối, không dùng source ID của nguồn này làm ID của nguồn kia và ưu tiên
false negative hơn false positive khi match còn mơ hồ.

Source priority chọn primary observation, không xóa nguồn còn lại và không cho
phép Power BI tự lặp lại logic match. Field riêng của nguồn như USGS PAGER alert
hoặc JMA intensity/quality phải giữ provenance rõ ràng.

## 8. Freshness và thay đổi nguồn

| Nguồn | Freshness contract | Khi nguồn thay đổi |
|---|---|---|
| USGS | Target day hoàn chỉnh được lấy ở daily run sau `00:15 UTC`; không cam kết real-time | Revision trong ba ngày được daily run bắt lại; revision cũ hơn dùng backfill/reprocessing có phạm vi |
| JMA | Dùng latest archive đã công bố, không có SLA event-level | So sánh metadata/checksum; chỉ ingest release/năm thay đổi và reprocess partition liên quan |

Nếu source tạm không truy cập được, pipeline dùng Bronze đã verify cho
reprocessing hoặc fixture cho test. Không đánh dấu dữ liệu cũ là mới và không
tự chuyển sang nguồn khác mà mất provenance.

## 9. Trách nhiệm downstream

| Task | Phải kế thừa từ contract này |
|---|---|
| `CON-02` | Hai source namespace và metadata version/retrieval riêng |
| `CON-03` | UTC/JST, source observation, canonical event, agency, era và source coverage |
| `CON-04` | Fixture biên ROI, biên ngày UTC/JST, overlap 2023, revision và ambiguous match |
| `USG-01` | ROI, seed start, half-open window, three-day revision window và không magnitude floor |
| `JMA-01` | Range 1984–2023, year inventory, 1997 split và update/version metadata |
| `SLV-02/03` | Parse đúng native time và giữ source-specific fields |
| `SLV-06` | Dedup/revision trong từng nguồn trước source linking |
| `SLV-07` | Bảng quyết định overlap, no double count và không auto-merge ambiguous candidate |
| `GLD-01` | Chỉ đếm canonical event và giữ source coverage/provenance |
| `BI-02` | KPI đếm canonical event; cho phép lọc source coverage/era khi cần |

## 10. Acceptance scenarios

| ID | Input | Kết quả mong đợi |
|---|---|---|
| `SC-01` | USGS event ở `20.0N, 120.0E` | Được post-filter vào ROI nếu payload hợp lệ |
| `SC-02` | USGS event đúng `window_end_utc` | Thuộc run kế tiếp, không tính hai lần |
| `SC-03` | JMA record `2023-12-31 23:59 JST` | Đổi sang `2023-12-31 14:59 UTC`, thuộc baseline |
| `SC-04` | Hai source mô tả cùng event với sai khác nhỏ | Hai observations, một canonical event khi score đạt ngưỡng |
| `SC-05` | Hai candidate JMA gần một USGS event | Không auto-merge; đánh dấu ambiguous |
| `SC-06` | JMA archive 2023 có SHA-256 mới | Tạo catalog release mới, reprocess năm 2023 và giữ bản cũ |
| `SC-07` | JMA artificial event | Giữ observation/lineage nhưng không vào canonical natural-earthquake KPI mặc định |
| `SC-08` | Event hợp lệ ngoài polygon đất liền nhưng trong ROI | Giữ với region `Offshore` hoặc `Unknown` |

## 11. Quản lý thay đổi

Thay đổi ROI, baseline year, source role, timezone, revision window hoặc source
priority là contract change. Pull request phải cập nhật tài liệu này, task tiêu
thụ, fixture và nêu rõ có cần reprocess/backfill hay không. Không hard-code một
giá trị khác trong DAG/Java/Power BI.

## 12. Nguồn chính thức

- [USGS Earthquake Catalog API](https://earthquake.usgs.gov/fdsnws/event/1/)
- [JMA Hypocenters](https://www.data.jma.go.jp/eqev/data/bulletin/hypo_e.html)
- [JMA 震源データ](https://www.data.jma.go.jp/eqev/data/bulletin/hypo.html)
- [JMA bulletin notes](https://www.data.jma.go.jp/eqev/data/bulletin/readme_e.html)
- [JMA update history](https://www.data.jma.go.jp/eqev/data/bulletin/update_e.html)
- [JMA hypocenter file format](https://www.data.jma.go.jp/eqev/data/bulletin/data/format/fmthyp_e.html)
- [JMA hypocenter record format](https://www.data.jma.go.jp/eqev/data/bulletin/data/format/hypfmt_e.html)
