# USGS request contract

| Thuộc tính | Giá trị |
|---|---|
| Task | `USG-01` |
| Trạng thái | Implemented |
| Source | USGS FDSN Event Web Service |
| Endpoint mặc định | `https://earthquake.usgs.gov/fdsnws/event/1/query` |
| Timezone request | UTC, có hậu tố `Z` |
| Response format | `geojson` |

## 1. Mục đích

Contract này là giao diện chung cho daily ingest, backfill và task HTTP client.
Nó tách việc tính cửa sổ và kiểm tra cấu hình khỏi network I/O, nhờ đó request
được tái lập bằng unit test offline trước khi `USG-02` thêm retry và pagination.

Java implementation nằm dưới
`vn.edu.uit.ie212.earthquake.spark.usgs`:

- `UsgsRequestConfig` đọc và validate environment.
- `UsgsRequestBuilder` tạo daily/backfill plan, không gọi mạng.
- `UsgsRequestPlan` giữ target window, query window và các chunk liên tiếp.
- `UsgsRequest` giữ URI, half-open window, `limit` và `offset` của một request.

## 2. Cấu hình bắt buộc

| Biến | Default local | Ý nghĩa và guardrail |
|---|---:|---|
| `USGS_API_BASE_URL` | `https://earthquake.usgs.gov/fdsnws/event/1/query` | HTTPS URI, không query/fragment/credential |
| `USGS_MIN_LATITUDE` / `USGS_MAX_LATITUDE` | `20.0` / `50.0` | ROI Nhật Bản, `-90 <= min <= max <= 90` |
| `USGS_MIN_LONGITUDE` / `USGS_MAX_LONGITUDE` | `120.0` / `155.0` | ROI Nhật Bản, `-180 <= min <= max <= 180` |
| `USGS_SEED_START_UTC` | `2023-01-01T00:00:00Z` | Mốc sớm nhất được phép query, phải có hậu tố `Z` |
| `PIPELINE_OVERLAP_DAYS` | `3` | Số ngày UTC hoàn chỉnh đọc lại cho revision, từ `0` đến `31` |
| `USGS_MAX_WINDOW_DAYS` | `3` | Kích thước tối đa mỗi request, từ `1` đến `31` |
| `USGS_REQUEST_LIMIT` | `20000` | `limit` gửi cho USGS, từ `1` đến `20000` |
| `USGS_HTTP_TIMEOUT_MS` | `30000` | Timeout dành cho `USG-02`, từ `1000` đến `300000` ms |
| `USGS_EVENT_TYPE` | `earthquake` | Chỉ lấy event type `earthquake` |

`PIPELINE_OVERLAP_DAYS` là biến overlap duy nhất; không tạo thêm biến revision
trùng nghĩa. `STRONG_MAGNITUDE_THRESHOLD` chỉ phục vụ KPI, không được đưa vào
request USGS vì tầng Bronze phải giữ đầy đủ earthquake trong ROI.

## 3. Khoảng thời gian

Mọi khoảng thời gian trong code là half-open `[start, end)`, đều ở UTC.
USGS API biểu diễn `endtime` là inclusive nên builder gửi `end - 1 ms` trong
URI. Như vậy hai chunk liền nhau không bị lấy trùng hoặc tạo gap ở ranh giới.

### Daily run

Với run lúc `2026-09-16 07:15 Asia/Ho_Chi_Minh` (`2026-09-16T00:15:00Z`):

- target bắt buộc: `[2026-09-15T00:00:00Z, 2026-09-16T00:00:00Z)`;
- query revision: `[2026-09-13T00:00:00Z, 2026-09-16T00:00:00Z)`;
- `starttime=2026-09-13T00:00:00Z`;
- `endtime=2026-09-15T23:59:59.999Z`.

Builder lấy ngày UTC hiện tại của `runAt`, lùi một ngày để chọn target, rồi lùi
`PIPELINE_OVERLAP_DAYS` ngày từ `targetEnd`. Nếu target/query chạm trước
`USGS_SEED_START_UTC`, khoảng đó được clip tại seed.

### Backfill

`buildBackfillPlan(targetStart, targetEnd)` nhận target half-open đã ở UTC và
không được bắt đầu trước seed. Query start cũng lùi overlap và clip tại seed.
Nếu query dài hơn `USGS_MAX_WINDOW_DAYS`, builder chia thành các chunk liên
tiếp đến đúng `queryEndExclusiveUtc`; không có gap giữa các chunk. Pagination
theo `limit`/`offset` không thuộc task này và do `USG-02` xử lý.

## 4. Query parameters cố định

Mỗi URI có các tham số sau:

```text
format=geojson
eventtype=earthquake
starttime=<UTC start>
endtime=<UTC exclusive end - 1 ms>
minlatitude=20.0
maxlatitude=50.0
minlongitude=120.0
maxlongitude=155.0
orderby=time-asc
limit=20000
offset=0
```

Builder không thêm `minmagnitude`, không lọc theo KPI và luôn đặt `offset=0` cho
request đầu tiên của mỗi chunk. URI được tạo theo thứ tự tham số ổn định để log,
fixture và test có thể so sánh deterministically.

## 5. Kiểm thử và ranh giới

Unit test `UsgsRequestBuilderTest` kiểm tra daily overlap, seed clipping,
backfill chunk không gap, input trước seed, endpoint/bounds/seed/limit không hợp
lệ và sự vắng mặt của `minmagnitude`. Test không gọi mạng.

USG-01 không triển khai HTTP client, retry/backoff, response validation, Bronze
writer, manifest hoặc Airflow task. Các phần đó lần lượt thuộc `USG-02`,
`USG-03` và `USG-04`; `USG-05` sẽ tích hợp kiểm thử đến Bronze.

Chạy test:

```bash
./mvnw --batch-mode --no-transfer-progress -pl spark -am test
```

Tham chiếu API chính thức: [USGS Earthquake Catalog API](https://earthquake.usgs.gov/fdsnws/event/1/).
