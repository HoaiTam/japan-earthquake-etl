# USGS HTTP client contract

| Thuộc tính | Giá trị |
|---|---|
| Task | `USG-02` |
| Trạng thái | Implemented |
| Input | `UsgsRequest` từ [USG-01 request contract](./USGS_REQUEST_CONTRACT.md) |
| Output | Raw bounded response page và HTTP metadata |
| Network | Chỉ gọi endpoint đã được cấu hình; unit test dùng localhost mock server |

## 1. Mục đích và ranh giới

`UsgsHttpClient` tách HTTP I/O khỏi Airflow và Bronze writer. Client chịu
timeout, retry an toàn, giới hạn kích thước body, count pre-check, pagination
và log metadata. Client không parse GeoJSON, không lọc business và không ghi
MinIO; các phần đó thuộc `USG-03`/`USG-04`.

Default guardrail trong `.env.example`:

| Biến | Default | Giới hạn |
|---|---:|---:|
| `USGS_HTTP_MAX_ATTEMPTS` | `4` | `1..8` lần thử tổng cộng |
| `USGS_HTTP_INITIAL_BACKOFF_MS` | `250` | `0..300000` ms |
| `USGS_HTTP_MAX_BACKOFF_MS` | `4000` | `0..300000` ms, không nhỏ hơn initial |
| `USGS_MAX_RESPONSE_BYTES` | `10485760` | `1024..50000000` bytes |

API chính:

- `fetch(request)`: lấy đúng một page, giữ bytes nguyên bản.
- `count(request)`: đổi `format=geojson` thành `format=count` và trả tổng số
  feature của cùng window/filter.
- `fetchAll(request)`: count trước, sau đó lấy các page với `offset=1,
  1+limit, 1+2*limit,...`; mỗi page trả riêng để Bronze quyết định cách lưu.

## 2. Retry policy

`USGS_HTTP_MAX_ATTEMPTS` là tổng số lần thử, không phải số lần retry. Backoff
exponential bắt đầu ở `USGS_HTTP_INITIAL_BACKOFF_MS` và bị chặn bởi
`USGS_HTTP_MAX_BACKOFF_MS`. Header `Retry-After` dạng số giây được tôn trọng
nhưng vẫn bị chặn bởi max backoff.

| Lỗi | Retry | Hành vi |
|---|---:|---|
| Connect/read timeout, `IOException` | Có | Retry đến max attempts, sau đó ném `UsgsHttpException` |
| HTTP `429` | Có | Backoff; dùng `Retry-After` nếu là số giây hợp lệ |
| HTTP `5xx` | Có | Backoff exponential |
| HTTP `4xx` khác `429` | Không | Ném ngay, không spam endpoint |
| HTTP `3xx` | Không | Không follow redirect; ném lỗi cấu hình/response |
| Thread bị interrupt | Không | Khôi phục interrupt flag và dừng request |

Mỗi retry giữ nguyên `UsgsRequest`, window và offset. Retry không tạo raw
response giả và không ghi đè payload; `USG-03` quyết định attempt/object path.

## 3. Response-size guard

`USGS_MAX_RESPONSE_BYTES` được kiểm tra ở hai lớp:

1. Nếu `Content-Length` đã lớn hơn guard, client từ chối trước khi giữ body.
2. Nếu header thiếu hoặc không đáng tin, client đọc streaming tối đa đến guard
   và dừng khi byte tiếp theo làm vượt giới hạn.

Body không được đưa vào log hoặc exception message. Response thành công trả
bytes nguyên bản cùng status, headers, số attempts, elapsed time và request ID.

## 4. Count pre-check và pagination

`fetchAll` gọi count trên cùng endpoint/filter, chỉ thay `format=count` và giữ
window/bounding box/event type. Nếu count bằng `0`, trả danh sách page rỗng;
empty result là trạng thái hợp lệ và không phải lỗi HTTP.

Nếu count lớn hơn `limit`, client tạo page mới bằng cách thay `offset`, không
thay đổi target/query window. Với limit `2` và count `5`, offset là `1, 3, 5`
theo contract một-gốc của USGS.
Client giới hạn tổng count trong miền offset Java và không lặp vô hạn nếu server
trả count bất thường.

## 5. Logging và lỗi

Mỗi request/retry/response log key-value tối thiểu có:

```text
request_id
window_start_utc
window_end_utc
offset
attempt
status hoặc backoff_ms
```

URI/query có thể xuất hiện để audit vì contract không chứa credential; response
body và header nhạy cảm không được log. `request_id` giữ nguyên qua các retry
của một page; count request có ID riêng.

Các lỗi public:

- `UsgsHttpException`: status, request ID, request, số attempts và retryability.
- `UsgsResponseTooLargeException`: request ID và giới hạn bytes, không chứa body.

## 6. Kiểm thử

`UsgsHttpClientTest` dùng `com.sun.net.httpserver.HttpServer` trên localhost để
kiểm tra 503 retry/backoff, 429/`Retry-After`, 4xx không retry, response-size
guard, log metadata và count pagination. Test không gọi USGS thật.

```bash
./mvnw --batch-mode --no-transfer-progress -pl spark -am test
```
