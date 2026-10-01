# USGS Bronze QA contract

| Thuộc tính | Giá trị |
|---|---|
| Task | `USG-05` |
| Phạm vi | USGS HTTP/GeoJSON đến Bronze |
| Network | Không gọi USGS thật; HTTP test dùng mock server loopback |
| Fixture | `tests/fixtures/usgs/*` từ `CON-04` |

## 1. Mục đích

USG-05 là integration gate cho block USGS Bronze. Test phải chứng minh payload
fixture đi qua validator/writer, object và manifest có thể đối soát theo
`run_id`, lỗi HTTP/network không retry vô hạn và dữ liệu chưa verify không được
đánh dấu `BronzeReady`.

## 2. Ma trận kiểm thử

| Scenario | Harness | Kỳ vọng |
|---|---|---|
| Success GeoJSON | `success.geojson` + `UsgsBronzeWriter` | `BronzeReady`, count `1`, raw/manifest/SHA-256 khớp |
| Empty response | `empty.geojson` + writer | `BronzeReady`, `record_count_estimate=0` |
| Invalid JSON | `invalid-json.geojson` + writer | Quarantine, `Rejected`, không có manifest Ready |
| HTTP `503` | `UsgsHttpClientTest` mock server | Retry bounded, sau đó nhận response thành công |
| HTTP `429` | Mock server + `Retry-After` | Tôn trọng backoff, không mất window/offset |
| HTTP timeout | Mock server response trễ | Dừng theo timeout, không retry khi hết attempts |
| Checksum mismatch | `checksum-mismatch.sha256` + corrupt readback store | Fail trước manifest Ready |
| Record-count/run audit | Fixture acceptance test | Manifest giữ `run_id`, byte length và count có thể đối soát |

## 3. Ranh giới test

- Unit/fixture tests không tải dữ liệu thật, không cần credential và không ghi
  MinIO volume.
- HTTP mock chỉ bind loopback trong thời gian test; response body không được
  đưa vào log assertion ngoài metadata cần thiết.
- `UsgsBronzeFixtureAcceptanceTest` đọc fixture từ repository root bằng đường
  dẫn tương đối, nên không tạo bản copy fixture dễ drift trong `spark/`.
- Integration run thật với MinIO hoặc một cửa sổ USGS nhỏ là bước vận hành có
  credential, không phải prerequisite để chạy unit suite.

## 4. Cách chạy

Kiểm tra fixture trước:

```bash
./scripts/check-shared-fixtures.sh
```

Chạy toàn bộ Java tests:

```bash
./mvnw --batch-mode --no-transfer-progress -pl spark -am test
```

Kết quả baseline của USG-05 là 25 tests, 0 failures, 0 errors. Test có
`HttpServer` loopback nên một số sandbox cần quyền bind socket; lỗi
`Operation not permitted` trong sandbox là giới hạn môi trường, không phải
failure của test logic.
