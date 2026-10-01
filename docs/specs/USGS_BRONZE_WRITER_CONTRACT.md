# USGS Bronze writer contract

| Thuộc tính | Giá trị |
|---|---|
| Task | `USG-03` |
| Trạng thái | Implemented |
| Input | `UsgsHttpResponse` + run context |
| Output | `response.geojson` và `manifest.json`, hoặc quarantine `Rejected` |
| Storage boundary | `BronzeObjectStore`; file-backed adapter dùng cho local/offline test |

## 1. Mục đích

`UsgsBronzeWriter` là commit point giữa HTTP extract và Silver. Writer chỉ kiểm
tra envelope GeoJSON, giữ nguyên bytes, tính SHA-256, đọc lại object và tạo
manifest. Writer không parse business fields, không lọc ROI/magnitude và không
ghi đè object đã tồn tại.

`BronzeObjectStore` tách lifecycle writer khỏi SDK storage. `FileBronzeObjectStore`
là adapter deterministic cho local test; task tích hợp có thể cấp adapter
S3-compatible/MinIO mà không đổi validator, key layout hoặc manifest contract.

## 2. Structural GeoJSON validation

Payload được coi là hợp lệ khi:

- JSON parse được và root là object;
- root `type` là `FeatureCollection`;
- `features` là array;
- mỗi phần tử là object có `type=Feature`.

`features=[]` là empty response hợp lệ. Các field business như magnitude,
coordinates, source ID hoặc timestamp không bị kiểm tra ở đây; validation và
reject record thuộc Silver. Invalid JSON, root sai hoặc feature sai cấu trúc đi
vào quarantine, không phát hành `BronzeReady`.

## 3. Ready lifecycle

Với response hợp lệ, writer thực hiện theo thứ tự:

1. Resolve key theo `ingest_date_utc`, `run_id` và `attempt`.
2. Kiểm tra key chưa tồn tại; nếu đã có thì chỉ reuse khi bytes và SHA-256 giống.
3. Ghi raw bytes vào `response.geojson`.
4. Đọc lại object, kiểm tra length và SHA-256.
5. Tạo manifest đầy đủ request/window/response/run context và upload manifest
   sau cùng.
6. Nếu manifest đã tồn tại, chỉ reuse khi status, raw key và SHA-256 khớp.

Object layout:

```text
bronze/usgs/ingest_date=YYYY-MM-DD/run_id=<run-id>/attempt=<nn>/response.geojson
bronze/usgs/ingest_date=YYYY-MM-DD/run_id=<run-id>/attempt=<nn>/manifest.json
```

Manifest `BronzeReady` có `record_count_estimate` bằng số feature, nhưng đây
không phải parsed/quality count của Silver.

## 4. Rejected/quarantine lifecycle

Payload invalid hoặc HTTP status không thành công được giữ nguyên để audit tại:

```text
bronze/_quarantine/usgs/ingest_date=YYYY-MM-DD/run_id=<run-id>/attempt=<nn>/payload.bin
bronze/_quarantine/usgs/ingest_date=YYYY-MM-DD/run_id=<run-id>/attempt=<nn>/failure_manifest.json
```

Failure manifest có `bronze_status=Rejected`, `failure_reason` và validation
flags; không có `bronze/usgs/.../manifest.json` `BronzeReady`. Payload invalid
không bị xóa để giữ bằng chứng cho USG-05.

## 5. Idempotency và lỗi

- Cùng key + cùng SHA-256: reuse, không ghi bản copy mới.
- Cùng key + khác SHA-256: dừng với `AMBIGUOUS_OVERWRITE`, không sửa object cũ.
- Readback khác length/checksum: dừng trước khi tạo `BronzeReady`.
- Manifest không khớp raw key/status/SHA-256: dừng, không overwrite manifest.
- Request ID, payload và response body không được ghi vào manifest ngoài metadata
  request/response cần thiết; credential/header nhạy cảm không được lưu.

## 6. Kiểm thử

`UsgsBronzeWriterTest` kiểm tra validator success/empty/invalid, raw + manifest
readback/checksum, idempotent rerun, quarantine và ambiguous overwrite.
`UsgsBronzeFixtureAcceptanceTest` của `USG-05` chạy cùng các fixture dùng chung
để đối soát `run_id`, byte length, `record_count_estimate` và checksum mismatch.

```bash
./mvnw --batch-mode --no-transfer-progress -pl spark -am test
```
