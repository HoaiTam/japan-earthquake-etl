# CON-04 test matrix

Ma trận này là bản đọc nhanh của [`cases.json`](./cases.json). Count ở cột kết
quả theo thứ tự `parsed / valid / rejected / current / canonical`. `Reason/state`
bao gồm cả rejection code và trạng thái dedup/link cần assert; duplicate hoặc
revision cũ không bị tính là parse reject.

| Case | Input | Mục tiêu | Expected output | Reason/state |
|---|---|---|---|---|
| `FX-USGS-01` | [`success.geojson`](./usgs/success.geojson) | USGS success và mapping chung | `BronzeReady`; `1/1/0/1/1`; UTC `2023-09-01T03:34:56.780Z`; M5.2 | Không có |
| `FX-USGS-02` | [`empty.geojson`](./usgs/empty.geojson) | Empty response hợp lệ | `BronzeReady`; `0/0/0/0/0`; `record_count_estimate=0` | Không có |
| `FX-USGS-03` | [`invalid-json.geojson`](./usgs/invalid-json.geojson) | JSON hỏng ở Bronze | `Rejected`; không publish manifest Ready | `SOURCE_STRUCTURE_INVALID` |
| `FX-USGS-04` | [`invalid-fields.geojson`](./usgs/invalid-fields.geojson) | Silver validation theo record | `BronzeReady`; `3/0/3/0/0` | `MISSING_SOURCE_KEY`, `INVALID_EVENT_TIME`, `INVALID_LATITUDE`, `INVALID_LONGITUDE`, `INVALID_NUMBER` |
| `FX-USGS-05` | [`duplicate.geojson`](./usgs/duplicate.geojson) | Duplicate cùng source key/revision | `BronzeReady`; `2/2/0/1/1`; rerun không đổi kết quả | `DUPLICATE_SOURCE_RECORD` |
| `FX-USGS-06` | [`revision-v1.geojson`](./usgs/revision-v1.geojson) + [`revision-v2.geojson`](./usgs/revision-v2.geojson) | Chọn `updated` mới nhất | `BronzeReady`; `2/2/0/1/1`; current M4.2; history 2 | `SOURCE_REVISION_SUPERSEDED` |
| `FX-USGS-07` | [`timezone-boundary.geojson`](./usgs/timezone-boundary.geojson) | Half-open UTC window và ROI inclusive | Parse/valid 2; target window chọn 1; event đúng `window_end` sang run kế | `WINDOW_END_EXCLUSIVE` |
| `FX-USGS-08` | [`success.geojson`](./usgs/success.geojson) + [`checksum-mismatch.sha256`](./usgs/checksum-mismatch.sha256) | Checksum sai | `Rejected`; resolver không chọn input | `CHECKSUM_MISMATCH` |
| `FX-JMA-01` | [`success.hyp`](./jma/fixed-width/success.hyp) + [`success.zip`](./jma/archives/success.zip) | JMA success, natural/artificial | `BronzeReady`; `2/2/0/2/2`; natural KPI count 1 | Không có |
| `FX-JMA-02` | [`empty.hyp`](./jma/fixed-width/empty.hyp) + [`empty.zip`](./jma/archives/empty.zip) | Empty archive hợp lệ | `BronzeReady`; `0/0/0/0/0` | Không có |
| `FX-JMA-03` | [`invalid-record-length.hyp`](./jma/fixed-width/invalid-record-length.hyp) + [`invalid-record-length.zip`](./jma/archives/invalid-record-length.zip) | Record 95 byte | `Rejected`; không publish manifest Ready | `INVALID_RECORD_LENGTH` |
| `FX-JMA-04` | [`duplicate.hyp`](./jma/fixed-width/duplicate.hyp) + [`duplicate.zip`](./jma/archives/duplicate.zip) | Duplicate trong release | `BronzeReady`; `2/2/0/1/1`; rerun ổn định | `DUPLICATE_SOURCE_RECORD` |
| `FX-JMA-05` | [`revision-v1.zip`](./jma/archives/revision-v1.zip) + [`revision-v2.zip`](./jma/archives/revision-v2.zip) | Cùng identity, release mới sửa record | `BronzeReady`; `2/2/0/1/1`; current M4.2; history 2 | `SOURCE_REVISION_SUPERSEDED` |
| `FX-JMA-06` | [`timezone-boundary.hyp`](./jma/fixed-width/timezone-boundary.hyp) + [`timezone-boundary.zip`](./jma/archives/timezone-boundary.zip) | JST→UTC và catalog era | `BronzeReady`; `3/3/0/3/3`; cuối 2023 → `14:59Z`; hai era đúng biên | Không có |
| `FX-JMA-07` | [`success.zip`](./jma/archives/success.zip) + [`checksum-mismatch.sha256`](./jma/checksum-mismatch.sha256) | Checksum archive sai | `Rejected`; resolver không chọn input | `CHECKSUM_MISMATCH` |
| `FX-LINK-01` | [`ambiguous.geojson`](./usgs/ambiguous.geojson) + [`ambiguous.zip`](./jma/archives/ambiguous.zip) | Một USGS gần hai JMA candidate | `3/3/0/3/3`; 2 candidate; không auto-merge | `AMBIGUOUS_SOURCE_MATCH` |
| `FX-LINK-02` | [`success.geojson`](./usgs/success.geojson) + [`success.zip`](./jma/archives/success.zip) | Overlap 2023 match 1–1 | `3/3/0/3/2`; một event `USGS_JMA`, bridge 2 dòng; JMA artificial giữ riêng | Không có |

## Quy tắc assertion

- `bronze_status=Rejected` nghĩa là không tạo manifest `BronzeReady`; count
  Silver giữ `0` vì payload chưa được resolver chọn.
- `valid_count + rejected_count = parsed_count` cho các case đã vào parser.
- `current_count` là số source revision hiện hành sau source-local dedup.
- `canonical_count` chỉ giảm khi match được chấp nhận; ambiguous không giảm.
- Reason/state của duplicate, revision, window boundary và ambiguous là test
  assertion, không nhất thiết nằm trong `silver.reject_record`.
