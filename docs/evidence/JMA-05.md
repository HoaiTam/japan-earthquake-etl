# JMA-05 — evidence Bronze live và handoff

## 1. Baseline / môi trường

- Task owner `HoaiTam`, reviewer `unassigned` (chưa có review độc lập).
- Branch `feat/jma-05-bronze-integration-qa` từ `main` `7e8d9d3` (merge PR #41).
- Ngày chạy `2026-10-07`, Airflow `3.3.2` / Python `3.13`, LocalExecutor;
  Java runtime trong image `17.0.19+10`, MinIO local với pipeline credential.
- Inventory version `1.0`, SHA-256
  `61dc2c73f1bb9cee48fba851cbb941cc62c893201c36db583cdd55ae19f8199a`;
  Bronze manifest `1.0`, verifier `jma-05-v1`, config version `1`.
- Không sửa `.env`, DAT-01 catalog/shared fixture hoặc Bronze contract; không
  tải 40 năm, xóa bucket/volume hay overwrite object đã publish.

## 2. Lệnh và kết quả live đầu tiên

```bash
make verify-samples
make smoke-jma-live
```

DAT-01 readback đạt: USGS `11771` byte / `16` events, JMA staged `6977812`
byte / `257020` records, checksum đúng hai sample đã khóa. Catalog vẫn có
**đúng hai entries**, JMA entry vẫn `STAGED_SOURCE`. BronzeReady phía JMA là
**manifest mới do writer tạo**, không phải sửa nhãn catalog.

Evidence ID `20261007T084758Z-253e5a73ff0b`:

- Preview `jma05-20261007T084758Z-253e5a73ff0b-preview`: DAG success,
  4 archive planned, 0 Ready, PREVIEW/verified=false.
- First `jma05-20261007T084758Z-253e5a73ff0b-first`: DAG success,
  mapped tasks, summary và gate đạt; Java readback chụp baseline manifest hash.
- Rerun `jma05-20261007T084758Z-253e5a73ff0b-rerun`: DAG success,
  Java report VERIFIED tại `2026-10-07T08:49:12.164709877Z`.
- Cả 4 archive rerun có `download_reused=true`, `publication_reused=true`,
  raw URI/SHA/release/size/count và manifest byte hash khớp baseline.
- Build live phát hiện thiếu inventory trong Maven stage và `.dockerignore`
  loại inventory/JMA wrapper; đã sửa allowlist/COPY và thêm regression test.

### Xác nhận bản cuối

Live QA chạy lại sau khi chốt verifier HTTP/key/run/attempt và pause-restoration
gate. Evidence ID `20261007T085218Z-06a45f424022`:

- `jma05-20261007T085218Z-06a45f424022-preview`: success, PREVIEW không Ready.
- `jma05-20261007T085218Z-06a45f424022-first`: success, readback VERIFIED.
- `jma05-20261007T085218Z-06a45f424022-rerun`: success, 4 mapped tasks,
  summary/gate đều success; readback VERIFIED lúc `2026-10-07T08:53:38.864234875Z`.
- `workflow-evidence.json` xác nhận cả ba run success, preview/rerun verified
  và `pause_state_restored=true`. Cả first/rerun reuse publication ban đầu;
  raw/manifest hashes, count, release/URI không đổi, không tạo raw copy mới.
- Report JSON trong Git là **nguyên metadata rerun report bản cuối**, không
  ZIP, source payload hoặc `.env`. Report staging bản cuối nằm tại
  `/opt/pipeline/staging/jma/qa/20261007T085218Z-06a45f424022/`.
- `make test-jma-qa`: 46/46 Java + 26/26 Python tests.
- `make check`: 162/162 Java + 41/41 Airflow tests, checker 73 task và các
  static contract/config/Compose/foundation/USG-06 gates đạt; host JDK21.0.11
  với `--release 17`, Docker Maven tests/runtime Java17 thật.

## 3. Output thực đo theo year/segment

| Năm / segment | Era | ZIP bytes | Structural records | SHA-256 prefix |
|---|---|---:|---:|---|
| 1997 / jan-sep | LEGACY | 1,083,079 | 39,951 | `c2ce3fd9f8fc` |
| 1997 / oct-dec | UNIFIED | 452,012 | 16,284 | `25349273dfc4` |
| 2000 / full-year | UNIFIED | 2,892,423 | 109,967 | `268441420d9c` |
| 2023 / full-year | UNIFIED | 6,977,812 | 257,020 | `e5ced2bf7275` |

Tổng `423222` structural records / `11405326` ZIP bytes, **không phải** số
natural event/valid observation hoặc canonical event sau Silver. Count 2000
được đếm từ archive thật; không bịa expected count hay mở rộng DAT-01.

Publication run ID gốc có prefix
`jma-922316b2ad88fa07587948c310215600`, suffix `<year>-<segment>`, attempt `1`,
ingest date UTC `2026-10-07`. Rerun giữ publication run ID gốc, không dùng
run ID mới để nhân bản raw/manifest.

## 4. Handoff / cách đọc evidence

Report JSON bàn giao: [JMA-05 live readback](./JMA-05-live-readback.json),
chứa exact S3 URI/key cho **từng raw và manifest**, full SHA-256, release,
manifest hash, count, native interval JST và publication run/attempt.
Consumer phải chọn exact manifest trong report, không scan `latest`/wildcard.
SLV-01 cần stage exact manifest về local path trước khi gọi resolver;
không dùng `Path.of("s3://...")`.

Report local trong shared staging của run đầu:

```text
/opt/pipeline/staging/jma/qa/20261007T084758Z-253e5a73ff0b/first-readback.json
/opt/pipeline/staging/jma/qa/20261007T084758Z-253e5a73ff0b/rerun-readback.json
/opt/pipeline/staging/jma/qa/20261007T084758Z-253e5a73ff0b/workflow-evidence.json
```

Coverage native JST: 1997 `[01-01, 10-01)` và `[10-01, 1998-01-01)`;
2000 `[2000-01-01, 2001-01-01)`; 2023 `[2023-01-01, 2024-01-01)`.
Các biên đổi sang UTC trừ 9 giờ, không filter research split ở Bronze.

Một năm 2000 chỉ là reproduction pilot; 2023 là extension; 1997 kiểm era/
segmentation. Không gọi QA này là training dataset, Mc hoặc HDBSCAN hoàn tất.

## 5. Giới hạn / bước tiếp theo

- [SLV-09 - Tích hợp và kiểm thử Silver đa nguồn](../task/tasks/SLV-09.md):
  dùng input này để đối soát parser/quality/dedup/link/SilverReady thật.
- [QA-03 - Kiểm thử JMA historical backfill](../task/tasks/QA-03.md): còn
  historical QA rộng hơn; ở đây chỉ 4 archive đến Bronze.
- [MLD-02 - Audit Gold và xác định magnitude of completeness](../task/tasks/MLD-02.md):
  còn audit/Mc với pinned Gold snapshot; pilot 2000 không thay full research coverage.
- [ORC-02 - Cấu hình lịch và readiness cho hai nguồn](../task/tasks/ORC-02.md):
  còn JMA revision schedule. [ORC-05 - Chốt recovery, concurrency và tài nguyên](../task/tasks/ORC-05.md):
  còn resource/recovery acceptance; cache loss chưa đảm bảo exactly-once
  physical publication. Revision/resume/corruption được test offline, không
  cố ý sửa nguồn hoặc phá object production để tạo lỗi live.
- Reviewer độc lập chưa ghi nhận; không tự commit/push/mở PR trong task.

Runbook: [JMA Bronze QA](../specs/JMA_BRONZE_QA.md).
