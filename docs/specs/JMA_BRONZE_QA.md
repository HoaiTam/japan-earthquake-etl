# JMA-05 — QA offline và live đến Bronze

## 1. Mục đích và thành phần

Gate này chứng minh workflow JMA-04 tạo **BronzeReady thật** trước khi bàn
giao cho Silver. Nó không chạy Silver/Gold/ML và không tải toàn bộ 40 năm.

| Thành phần | Làm gì | Dùng để làm gì |
|---|---|---|
| `make test-jma-qa` | Chạy downloader/validator/writer/runner/verifier và orchestration tests offline | Test nhanh bằng fixture/mock, không cần nguồn hay MinIO thật |
| `scripts/smoke-jma-live.sh` / `make smoke-jma-live` | Validate config/catalog, build/start Airflow và chạy live profile | Một entry point chạy đúng môi trường Java17/MinIO/Airflow |
| `compose/airflow/jma-live-qa.py` | Trigger preview → first → rerun trên DAG thật, chờ trạng thái, giữ/khôi phục pause | Kiểm tra scheduler/mapped tasks thực thi và không thay lịch DAG khác |
| `JmaBronzeQaVerifier` | Đọc exact manifest/raw, ZIP/96-byte/count/SHA/release, DAT-01 và baseline report | Đối soát độc lập với summary, phát hiện raw/manifest bị sửa giữa hai run |
| Report metadata | Year/segment/release/run/attempt, URI, SHA, bytes/count, coverage, readback/reuse | Handoff input thật mà không commit ZIP, credential hoặc log payload |

Python chỉ điều phối Airflow/Java và đọc metadata summary. Tải nguồn, mở ZIP,
đếm record và đối soát raw nằm trong Java. Verifier không ghi/xóa object MinIO.
Image build phải copy `config/jma` vào Maven stage vì runner/QA tests pin inventory.

## 2. Phạm vi và count

- **1997:** hai ZIP độc lập `h199701`/`jan-sep` (LEGACY) và
  `h199710`/`oct-dec` (UNIFIED). Thiếu một segment không đạt year/run gate.
- **2000:** `h2000`, full year JST, reproduction **pilot**. Count được đo từ
  ZIP thật, không đặt expected count trước và không thêm vào DAT-01.
- **2023:** `h2023`, full year JST, extension. Output phải khớp SHA/size/count/
  release của sample JMA DAT-01 đã khóa; catalog vẫn là `STAGED_SOURCE`.
- Verifier đồng thời đọc lại USGS manifest/raw và JMA staged ZIP **đã tồn tại**
  theo exact DAT-01 identities. Thiếu sample hoặc nguồn 2023 đã đổi thì gate
  fail; không tải raw khác để thay sample cũ hoặc sửa checksum/catalog.

Bronze count là **structural records** trước parse/validate business/dedup.
JST `[01-01, 01-01 năm sau)` lệch UTC 9 giờ; Bronze không filter event theo
research split. Một năm 2000 không bao phủ toàn reproduction period. Không
gọi các archive này là training dataset, Mc hay kết quả HDBSCAN.

## 3. Cách chạy lặp lại

Từ root repo, dùng JDK được Maven chấp nhận:

```bash
make test-jma-qa
make check
git diff --check
```

Live cần Docker Desktop đang chạy, `.env` hợp lệ, Internet tới JMA và hai
object DAT-01 còn tồn tại trên MinIO. Không gửi nội dung `.env`/credential.

```bash
make check-config-local
make verify-samples
make smoke-jma-live
```

Lệnh live build image, start đúng Airflow/MinIO/PostgreSQL dependencies và
chạy service `jma-live-qa` trong profile `live`. Không bật full Spark/Trino,
không tự chạy full 40 năm, không xóa volume/bucket và không sửa `.env`.
DAG manual được unpause trong QA và khôi phục đúng trạng thái ban đầu cuối
run. Với active run của operator khác, `max_active_runs=1` có thể làm QA chờ;
không cancel/clear run của người khác. Những run daily đã unpause trước đó
có thể tiếp tục theo lịch khi scheduler khởi động; QA không đổi lịch của chúng.

Mỗi lần tạo ba run ID riêng `jma05-<UTC>-<random>-preview/first/rerun`:

1. Preview: đúng 4 archive, `PREVIEW`, `verified=false`, `ready_archives=0`.
2. First: tất cả mapped archives + summary + gate thành công; Java đọc lại
   raw/manifest và chụp SHA manifest **trước** rerun vào `first-readback.json`.
3. Rerun: cùng scope, run ID mới, `force_download=false`; phải reuse download
   cache và exact publication. Java đọc lại và so URI/raw SHA/manifest SHA/
   release/bytes/count/publication run/attempt với baseline first.

Nếu source đổi giữa hai run thì QA fail có chủ đích, không nhận revision là
rerun reuse. Revision được kiểm bằng fixture/mock; không sửa nguồn thật hay
overwrite published object để tạo tình huống lỗi live. Thời hạn mỗi DAG run
20 phút; verifier 3 phút. Khi timeout, run có thể vẫn tiếp tục: giữ run ID,
kiểm tra trạng thái trước khi chạy lại, không tự xóa/cancel.

## 4. Evidence và handoff

Trong volume `pipeline_staging`:

```text
<JMA_STAGING_ROOT>/qa/<evidence-id>/first-readback.json
<JMA_STAGING_ROOT>/qa/<evidence-id>/rerun-readback.json
<JMA_STAGING_ROOT>/qa/<evidence-id>/workflow-evidence.json
```

Fail ghi `workflow-failure.json`, không tạo workflow VERIFIED. First report
có thể tồn tại nếu chỉ rerun lỗi; không coi report first là nghiệm thu toàn QA.
Report là evidence, **Bronze manifest mới là commit point**. Không scan latest
để consumer chọn release; dùng exact URI/keys trong report đã nghiệm thu.

CLI Java trong image nhận các cặp tham số `--summary`, `--catalog`,
`--inventory`, `--report` và optional `--baseline`: tất cả là exact local
JSON/CSV paths. Baseline là report first đã đọc lại trước rerun, không phải
summary do runner tự khai. `--report` phải là path mới (CREATE_NEW); đọc/verify
thất bại trả exit `2`, không xuất report VERIFIED. Host không cần gọi CLI
thủ công vì live profile truyền các path này sau mỗi DAG run thành công.

Log live in `evidence_id` và run ID, không in ZIP/SDK stderr/credential.
Xem report bằng lệnh, thay `EVIDENCE_ID` bằng ID đã được log:

```bash
docker compose --env-file .env -f compose.yaml exec -T airflow-api-server \
  python -m json.tool /opt/pipeline/staging/jma/qa/EVIDENCE_ID/rerun-readback.json
```

Path QA chứa ID đã resolve, không wildcard. Raw nằm MinIO, không đưa report
local path cá nhân, credential hoặc data dump vào Git. Metadata handoff được
ghi tại [evidence JMA-05](../evidence/JMA-05.md) sau run thật; năm 2000 lưu
riêng tại đó, DAT-01 catalog/schema/checksum không thay đổi.

## 5. Ma trận test offline

| Case | Evidence tự động | Gate mong đợi |
|---|---|---|
| Valid, empty, LF/CRLF, duplicate/business values | `JmaArchiveValidatorTest`, `JmaBronzeWriterTest` | Giữ raw, count cấu trúc; không business filter |
| ZIP hỏng/CRC/member/path/size, record 95 byte | Validator/writer, `JmaYearIngestRunnerTest` | Rejected/quarantine; không Ready pointer |
| Checksum, HTTP type/status/length, raw readback/storage failure | Writer/runner | Không publish Ready; manifest-last recovery |
| Changed file, same bytes/header-only, forced revision | Downloader/runner | SHA khác tạo release mới, bytes/manifest cũ giữ nguyên |
| Tracked resume 206 và invalid Content-Range | Downloader/runner | Đối soát full ZIP/size/count, không chỉ phần tail |
| New-run rerun, tampered raw/manifest, thiếu segment | Runner/QA verifier | Exact reuse hoặc fail closed, không silent repair |
| Preview/partial/missing/wrong count, catalog signed URI/state | `JmaBronzeQaVerifierTest` | Không tin summary hay sửa catalog để mở gate |
| First-readback trước rerun, pause restoration, failed subprocess | `test_jma_live_qa.py` | Fail/report an toàn; mocks không thay live acceptance |

## 6. Giới hạn và task downstream

- [SLV-09 - Tích hợp và kiểm thử Silver đa nguồn](../task/tasks/SLV-09.md):
  còn phải parse/quality/dedup/link/publish và đối soát output thật từ input này.
- [QA-03 - Kiểm thử JMA historical backfill](../task/tasks/QA-03.md): còn
  kiểm thử historical flow rộng hơn; JMA-05 chỉ 4 archive đại diện đến Bronze.
- [ORC-02 - Cấu hình lịch và readiness cho hai nguồn](../task/tasks/ORC-02.md):
  chưa có JMA revision schedule tự động. [ORC-05 - Chốt recovery, concurrency
  và tài nguyên](../task/tasks/ORC-05.md) còn phải nghiệm thu resource/recovery
  rộng hơn; cache loss vẫn có thể tạo publication vật lý mới theo JMA-04.
- [MLD-02 - Audit Gold và xác định magnitude of completeness](../task/tasks/MLD-02.md):
  cần Gold snapshot/coverage/Mc thật; sample 2000 chưa thay full input khoa học.
- Reviewer độc lập chưa ghi nhận, giữ `unassigned`; không bịa approval.

Contract liên quan: [JMA year backfill](./JMA_YEAR_BACKFILL.md),
[JMA writer](./JMA_BRONZE_WRITER_CONTRACT.md),
[DAT-01](./SHARED_REAL_SAMPLE_DATA.md),
[task JMA-05](../task/tasks/JMA-05.md).
