# QA-01 — Source capture/Silver component, 22/10/2026

## Phạm vi

- Tiếp tục đúng branch PR #96 `test/qa-01-scheduled-runtime-acceptance`, base/head
  đã kiểm tra trên GitHub `f4194703e61484d661e62a56ad8cc0c5d3e3c894`.
- Owner HoaiTam, reviewer unassigned; không có approval được tự điền.
- Theo yêu cầu của owner: bước 1/3 **Source capture và Silver** trước; chưa
  làm Gold/publication operational hoặc Airflow/collector v2.
- [QA-01 — Daily E2E](../task/tasks/QA-01.md) vẫn **In Progress**. Đây là component
  acceptance bằng fixture và source thật trên file stores, **không** scheduled
  deployment acceptance. [Runbook](../runbooks/CURRENT_MONTH_SOURCE_SILVER.md).

## Code và hành vi

`current-month-source-silver-v1` validate exact metadata request, tính UTC cutoff
và capture toàn phần tháng liên quan overlap ba ngày. Java HTTP client giữ
timeout/retry/size guard; source capture thêm daily chunks, pre/post count text,
immutable raw/manifest/part pins, contiguous coverage và marker-last selection.

Silver dùng lại parser/quality/dedup/membership, có Spark grain check, giữ
revision history và input lineage; chặn missing ID, stale update, invalid
record hoặc move ngoài closure. Không delete/tombstone, không ghi đè partition.
State/bundle nằm dưới `silver/current-month/`, không thay historical v1.

## Lỗi phát hiện từ source thật và cách xử lý

Lần đầu gọi USGS thật trả HTTP 200, 5 features nhưng GeoJSON metadata không có
`count` (observed API `2.8.2`). Gate dừng `ASOF_SOURCE_COUNT_CHANGED`, trước
Silver, tại root `staging/current-month-live-20261022/`. Root/partial raw được
giữ nguyên; không sửa/xóa manifest hoặc gán success để vượt gate.

Bổ sung `/count?format=text` chính thức với cùng time/ROI/event filters, bỏ
pagination, gọi trước/sau mỗi GeoJSON request. Raw count text giữ riêng, exact
URI/SHA/count trong pin; re-read source count, GeoJSON size và manifest count
phải khớp. Nếu metadata.count có thì vẫn kiểm tra trường đó, không giả tạo field
trong raw. Count drift, thiếu proof, count vượt giới hạn hoặc checksum sai đều
dừng. Hai count bằng nhau không chứng minh catalog là atomic point-in-time;
output vẫn OBSERVED_AS_OF/PROVISIONAL.

## Test nguồn thật → local Bronze/Silver

Lệnh đã chạy, sau explicit `make prepare-current-month` với JDK21:

```bash
USGS_HTTP_MAX_ATTEMPTS=1 make current-month-local \
  CURRENT_MONTH_REQUEST=airflow/dags/fixtures/current_month_source_silver.json \
  CURRENT_MONTH_ROOT=staging/current-month-live-20261022-count-proof
```

Request fixture có **metadata scheduled giả lập**, không khẳng định đã có
DagRun đó trong Airflow. HTTP nguồn là thật; không dùng fixture event payload
để thay response nguồn trong lần này.

| Metric | Actual |
|---|---|
| Operation | `cm1-8f35db49c6dc7f57ab6a46221c878649e360c643` |
| Event window | `[2026-10-01T00:00:00Z, 2026-10-22T00:00:00Z)` |
| Capture start | `2026-10-22T10:05:58.078162Z` |
| Observation cutoff | `2026-10-22T10:06:33.662034Z` |
| Leaf chunks / HTTP requests | 21 / 63 (pre-count + GeoJSON + post-count) |
| Capture records / unique revisions | 55 / 55 |
| Current / history / memberships / links | 55 / 0 / 55 / 0 |
| Parser rejects / quality rejects | 0 / 0 |
| Reconciliation | balanced=true; output_revisions=55 |
| Result | ObservedAsOfReady; published=false, research_ready=false, ml_upstream_eligible=false |

55 là số USGS observations trong query ROI 20–50°N, 120–155°E, loại earthquake,
không magnitude filter tại lần retrieval trên. Không phải toàn bộ JMA hoặc
bằng chứng đầy đủ mọi động đất ở Nhật Bản. Không dùng count này làm kỳ vọng
cứng cho fresh capture lần khác.

| Pin | SHA-256 |
|---|---|
| Request canonical JSON | `f821158f8956dc89fd9b7412731e9ca7ff1809be7c4c712cf2cae5ce5040711b` |
| Capture selection | `c67731f9850c2c141a11483593a5003a721ef830d533d344b89345802f03295a` |
| Registry bytes | `5ddf3062ee5a01e99582074d4f053d4b168335a64f26a00ada1581daab3336a7` |
| Silver bundle manifest | `7238937a3287a3c2b681e21c40f57b0bf3c69aff291441d579efb4459f12ac2b` |
| Receipt bytes | `1cde0b5c009ca8e0f7234ce17a80503cbc3e4130179cbf6231a8e64c38e0e5cb` |

Chạy lại **cùng** request/root/query policy: exit 0, receipt SHA giữ nguyên,
private rerun log có **0 HTTP requests** so với 63 lần đầu. Java re-verify
manifest/raw/count proofs, registry và bốn Parquet datasets; không fetch lại
nguồn hoặc tạo duplicate. Raw/log/staging ignored, không commit vào Git.

## Regression

- `make test-current-month`: 19 Java/Spark tests (10 capture + 9 Silver) và
  5 host CLI tests đạt. Coverage gồm independent count không có metadata.count,
  tampered count/raw/manifest/marker, source drift, saturation/split budget,
  partial resume, UTC/rollover/seed, missing ID, revision/history/closure,
  bad input, invalid parent, Parquet corruption và loss of lease.
- Toàn bộ Java/Spark regression trên code cuối:
  `./mvnw --batch-mode --no-transfer-progress -pl spark -am test`:
  **453 passed, 0 failures/errors/skipped**, JDK21, target Java17. ORC-06/07 v1
  tests vẫn đạt. Trước bổ sung count proof cũng đã chạy clean verify thành công
  với 450 tests; không dùng số cũ thay regression cuối.
- `TMPDIR=/private/tmp make test-qa-01`: **49 passed**, gồm 5 host CLI mới;
  không có live Source/Silver hoặc scheduled acceptance trong target này.
- `TMPDIR=/private/tmp make ci-airflow`: **428 passed**.
- `make ci-docs-base`: docs/contracts/task tracking, CLI/security/runtime guard
  và CI workflow checks đạt. Không gọi Docker daemon/nguồn thật trong nhóm này.
- Lần test Spark đầu bị sandbox chặn loopback; rerun với quyền loopback đạt,
  không bỏ test hay đổi gate. Warning Maven shade/Yetus/Hadoop native có sẵn
  không phải kết quả scientific hoặc lỗi runtime component.

## Giới hạn và phần tiếp theo

**QA-01 — Daily E2E** còn bước 2 Gold/publication operational + consumer guards
và bước 3 Airflow/collector/scheduled acceptance thật; chưa có task ID/effort
riêng cho các work packages này. Không nâng QA-01 Done từ component report.

MinIO entrypoint có exact shared lease guard nhưng **chưa nghiệm thu MinIO live**
cho protocol này; lần source thật ở trên chỉ ghi file stores local. Không
start/rebuild/reset Docker, unpause/trigger/clear Airflow, release foreign lease
hoặc ghi Gold của môi trường đang dùng.

Compaction/archive state scalable, JMA sau baseline và automatic ID/deletion/
closure reprocess ngoài policy hai tháng **chưa có task riêng**. State bounded
64 active manifests/128MiB/100.000 revisions; vượt budget phải dừng, không bỏ
history hoặc events để chạy tiếp.

Nguồn đối chiếu: [USGS Event API — count method](https://earthquake.usgs.gov/fdsnws/event/1/),
[GeoJSON format](https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php).
Mô tả format có metadata.count; response live kiểm tra ở trên không có field
đó, vì vậy evidence dựa trên raw captures và independent count proof, không
suy diễn mọi API response đều cùng shape.
