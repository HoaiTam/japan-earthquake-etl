# QA-01 — Source capture và Silver current-month

## 1. Làm gì, có gì và dùng để làm gì?

Đây là **bước 1/3** được HoaiTam yêu cầu trên PR #96: component Java/Spark
`current-month-source-silver-v1` triển khai capture USGS → Bronze → Silver
operational. Nó không nối vào cron, không ghi Gold/Iceberg, không Published và
không cấp official ML eligibility. [QA-01 — Daily E2E](../task/tasks/QA-01.md)
vẫn In Progress. [Thiết kế](../specs/CURRENT_MONTH_AS_OF_CUTOFF.md) giữ guard
historical v1; policy planner `DESIGN_ONLY` không phải nút bật component này.

| Thành phần | Chức năng |
|---|---|
| `CurrentMonthContract` | Validate request version/UTC/context, tính cutoff và một/hai tháng cần capture |
| `CurrentMonthCapture` | Chunk theo ngày, pin raw/manifest, resume, kiểm coverage liên tục và marker-last |
| `CurrentMonthSilverResolver` | Re-read pins, quality/dedup/revision/closure, Spark grain check, immutable Parquet/registry/receipt |
| `CurrentMonthSourceSilverJob` | Explicit local hoặc MinIO entrypoint, chỉ Source/Silver |
| `scripts/current-month.py` | Host prebuilt-JAR guard, public request config, private log và readback control receipt; không ETL trong Python |

## 2. Phạm vi và các guard

- Target day/cutoff từ closed 24h interval. Ba ngày overlap xác định tháng bị
  ảnh hưởng; capture từ **đầu mỗi tháng** đến `min(month_end,event_cutoff)`.
  Không overwrite tháng bằng delta ba ngày. Ngày đầu tháng không tạo tháng mới rỗng.
- USGS ROI cố định 20–50°N, 120–155°E, earthquake, không magnitude filter.
  Seed 2023-01-01; JMA không được giả định cung cấp observation hiện tại.
  Component ghi `HISTORICAL_READINESS_ONLY` là vai trò JMA, **không** tự thực hiện
  JMA readiness. Kiểm readiness hai nguồn thuộc wiring Airflow ở bước 3.
- Raw response và USG-03 manifest được lưu nguyên bản. Request `endtime` được
  builder đổi thành exclusive end trừ 1 ms, đúng độ phân giải epoch-ms của nguồn.
  Silver chặn toàn run nếu có record ngoài query interval; không âm thầm bỏ row.
- Response chạm limit bị chia đôi theo thời gian; raw response bão hòa vẫn giữ
  nhưng không vào leaf selection. Mỗi leaf phải count < limit, exact raw/manifest
  SHA và không có gap/overlap. Luôn gọi `/count?format=text` với cùng window/filter
  trước/sau mỗi GeoJSON request, giữ raw count text + URI/SHA trong pin. Count
  phải không đổi và khớp features ở leaf; nếu GeoJSON có metadata.count thì trường
  đó cũng phải khớp. Một response live API 2.8.2 kiểm tra ngày 22/10 không có
  metadata.count; không tự thêm trường vào raw hoặc bỏ count gate. HTTP 400/timeout không bị coi
  là empty hoặc tự bỏ qua. Nếu USGS từ chối quá giới hạn trước khi trả body,
  component fail-closed, không giả định tự chia được mọi HTTP 400.
- Tối đa 64 query nodes/capture, mỗi node tối đa ba HTTP reads (count/GeoJSON/count),
  128MiB GeoJSON captured bytes và 100.000 leaf records;
  split tối thiểu 1 giây. Source HTTP retries giữ giới hạn USG-02. Completed
  parts reuse không gọi API lại. Raw đã ghi nhưng chưa có part receipt gặp
  bytes khác khi retry sẽ dừng identity conflict, không overwrite/cleanup.
- `event_cutoff_utc` khác `observation_cutoff_utc` (max completion time của
  các parts). Retry giữ selection/pins/clock đã pin. Nhiều requests không phải
  atomic point-in-time catalog, chỉ là **OBSERVED_AS_OF / PROVISIONAL**.
- Nếu có baseline, giữ revision history và events ngoài overlap; cùng revision
  ID không tăng row. Missing source ID, stale/ambiguous update hoặc old/new
  month nằm ngoài closure phải dừng trước Silver writes. Không diễn giải thiếu
  row hoặc HTTP 404 thành tombstone, không tự query ID/deletion để xóa.
- Event chuyển giữa **hai tháng đều đang được capture** có thể cập nhật current
  và giữ history. Chuyển khỏi scope thì cần replan/bounded reprocess; chưa có
  executable xử lý deletion/closure ngoài policy này. Source key xuất hiện với
  hai nội dung khác nhau trong cùng capture cũng bị chặn.
- Registry giữ latest observed coverage theo từng tháng, không gọi tháng cũ
  complete; gap lịch chạy không được biến thành coverage chưa từng thu thập.
  Raw evidence được giữ cho mọi revision và latest coverage; prune chỉ bỏ
  reference thừa khỏi registry mới, **không xóa object**. State vẫn bị giới hạn
  64 active manifests, 128MiB và 100.000 unique revisions. Vượt budget dừng,
  không truncate; compaction/archive scalable chưa có task triển khai riêng.

Silver dùng lại parser/quality/dedup/canonical membership USGS đã có. Không có
cross-source current link nên không tuyên bố JMA/USGS halo completeness. Bundle
là immutable state mới chứa cả history và registered observations, không sửa
partition/alias đã publish. `affected_months` là tháng của capture hiện tại;
Gold adapter bước 2 phải áp dụng exact scope/closure, không ghi cả bundle như
replacement toàn historical namespace.

## 3. Chạy tests và chuẩn bị runtime riêng

Tại thư mục gốc repo, đặt `JAVA_HOME` là JDK17/21 đang cài:

```bash
make test-current-month
make test-qa-01
make prepare-current-month
```

`test-current-month` dùng fake HTTP/file stores và Spark local, không gọi USGS,
không ghi MinIO/Gold. `prepare-current-month` explicit package JAR/classpath;
chỉ chạy lại khi source/dependency thay đổi. Run command **không** tự build hoặc
start Docker. Không cần `.env` cho local: lấy public USGS settings từ
`.env.example`, environment explicit được ưu tiên; không nạp example MinIO creds.

## 4. Test với USGS thật, output local tách biệt

Request fixture dưới đây có metadata **giả lập**, fixed cutoff 22/10/2026.
Nó không xác nhận đã tồn tại DagRun đó; dùng để test component, không nghiệm thu
scheduler. Chỉ chạy sau interval đã đóng; muốn mốc khác, tạo request JSON riêng
theo exact schema, không sửa identity của run đã thực hiện.

```bash
make current-month-local \
  CURRENT_MONTH_REQUEST=airflow/dags/fixtures/current_month_source_silver.json \
  CURRENT_MONTH_ROOT=staging/current-month-component
```

Lệnh gọi USGS thật, tạo file trong scoped staging, **không upload MinIO**. Chạy
lại cùng lệnh/root/USGS query policy để kiểm reuse. Dùng root mới khi thay
request cùng owner hoặc query policy; không xóa root cũ để che conflict.
Chạy ngày tiếp theo giữ cùng root và pin baseline từ receipt trước:
`baseline={"operation_id":"<giá trị receipt>","registry_sha256":"<giá trị receipt>"}`
trong request mới, với run/interval mới. Không dùng `latest` hoặc tự đoán SHA.

Nhận biết thành công:

1. Exit 0, `event=current_month_component_passed`, receipt được đọc lại cùng `_SUCCESS`.
2. `status=ObservedAsOfReady`, coverage OBSERVED_AS_OF, `published=false`,
   `research_ready=false`, `ml_upstream_eligible=false`.
3. Fixture cutoff 22/10 có 21 daily leaf chunks nếu không bão hòa; toàn cửa sổ
   `[2026-10-01,2026-10-22)` được pin, không chỉ ba ngày gần nhất.
4. `current + history = output_revisions`; `memberships=current`, links/rejects=0,
   balanced=true. Raw count có thể khác số unique revisions do overlap/reuse.
5. Rerun giữ receipt/manifest SHA và không gọi HTTP lại. Không đòi count cứng
   từ catalog live, không coi source response rỗng là scientific coverage proof.

Các vị trí kiểm tra (sau `<root>/`):

- `bronze/bronze/usgs/ingest_date=.../run_id=.../attempt=01/`: raw + USG-03 manifest.
- `bronze/bronze/usgs/current-month/<operation>/`: identity, parts, selection, `_SUCCESS`.
- `silver/current-month/state/<operation>/`: identity, registry, receipt, `_SUCCESS`.
- `silver/current-month/bundles/<operation>/`: bốn datasets Parquet, manifests và checksums.
- Log private 0600 ở `staging/current-month-logs/`, không commit raw/log/staging.

CLI Java `--minio <request.json>` cũng có store adapters nhưng yêu cầu exact
shared source lease `SOURCE_GUARD_ROOT/owner.json` đang được caller hợp lệ giữ.
Nó không acquire/release/takeover lease. Chưa có Make smoke/wiring Airflow cho
đường này; không tự tạo owner.json hoặc bật cron để vượt gate. MinIO paths lần
lượt là `bronze/usgs/...` và `${SILVER_PREFIX}/current-month/...` trong data bucket.

## 5. Giới hạn và bước tiếp theo

**QA-01 — Daily E2E** còn hai bước đã thống nhất: (2) Gold/publication operational
và research/ML consumer guards, (3) Airflow/collector v2 + scheduled acceptance
2026 thật. Chưa có task ID riêng cho các work packages đó; component này không
đổi ORC-06/07 historical v1 hoặc trạng thái Done của các task upstream.
JMA sau baseline và ID/deletion reprocess ngoài scope cũng chưa có task riêng.
Không restore marker/state giả, xóa warehouse hoặc release lease khác owner.
